pub mod cli {
    use clap::{Parser, Subcommand};

    #[derive(Parser)]
    #[command(
        version,
        about = "An easy-to-use, fast and full-featured CLI for managing logs"
    )]
    pub struct Cli {
        #[command(subcommand)]
        pub command: Commands,
    }

    #[derive(Subcommand)]
    pub enum Commands {
        /// Runs seraphim server without an interface (keep running until exit the process)
        Watch {
            #[arg(short, long, default_value_t = 7777)]
            port: u16,
        },
    }
}

pub mod server {
    use std::net::Ipv4Addr;

    use super::db::{Database, store_db};
    use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, rt, web};
    use actix_ws::AggregatedMessage;
    use futures_util::StreamExt as _;
    use serde::Deserialize;

    pub struct Server {
        pub database: Database,
    }

    impl Server {
        pub async fn new() -> Result<Self, sqlx::Error> {
            Ok(Self {
                database: Database::new().await?,
            })
        }

        pub async fn run(self, port: u16) -> Result<(), std::io::Error> {
            let database = web::Data::new(self.database);

            HttpServer::new(move || {
                App::new()
                    .app_data(database.clone())
                    .route("/", web::get().to(handle_wsmessage))
            })
            .bind(("127.0.0.1", port))?
            .run()
            .await
        }
    }

    async fn handle_wsmessage(
        req: HttpRequest,
        stream: web::Payload,
        database: web::Data<Database>,
    ) -> Result<HttpResponse, Error> {
        let (res, session, stream) = actix_ws::handle(&req, stream)?;
        let database = database.clone();
        let mut stream = stream
            .aggregate_continuations()
            .max_continuation_size(2_usize.pow(20));

        rt::spawn(async move {
            let _session = session;

            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(AggregatedMessage::Text(msg)) => match parse_message(msg.to_string()) {
                        Ok(payload) => match store_db(&payload, &database.pool).await {
                            Ok(()) => {}
                            Err(err) => {
                                eprintln!(
                                    "Error: Failed to save the WebSocket input content in Database."
                                );
                                eprintln!("Error output was: {err}");
                            }
                        },
                        Err(err) => {
                            eprintln!("Error: Failed to parse the WebSocket input.");
                            eprintln!("Error output was: {err}");
                            eprintln!("Tip: the message must be a JSON in this format:");
                            eprintln!(
                                r#"{{ "ip": "123.123.123.123", "app": "text", "content": "text" }}"#
                            );
                        }
                    },
                    Ok(_) => {
                        eprintln!("Error: Invalid message.");
                        eprintln!("Tip: the message must be a text message");
                    }
                    Err(err) => {
                        eprintln!("Error: WebSocket stream error: {err}");
                        break;
                    }
                }
            }
        });

        Ok(res)
    }

    fn parse_message(text: String) -> Result<LogPayload, serde_json::Error> {
        serde_json::from_str(&text)
    }

    #[derive(Debug, Deserialize, PartialEq)]
    pub struct LogPayload {
        pub ip: Ipv4Addr,
        pub app: String,
        pub content: String,
    }

    #[cfg(test)]
    mod tests {
        use std::net::Ipv4Addr;

        use super::{LogPayload, parse_message};

        #[test]
        fn it_parses_correct_json() {
            let expected = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo"),
            };

            let result = parse_message(String::from(
                r#"{ "ip": "127.0.0.1", "app": "test_app", "content": "foo" }"#,
            ));

            assert_eq!(result.unwrap(), expected);
        }

        #[test]
        fn it_rejects_invalid_ip() {
            let result = parse_message(String::from(
                r#"{ "ip": "abc", "app": "test_app", "content": "foo" }"#,
            ));

            assert!(result.is_err());
        }

        #[test]
        fn it_rejects_non_string_app() {
            let result = parse_message(String::from(
                r#"{ "ip": "127.0.0.1", "app": 123, "content": "foo" }"#,
            ));

            assert!(result.is_err());
        }

        #[test]
        fn it_rejects_non_string_content() {
            let result = parse_message(String::from(
                r#"{ "ip": "127.0.0.1", "app": "test_app", "content": true }"#,
            ));

            assert!(result.is_err());
        }

        #[test]
        fn it_rejects_missing_content_field() {
            let result = parse_message(String::from(r#"{ "ip": "127.0.0.1", "app": "test_app" }"#));

            assert!(result.is_err());
        }

        #[test]
        fn it_rejects_malformed_json() {
            let result = parse_message(String::from(
                r#"{ "ip": "127.0.0.1", "app": "test_app", "content": "foo" "#,
            ));

            assert!(result.is_err());
        }

        #[test]
        fn it_accepts_empty_strings() {
            let result = parse_message(String::from(
                r#"{ "ip": "127.0.0.1", "app": "", "content": "" }"#,
            ));

            assert!(result.is_ok());
        }
    }
}

pub mod db {
    use super::server::LogPayload;
    use email_address::EmailAddress;
    use sqlx::{Pool, Sqlite, SqlitePool, sqlite::SqliteConnectOptions};
    use std::{fmt, str::FromStr};

    pub struct Database {
        pub pool: Pool<Sqlite>,
    }

    impl Database {
        pub async fn new() -> Result<Database, sqlx::Error> {
            let options =
                SqliteConnectOptions::from_str("sqlite://seraphim.db")?.create_if_missing(true);

            let pool = SqlitePool::connect_with(options).await?;

            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ip TEXT,
                    app TEXT,
                    content TEXT,
                    type TEXT
                    );
            "#,
            )
            .execute(&pool)
            .await?;

            Ok(Database { pool })
        }
    }

    async fn check_last_log_type(
        ip: &str,
        app: &str,
        pool: &Pool<Sqlite>,
    ) -> Result<Option<String>, sqlx::Error> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT type FROM logs WHERE ip = ? AND app = ? ORDER BY id DESC LIMIT 1",
        )
        .bind(ip)
        .bind(app)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| r.0))
    }

    async fn check_content(
        payload: &LogPayload,
        pool: &Pool<Sqlite>,
    ) -> Result<ContentTypes, sqlx::Error> {
        if EmailAddress::is_valid(&payload.content) {
            return Ok(ContentTypes::Email);
        }

        let last_type = check_last_log_type(&payload.ip.to_string(), &payload.app, pool).await?;

        match last_type {
            Some(last_type) => {
                if last_type == "Email" {
                    Ok(ContentTypes::Password)
                } else if last_type == "Password" {
                    Ok(ContentTypes::TFA)
                } else {
                    Ok(ContentTypes::PlainText)
                }
            }
            None => Ok(ContentTypes::PlainText),
        }
    }

    pub async fn store_db(payload: &LogPayload, pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
        let content_type = check_content(&payload, pool).await?;

        sqlx::query(
            r#"
                        INSERT INTO logs (ip, app, content, type)
                        VALUES (?, ?, ?, ?)
                    "#,
        )
        .bind(&payload.ip.to_string())
        .bind(&payload.app)
        .bind(&payload.content)
        .bind(content_type.to_string())
        .execute(pool)
        .await?;

        Ok(())
    }

    #[derive(Debug, PartialEq)]
    enum ContentTypes {
        Email,
        Password,
        TFA,
        PlainText,
    }

    impl fmt::Display for ContentTypes {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                ContentTypes::Email => write!(f, "Email"),
                ContentTypes::Password => write!(f, "Password"),
                ContentTypes::TFA => write!(f, "TFA"),
                ContentTypes::PlainText => write!(f, "PlainText"),
            }
        }
    }

    #[cfg(test)]
    mod test {
        use sqlx::{Pool, Sqlite, SqlitePool, sqlite::SqliteConnectOptions};
        use temp_testdir::TempDir;

        struct DatabaseTest {
            pool: Pool<Sqlite>,
            _temp: TempDir,
        }

        use crate::{
            db::{ContentTypes, check_content, check_last_log_type, store_db},
            server::LogPayload,
        };
        use std::{net::Ipv4Addr, path::PathBuf, str::FromStr};

        async fn setup_database(name: &str) -> Result<DatabaseTest, sqlx::Error> {
            let temp = TempDir::default();
            let mut db_path = PathBuf::from(temp.as_ref());
            db_path.push(name);

            let db_path = db_path
                .into_string()
                .expect("Failed to parse DB path to a string");

            let options = SqliteConnectOptions::from_str(&format!("sqlite://{db_path}.db",))?
                .create_if_missing(true);

            let pool = SqlitePool::connect_with(options).await?;

            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ip TEXT,
                    app TEXT,
                    content TEXT,
                    type TEXT
                    );
            "#,
            )
            .execute(&pool)
            .await?;

            Ok(DatabaseTest { pool, _temp: temp })
        }

        #[tokio::test]
        async fn it_stores_data_in_database() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo"),
            };

            let db = setup_database("test_store")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            let query = "SELECT ip, app, content FROM logs ORDER BY id DESC LIMIT 1";

            let result: Option<(String, String, String)> = sqlx::query_as(query)
                .fetch_optional(&db.pool)
                .await
                .expect("Query failed");

            let result = result.expect("No row found in database");

            assert_eq!(result.0, "127.0.0.1");
            assert_eq!(result.1, "test_app");
            assert_eq!(result.2, "foo");
        }

        #[tokio::test]
        async fn it_checks_last_log_type_plain_text() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo"),
            };

            let db = setup_database("test_last_log_plain")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_last_log_type(&payload.ip.to_string(), &payload.app, &db.pool).await;

            assert!(result.is_ok());

            if let Some(result_content) = result.unwrap() {
                assert_eq!(result_content, "PlainText");
            }
        }

        #[tokio::test]
        async fn it_checks_last_log_type_email() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let db = setup_database("test_last_log_email")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_last_log_type(&payload.ip.to_string(), &payload.app, &db.pool).await;

            assert!(result.is_ok());

            if let Some(result_content) = result.unwrap() {
                assert_eq!(result_content, "Email");
            }
        }

        #[tokio::test]
        async fn it_checks_last_log_type_password() {
            let payload_email = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let payload_pass = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("123"),
            };

            let db = setup_database("test_last_log_password")
                .await
                .expect("Failed to create database");

            store_db(&payload_email, &db.pool)
                .await
                .expect("Failed to store data");

            store_db(&payload_pass, &db.pool)
                .await
                .expect("Failed to store data");

            let result =
                check_last_log_type(&payload_pass.ip.to_string(), &payload_pass.app, &db.pool)
                    .await;

            assert!(result.is_ok());

            if let Some(result_content) = result.unwrap() {
                assert_eq!(result_content, "Password");
            }
        }

        #[tokio::test]
        async fn it_checks_last_log_type_tfa() {
            let payload_email = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let payload_pass = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("123"),
            };

            let payload_tfa = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("123-456"),
            };

            let db = setup_database("test_last_log_tfa")
                .await
                .expect("Failed to create database");

            store_db(&payload_email, &db.pool)
                .await
                .expect("Failed to store data");

            store_db(&payload_pass, &db.pool)
                .await
                .expect("Failed to store data");

            store_db(&payload_tfa, &db.pool)
                .await
                .expect("Failed to store data");

            let result =
                check_last_log_type(&payload_tfa.ip.to_string(), &payload_tfa.app, &db.pool).await;

            assert!(result.is_ok());

            if let Some(result_content) = result.unwrap() {
                assert_eq!(result_content, "TFA");
            }
        }

        #[tokio::test]
        async fn it_checks_content_type_plain() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo"),
            };

            let db = setup_database("test_check_content_plain")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_content(&payload, &db.pool).await;

            assert!(result.is_ok());

            if let Ok(result_content) = result {
                assert_eq!(result_content, ContentTypes::PlainText);
            }
        }

        #[tokio::test]
        async fn it_checks_content_type_plain_with_older_row() {
            let payload_a = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo"),
            };

            let payload_b = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("blah"),
            };

            let db = setup_database("test_check_content_plain_older")
                .await
                .expect("Failed to create database");

            store_db(&payload_a, &db.pool)
                .await
                .expect("Failed to store data");

            store_db(&payload_b, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_content(&payload_b, &db.pool).await;

            assert!(result.is_ok());

            if let Ok(result_content) = result {
                assert_eq!(result_content, ContentTypes::PlainText);
            }
        }

        #[tokio::test]
        async fn it_checks_content_type_email() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let db = setup_database("test_check_content_email")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_content(&payload, &db.pool).await;

            assert!(result.is_ok());

            if let Ok(result_content) = result {
                assert_eq!(result_content, ContentTypes::Email);
            }
        }

        #[tokio::test]
        async fn it_checks_content_type_email_without_previous_content() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let db = setup_database("test_check_content_email_without")
                .await
                .expect("Failed to create database");

            let result = check_content(&payload, &db.pool).await;

            assert!(result.is_ok());

            if let Ok(result_content) = result {
                assert_eq!(result_content, ContentTypes::Email);
            }
        }

        #[tokio::test]
        async fn it_checks_content_type_password() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let payload_b = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("12345"),
            };

            let db = setup_database("test_check_content_password")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_content(&payload_b, &db.pool).await;

            assert!(result.is_ok());

            if let Ok(result_content) = result {
                assert_eq!(result_content, ContentTypes::Password);
            }
        }

        #[tokio::test]
        async fn it_checks_content_type_tfa() {
            let payload = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("foo@example.com"),
            };

            let payload_b = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("12345"),
            };

            let payload_c = LogPayload {
                ip: Ipv4Addr::new(127, 0, 0, 1),
                app: String::from("test_app"),
                content: String::from("123-456"),
            };

            let db = setup_database("test_check_content_tfa")
                .await
                .expect("Failed to create database");

            store_db(&payload, &db.pool)
                .await
                .expect("Failed to store data");

            store_db(&payload_b, &db.pool)
                .await
                .expect("Failed to store data");

            let result = check_content(&payload_c, &db.pool).await;

            assert!(result.is_ok());

            if let Ok(result_content) = result {
                assert_eq!(result_content, ContentTypes::TFA);
            }
        }
    }
}
