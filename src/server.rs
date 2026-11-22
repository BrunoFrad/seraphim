pub mod server {
    use std::net::Ipv4Addr;

    use crate::db::db::{Database, store_db};
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