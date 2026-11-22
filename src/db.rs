pub mod db {
    use crate::server::server::LogPayload;
    use sqlx::{Pool, Sqlite, SqlitePool, sqlite::SqliteConnectOptions};
    use std::str::FromStr;

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
                        content TEXT
                    );
            "#,
            )
            .execute(&pool)
            .await?;

            Ok(Database { pool })
        }
    }

    pub async fn store_db(payload: &LogPayload, pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
                        INSERT INTO logs (ip, app, content, type)
                        VALUES (?, ?, ?, ?)
                    "#,
        )
        .bind(&payload.ip.to_string())
        .bind(&payload.app)
        .bind(&payload.content)
        .execute(pool)
        .await?;

        Ok(())
    }

    #[cfg(test)]
    mod test {
        use crate::{db::db::store_db, server::server::LogPayload};
        use sqlx::{Pool, Sqlite, SqlitePool, sqlite::SqliteConnectOptions};
        use std::{net::Ipv4Addr, path::PathBuf, str::FromStr};
        use temp_testdir::TempDir;

        struct DatabaseTest {
            pool: Pool<Sqlite>,
            _temp: TempDir,
        }

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
    }
}
