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
            #[arg(short, long)]
            port: Option<u16>,
        },
    }
}

pub mod server {

    use std::{net::Ipv4Addr, str::FromStr};

    use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, rt, web};
    use actix_ws::AggregatedMessage;
    use futures_util::StreamExt as _;
    use serde::Deserialize;
    use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};

    pub async fn run(port: u16) -> Result<(), std::io::Error> {
        HttpServer::new(|| App::new().route("/", web::get().to(handle_wsmessage)))
            .bind(("127.0.0.1", port))?
            .run()
            .await
    }

    async fn handle_wsmessage(
        req: HttpRequest,
        stream: web::Payload,
    ) -> Result<HttpResponse, Error> {
        let (res, session, stream) = actix_ws::handle(&req, stream)?;

        let mut stream = stream
            .aggregate_continuations()
            .max_continuation_size(2_usize.pow(20));

        rt::spawn(async move {
            let _session = session;

            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(AggregatedMessage::Text(msg)) => match parse_message(msg.to_string()) {
                        Ok(msg) => match store_in_db(msg).await {
                            Err(err) => {
                                eprintln!(
                                    "Error: Failed to save the WebSocket input content in Database."
                                );
                                eprintln!("Error output was: {err}");
                            }
                            _ => (),
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

                    _ => {}
                }
            }
        });

        Ok(res)
    }

    fn parse_message(text: String) -> Result<LogPayload, serde_json::Error> {
        let payload: LogPayload = serde_json::from_str(&text)?;
        Ok(payload)
    }

    async fn store_in_db(payload: LogPayload) -> Result<(), sqlx::Error> {
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

        sqlx::query(
            r#"
            INSERT INTO logs (ip, app, content)
            VALUES (?, ?, ?)
        "#,
        )
        .bind(&payload.ip.to_string())
        .bind(&payload.app)
        .bind(&payload.content)
        .execute(&pool)
        .await?;

        Ok(())
    }

    #[derive(Deserialize)]
    struct LogPayload {
        ip: Ipv4Addr,
        app: String,
        content: String,
    }
}
