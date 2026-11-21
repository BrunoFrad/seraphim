pub mod cli {
    use clap::Parser;

    #[derive(Parser)]
    #[command(
        version,
        about = "An easy-to-use, fast and full-featured CLI for managing logs"
    )]
    pub struct Cli {}
}

pub mod server {
    use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, rt, web};
    use actix_ws::AggregatedMessage;
    use futures_util::StreamExt as _;

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
        let (res, _, stream) = actix_ws::handle(&req, stream)?;

        let mut stream = stream
            .aggregate_continuations()
            .max_continuation_size(2_usize.pow(20));

        rt::spawn(async move {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(AggregatedMessage::Text(text)) => {
                        println!("Got {text}");
                    }

                    _ => {}
                }
            }
        });

        Ok(res)
    }
}
