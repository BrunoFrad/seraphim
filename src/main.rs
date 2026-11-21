use clap::Parser;
use seraphim::{cli::*, server::{Server}};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Watch { port: None } => {
            println!("=> Server is running on ws://127.0.0.1:7777");

            match Server::new().await {
                Ok(server) => {
                    server.run(7777).await?;
                }

                Err(err) => {
                    eprintln!("error: failed to start the server.");
                    eprintln!("The error message was: {err}");
                }
            }
        }
        Commands::Watch { port: Some(port) } => {
            println!("=> Server is running on ws://127.0.0.1:{port}");

            match Server::new().await {
                Ok(server) => {
                    server.run(port).await?;
                }

                Err(err) => {
                    eprintln!("error: failed to start the server.");
                    eprintln!("The error message was: {err}");
                }
            }
        }
    }

    Ok(())
}
