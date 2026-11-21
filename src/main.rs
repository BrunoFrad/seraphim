use clap::Parser;
use seraphim::{cli::*, server::Server};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Watch { port } => {
            println!("=> Server is running on ws://127.0.0.1:{port}");

            let server = Server::new().await.map_err(std::io::Error::other)?;

            server.run(port).await?;
        }
    }

    Ok(())
}
