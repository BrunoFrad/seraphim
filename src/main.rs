use clap::Parser;
use seraphim::{
    cli::cli::{Cli, Commands},
    server::server::Server,
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Watch { port } => {
            seraphim::banner(port);

            let server = Server::new().await.map_err(std::io::Error::other)?;

            server.run(port).await?;
        }
    }

    Ok(())
}
