use clap::Parser;
use seraphim::{cli::*, server};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Watch { port: None } => {
            println!("=> Server is running on ws://127.0.0.1:7777");
            server::run(7777).await?;
        }
        Commands::Watch { port: Some(port) } => {
            println!("=> Server is running on ws://127.0.0.1:{port}");
            server::run(port).await?;
        }
    }

    Ok(())
}
