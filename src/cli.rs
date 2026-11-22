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
