use clap::{Parser};

fn main() {
    Cli::parse();
}

#[derive(Parser)]
#[command(version, about="An easy-to-use, fast and full-featured CLI for managing logs")]
struct Cli {}