//! hiveflow-cli — Command line operator interface for HiveFlow Engine.

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "hiveflow", author, version, about = "HiveFlow High-Performance Engine CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Inspect metadata catalogue
    Catalog,
    /// Trigger an execution run
    Run {
        #[arg(short, long)]
        source_tag: String,
        #[arg(short, long)]
        table: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Catalog) => {
            println!("HiveFlow Catalog: Connected to metadata ledger");
        }
        Some(Commands::Run { source_tag, table }) => {
            println!("Triggering run for source: {}, table: {:?}", source_tag, table);
        }
        None => {
            println!("HiveFlow Engine v0.1.0 — specify --help for available commands.");
        }
    }

    Ok(())
}
