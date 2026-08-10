//! HypeEdge binary entry point.

use clap::{Parser, Subcommand};
use hypeedge_app::HypeEdgeApp;

/// HypeEdge quantitative trading system.
#[derive(Parser, Debug)]
#[command(
    name = "hypeedge",
    version,
    about = "HypeEdge quantitative trading system"
)]
struct Cli {
    /// Configuration environment (dev | testnet | mainnet). When unset, the
    /// config loader resolves it: process `HYPE_ENV` > `.env` > "dev".
    #[arg(long, env = "HYPE_ENV")]
    environment: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Export ClickHouse market data to a local DuckDB file (offline research).
    Export {
        /// Output DuckDB file path.
        #[arg(long, default_value = "research.duckdb")]
        output: String,

        /// Coins to export (repeatable).
        #[arg(long, default_value = "BTC")]
        coin: Vec<String>,

        /// Start time as Unix millis (default: 30 days ago).
        #[arg(long)]
        start_ms: Option<i64>,

        /// End time as Unix millis (default: now).
        #[arg(long)]
        end_ms: Option<i64>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();

    let settings = match hypeedge_config::loader::load_settings(cli.environment.as_deref()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "settings load failed");
            std::process::exit(1);
        }
    };

    match cli.command {
        Some(Command::Export {
            output,
            coin,
            start_ms,
            end_ms,
        }) => {
            let ch = settings.clickhouse.clone();
            let client = clickhouse::Client::default()
                .with_url(format!("http://{}:{}", ch.host, ch.port))
                .with_database(&ch.database)
                .with_user(&ch.username)
                .with_password(&ch.password);
            let now = chrono::Utc::now().timestamp_millis();
            let start = start_ms.unwrap_or(now - 30 * 24 * 3600 * 1000);
            let end = end_ms.unwrap_or(now);
            let coins: Vec<&str> = coin.iter().map(|c| c.as_str()).collect();
            match hypeedge_storage::duckdb_export::export_all(&client, &output, &coins, start, end)
                .await
            {
                Ok(totals) => {
                    tracing::info!(file = %output, totals = ?totals, "duckdb_export_all_complete");
                }
                Err(e) => {
                    tracing::error!(error = %e, "duckdb_export_failed");
                    std::process::exit(1);
                }
            }
        }
        None => {
            let app = HypeEdgeApp::new(settings);
            if let Err(e) = app.serve().await {
                tracing::error!(error = %e, "api server exited");
                std::process::exit(1);
            }
        }
    }
}
