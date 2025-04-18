use cql_lsp::lsp::*;
use cql_lsp::setup::{setup_config, setup_logger};
use log::info;
use std::collections::HashMap;
use tokio::io::{stdin, stdout};
use tokio::sync::RwLock;
use tower_lsp::{LspService, Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting logger setup...");
    setup_logger().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    setup_config().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    println!("Logger setup complete");

    info!("Server starting");

    let stdin = stdin();
    let stdout = stdout();
    let (service, socket) = LspService::new(|client| Backend {
        client,
        documents: RwLock::new(HashMap::new()),
    });
    Server::new(stdin, stdout, socket).serve(service).await;

    Ok(())
}
