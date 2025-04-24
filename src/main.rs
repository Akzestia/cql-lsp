use cql_lsp::cqlsh::CqlSettings;
use cql_lsp::lsp::*;
use cql_lsp::setup::setup_logger;
use log::info;
use std::collections::HashMap;
use tokio::io::{stdin, stdout};
use tokio::sync::RwLock;
use tower_lsp::{LspService, Server};

/*
    Default values for localhosted DB (Tested With ScyllaDB)

    CQL_LSP_DB_URL = "127.0.0.1:9042"
    CQL_LSP_DB_PASSWD = "cassandra"
    CQL_LSP_DB_USER = "cassandra"
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting logger setup...");
    setup_logger().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    println!("Logger setup complete");

    // Set missing env variables to default ones
    let url = std::env::var("CQL_LSP_DB_URL").unwrap_or_else(|_| {
        info!("Db url wasn't provided. Setting url to 127.0.0.1");
        "127.0.0.1".to_string()
    });
    let pswd = std::env::var("CQL_LSP_DB_PASSWD").unwrap_or_else(|_| {
        info!("Db pswd wasn't provided.\nSetting pswd to default(cassandra)");
        "cassandra".to_string()
    });
    let user = std::env::var("CQL_LSP_DB_USER").unwrap_or_else(|_| {
        info!("Db user wasn't provided.\nSetting user to default(cassandra)");
        "cassandra".to_string()
    });

    let settings = CqlSettings::from_env(&url, &pswd, &user);

    info!("Server starting");

    let stdin = stdin();
    let stdout = stdout();
    let (service, socket) = LspService::new(|client| Backend {
        client,
        documents: RwLock::new(HashMap::new()),
        current_document: RwLock::new(None),
        config: settings,
    });
    Server::new(stdin, stdout, socket).serve(service).await;

    Ok(())
}
