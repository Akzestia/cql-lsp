use cql_lsp::cqlsh::CqlSettings;
use cql_lsp::lsp::Backend;
use cql_lsp::setup::setup_logger;
use log::info;
use std::collections::HashMap;
use tokio::io::{stdin, stdout};
use tokio::sync::RwLock;
use tower_lsp::{LspService, Server};

/*
    Based on DataStax HCD && CQL versions 3.4+

    [HCD]
    https://docs.datastax.com/en/cql/hcd/reference/cql-reference-about.html
    [CQL]
    https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html

    Note!

    Some of the default CQL functions will be different because of DataStax HCD extensions
*/

/*
    Default values for localhosted DB (Tested With ScyllaDB)


    [LocalHost]
    CQL_LSP_DB_URL = "127.0.0.1:9042"
    CQL_LSP_DB_PASSWD = "cassandra"
    CQL_LSP_DB_USER = "cassandra"
    CQL_LSP_ENABLE_LOGGING = false | Used for development

    [Dockerults]
    CQL_LSP_DB_URL = "172.17.0.2:9042"
    CQL_LSP_DB_PASSWD = "cassandra"
    CQL_LSP_DB_USER = "cassandra"
    CQL_LSP_ENABLE_LOGGING = false | Used for development
*/

/*
    Lowercase keyword support

    This CQL LSP implementation supports lowercase usage for almost
    all keyword types, even though not all lowercase keywords are
    valid in standard CQL syntax. This approach helps future-proof
    the LSP implementation.
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logger
    let enable_logging = std::env::var("CQL_LSP_ENABLE_LOGGING").unwrap_or_else(|_| {
        info!("Logging mode wasn't provided. Setting Logging mode to default(false)");
        "false".to_string()
    });

    // Enabel logging if env variable was set to true
    if enable_logging == "true" {
        setup_logger().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    }

    // Set missing env variables to default ones
    let url = std::env::var("CQL_LSP_DB_URL").unwrap_or_else(|_| {
        // Defaults to localhost and NOT docker
        info!("Db url wasn't provided. Setting url to default(127.0.0.1)");
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

    // Init CqlSettings settings
    let settings = CqlSettings::from_env(&url, &pswd, &user);

    // Start LSP
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
