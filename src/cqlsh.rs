use futures::stream::StreamExt;
use scylla::{
    DeserializeRow,
    client::session_builder::SessionBuilder,
    statement::{Statement, prepared::PreparedStatement},
};
use std::fmt;
use std::time::Duration;

use log::info;

#[derive(DeserializeRow)]
pub struct Table {
    pub keyspace_name: String,
    pub table_name: String,
}

impl Table {
    pub fn united(&self) -> String {
        format!("{}.{}", self.keyspace_name, self.table_name)
    }
}

#[derive(DeserializeRow)]
pub struct KeySpace {
    pub keyspace_name: String,
    pub durable_writes: bool,
    pub replication: std::collections::HashMap<String, String>,
}

impl FromIterator<KeySpace> for Vec<String> {
    fn from_iter<I: IntoIterator<Item = KeySpace>>(iter: I) -> Self {
        iter.into_iter().map(|item| item.keyspace_name).collect()
    }
}

#[derive(Debug)]
pub struct CqlSettings {
    pub url: String,
    pub pswd: String,
    pub user: String,
}

impl CqlSettings {
    pub fn new() -> Self {
        Self {
            url: String::from("127.0.0.1:9042"),
            pswd: String::from("cassandra"),
            user: String::from("cassandra"),
        }
    }

    pub fn from_env(url: &str, pswd: &str, user: &str) -> Self {
        Self {
            url: String::from(url),
            pswd: String::from(pswd),
            user: String::from(user),
        }
    }
}

/*
    Queries all keyspaces from system_schema
*/

pub async fn query_keyspaces(
    config: &CqlSettings,
) -> Result<Vec<KeySpace>, Box<dyn std::error::Error>> {
    info!("Start transaction");
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let select_statement: Statement = Statement::new("SELECT * FROM system_schema.keyspaces;");
    let statement: PreparedStatement = session.prepare(select_statement).await?;

    let mut rows_stream = session
        .execute_iter(statement, &[])
        .await?
        .rows_stream::<KeySpace>()?;

    let mut items = Vec::<KeySpace>::new();

    while let Some(next_row_res) = rows_stream.next().await {
        let keyspace: KeySpace = next_row_res?;
        info!("Keyspace {:?}", keyspace.keyspace_name);
        items.push(keyspace);
    }

    info!("End transaction");

    Ok(items)
}

pub async fn query_fields(
    config: &CqlSettings,
    keyspace: &str,
    table: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!(
        "SELECT column_name FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}';",
        keyspace, table
    );

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<String>::new();

    for row in result_rows.rows::<(String,)>()? {
        let field = row?;
        info!("Found field: {}", field.0);
        items.push(field.0);
    }

    Ok(items)
}

pub async fn check_connection(config: &CqlSettings) -> Result<bool, Box<dyn std::error::Error>> {
    _ = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    Ok(true)
}
