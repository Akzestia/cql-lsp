use futures::stream::StreamExt;
use scylla::{
    DeserializeRow,
    client::session_builder::SessionBuilder,
    statement::{Statement, prepared::PreparedStatement},
};
use std::fmt;
use std::time::Duration;

use log::info;

/*
    cqlsh.rs

    A custom CQL shell implementation in Rust using the ScyllaDB Rust driver.
    This should be compatible with most Cassandra Query Language (CQL) based
    databases, including ScyllaDB and Apache Cassandra.
*/

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

#[derive(Debug)]
pub struct Column {
    pub keyspace_name: String,
    pub table_name: String,
    pub column_name: String,
    pub column_type: String,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Column [keyspace: {}, table: {}, column: {}, type: {}]",
            self.keyspace_name, self.table_name, self.column_name, self.column_type
        )
    }
}

impl FromIterator<KeySpace> for Vec<String> {
    fn from_iter<I: IntoIterator<Item = KeySpace>>(iter: I) -> Self {
        iter.into_iter().map(|item| item.keyspace_name).collect()
    }
}

// CQL types
#[derive(Debug)]
pub struct Role {
    pub name: String,
}

#[derive(Debug)]
pub struct Aggregate {
    pub keyspace_name: String,
    pub aggregate_name: String,
}

#[derive(Debug)]
pub struct Function {
    pub keyspace_name: String,
    pub function_name: String,
}

#[derive(Debug)]
pub struct Index {
    pub keyspace_name: String,
    pub index_name: String,
}

#[derive(Debug)]
pub struct Type {
    pub keyspace_name: String,
    pub type_name: String,
}

#[derive(Debug)]
pub struct View {
    pub keyspace_name: String,
    pub view_name: String,
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

pub async fn query_g_fields(
    config: &CqlSettings,
) -> Result<Vec<Column>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;
    let mut items = Vec::<Column>::new();

    let tables = query_g_tables(config).await?;

    for table in tables {
        let query = format!(
            "SELECT column_name, type  FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}';",
            table.keyspace_name, table.table_name
        );

        let result_rows = session
            .query_unpaged(query, &[])
            .await?
            .into_rows_result()?;

        for row in result_rows.rows::<(String, String)>()? {
            let column = row?;
            info!("Found field: {}", column.0);
            items.push(Column {
                column_name: column.0,
                keyspace_name: table.keyspace_name.clone(),
                table_name: table.table_name.clone(),
                column_type: column.1,
            });
        }
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

pub async fn query_keyspace_scoped_tables(
    config: &CqlSettings,
    keyspace: &str,
) -> Result<Vec<Table>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!(
        "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}';"
    );

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Table>::new();

    for row in result_rows.rows::<Table>()? {
        let table = row?;
        items.push(table);
    }
    Ok(items)
}

pub async fn query_g_tables(
    config: &CqlSettings,
) -> Result<Vec<Table>, Box<dyn std::error::Error>> {
    let keyspaces = query_keyspaces(&config).await?;
    let mut items = Vec::<Table>::new();

    for keyspace in keyspaces {
        let mut tables = query_keyspace_scoped_tables(&config, &keyspace.keyspace_name).await?;
        items.append(&mut tables);
    }

    Ok(items)
}

pub async fn query_keyspace_scoped_fields(
    config: &CqlSettings,
    keyspace: &str,
) -> Result<Vec<Column>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    // SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{}';
    // Sshort row_result query instead of using query_g_tables()
    // Ccause query_g_tables() returns not just table names, but a Ve<Tables> insteads
    let select_tables_query =
        format!("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}';");

    let result_rows = session
        .query_unpaged(select_tables_query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Column>::new();

    for row in result_rows.rows::<(String,)>()? {
        let row_result = row?;
        info!("Table_name: {}", row_result.0);
        let table = row_result.0;

        // SELECT * FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}';
        let select_columns_query = format!(
            "SELECT keyspace_name, table_name, column_name, type FROM system_schema.columns WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'"
        );

        let result_rows = session
            .query_unpaged(select_columns_query, &[])
            .await?
            .into_rows_result()?;

        for jrow in result_rows.rows::<(String, String, String, String)>()? {
            let jrow_result = jrow?;
            let column = Column {
                keyspace_name: jrow_result.0,
                table_name: jrow_result.1,
                column_name: jrow_result.2,
                column_type: jrow_result.3,
            };

            items.push(column);
        }
    }

    Ok(items)
}

pub async fn query_hard_scoped_fields(
    config: &CqlSettings,
    keyspace_name: &str,
    table_name: &str,
) -> Result<Vec<Column>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!(
        "SELECT column_name, type  FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}';",
        keyspace_name, table_name
    );

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Column>::new();

    for row in result_rows.rows::<(String, String)>()? {
        let row_result = row?;
        let column_name = row_result.0;
        let column_type = row_result.1;
        items.push(Column {
            keyspace_name: keyspace_name.to_string(),
            table_name: table_name.to_string(),
            column_name,
            column_type,
        });
    }

    Ok(items)
}

/*
    keyspace_name |
    aggregate_name |
    argument_types |
    final_func |
    initcond |
    return_type |
    state_func |
    state_type
*/
pub async fn query_aggregates(
    config: &CqlSettings,
) -> Result<Vec<Aggregate>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!("SELECT keyspace_name, aggregate_name FROM system_schema.aggregates;");

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Aggregate>::new();

    for row in result_rows.rows::<(String, String)>()? {
        let row_result = row?;
        let keyspace_name = row_result.0;
        let aggregate_name = row_result.1;
        items.push(Aggregate {
            keyspace_name,
            aggregate_name,
        });
    }

    Ok(items)
}

/*
    keyspace_name |
    function_name |
    argument_types |
    argument_names |
    body |
    called_on_null_input |
    language |
    return_type
*/
pub async fn query_functions(
    config: &CqlSettings,
) -> Result<Vec<Function>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!("SELECT keyspace_name, function_name FROM system_schema.functions;");

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Function>::new();

    for row in result_rows.rows::<(String, String)>()? {
        let row_result = row?;
        let keyspace_name = row_result.0;
        let function_name = row_result.1;
        items.push(Function {
            keyspace_name,
            function_name,
        });
    }

    Ok(items)
}

/*
    keyspace_name |
    table_name |
    index_name |
    kind |
    options
*/
pub async fn query_indexes(config: &CqlSettings) -> Result<Vec<Index>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!("SELECT keyspace_name, index_name FROM system_schema.indexes;");

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Index>::new();

    for row in result_rows.rows::<(String, String)>()? {
        let row_result = row?;
        let keyspace_name = row_result.0;
        let index_name = row_result.1;
        items.push(Index {
            keyspace_name,
            index_name,
        });
    }

    Ok(items)
}

/*
    keyspace_name |
    type_name   |
    field_names |
    field_type
*/
pub async fn query_types(config: &CqlSettings) -> Result<Vec<Type>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!("SELECT keyspace_name, type_name FROM system_schema.types;");

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<Type>::new();

    for row in result_rows.rows::<(String, String)>()? {
        let row_result = row?;
        let keyspace_name = row_result.0;
        let type_name = row_result.1;
        items.push(Type {
            keyspace_name,
            type_name,
        });
    }

    Ok(items)
}

/*
    keyspace_name |
    view_name |
    base_table_id |
    base_table_name |
    bloom_filter_fp_chance |
    caching |
    comment |
    compaction |
    compression |
    crc_check_chance |
    dclocal_read_repair_chance |
    default_time_to_live |
    extensions | gc_grace_seconds |
    id | include_all_columns |
    max_index_interval |
    memtable_flush_period_in_ms |
    min_index_interval |
    read_repair_chance |
    speculative_retry |
    where_clause
*/
pub async fn query_views(config: &CqlSettings) -> Result<Vec<View>, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(&config.url)
        .user(&config.user, &config.pswd)
        .connection_timeout(Duration::from_secs(3))
        .build()
        .await?;

    let query = format!("SELECT keyspace_name, view_name FROM system_schema.views;");

    let result_rows = session
        .query_unpaged(query, &[])
        .await?
        .into_rows_result()?;

    let mut items = Vec::<View>::new();

    for row in result_rows.rows::<(String, String)>()? {
        let row_result = row?;
        let keyspace_name = row_result.0;
        let view_name = row_result.1;
        items.push(View {
            keyspace_name,
            view_name,
        });
    }

    Ok(items)
}
