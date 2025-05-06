use once_cell::sync::Lazy;
use tower_lsp::lsp_types::*;

/*
    Based on DataStax HCD && CQL versions 3.4+

    HCD
    https://docs.datastax.com/en/cql/hcd/reference/cql-reference-about.html
    CQL
    https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html

    Note!

    Some of the default CQL functions will be different because of DataStax HCD extensions
*/

/*
    CQL Native functions

    While they are already included inside KEYWORDS,
    We still need to have a separate list of functions, in order
    to suggest them in specific cases like inside SELECT ... FROM statement
    or inside WHERE ... && AND ... where u can't sugggest full list of keywords.

    Note!

    This list doesn't include deprecated functions:

    -------------------[DEPRECATED FUNCTIONS]-------------------
    dateOf
    unixTimestampOf
    -------------------[DEPRECATED FUNCTIONS]-------------------
*/

pub static CQL_NATIVE_FUNCTIONS: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    vec![
        // -----------------------[Scalar functions]-----------------------

        // CAST
        CompletionItem {
            label: "CAST".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case CAST functions".to_string()),
            documentation: Some(Documentation::String("CAST function".to_string())),
            insert_text: Some(r#"CAST("$0" AS "1$")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "cast".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case CAST functions".to_string()),
            documentation: Some(Documentation::String("CAST function".to_string())),
            insert_text: Some(r#"cast("$0" AS "1$")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TOKEN
        CompletionItem {
            label: "TOKEN".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case TOKEN functions".to_string()),
            documentation: Some(Documentation::String("TOKEN function".to_string())),
            insert_text: Some(r#"TOKEN("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "token".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case TOKEN functions".to_string()),
            documentation: Some(Documentation::String("TOKEN function".to_string())),
            insert_text: Some(r#"token("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TTL
        CompletionItem {
            label: "TTL".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case case TTL functions".to_string()),
            documentation: Some(Documentation::String("TTL function".to_string())),
            insert_text: Some(r#"TTL("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "ttl".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case TTL functions".to_string()),
            documentation: Some(Documentation::String("TTL function".to_string())),
            insert_text: Some(r#"ttl("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // UUID
        CompletionItem {
            label: "UUID".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case UUID functions".to_string()),
            documentation: Some(Documentation::String("UUID function".to_string())),
            insert_text: Some(r#"UUID() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "uuid".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case UUID functions".to_string()),
            documentation: Some(Documentation::String("UUID function".to_string())),
            insert_text: Some(r#"uuid() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // WriteTime
        CompletionItem {
            label: "WRITETIME".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case WRITETIME functions".to_string()),
            documentation: Some(Documentation::String("WRITETIME function".to_string())),
            insert_text: Some(r#"WRITETIME("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "writetime".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case WRITETIME functions".to_string()),
            documentation: Some(Documentation::String("WRITETIME function".to_string())),
            insert_text: Some(r#"writetime("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // -----------------------[Scalar functions]----------------------

        // -----------------------[Date and time functions]------------------------

        // CurrentDate
        CompletionItem {
            label: "CURRENT_DATE".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case CURRENT_DATE functions".to_string()),
            documentation: Some(Documentation::String("CURRENT_DATE function".to_string())),
            insert_text: Some(r#"CURRENT_DATE() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "current_date".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case CURRENT_DATE functions".to_string()),
            documentation: Some(Documentation::String("CURRENT_DATE function".to_string())),
            insert_text: Some(r#"current_date() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CurrentTime
        CompletionItem {
            label: "CURRENT_TIME".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case CURRENT_TIME functions".to_string()),
            documentation: Some(Documentation::String("CURRENT_TIME function".to_string())),
            insert_text: Some(r#"CURRENT_TIME() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "current_time".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case CURRENT_TIME functions".to_string()),
            documentation: Some(Documentation::String("CURRENT_TIME function".to_string())),
            insert_text: Some(r#"current_time() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CurrentTimestamp
        CompletionItem {
            label: "CURRENT_TIMESTAMP".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case CURRENT_TIMESTAMP functions".to_string()),
            documentation: Some(Documentation::String(
                "CURRENT_TIMESTAMP function".to_string(),
            )),
            insert_text: Some(r#"CURRENT_TIMESTAMP() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "current_timestamp".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case CURRENT_TIMESTAMP functions".to_string()),
            documentation: Some(Documentation::String(
                "CURRENT_TIMESTAMP function".to_string(),
            )),
            insert_text: Some(r#"current_timestamp() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CurrentTimeUuid
        CompletionItem {
            label: "CURRENT_TIMEUUID".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case CURRENT_TIMEUUID functions".to_string()),
            documentation: Some(Documentation::String(
                "CURRENT_TIMEUUID function".to_string(),
            )),
            insert_text: Some(r#"CURRENT_TIMEUUID() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "current_timeuuid".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case CURRENT_TIMEUUID functions".to_string()),
            documentation: Some(Documentation::String(
                "CURRENT_TIMEUUID function".to_string(),
            )),
            insert_text: Some(r#"current_timeuuid() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // Floor
        CompletionItem {
            label: "FLOOR".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case FLOOR functions".to_string()),
            documentation: Some(Documentation::String("FLOOR function".to_string())),
            insert_text: Some(r#"FLOOR("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "floor".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case FLOOR functions".to_string()),
            documentation: Some(Documentation::String("FLOOR function".to_string())),
            insert_text: Some(r#"floor("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // Now
        CompletionItem {
            label: "NOW".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case NOW functions".to_string()),
            documentation: Some(Documentation::String("NOW function".to_string())),
            insert_text: Some(r#"NOW() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "now".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case NOW functions".to_string()),
            documentation: Some(Documentation::String("NOW function".to_string())),
            insert_text: Some(r#"now() "$0""#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // minTimeUuid
        CompletionItem {
            label: "MIN_TIMEUUID".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case MIN_TIMEUUID functions".to_string()),
            documentation: Some(Documentation::String("MIN_TIMEUUID function".to_string())),
            insert_text: Some(r#"MIN_TIMEUUID("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "min_timeuuid".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case MIN_TIMEUUID functions".to_string()),
            documentation: Some(Documentation::String("MIN_TIMEUUID function".to_string())),
            insert_text: Some(r#"min_timeuuid("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // maxTimeUuid
        CompletionItem {
            label: "MAX_TIMEUUID".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case MAX_TIMEUUID functions".to_string()),
            documentation: Some(Documentation::String("MAX_TIMEUUID function".to_string())),
            insert_text: Some(r#"MAX_TIMEUUID("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "max_timeuuid".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case MAX_TIMEUUID functions".to_string()),
            documentation: Some(Documentation::String("MAX_TIMEUUID function".to_string())),
            insert_text: Some(r#"max_timeuuid("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // -----------------------[Date and time functions]------------------------

        // -----------------------[Date and time conversion]------------------------

        // toDate
        CompletionItem {
            label: "TODATE".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case TODATE functions".to_string()),
            documentation: Some(Documentation::String("TODATE function".to_string())),
            insert_text: Some(r#"TODATE("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "todate".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case TODATE functions".to_string()),
            documentation: Some(Documentation::String("TODATE function".to_string())),
            insert_text: Some(r#"todate("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // toTimestamp
        CompletionItem {
            label: "TOTIMESTAMP".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case TOTIMESTAMP functions".to_string()),
            documentation: Some(Documentation::String("TOTIMESTAMP function".to_string())),
            insert_text: Some(r#"TOTIMESTAMP("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "totimestamp".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case TOTIMESTAMP functions".to_string()),
            documentation: Some(Documentation::String("TOTIMESTAMP function".to_string())),
            insert_text: Some(r#"totimestamp("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // toUnixTimestamp
        CompletionItem {
            label: "TOUNIXTIMESTAMP".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Upper case TOUNIXTIMESTAMP functions".to_string()),
            documentation: Some(Documentation::String(
                "TOUNIXTIMESTAMP function".to_string(),
            )),
            insert_text: Some(r#"TOUNIXTIMESTAMP("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "tounixtimestamp".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("Lower case TOUNIXTIMESTAMP functions".to_string()),
            documentation: Some(Documentation::String(
                "TOUNIXTIMESTAMP function".to_string(),
            )),
            insert_text: Some(r#"tounixtimestamp("$0")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // -----------------------[Date and time conversion]------------------------

        // -----------------------[Blob conversion]------------------------

        // blobAs
        CompletionItem {
            label: "blobAs".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("blobAs functions".to_string()),
            documentation: Some(Documentation::String("blobAs function".to_string())),
            insert_text: Some(r#"blobAs<"$0">("$1")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // AsBlob
        CompletionItem {
            label: "AsBlob".to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: Some("AsBlob functions".to_string()),
            documentation: Some(Documentation::String("AsBlob function".to_string())),
            insert_text: Some(r#"<"$0">AsBlob("1$")"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // -----------------------[Blob conversion]------------------------
    ]
});

/*
    Lower case cql_keywords

    Separate list of keywords which
    are starting CQL commands;

    e.g

    |ALTER| TABLE ...
    |ALTER| MATERIALIZED VIEW ...

    |USE| ...

    |INSERT| ... INTO ...

    Note!

    CQL keywords ARE NOT affected by styled formatting!
*/
pub static CQL_KEYWORDS_LWC: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "alter".to_string(),
        /*
            because batch involves using

            BEGIN BATCH

            it's represented as begin kw
        */
        "begin".to_string(),
        "apply".to_string(),
        "create".to_string(),
        "drop".to_string(),
        "grant".to_string(),
        "list".to_string(),
        "revoke".to_string(),
        "select".to_string(),
        "truncate".to_string(),
        "use".to_string(),
        /*
            Dml statements keywords

            Dml statements CAN be a part of BATCH!
        */
        "delete".to_string(),
        "insert".to_string(),
        "update".to_string(),
        /*
            Unsupported keywords

            Those which has no documentation,
            or syntax synopsis described on DataStax HCD
        */
        "restrcit".to_string(),
        "unrestrict".to_string(),
    ]
});

/*
    Lower case CQL types

    Separate list of CQL tyes used mostly for
    formatting logic e.g

    if CQL_TYPES_LWC.contains(&split[index]) {
        ...
    }

    Where &split is a lower case split of the line &&
    &splitp[index] is a candidate for a type

    Note!

    CQL types ARE affected by styled formatting!
*/
pub static CQL_TYPES_LWC: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "ascii".to_string(),
        "bigint".to_string(),
        "blob".to_string(),
        "boolean".to_string(),
        "counter".to_string(),
        "date".to_string(),
        "decimal".to_string(),
        "double".to_string(),
        "float".to_string(),
        "frozen".to_string(),
        "inet".to_string(),
        "int".to_string(),
        "list".to_string(),
        "map".to_string(),
        "set".to_string(),
        "smallint".to_string(),
        "text".to_string(),
        "time".to_string(),
        "timestamp".to_string(),
        "timeuuid".to_string(),
        "tinyint".to_string(),
        "tuple".to_string(),
        "uuid".to_string(),
        "varchar".to_string(),
        "varint".to_string(),
    ]
});

/*
    List of keywords represented as String
*/
pub static KEYWORDS_STRINGS_LWC: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "use".to_string(),
        "alter".to_string(),
        "create".to_string(),
        "keyspace".to_string(),
        "table".to_string(),
        "with".to_string(),
        "where".to_string(),
        "if".to_string(),
        "and".to_string(),
        "set".to_string(),
        "in".to_string(),
        "to".to_string(),
        "from".to_string(),
        "using".to_string(),
        "timestamp".to_string(),
        "ttl".to_string(),
        "exists".to_string(),
        "not".to_string(),
        "type".to_string(),
        "view".to_string(),
        "materialized".to_string(),
        "replication".to_string(),
        "durable_writes".to_string(),
        "batch".to_string(),
        "apply".to_string(),
        "begin".to_string(),
        "unlogged".to_string(),
        "logged".to_string(),
        "counter".to_string(),
        "truncate".to_string(),
        "insert".to_string(),
        "into".to_string(),
        "values".to_string(),
        "update".to_string(),
        "delete".to_string(),
        "role".to_string(),
        "password".to_string(),
        "user".to_string(),
        "superuser".to_string(),
        "nosuperuser".to_string(),
        "add".to_string(),
        "drop".to_string(),
        "rename".to_string(),
        "compact".to_string(),
        "storage".to_string(),
        "contains".to_string(),
        "key".to_string(),
        "login".to_string(),
        "options".to_string(),
        "or".to_string(),
        "replace".to_string(),
        "sfunc".to_string(),
        "stype".to_string(),
        "finalfunc".to_string(),
        "initcond".to_string(),
        "language".to_string(),
        "input".to_string(),
        "on".to_string(),
        "function".to_string(),
        "called".to_string(),
        "returns".to_string(),
        "filtering".to_string(),
        "distinct".to_string(),
        "as".to_string(),
        "keys".to_string(),
        "group".to_string(),
        "by".to_string(),
        "json".to_string(),
        "null".to_string(),
        "custom".to_string(),
        "aggregate".to_string(),
        "all".to_string(),
        "allow".to_string(),
        "asc".to_string(),
        "authorize".to_string(),
        "clustering".to_string(),
        "desc".to_string(),
        "describe".to_string(),
        "entries".to_string(),
        "full".to_string(),
        "grant".to_string(),
        "index".to_string(),
        "keyspaces".to_string(),
        "limit".to_string(),
        "modify".to_string(),
        "norecursive".to_string(),
        "of".to_string(),
        "order".to_string(),
        "partition".to_string(),
        "per".to_string(),
        "permissions".to_string(),
        "primary".to_string(),
        "revoke".to_string(),
        "select".to_string(),
        "users".to_string(),
        "commit".to_string(),
        "search".to_string(),
        "roles".to_string(),
        "deterministic".to_string(),
        "monotonic".to_string(),
        "java".to_string(),
        "javascript".to_string(),
        "is".to_string(),
        "hashed".to_string(),
        "access".to_string(),
        "datacenters".to_string(),
        "cidrs".to_string(),
        "columns".to_string(),
        "profiles".to_string(),
        "config".to_string(),
        "rows".to_string(),
        "functions".to_string(),
        "mbeans".to_string(),
        "mbean".to_string(),
        "pattern".to_string(),
        "execute".to_string(),
        "proxy".to_string(),
        "id".to_string(),
        "like".to_string(),
        "ann".to_string(),
        "offset".to_string(),
        "list".to_string(),
        "max".to_string(),
        "min".to_string(),
        "sum".to_string(),
        "avg".to_string(),
        "token".to_string(),
        "writetime".to_string(),
        "count".to_string(),
        "infinity".to_string(),
        "nan".to_string(),
        "static".to_string(),
        "any".to_string(),
        "having".to_string(),
        "consistency".to_string(),
        "level".to_string(),
        "one".to_string(),
        "two".to_string(),
        "three".to_string(),
        "quorum".to_string(),
        "local_one".to_string(),
        "local_quorum".to_string(),
        "each_quorum".to_string(),
    ]
});

// XAR-1 2.7k lines of pure KEYWORDS だよ　www
// XAR-1 2.7k lines of pure KEYWORDS that are working だよ :D
pub static KEYWORDS: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    vec![
        // USE
        CompletionItem {
            label: "USE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case USE keyword".to_string()),
            documentation: Some(Documentation::String("USE keyword".to_string())),
            insert_text: Some(r#"USE "$0";"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "use".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case use keyword".to_string()),
            documentation: Some(Documentation::String("USE keyword".to_string())),
            insert_text: Some(r#"use "$0";"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ALTER
        CompletionItem {
            label: "ALTER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ALTER keyword".to_string()),
            documentation: Some(Documentation::String("ALTER keyword".to_string())),
            insert_text: Some(r#"ALTER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "alter".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case alter keyword".to_string()),
            documentation: Some(Documentation::String("ALTER keyword".to_string())),
            insert_text: Some(r#"alter $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CREATE
        CompletionItem {
            label: "CREATE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CREATE keyword".to_string()),
            documentation: Some(Documentation::String("CREATE keyword".to_string())),
            insert_text: Some(r#"CREATE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "create".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case create keyword".to_string()),
            documentation: Some(Documentation::String("CREATE keyword".to_string())),
            insert_text: Some(r#"create $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // KEYSPACE
        CompletionItem {
            label: "KEYSPACE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case KEYSPACE keyword".to_string()),
            documentation: Some(Documentation::String("KEYSPACE keyword".to_string())),
            insert_text: Some(r#"KEYSPACE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "keyspace".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case keyspace keyword".to_string()),
            documentation: Some(Documentation::String("KEYSPACE keyword".to_string())),
            insert_text: Some(r#"keyspace $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TABLE
        CompletionItem {
            label: "TABLE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TABLE keyword".to_string()),
            documentation: Some(Documentation::String("TABLE keyword".to_string())),
            insert_text: Some(r#"TABLE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "table".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case table keyword".to_string()),
            documentation: Some(Documentation::String("TABLE keyword".to_string())),
            insert_text: Some(r#"table $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // WITH
        CompletionItem {
            label: "WITH".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case WITH keyword".to_string()),
            documentation: Some(Documentation::String("WITH keyword".to_string())),
            insert_text: Some(r#"WITH $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "with".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case with keyword".to_string()),
            documentation: Some(Documentation::String("WITH keyword".to_string())),
            insert_text: Some(r#"with $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // WHERE
        CompletionItem {
            label: "WHERE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case WHERE keyword".to_string()),
            documentation: Some(Documentation::String("WHERE keyword".to_string())),
            insert_text: Some(r#"WHERE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "where".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case where keyword".to_string()),
            documentation: Some(Documentation::String("WHERE keyword".to_string())),
            insert_text: Some(r#"where $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // IF
        CompletionItem {
            label: "IF".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case IF keyword".to_string()),
            documentation: Some(Documentation::String("IF keyword".to_string())),
            insert_text: Some(r#"IF $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "if".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case if keyword".to_string()),
            documentation: Some(Documentation::String("IF keyword".to_string())),
            insert_text: Some(r#"if $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // AND
        CompletionItem {
            label: "AND".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case AND keyword".to_string()),
            documentation: Some(Documentation::String("AND keyword".to_string())),
            insert_text: Some(r#"AND $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "and".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case and keyword".to_string()),
            documentation: Some(Documentation::String("AND keyword".to_string())),
            insert_text: Some(r#"and $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SET
        CompletionItem {
            label: "SET".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case SET keyword".to_string()),
            documentation: Some(Documentation::String("SET keyword".to_string())),
            insert_text: Some(r#"SET $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "set".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case set keyword".to_string()),
            documentation: Some(Documentation::String("SET keyword".to_string())),
            insert_text: Some(r#"set $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // IN
        CompletionItem {
            label: "IN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case IN keyword".to_string()),
            documentation: Some(Documentation::String("IN keyword".to_string())),
            insert_text: Some(r#"IN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "in".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case in keyword".to_string()),
            documentation: Some(Documentation::String("IN keyword".to_string())),
            insert_text: Some(r#"in $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TO
        CompletionItem {
            label: "TO".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TO keyword".to_string()),
            documentation: Some(Documentation::String("TO keyword".to_string())),
            insert_text: Some(r#"TO $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "to".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case to keyword".to_string()),
            documentation: Some(Documentation::String("TO keyword".to_string())),
            insert_text: Some(r#"to $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FROM
        CompletionItem {
            label: "FROM".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case FROM keyword".to_string()),
            documentation: Some(Documentation::String("FROM keyword".to_string())),
            insert_text: Some(r#"FROM $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "from".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case from keyword".to_string()),
            documentation: Some(Documentation::String("FROM keyword".to_string())),
            insert_text: Some(r#"from $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // USING
        CompletionItem {
            label: "USING".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case USING keyword".to_string()),
            documentation: Some(Documentation::String("USING keyword".to_string())),
            insert_text: Some(r#"USING $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "using".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case using keyword".to_string()),
            documentation: Some(Documentation::String("USING keyword".to_string())),
            insert_text: Some(r#"using $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TIMESTAMP
        CompletionItem {
            label: "TIMESTAMP".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TIMESTAMP keyword".to_string()),
            documentation: Some(Documentation::String("TIMESTAMP keyword".to_string())),
            insert_text: Some(r#"TIMESTAMP $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "timestamp".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case timestamp keyword".to_string()),
            documentation: Some(Documentation::String("TIMESTAMP keyword".to_string())),
            insert_text: Some(r#"timestamp $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TTL
        CompletionItem {
            label: "TTL".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TTL keyword".to_string()),
            documentation: Some(Documentation::String("TTL keyword".to_string())),
            insert_text: Some(r#"TTL $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "ttl".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case ttl keyword".to_string()),
            documentation: Some(Documentation::String("TTL keyword".to_string())),
            insert_text: Some(r#"ttl $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // EXISTS
        CompletionItem {
            label: "EXISTS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case EXISTS keyword".to_string()),
            documentation: Some(Documentation::String("EXISTS keyword".to_string())),
            insert_text: Some(r#"EXISTS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "exists".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case exists keyword".to_string()),
            documentation: Some(Documentation::String("EXISTS keyword".to_string())),
            insert_text: Some(r#"exists $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // NOT
        CompletionItem {
            label: "NOT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case NOT keyword".to_string()),
            documentation: Some(Documentation::String("NOT keyword".to_string())),
            insert_text: Some(r#"NOT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "not".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case not keyword".to_string()),
            documentation: Some(Documentation::String("NOT keyword".to_string())),
            insert_text: Some(r#"not $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TYPE
        CompletionItem {
            label: "TYPE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TYPE keyword".to_string()),
            documentation: Some(Documentation::String("TYPE keyword".to_string())),
            insert_text: Some(r#"TYPE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "type".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case type keyword".to_string()),
            documentation: Some(Documentation::String("TYPE keyword".to_string())),
            insert_text: Some(r#"type $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // VIEW
        CompletionItem {
            label: "VIEW".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case VIEW keyword".to_string()),
            documentation: Some(Documentation::String("VIEW keyword".to_string())),
            insert_text: Some(r#"VIEW $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "view".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case view keyword".to_string()),
            documentation: Some(Documentation::String("VIEW keyword".to_string())),
            insert_text: Some(r#"view $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MATERIALIZED
        CompletionItem {
            label: "MATERIALIZED".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MATERIALIZED keyword".to_string()),
            documentation: Some(Documentation::String("MATERIALIZED keyword".to_string())),
            insert_text: Some(r#"MATERIALIZED $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "materialized".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case materialized keyword".to_string()),
            documentation: Some(Documentation::String("MATERIALIZED keyword".to_string())),
            insert_text: Some(r#"materialized $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // REPLICATION
        CompletionItem {
            label: "REPLICATION".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case REPLICATION keyword".to_string()),
            documentation: Some(Documentation::String("REPLICATION keyword".to_string())),
            insert_text: Some(r#"REPLICATION $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "replication".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case replication keyword".to_string()),
            documentation: Some(Documentation::String("REPLICATION keyword".to_string())),
            insert_text: Some(r#"replication $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DURABLE_WRITES
        CompletionItem {
            label: "DURABLE_WRITES".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DURABLE_WRITES keyword".to_string()),
            documentation: Some(Documentation::String("DURABLE_WRITES keyword".to_string())),
            insert_text: Some(r#"DURABLE_WRITES $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "durable_writes".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case durable_writes keyword".to_string()),
            documentation: Some(Documentation::String("DURABLE_WRITES keyword".to_string())),
            insert_text: Some(r#"durable_writes $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // BATCH
        CompletionItem {
            label: "BATCH".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case BATCH keyword".to_string()),
            documentation: Some(Documentation::String("BATCH keyword".to_string())),
            insert_text: Some(r#"BATCH $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "batch".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case batch keyword".to_string()),
            documentation: Some(Documentation::String("BATCH keyword".to_string())),
            insert_text: Some(r#"batch $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // APPLY
        CompletionItem {
            label: "APPLY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case APPLY keyword".to_string()),
            documentation: Some(Documentation::String("APPLY keyword".to_string())),
            insert_text: Some(r#"APPLY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "apply".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case apply keyword".to_string()),
            documentation: Some(Documentation::String("APPLY keyword".to_string())),
            insert_text: Some(r#"apply $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // BEGIN
        CompletionItem {
            label: "BEGIN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case BEGIN keyword".to_string()),
            documentation: Some(Documentation::String("BEGIN keyword".to_string())),
            insert_text: Some(r#"BEGIN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "begin".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case begin keyword".to_string()),
            documentation: Some(Documentation::String("BEGIN keyword".to_string())),
            insert_text: Some(r#"begin $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // UNLOGGED
        CompletionItem {
            label: "UNLOGGED".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case UNLOGGED keyword".to_string()),
            documentation: Some(Documentation::String("UNLOGGED keyword".to_string())),
            insert_text: Some(r#"UNLOGGED $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "unlogged".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case unlogged keyword".to_string()),
            documentation: Some(Documentation::String("UNLOGGED keyword".to_string())),
            insert_text: Some(r#"unlogged $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LOGGED
        CompletionItem {
            label: "LOGGED".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LOGGED keyword".to_string()),
            documentation: Some(Documentation::String("LOGGED keyword".to_string())),
            insert_text: Some(r#"LOGGED $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "logged".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case logged keyword".to_string()),
            documentation: Some(Documentation::String("LOGGED keyword".to_string())),
            insert_text: Some(r#"logged $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // COUNTER
        CompletionItem {
            label: "COUNTER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case COUNTER keyword".to_string()),
            documentation: Some(Documentation::String("COUNTER keyword".to_string())),
            insert_text: Some(r#"COUNTER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "counter".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case counter keyword".to_string()),
            documentation: Some(Documentation::String("COUNTER keyword".to_string())),
            insert_text: Some(r#"counter $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TRUNCATE
        CompletionItem {
            label: "TRUNCATE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TRUNCATE keyword".to_string()),
            documentation: Some(Documentation::String("TRUNCATE keyword".to_string())),
            insert_text: Some(r#"TRUNCATE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "truncate".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case truncate keyword".to_string()),
            documentation: Some(Documentation::String("TRUNCATE keyword".to_string())),
            insert_text: Some(r#"truncate $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INSERT
        CompletionItem {
            label: "INSERT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case INSERT keyword".to_string()),
            documentation: Some(Documentation::String("INSERT keyword".to_string())),
            insert_text: Some(r#"INSERT INTO $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "insert".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case insert keyword".to_string()),
            documentation: Some(Documentation::String("INSERT keyword".to_string())),
            insert_text: Some(r#"insert into $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INTO
        CompletionItem {
            label: "INTO".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case INTO keyword".to_string()),
            documentation: Some(Documentation::String("INTO keyword".to_string())),
            insert_text: Some(r#"INTO $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "into".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case into keyword".to_string()),
            documentation: Some(Documentation::String("INTO keyword".to_string())),
            insert_text: Some(r#"into $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // VALUES
        CompletionItem {
            label: "VALUES".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case VALUES keyword".to_string()),
            documentation: Some(Documentation::String("VALUES keyword".to_string())),
            insert_text: Some(r#"VALUES ($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "values".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case values keyword".to_string()),
            documentation: Some(Documentation::String("VALUES keyword".to_string())),
            insert_text: Some(r#"values ($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // UPDATE
        CompletionItem {
            label: "UPDATE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case UPDATE keyword".to_string()),
            documentation: Some(Documentation::String("UPDATE keyword".to_string())),
            insert_text: Some(r#"UPDATE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "update".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case update keyword".to_string()),
            documentation: Some(Documentation::String("UPDATE keyword".to_string())),
            insert_text: Some(r#"update $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DELETE
        CompletionItem {
            label: "DELETE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DELETE keyword".to_string()),
            documentation: Some(Documentation::String("DELETE keyword".to_string())),
            insert_text: Some(r#"DELETE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "delete".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case delete keyword".to_string()),
            documentation: Some(Documentation::String("DELETE keyword".to_string())),
            insert_text: Some(r#"delete $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ROLE
        CompletionItem {
            label: "ROLE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ROLE keyword".to_string()),
            documentation: Some(Documentation::String("ROLE keyword".to_string())),
            insert_text: Some(r#"ROLE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "role".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case role keyword".to_string()),
            documentation: Some(Documentation::String("ROLE keyword".to_string())),
            insert_text: Some(r#"role $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PASSWORD
        CompletionItem {
            label: "PASSWORD".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PASSWORD keyword".to_string()),
            documentation: Some(Documentation::String("PASSWORD keyword".to_string())),
            insert_text: Some(r#"PASSWORD $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "password".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case password keyword".to_string()),
            documentation: Some(Documentation::String("PASSWORD keyword".to_string())),
            insert_text: Some(r#"password $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // USER
        CompletionItem {
            label: "USER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case USER keyword".to_string()),
            documentation: Some(Documentation::String("USER keyword".to_string())),
            insert_text: Some(r#"USER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "user".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case user keyword".to_string()),
            documentation: Some(Documentation::String("USER keyword".to_string())),
            insert_text: Some(r#"user $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SUPERUSER
        CompletionItem {
            label: "SUPERUSER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case SUPERUSER keyword".to_string()),
            documentation: Some(Documentation::String("SUPERUSER keyword".to_string())),
            insert_text: Some(r#"SUPERUSER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "superuser".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case superuser keyword".to_string()),
            documentation: Some(Documentation::String("SUPERUSER keyword".to_string())),
            insert_text: Some(r#"superuser $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // NOSUPERUSER
        CompletionItem {
            label: "NOSUPERUSER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case NOSUPERUSER keyword".to_string()),
            documentation: Some(Documentation::String("NOSUPERUSER keyword".to_string())),
            insert_text: Some(r#"NOSUPERUSER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "nosuperuser".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case nosuperuser keyword".to_string()),
            documentation: Some(Documentation::String("NOSUPERUSER keyword".to_string())),
            insert_text: Some(r#"nosuperuser $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ADD
        CompletionItem {
            label: "ADD".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ADD keyword".to_string()),
            documentation: Some(Documentation::String("ADD keyword".to_string())),
            insert_text: Some(r#"ADD $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "add".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case add keyword".to_string()),
            documentation: Some(Documentation::String("ADD keyword".to_string())),
            insert_text: Some(r#"add $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DROP
        CompletionItem {
            label: "DROP".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DROP keyword".to_string()),
            documentation: Some(Documentation::String("DROP keyword".to_string())),
            insert_text: Some(r#"DROP $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "drop".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case drop keyword".to_string()),
            documentation: Some(Documentation::String("DROP keyword".to_string())),
            insert_text: Some(r#"drop $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // RENAME
        CompletionItem {
            label: "RENAME".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case RENAME keyword".to_string()),
            documentation: Some(Documentation::String("RENAME keyword".to_string())),
            insert_text: Some(r#"RENAME $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "rename".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case rename keyword".to_string()),
            documentation: Some(Documentation::String("RENAME keyword".to_string())),
            insert_text: Some(r#"rename $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // COMPACT
        CompletionItem {
            label: "COMPACT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case COMPACT keyword".to_string()),
            documentation: Some(Documentation::String("COMPACT keyword".to_string())),
            insert_text: Some(r#"COMPACT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "compact".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case compact keyword".to_string()),
            documentation: Some(Documentation::String("COMPACT keyword".to_string())),
            insert_text: Some(r#"compact $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // STORAGE
        CompletionItem {
            label: "STORAGE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case STORAGE keyword".to_string()),
            documentation: Some(Documentation::String("STORAGE keyword".to_string())),
            insert_text: Some(r#"STORAGE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "storage".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case storage keyword".to_string()),
            documentation: Some(Documentation::String("STORAGE keyword".to_string())),
            insert_text: Some(r#"storage $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CONTAINS
        CompletionItem {
            label: "CONTAINS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CONTAINS keyword".to_string()),
            documentation: Some(Documentation::String("CONTAINS keyword".to_string())),
            insert_text: Some(r#"CONTAINS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "contains".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case contains keyword".to_string()),
            documentation: Some(Documentation::String("CONTAINS keyword".to_string())),
            insert_text: Some(r#"contains $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // KEY
        CompletionItem {
            label: "KEY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case KEY keyword".to_string()),
            documentation: Some(Documentation::String("KEY keyword".to_string())),
            insert_text: Some(r#"KEY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "key".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case key keyword".to_string()),
            documentation: Some(Documentation::String("KEY keyword".to_string())),
            insert_text: Some(r#"key $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LOGIN
        CompletionItem {
            label: "LOGIN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LOGIN keyword".to_string()),
            documentation: Some(Documentation::String("LOGIN keyword".to_string())),
            insert_text: Some(r#"LOGIN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "login".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case login keyword".to_string()),
            documentation: Some(Documentation::String("LOGIN keyword".to_string())),
            insert_text: Some(r#"login $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // OPTIONS
        CompletionItem {
            label: "OPTIONS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case OPTIONS keyword".to_string()),
            documentation: Some(Documentation::String("OPTIONS keyword".to_string())),
            insert_text: Some(r#"OPTIONS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "options".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case options keyword".to_string()),
            documentation: Some(Documentation::String("OPTIONS keyword".to_string())),
            insert_text: Some(r#"options $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // OR
        CompletionItem {
            label: "OR".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case OR keyword".to_string()),
            documentation: Some(Documentation::String("OR keyword".to_string())),
            insert_text: Some(r#"OR $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "or".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case or keyword".to_string()),
            documentation: Some(Documentation::String("OR keyword".to_string())),
            insert_text: Some(r#"or $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // REPLACE
        CompletionItem {
            label: "REPLACE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case REPLACE keyword".to_string()),
            documentation: Some(Documentation::String("REPLACE keyword".to_string())),
            insert_text: Some(r#"REPLACE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "replace".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case replace keyword".to_string()),
            documentation: Some(Documentation::String("REPLACE keyword".to_string())),
            insert_text: Some(r#"replace $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SFUNC
        CompletionItem {
            label: "SFUNC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case SFUNC keyword".to_string()),
            documentation: Some(Documentation::String("SFUNC keyword".to_string())),
            insert_text: Some(r#"SFUNC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "sfunc".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case sfunc keyword".to_string()),
            documentation: Some(Documentation::String("SFUNC keyword".to_string())),
            insert_text: Some(r#"sfunc $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // STYPE
        CompletionItem {
            label: "STYPE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case STYPE keyword".to_string()),
            documentation: Some(Documentation::String("STYPE keyword".to_string())),
            insert_text: Some(r#"STYPE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "stype".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case stype keyword".to_string()),
            documentation: Some(Documentation::String("STYPE keyword".to_string())),
            insert_text: Some(r#"stype $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FINALFUNC
        CompletionItem {
            label: "FINALFUNC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case FINALFUNC keyword".to_string()),
            documentation: Some(Documentation::String("FINALFUNC keyword".to_string())),
            insert_text: Some(r#"FINALFUNC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "finalfunc".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case finalfunc keyword".to_string()),
            documentation: Some(Documentation::String("FINALFUNC keyword".to_string())),
            insert_text: Some(r#"finalfunc $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INITCOND
        CompletionItem {
            label: "INITCOND".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case INITCOND keyword".to_string()),
            documentation: Some(Documentation::String("INITCOND keyword".to_string())),
            insert_text: Some(r#"INITCOND $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "initcond".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case initcond keyword".to_string()),
            documentation: Some(Documentation::String("INITCOND keyword".to_string())),
            insert_text: Some(r#"initcond $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LANGUAGE
        CompletionItem {
            label: "LANGUAGE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LANGUAGE keyword".to_string()),
            documentation: Some(Documentation::String("LANGUAGE keyword".to_string())),
            insert_text: Some(r#"LANGUAGE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "language".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case language keyword".to_string()),
            documentation: Some(Documentation::String("LANGUAGE keyword".to_string())),
            insert_text: Some(r#"language $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INPUT
        CompletionItem {
            label: "INPUT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case INPUT keyword".to_string()),
            documentation: Some(Documentation::String("INPUT keyword".to_string())),
            insert_text: Some(r#"INPUT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "input".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case input keyword".to_string()),
            documentation: Some(Documentation::String("INPUT keyword".to_string())),
            insert_text: Some(r#"input $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ON
        CompletionItem {
            label: "ON".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ON keyword".to_string()),
            documentation: Some(Documentation::String("ON keyword".to_string())),
            insert_text: Some(r#"ON $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "on".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case on keyword".to_string()),
            documentation: Some(Documentation::String("ON keyword".to_string())),
            insert_text: Some(r#"on $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FUNCTION
        CompletionItem {
            label: "FUNCTION".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case FUNCTION keyword".to_string()),
            documentation: Some(Documentation::String("FUNCTION keyword".to_string())),
            insert_text: Some(r#"FUNCTION $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "function".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case function keyword".to_string()),
            documentation: Some(Documentation::String("FUNCTION keyword".to_string())),
            insert_text: Some(r#"function $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CALLED
        CompletionItem {
            label: "CALLED".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CALLED keyword".to_string()),
            documentation: Some(Documentation::String("CALLED keyword".to_string())),
            insert_text: Some(r#"CALLED $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "called".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case called keyword".to_string()),
            documentation: Some(Documentation::String("CALLED keyword".to_string())),
            insert_text: Some(r#"called $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // RETURNS
        CompletionItem {
            label: "RETURNS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case RETURNS keyword".to_string()),
            documentation: Some(Documentation::String("RETURNS keyword".to_string())),
            insert_text: Some(r#"RETURNS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "returns".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case returns keyword".to_string()),
            documentation: Some(Documentation::String("RETURNS keyword".to_string())),
            insert_text: Some(r#"returns $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FILTERING
        CompletionItem {
            label: "FILTERING".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case FILTERING keyword".to_string()),
            documentation: Some(Documentation::String("FILTERING keyword".to_string())),
            insert_text: Some(r#"FILTERING $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "filtering".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case filtering keyword".to_string()),
            documentation: Some(Documentation::String("FILTERING keyword".to_string())),
            insert_text: Some(r#"filtering $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DISTINCT
        CompletionItem {
            label: "DISTINCT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DISTINCT keyword".to_string()),
            documentation: Some(Documentation::String("DISTINCT keyword".to_string())),
            insert_text: Some(r#"DISTINCT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "distinct".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case distinct keyword".to_string()),
            documentation: Some(Documentation::String("DISTINCT keyword".to_string())),
            insert_text: Some(r#"distinct $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // AS
        CompletionItem {
            label: "AS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case AS keyword".to_string()),
            documentation: Some(Documentation::String("AS keyword".to_string())),
            insert_text: Some(r#"AS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "as".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case as keyword".to_string()),
            documentation: Some(Documentation::String("AS keyword".to_string())),
            insert_text: Some(r#"as $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // KEYS
        CompletionItem {
            label: "KEYS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case KEYS keyword".to_string()),
            documentation: Some(Documentation::String("KEYS keyword".to_string())),
            insert_text: Some(r#"KEYS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "keys".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case keys keyword".to_string()),
            documentation: Some(Documentation::String("KEYS keyword".to_string())),
            insert_text: Some(r#"keys $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // GROUP
        CompletionItem {
            label: "GROUP".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case GROUP keyword".to_string()),
            documentation: Some(Documentation::String("GROUP keyword".to_string())),
            insert_text: Some(r#"GROUP $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "group".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case group keyword".to_string()),
            documentation: Some(Documentation::String("GROUP keyword".to_string())),
            insert_text: Some(r#"group $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // BY
        CompletionItem {
            label: "BY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case BY keyword".to_string()),
            documentation: Some(Documentation::String("BY keyword".to_string())),
            insert_text: Some(r#"BY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "by".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case by keyword".to_string()),
            documentation: Some(Documentation::String("BY keyword".to_string())),
            insert_text: Some(r#"by $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // JSON
        CompletionItem {
            label: "JSON".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case JSON keyword".to_string()),
            documentation: Some(Documentation::String("JSON keyword".to_string())),
            insert_text: Some(r#"JSON $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "json".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case json keyword".to_string()),
            documentation: Some(Documentation::String("JSON keyword".to_string())),
            insert_text: Some(r#"json $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // NULL
        CompletionItem {
            label: "NULL".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case NULL keyword".to_string()),
            documentation: Some(Documentation::String("NULL keyword".to_string())),
            insert_text: Some(r#"NULL $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "null".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case null keyword".to_string()),
            documentation: Some(Documentation::String("NULL keyword".to_string())),
            insert_text: Some(r#"null $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CUSTOM
        CompletionItem {
            label: "CUSTOM".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CUSTOM keyword".to_string()),
            documentation: Some(Documentation::String("CUSTOM keyword".to_string())),
            insert_text: Some(r#"CUSTOM $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "custom".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case custom keyword".to_string()),
            documentation: Some(Documentation::String("CUSTOM keyword".to_string())),
            insert_text: Some(r#"custom $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // AGGREGATE
        CompletionItem {
            label: "AGGREGATE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case AGGREGATE keyword".to_string()),
            documentation: Some(Documentation::String("AGGREGATE keyword".to_string())),
            insert_text: Some(r#"AGGREGATE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "aggregate".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case aggregate keyword".to_string()),
            documentation: Some(Documentation::String("AGGREGATE keyword".to_string())),
            insert_text: Some(r#"aggregate $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ALL
        CompletionItem {
            label: "ALL".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ALL keyword".to_string()),
            documentation: Some(Documentation::String("ALL keyword".to_string())),
            insert_text: Some(r#"ALL $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "all".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case all keyword".to_string()),
            documentation: Some(Documentation::String("ALL keyword".to_string())),
            insert_text: Some(r#"all $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ALLOW
        CompletionItem {
            label: "ALLOW".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ALLOW keyword".to_string()),
            documentation: Some(Documentation::String("ALLOW keyword".to_string())),
            insert_text: Some(r#"ALLOW $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "allow".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case allow keyword".to_string()),
            documentation: Some(Documentation::String("ALLOW keyword".to_string())),
            insert_text: Some(r#"allow $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ASC
        CompletionItem {
            label: "ASC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ASC keyword".to_string()),
            documentation: Some(Documentation::String("ASC keyword".to_string())),
            insert_text: Some(r#"ASC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "asc".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case asc keyword".to_string()),
            documentation: Some(Documentation::String("ASC keyword".to_string())),
            insert_text: Some(r#"asc $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // AUTHORIZE
        CompletionItem {
            label: "AUTHORIZE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case AUTHORIZE keyword".to_string()),
            documentation: Some(Documentation::String("AUTHORIZE keyword".to_string())),
            insert_text: Some(r#"AUTHORIZE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "authorize".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case authorize keyword".to_string()),
            documentation: Some(Documentation::String("AUTHORIZE keyword".to_string())),
            insert_text: Some(r#"authorize $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CLUSTERING
        CompletionItem {
            label: "CLUSTERING".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CLUSTERING keyword".to_string()),
            documentation: Some(Documentation::String("CLUSTERING keyword".to_string())),
            insert_text: Some(r#"CLUSTERING $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "clustering".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case clustering keyword".to_string()),
            documentation: Some(Documentation::String("CLUSTERING keyword".to_string())),
            insert_text: Some(r#"clustering $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DESC
        CompletionItem {
            label: "DESC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DESC keyword".to_string()),
            documentation: Some(Documentation::String("DESC keyword".to_string())),
            insert_text: Some(r#"DESC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "desc".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case desc keyword".to_string()),
            documentation: Some(Documentation::String("DESC keyword".to_string())),
            insert_text: Some(r#"desc $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DESCRIBE
        CompletionItem {
            label: "DESCRIBE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DESCRIBE keyword".to_string()),
            documentation: Some(Documentation::String("DESCRIBE keyword".to_string())),
            insert_text: Some(r#"DESCRIBE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "describe".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case describe keyword".to_string()),
            documentation: Some(Documentation::String("DESCRIBE keyword".to_string())),
            insert_text: Some(r#"describe $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ENTRIES
        CompletionItem {
            label: "ENTRIES".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ENTRIES keyword".to_string()),
            documentation: Some(Documentation::String("ENTRIES keyword".to_string())),
            insert_text: Some(r#"ENTRIES $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "entries".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case entries keyword".to_string()),
            documentation: Some(Documentation::String("ENTRIES keyword".to_string())),
            insert_text: Some(r#"entries $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FULL
        CompletionItem {
            label: "FULL".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case FULL keyword".to_string()),
            documentation: Some(Documentation::String("FULL keyword".to_string())),
            insert_text: Some(r#"FULL $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "full".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case full keyword".to_string()),
            documentation: Some(Documentation::String("FULL keyword".to_string())),
            insert_text: Some(r#"full $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // GRANT
        CompletionItem {
            label: "GRANT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case GRANT keyword".to_string()),
            documentation: Some(Documentation::String("GRANT keyword".to_string())),
            insert_text: Some(r#"GRANT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "grant".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case grant keyword".to_string()),
            documentation: Some(Documentation::String("GRANT keyword".to_string())),
            insert_text: Some(r#"grant $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INDEX
        CompletionItem {
            label: "INDEX".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case INDEX keyword".to_string()),
            documentation: Some(Documentation::String("INDEX keyword".to_string())),
            insert_text: Some(r#"INDEX $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "index".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case index keyword".to_string()),
            documentation: Some(Documentation::String("INDEX keyword".to_string())),
            insert_text: Some(r#"index $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // KEYSPACES
        CompletionItem {
            label: "KEYSPACES".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case KEYSPACES keyword".to_string()),
            documentation: Some(Documentation::String("KEYSPACES keyword".to_string())),
            insert_text: Some(r#"KEYSPACES $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "keyspaces".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case keyspaces keyword".to_string()),
            documentation: Some(Documentation::String("KEYSPACES keyword".to_string())),
            insert_text: Some(r#"keyspaces $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LIMIT
        CompletionItem {
            label: "LIMIT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LIMIT keyword".to_string()),
            documentation: Some(Documentation::String("LIMIT keyword".to_string())),
            insert_text: Some(r#"LIMIT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "limit".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case limit keyword".to_string()),
            documentation: Some(Documentation::String("LIMIT keyword".to_string())),
            insert_text: Some(r#"limit $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MODIFY
        CompletionItem {
            label: "MODIFY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MODIFY keyword".to_string()),
            documentation: Some(Documentation::String("MODIFY keyword".to_string())),
            insert_text: Some(r#"MODIFY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "modify".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case modify keyword".to_string()),
            documentation: Some(Documentation::String("MODIFY keyword".to_string())),
            insert_text: Some(r#"modify $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // NORECURSIVE
        CompletionItem {
            label: "NORECURSIVE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case NORECURSIVE keyword".to_string()),
            documentation: Some(Documentation::String("NORECURSIVE keyword".to_string())),
            insert_text: Some(r#"NORECURSIVE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "norecursive".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case norecursive keyword".to_string()),
            documentation: Some(Documentation::String("NORECURSIVE keyword".to_string())),
            insert_text: Some(r#"norecursive $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // OF
        CompletionItem {
            label: "OF".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case OF keyword".to_string()),
            documentation: Some(Documentation::String("OF keyword".to_string())),
            insert_text: Some(r#"OF $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "of".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case of keyword".to_string()),
            documentation: Some(Documentation::String("OF keyword".to_string())),
            insert_text: Some(r#"of $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ORDER
        CompletionItem {
            label: "ORDER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ORDER keyword".to_string()),
            documentation: Some(Documentation::String("ORDER keyword".to_string())),
            insert_text: Some(r#"ORDER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "order".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case order keyword".to_string()),
            documentation: Some(Documentation::String("ORDER keyword".to_string())),
            insert_text: Some(r#"order $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PARTITION
        CompletionItem {
            label: "PARTITION".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PARTITION keyword".to_string()),
            documentation: Some(Documentation::String("PARTITION keyword".to_string())),
            insert_text: Some(r#"PARTITION $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "partition".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case partition keyword".to_string()),
            documentation: Some(Documentation::String("PARTITION keyword".to_string())),
            insert_text: Some(r#"partition $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PER
        CompletionItem {
            label: "PER".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PER keyword".to_string()),
            documentation: Some(Documentation::String("PER keyword".to_string())),
            insert_text: Some(r#"PER $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "per".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case per keyword".to_string()),
            documentation: Some(Documentation::String("PER keyword".to_string())),
            insert_text: Some(r#"per $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PERMISSIONS
        CompletionItem {
            label: "PERMISSIONS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PERMISSIONS keyword".to_string()),
            documentation: Some(Documentation::String("PERMISSIONS keyword".to_string())),
            insert_text: Some(r#"PERMISSIONS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "permissions".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case permissions keyword".to_string()),
            documentation: Some(Documentation::String("PERMISSIONS keyword".to_string())),
            insert_text: Some(r#"permissions $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PRIMARY
        CompletionItem {
            label: "PRIMARY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PRIMARY keyword".to_string()),
            documentation: Some(Documentation::String("PRIMARY keyword".to_string())),
            insert_text: Some(r#"PRIMARY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "primary".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case primary keyword".to_string()),
            documentation: Some(Documentation::String("PRIMARY keyword".to_string())),
            insert_text: Some(r#"primary $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // REVOKE
        CompletionItem {
            label: "REVOKE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case REVOKE keyword".to_string()),
            documentation: Some(Documentation::String("REVOKE keyword".to_string())),
            insert_text: Some(r#"REVOKE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "revoke".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case revoke keyword".to_string()),
            documentation: Some(Documentation::String("REVOKE keyword".to_string())),
            insert_text: Some(r#"revoke $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SELECT
        CompletionItem {
            label: "SELECT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case SELECT keyword".to_string()),
            documentation: Some(Documentation::String("SELECT keyword".to_string())),
            insert_text: Some(r#"SELECT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "select".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case select keyword".to_string()),
            documentation: Some(Documentation::String("SELECT keyword".to_string())),
            insert_text: Some(r#"select $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // USERS
        CompletionItem {
            label: "USERS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case USERS keyword".to_string()),
            documentation: Some(Documentation::String("USERS keyword".to_string())),
            insert_text: Some(r#"USERS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "users".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case users keyword".to_string()),
            documentation: Some(Documentation::String("USERS keyword".to_string())),
            insert_text: Some(r#"users $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // COMMIT
        CompletionItem {
            label: "COMMIT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case COMMIT keyword".to_string()),
            documentation: Some(Documentation::String("COMMIT keyword".to_string())),
            insert_text: Some(r#"COMMIT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "commit".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case commit keyword".to_string()),
            documentation: Some(Documentation::String("COMMIT keyword".to_string())),
            insert_text: Some(r#"commit $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SEARCH
        CompletionItem {
            label: "SEARCH".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case SEARCH keyword".to_string()),
            documentation: Some(Documentation::String("SEARCH keyword".to_string())),
            insert_text: Some(r#"SEARCH $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "search".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case search keyword".to_string()),
            documentation: Some(Documentation::String("SEARCH keyword".to_string())),
            insert_text: Some(r#"search $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ROLES
        CompletionItem {
            label: "ROLES".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ROLES keyword".to_string()),
            documentation: Some(Documentation::String("ROLES keyword".to_string())),
            insert_text: Some(r#"ROLES $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "roles".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case roles keyword".to_string()),
            documentation: Some(Documentation::String("ROLES keyword".to_string())),
            insert_text: Some(r#"roles $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DETERMINISTIC
        CompletionItem {
            label: "DETERMINISTIC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DETERMINISTIC keyword".to_string()),
            documentation: Some(Documentation::String("DETERMINISTIC keyword".to_string())),
            insert_text: Some(r#"DETERMINISTIC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "deterministic".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case deterministic keyword".to_string()),
            documentation: Some(Documentation::String("DETERMINISTIC keyword".to_string())),
            insert_text: Some(r#"deterministic $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MONOTONIC
        CompletionItem {
            label: "MONOTONIC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MONOTONIC keyword".to_string()),
            documentation: Some(Documentation::String("MONOTONIC keyword".to_string())),
            insert_text: Some(r#"MONOTONIC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "monotonic".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case monotonic keyword".to_string()),
            documentation: Some(Documentation::String("MONOTONIC keyword".to_string())),
            insert_text: Some(r#"monotonic $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // JAVA
        CompletionItem {
            label: "JAVA".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case JAVA keyword".to_string()),
            documentation: Some(Documentation::String("JAVA keyword".to_string())),
            insert_text: Some(r#"JAVA $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "java".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case java keyword".to_string()),
            documentation: Some(Documentation::String("JAVA keyword".to_string())),
            insert_text: Some(r#"java $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // JAVASCRIPT
        CompletionItem {
            label: "JAVASCRIPT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case JAVASCRIPT keyword".to_string()),
            documentation: Some(Documentation::String("JAVASCRIPT keyword".to_string())),
            insert_text: Some(r#"JAVASCRIPT $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "javascript".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case javascript keyword".to_string()),
            documentation: Some(Documentation::String("JAVASCRIPT keyword".to_string())),
            insert_text: Some(r#"javascript $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // IS
        CompletionItem {
            label: "IS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case IS keyword".to_string()),
            documentation: Some(Documentation::String("IS keyword".to_string())),
            insert_text: Some(r#"IS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "is".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case is keyword".to_string()),
            documentation: Some(Documentation::String("IS keyword".to_string())),
            insert_text: Some(r#"is $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // HASHED
        CompletionItem {
            label: "HASHED".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case HASHED keyword".to_string()),
            documentation: Some(Documentation::String("HASHED keyword".to_string())),
            insert_text: Some(r#"HASHED $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "hashed".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case hashed keyword".to_string()),
            documentation: Some(Documentation::String("HASHED keyword".to_string())),
            insert_text: Some(r#"hashed $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ACCESS
        CompletionItem {
            label: "ACCESS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ACCESS keyword".to_string()),
            documentation: Some(Documentation::String("ACCESS keyword".to_string())),
            insert_text: Some(r#"ACCESS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "access".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case access keyword".to_string()),
            documentation: Some(Documentation::String("ACCESS keyword".to_string())),
            insert_text: Some(r#"access $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DATACENTERS
        CompletionItem {
            label: "DATACENTERS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case DATACENTERS keyword".to_string()),
            documentation: Some(Documentation::String("DATACENTERS keyword".to_string())),
            insert_text: Some(r#"DATACENTERS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "datacenters".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case datacenters keyword".to_string()),
            documentation: Some(Documentation::String("DATACENTERS keyword".to_string())),
            insert_text: Some(r#"datacenters $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CIDRS
        CompletionItem {
            label: "CIDRS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CIDRS keyword".to_string()),
            documentation: Some(Documentation::String("CIDRS keyword".to_string())),
            insert_text: Some(r#"CIDRS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "cidrs".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case cidrs keyword".to_string()),
            documentation: Some(Documentation::String("CIDRS keyword".to_string())),
            insert_text: Some(r#"cidrs $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // COLUMNS
        CompletionItem {
            label: "COLUMNS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case COLUMNS keyword".to_string()),
            documentation: Some(Documentation::String("COLUMNS keyword".to_string())),
            insert_text: Some(r#"COLUMNS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "columns".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case columns keyword".to_string()),
            documentation: Some(Documentation::String("COLUMNS keyword".to_string())),
            insert_text: Some(r#"columns $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PROFILES
        CompletionItem {
            label: "PROFILES".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PROFILES keyword".to_string()),
            documentation: Some(Documentation::String("PROFILES keyword".to_string())),
            insert_text: Some(r#"PROFILES $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "profiles".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case profiles keyword".to_string()),
            documentation: Some(Documentation::String("PROFILES keyword".to_string())),
            insert_text: Some(r#"profiles $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CONFIG
        CompletionItem {
            label: "CONFIG".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CONFIG keyword".to_string()),
            documentation: Some(Documentation::String("CONFIG keyword".to_string())),
            insert_text: Some(r#"CONFIG $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "config".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case config keyword".to_string()),
            documentation: Some(Documentation::String("CONFIG keyword".to_string())),
            insert_text: Some(r#"config $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ROWS
        CompletionItem {
            label: "ROWS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ROWS keyword".to_string()),
            documentation: Some(Documentation::String("ROWS keyword".to_string())),
            insert_text: Some(r#"ROWS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "rows".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case rows keyword".to_string()),
            documentation: Some(Documentation::String("ROWS keyword".to_string())),
            insert_text: Some(r#"rows $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FUNCTIONS
        CompletionItem {
            label: "FUNCTIONS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case FUNCTIONS keyword".to_string()),
            documentation: Some(Documentation::String("FUNCTIONS keyword".to_string())),
            insert_text: Some(r#"FUNCTIONS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "functions".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case functions keyword".to_string()),
            documentation: Some(Documentation::String("FUNCTIONS keyword".to_string())),
            insert_text: Some(r#"functions $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MBEANS
        CompletionItem {
            label: "MBEANS".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MBEANS keyword".to_string()),
            documentation: Some(Documentation::String("MBEANS keyword".to_string())),
            insert_text: Some(r#"MBEANS $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "mbeans".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case mbeans keyword".to_string()),
            documentation: Some(Documentation::String("MBEANS keyword".to_string())),
            insert_text: Some(r#"mbeans $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MBEAN
        CompletionItem {
            label: "MBEAN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MBEAN keyword".to_string()),
            documentation: Some(Documentation::String("MBEAN keyword".to_string())),
            insert_text: Some(r#"MBEAN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "mbean".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case mbean keyword".to_string()),
            documentation: Some(Documentation::String("MBEAN keyword".to_string())),
            insert_text: Some(r#"mbean $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PATTERN
        CompletionItem {
            label: "PATTERN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PATTERN keyword".to_string()),
            documentation: Some(Documentation::String("PATTERN keyword".to_string())),
            insert_text: Some(r#"PATTERN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "pattern".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case pattern keyword".to_string()),
            documentation: Some(Documentation::String("PATTERN keyword".to_string())),
            insert_text: Some(r#"pattern $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // EXECUTE
        CompletionItem {
            label: "EXECUTE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case EXECUTE keyword".to_string()),
            documentation: Some(Documentation::String("EXECUTE keyword".to_string())),
            insert_text: Some(r#"EXECUTE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "execute".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case execute keyword".to_string()),
            documentation: Some(Documentation::String("EXECUTE keyword".to_string())),
            insert_text: Some(r#"execute $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // PROXY
        CompletionItem {
            label: "PROXY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case PROXY keyword".to_string()),
            documentation: Some(Documentation::String("PROXY keyword".to_string())),
            insert_text: Some(r#"PROXY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "proxy".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case proxy keyword".to_string()),
            documentation: Some(Documentation::String("PROXY keyword".to_string())),
            insert_text: Some(r#"proxy $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ID
        CompletionItem {
            label: "ID".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ID keyword".to_string()),
            documentation: Some(Documentation::String("ID keyword".to_string())),
            insert_text: Some(r#"ID $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "id".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case id keyword".to_string()),
            documentation: Some(Documentation::String("ID keyword".to_string())),
            insert_text: Some(r#"id $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LIKE
        CompletionItem {
            label: "LIKE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LIKE keyword".to_string()),
            documentation: Some(Documentation::String("LIKE keyword".to_string())),
            insert_text: Some(r#"LIKE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "like".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case like keyword".to_string()),
            documentation: Some(Documentation::String("LIKE keyword".to_string())),
            insert_text: Some(r#"like $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ANN
        CompletionItem {
            label: "ANN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ANN keyword".to_string()),
            documentation: Some(Documentation::String("ANN keyword".to_string())),
            insert_text: Some(r#"ANN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "ann".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case ann keyword".to_string()),
            documentation: Some(Documentation::String("ANN keyword".to_string())),
            insert_text: Some(r#"ann $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // OFFSET
        CompletionItem {
            label: "OFFSET".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case OFFSET keyword".to_string()),
            documentation: Some(Documentation::String("OFFSET keyword".to_string())),
            insert_text: Some(r#"OFFSET $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "offset".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case offset keyword".to_string()),
            documentation: Some(Documentation::String("OFFSET keyword".to_string())),
            insert_text: Some(r#"offset $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LIST
        CompletionItem {
            label: "LIST".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LIST keyword".to_string()),
            documentation: Some(Documentation::String("LIST keyword".to_string())),
            insert_text: Some(r#"LIST $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "list".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case list keyword".to_string()),
            documentation: Some(Documentation::String("LIST keyword".to_string())),
            insert_text: Some(r#"list $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MAX
        CompletionItem {
            label: "MAX".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MAX keyword".to_string()),
            documentation: Some(Documentation::String("MAX keyword".to_string())),
            insert_text: Some(r#"MAX($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "max".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case max keyword".to_string()),
            documentation: Some(Documentation::String("MAX keyword".to_string())),
            insert_text: Some(r#"max($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MIN
        CompletionItem {
            label: "MIN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case MIN keyword".to_string()),
            documentation: Some(Documentation::String("MIN keyword".to_string())),
            insert_text: Some(r#"MIN($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "min".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case min keyword".to_string()),
            documentation: Some(Documentation::String("MIN keyword".to_string())),
            insert_text: Some(r#"min($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SUM
        CompletionItem {
            label: "SUM".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case SUM keyword".to_string()),
            documentation: Some(Documentation::String("SUM keyword".to_string())),
            insert_text: Some(r#"SUM($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "sum".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case sum keyword".to_string()),
            documentation: Some(Documentation::String("SUM keyword".to_string())),
            insert_text: Some(r#"sum($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // AVG
        CompletionItem {
            label: "AVG".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case AVG keyword".to_string()),
            documentation: Some(Documentation::String("AVG keyword".to_string())),
            insert_text: Some(r#"AVG($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "avg".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case avg keyword".to_string()),
            documentation: Some(Documentation::String("AVG keyword".to_string())),
            insert_text: Some(r#"avg($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TOKEN
        CompletionItem {
            label: "TOKEN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TOKEN keyword".to_string()),
            documentation: Some(Documentation::String("TOKEN keyword".to_string())),
            insert_text: Some(r#"TOKEN($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "token".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case token keyword".to_string()),
            documentation: Some(Documentation::String("TOKEN keyword".to_string())),
            insert_text: Some(r#"token($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // WRITETIME
        CompletionItem {
            label: "WRITETIME".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case WRITETIME keyword".to_string()),
            documentation: Some(Documentation::String("WRITETIME keyword".to_string())),
            insert_text: Some(r#"WRITETIME($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "writetime".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case writetime keyword".to_string()),
            documentation: Some(Documentation::String("WRITETIME keyword".to_string())),
            insert_text: Some(r#"writetime($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // COUNT
        CompletionItem {
            label: "COUNT".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case COUNT keyword".to_string()),
            documentation: Some(Documentation::String("COUNT keyword".to_string())),
            insert_text: Some(r#"COUNT($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "count".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case count keyword".to_string()),
            documentation: Some(Documentation::String("COUNT keyword".to_string())),
            insert_text: Some(r#"count($0)"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INFINITY
        CompletionItem {
            label: "INFINITY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case INFINITY keyword".to_string()),
            documentation: Some(Documentation::String("INFINITY keyword".to_string())),
            insert_text: Some(r#"INFINITY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "infinity".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case infinity keyword".to_string()),
            documentation: Some(Documentation::String("INFINITY keyword".to_string())),
            insert_text: Some(r#"infinity $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // NAN
        CompletionItem {
            label: "NAN".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case NAN keyword".to_string()),
            documentation: Some(Documentation::String("NAN keyword".to_string())),
            insert_text: Some(r#"NAN $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "nan".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case nan keyword".to_string()),
            documentation: Some(Documentation::String("NAN keyword".to_string())),
            insert_text: Some(r#"nan $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // STATIC
        CompletionItem {
            label: "STATIC".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case STATIC keyword".to_string()),
            documentation: Some(Documentation::String("STATIC keyword".to_string())),
            insert_text: Some(r#"STATIC $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "static".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case static keyword".to_string()),
            documentation: Some(Documentation::String("STATIC keyword".to_string())),
            insert_text: Some(r#"static $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ANY
        CompletionItem {
            label: "ANY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ANY keyword".to_string()),
            documentation: Some(Documentation::String("ANY keyword".to_string())),
            insert_text: Some(r#"ANY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "any".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case any keyword".to_string()),
            documentation: Some(Documentation::String("ANY keyword".to_string())),
            insert_text: Some(r#"any $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // HAVING
        CompletionItem {
            label: "HAVING".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case HAVING keyword".to_string()),
            documentation: Some(Documentation::String("HAVING keyword".to_string())),
            insert_text: Some(r#"HAVING $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "having".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case having keyword".to_string()),
            documentation: Some(Documentation::String("HAVING keyword".to_string())),
            insert_text: Some(r#"having $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // CONSISTENCY
        CompletionItem {
            label: "CONSISTENCY".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case CONSISTENCY keyword".to_string()),
            documentation: Some(Documentation::String("CONSISTENCY keyword".to_string())),
            insert_text: Some(r#"CONSISTENCY $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "consistency".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case consistency keyword".to_string()),
            documentation: Some(Documentation::String("CONSISTENCY keyword".to_string())),
            insert_text: Some(r#"consistency $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LEVEL
        CompletionItem {
            label: "LEVEL".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LEVEL keyword".to_string()),
            documentation: Some(Documentation::String("LEVEL keyword".to_string())),
            insert_text: Some(r#"LEVEL $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "level".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case level keyword".to_string()),
            documentation: Some(Documentation::String("LEVEL keyword".to_string())),
            insert_text: Some(r#"level $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // ONE
        CompletionItem {
            label: "ONE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case ONE keyword".to_string()),
            documentation: Some(Documentation::String("ONE keyword".to_string())),
            insert_text: Some(r#"ONE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "one".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case one keyword".to_string()),
            documentation: Some(Documentation::String("ONE keyword".to_string())),
            insert_text: Some(r#"one $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TWO
        CompletionItem {
            label: "TWO".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case TWO keyword".to_string()),
            documentation: Some(Documentation::String("TWO keyword".to_string())),
            insert_text: Some(r#"TWO $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "two".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case two keyword".to_string()),
            documentation: Some(Documentation::String("TWO keyword".to_string())),
            insert_text: Some(r#"two $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // THREE
        CompletionItem {
            label: "THREE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case THREE keyword".to_string()),
            documentation: Some(Documentation::String("THREE keyword".to_string())),
            insert_text: Some(r#"THREE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "three".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case three keyword".to_string()),
            documentation: Some(Documentation::String("THREE keyword".to_string())),
            insert_text: Some(r#"three $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // QUORUM
        CompletionItem {
            label: "QUORUM".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case QUORUM keyword".to_string()),
            documentation: Some(Documentation::String("QUORUM keyword".to_string())),
            insert_text: Some(r#"QUORUM $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "quorum".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case quorum keyword".to_string()),
            documentation: Some(Documentation::String("QUORUM keyword".to_string())),
            insert_text: Some(r#"quorum $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LOCAL_ONE
        CompletionItem {
            label: "LOCAL_ONE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LOCAL_ONE keyword".to_string()),
            documentation: Some(Documentation::String("LOCAL_ONE keyword".to_string())),
            insert_text: Some(r#"LOCAL_ONE $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "local_one".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case local_one keyword".to_string()),
            documentation: Some(Documentation::String("LOCAL_ONE keyword".to_string())),
            insert_text: Some(r#"local_one $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LOCAL_QUORUM
        CompletionItem {
            label: "LOCAL_QUORUM".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case LOCAL_QUORUM keyword".to_string()),
            documentation: Some(Documentation::String("LOCAL_QUORUM keyword".to_string())),
            insert_text: Some(r#"LOCAL_QUORUM $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "local_quorum".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case local_quorum keyword".to_string()),
            documentation: Some(Documentation::String("LOCAL_QUORUM keyword".to_string())),
            insert_text: Some(r#"local_quorum $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // EACH_QUORUM
        CompletionItem {
            label: "EACH_QUORUM".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Upper case EACH_QUORUM keyword".to_string()),
            documentation: Some(Documentation::String("EACH_QUORUM keyword".to_string())),
            insert_text: Some(r#"EACH_QUORUM $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "each_quorum".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("Lower case each_quorum keyword".to_string()),
            documentation: Some(Documentation::String("EACH_QUORUM keyword".to_string())),
            insert_text: Some(r#"each_quorum $0"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
    ]
});

// XAR-2 700 lines of pure TYPES だよ www
// XAR-2 700 lines of pure TYPES that are working だよ :D
pub static TYPES: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    vec![
        // LIST<>
        CompletionItem {
            label: "LIST<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("LIST type with parameter".to_string()),
            documentation: Some(Documentation::String(
                "LIST<type> - Collection type".to_string(),
            )),
            insert_text: Some(r#"LIST<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "list<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("list type with parameter".to_string()),
            documentation: Some(Documentation::String(
                "list<type> - Collection type".to_string(),
            )),
            insert_text: Some(r#"list<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SET<>
        CompletionItem {
            label: "SET<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("SET type with parameter".to_string()),
            documentation: Some(Documentation::String(
                "SET<type> - Collection type".to_string(),
            )),
            insert_text: Some(r#"SET<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "set<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("set type with parameter".to_string()),
            documentation: Some(Documentation::String(
                "set<type> - Collection type".to_string(),
            )),
            insert_text: Some(r#"set<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MAP<>
        CompletionItem {
            label: "MAP<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("MAP type with parameters".to_string()),
            documentation: Some(Documentation::String(
                "MAP<key, value> - Collection type".to_string(),
            )),
            insert_text: Some(r#"MAP<$0, $1>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "map<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("map type with parameters".to_string()),
            documentation: Some(Documentation::String(
                "map<key, value> - Collection type".to_string(),
            )),
            insert_text: Some(r#"map<$0, $1>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TUPLE<>
        CompletionItem {
            label: "TUPLE<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TUPLE type with parameters".to_string()),
            documentation: Some(Documentation::String(
                "TUPLE<type1, type2, ...> - Composite type".to_string(),
            )),
            insert_text: Some(r#"TUPLE<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "tuple<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("tuple type with parameters".to_string()),
            documentation: Some(Documentation::String(
                "tuple<type1, type2, ...> - Composite type".to_string(),
            )),
            insert_text: Some(r#"tuple<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FROZEN<>
        CompletionItem {
            label: "FROZEN<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("FROZEN type with parameter".to_string()),
            documentation: Some(Documentation::String(
                "FROZEN<type> - Frozen collection type".to_string(),
            )),
            insert_text: Some(r#"FROZEN<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "frozen<>".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("frozen type with parameter".to_string()),
            documentation: Some(Documentation::String(
                "frozen<type> - Frozen collection type".to_string(),
            )),
            insert_text: Some(r#"frozen<$0>"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // Simple types (non-parameterized)

        // ASCII
        CompletionItem {
            label: "ASCII".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("ASCII type".to_string()),
            documentation: Some(Documentation::String(
                "ASCII - Character string type".to_string(),
            )),
            insert_text: Some(r#"ASCII"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "ascii".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("ascii type".to_string()),
            documentation: Some(Documentation::String(
                "ascii - Character string type".to_string(),
            )),
            insert_text: Some(r#"ascii"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // BIGINT
        CompletionItem {
            label: "BIGINT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("BIGINT type".to_string()),
            documentation: Some(Documentation::String(
                "BIGINT - 64-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"BIGINT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "bigint".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("bigint type".to_string()),
            documentation: Some(Documentation::String(
                "bigint - 64-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"bigint"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // BLOB
        CompletionItem {
            label: "BLOB".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("BLOB type".to_string()),
            documentation: Some(Documentation::String(
                "BLOB - Binary large object".to_string(),
            )),
            insert_text: Some(r#"BLOB"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "blob".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("blob type".to_string()),
            documentation: Some(Documentation::String(
                "blob - Binary large object".to_string(),
            )),
            insert_text: Some(r#"blob"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // BOOLEAN
        CompletionItem {
            label: "BOOLEAN".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("BOOLEAN type".to_string()),
            documentation: Some(Documentation::String("BOOLEAN - True or false".to_string())),
            insert_text: Some(r#"BOOLEAN"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "boolean".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("boolean type".to_string()),
            documentation: Some(Documentation::String("boolean - True or false".to_string())),
            insert_text: Some(r#"boolean"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // COUNTER
        CompletionItem {
            label: "COUNTER".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("COUNTER type".to_string()),
            documentation: Some(Documentation::String(
                "COUNTER - Distributed counter".to_string(),
            )),
            insert_text: Some(r#"COUNTER"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "counter".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("counter type".to_string()),
            documentation: Some(Documentation::String(
                "counter - Distributed counter".to_string(),
            )),
            insert_text: Some(r#"counter"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DATE
        CompletionItem {
            label: "DATE".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("DATE type".to_string()),
            documentation: Some(Documentation::String(
                "DATE - Date without time".to_string(),
            )),
            insert_text: Some(r#"DATE"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "date".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("date type".to_string()),
            documentation: Some(Documentation::String(
                "date - Date without time".to_string(),
            )),
            insert_text: Some(r#"date"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DECIMAL
        CompletionItem {
            label: "DECIMAL".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("DECIMAL type".to_string()),
            documentation: Some(Documentation::String(
                "DECIMAL - Variable-precision decimal".to_string(),
            )),
            insert_text: Some(r#"DECIMAL"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "decimal".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("decimal type".to_string()),
            documentation: Some(Documentation::String(
                "decimal - Variable-precision decimal".to_string(),
            )),
            insert_text: Some(r#"decimal"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // DOUBLE
        CompletionItem {
            label: "DOUBLE".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("DOUBLE type".to_string()),
            documentation: Some(Documentation::String(
                "DOUBLE - 64-bit floating point".to_string(),
            )),
            insert_text: Some(r#"DOUBLE"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "double".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("double type".to_string()),
            documentation: Some(Documentation::String(
                "double - 64-bit floating point".to_string(),
            )),
            insert_text: Some(r#"double"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FLOAT
        CompletionItem {
            label: "FLOAT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("FLOAT type".to_string()),
            documentation: Some(Documentation::String(
                "FLOAT - 32-bit floating point".to_string(),
            )),
            insert_text: Some(r#"FLOAT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "float".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("float type".to_string()),
            documentation: Some(Documentation::String(
                "float - 32-bit floating point".to_string(),
            )),
            insert_text: Some(r#"float"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INET
        CompletionItem {
            label: "INET".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("INET type".to_string()),
            documentation: Some(Documentation::String("INET - IP address".to_string())),
            insert_text: Some(r#"INET"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "inet".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("inet type".to_string()),
            documentation: Some(Documentation::String("inet - IP address".to_string())),
            insert_text: Some(r#"inet"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // INT
        CompletionItem {
            label: "INT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("INT type".to_string()),
            documentation: Some(Documentation::String(
                "INT - 32-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"INT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "int".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("int type".to_string()),
            documentation: Some(Documentation::String(
                "int - 32-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"int"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // LIST
        CompletionItem {
            label: "LIST".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("LIST type".to_string()),
            documentation: Some(Documentation::String("LIST - Collection type".to_string())),
            insert_text: Some(r#"LIST"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "list".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("list type".to_string()),
            documentation: Some(Documentation::String("list - Collection type".to_string())),
            insert_text: Some(r#"list"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // MAP
        CompletionItem {
            label: "MAP".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("MAP type".to_string()),
            documentation: Some(Documentation::String(
                "MAP - Key-value collection type".to_string(),
            )),
            insert_text: Some(r#"MAP"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "map".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("map type".to_string()),
            documentation: Some(Documentation::String(
                "map - Key-value collection type".to_string(),
            )),
            insert_text: Some(r#"map"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SET
        CompletionItem {
            label: "SET".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("SET type".to_string()),
            documentation: Some(Documentation::String(
                "SET - Unique collection type".to_string(),
            )),
            insert_text: Some(r#"SET"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "set".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("set type".to_string()),
            documentation: Some(Documentation::String(
                "set - Unique collection type".to_string(),
            )),
            insert_text: Some(r#"set"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // SMALLINT
        CompletionItem {
            label: "SMALLINT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("SMALLINT type".to_string()),
            documentation: Some(Documentation::String(
                "SMALLINT - 16-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"SMALLINT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "smallint".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("smallint type".to_string()),
            documentation: Some(Documentation::String(
                "smallint - 16-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"smallint"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TEXT
        CompletionItem {
            label: "TEXT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TEXT type".to_string()),
            documentation: Some(Documentation::String(
                "TEXT - UTF-8 encoded string".to_string(),
            )),
            insert_text: Some(r#"TEXT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "text".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("text type".to_string()),
            documentation: Some(Documentation::String(
                "text - UTF-8 encoded string".to_string(),
            )),
            insert_text: Some(r#"text"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TIME
        CompletionItem {
            label: "TIME".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TIME type".to_string()),
            documentation: Some(Documentation::String(
                "TIME - Time without date".to_string(),
            )),
            insert_text: Some(r#"TIME"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "time".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("time type".to_string()),
            documentation: Some(Documentation::String(
                "time - Time without date".to_string(),
            )),
            insert_text: Some(r#"time"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TIMESTAMP
        CompletionItem {
            label: "TIMESTAMP".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TIMESTAMP type".to_string()),
            documentation: Some(Documentation::String(
                "TIMESTAMP - Date and time".to_string(),
            )),
            insert_text: Some(r#"TIMESTAMP"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "timestamp".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("timestamp type".to_string()),
            documentation: Some(Documentation::String(
                "timestamp - Date and time".to_string(),
            )),
            insert_text: Some(r#"timestamp"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TIMEUUID
        CompletionItem {
            label: "TIMEUUID".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TIMEUUID type".to_string()),
            documentation: Some(Documentation::String(
                "TIMEUUID - Version 1 UUID".to_string(),
            )),
            insert_text: Some(r#"TIMEUUID"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "timeuuid".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("timeuuid type".to_string()),
            documentation: Some(Documentation::String(
                "timeuuid - Version 1 UUID".to_string(),
            )),
            insert_text: Some(r#"timeuuid"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TINYINT
        CompletionItem {
            label: "TINYINT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TINYINT type".to_string()),
            documentation: Some(Documentation::String(
                "TINYINT - 8-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"TINYINT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "tinyint".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("tinyint type".to_string()),
            documentation: Some(Documentation::String(
                "tinyint - 8-bit signed integer".to_string(),
            )),
            insert_text: Some(r#"tinyint"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // TUPLE
        CompletionItem {
            label: "TUPLE".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("TUPLE type".to_string()),
            documentation: Some(Documentation::String("TUPLE - Composite type".to_string())),
            insert_text: Some(r#"TUPLE"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "tuple".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("tuple type".to_string()),
            documentation: Some(Documentation::String("tuple - Composite type".to_string())),
            insert_text: Some(r#"tuple"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // UUID
        CompletionItem {
            label: "UUID".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("UUID type".to_string()),
            documentation: Some(Documentation::String(
                "UUID - Universally unique identifier".to_string(),
            )),
            insert_text: Some(r#"UUID"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "uuid".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("uuid type".to_string()),
            documentation: Some(Documentation::String(
                "uuid - Universally unique identifier".to_string(),
            )),
            insert_text: Some(r#"uuid"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // VARCHAR
        CompletionItem {
            label: "VARCHAR".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("VARCHAR type".to_string()),
            documentation: Some(Documentation::String(
                "VARCHAR - Variable-length string".to_string(),
            )),
            insert_text: Some(r#"VARCHAR"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "varchar".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("varchar type".to_string()),
            documentation: Some(Documentation::String(
                "varchar - Variable-length string".to_string(),
            )),
            insert_text: Some(r#"varchar"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // VARINT
        CompletionItem {
            label: "VARINT".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("VARINT type".to_string()),
            documentation: Some(Documentation::String(
                "VARINT - Arbitrary-precision integer".to_string(),
            )),
            insert_text: Some(r#"VARINT"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "varint".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("varint type".to_string()),
            documentation: Some(Documentation::String(
                "varint - Arbitrary-precision integer".to_string(),
            )),
            insert_text: Some(r#"varint"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        // FROZEN
        CompletionItem {
            label: "FROZEN".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("FROZEN type".to_string()),
            documentation: Some(Documentation::String(
                "FROZEN - Frozen collection type".to_string(),
            )),
            insert_text: Some(r#"FROZEN"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "frozen".to_string(),
            kind: Some(CompletionItemKind::TYPE_PARAMETER),
            detail: Some("frozen type".to_string()),
            documentation: Some(Documentation::String(
                "frozen - Frozen collection type".to_string(),
            )),
            insert_text: Some(r#"frozen"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
    ]
});

pub static COMMAND_SEQUENCE: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    vec![
        CompletionItem {
            label: "ALTER KEYSPACE".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("ALTER KEYSPACE cql command".to_string()),
            documentation: Some(Documentation::String(
                "ALTER KEYSPACE cql command".to_string(),
            )),
            insert_text: Some(r#"ALTER KEYSPACE $0";"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
        CompletionItem {
            label: "ALTER MATERIALIZED VIEW".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("ALTER MATERIALIZED VIEW cql command".to_string()),
            documentation: Some(Documentation::String(
                "ALTER MATERIALIZED VIEW cql command".to_string(),
            )),
            insert_text: Some(r#"ALTER MATERIALIZED VIEW $0";"#.to_string()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            ..Default::default()
        },
    ]
});

// Advanced Completions
//
// CREATE -> CREATE [TABLE|KEYSPACE|MATERIALIZED VIEW|...]
pub static UNION_COMMANDS_KEYWORDS: Lazy<Vec<CompletionItem>> = Lazy::new(|| {
    let mut sequence = Vec::new();
    sequence.extend(COMMAND_SEQUENCE.iter().cloned());
    sequence.extend(KEYWORDS.iter().cloned());
    sequence
});
