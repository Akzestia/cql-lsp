use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tree_sitter::{Node, Parser, TreeCursor};

pub static TS_CQL: Lazy<Mutex<Parser>> = Lazy::new(|| {
    let mut parser = Parser::new();
    parser
        .set_language(&tttx_tree_sitter_cql::LANGUAGE.into())
        .expect("Error loading CQL grammar");
    Mutex::new(parser)
});
