use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::formatting::*;
use crate::handlers::*;

use crate::cqlsh::CqlSettings;

/*
    Based on DataStax HCD && CQL versions 3.4+

    HCD
    https://docs.datastax.com/en/cql/hcd/reference/cql-reference-about.html
    CQL
    https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html

    Note!

    Some of the default CQL functions will be different because of DataStax HCD extensions
*/

#[derive(Debug)]
pub struct FormattingSettings {
    pub type_alignment_offset: usize,
}

impl FormattingSettings {
    pub fn from_env(type_alignment_offset: &str) -> Self {
        Self {
            type_alignment_offset: type_alignment_offset.parse().unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct Backend {
    pub client: Client,
    pub documents: RwLock<HashMap<Url, String>>,
    pub current_document: RwLock<Option<RwLock<Document>>>,
    pub config: CqlSettings,
    pub formatting_config: FormattingSettings,
}

#[derive(Debug, Clone)]
pub struct Document {
    pub uri: Url,
    pub text: String,
}

impl Document {
    pub fn new(uri: Url, text: String) -> Self {
        Self { uri, text }
    }

    fn change(&mut self, uri: Url, text: String) {
        self.uri = uri;
        self.text = text;
    }
}

impl Backend {
    // -----------------------------[Helper Functions]-----------------------------

    // utils.rs

    // -----------------------------[Formatting]-----------------------------

    // formatting.rs

    // -----------------------------[Completions]-----------------------------

    // completions.rs

    // -----------------------------[Handlers]-----------------------------

    // handlers.rs
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(
        &self,
        _: InitializeParams,
    ) -> tower_lsp::jsonrpc::Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: Some(vec![
                        ".".to_string(),
                        "\"".to_string(),
                        "'".to_string(),
                        " ".to_string(),
                    ]),
                    ..Default::default()
                }),
                document_formatting_provider: Some(OneOf::Left(true)),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn formatting(
        &self,
        params: DocumentFormattingParams,
    ) -> tower_lsp::jsonrpc::Result<Option<Vec<TextEdit>>> {
        let document = params.text_document.uri;

        if let Some(current_doc) = self.documents.read().await.get(&document) {
            let lines: Vec<&str> = current_doc.split('\n').collect();

            return Ok(Some(self.format_file(&lines, &document).await));
        } else {
            return Ok(Some(vec![]));
        }
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "LSP initialized!")
            .await;
    }

    async fn shutdown(&self) -> tower_lsp::jsonrpc::Result<()> {
        Ok(())
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        let changes = &params.content_changes;

        if let Some(change) = changes.first() {
            self.documents
                .write()
                .await
                .insert(uri.clone(), change.text.clone());

            let mut current = self.current_document.write().await;
            if let Some(ref mut document_lock) = *current {
                let mut document = document_lock.write().await;
                if document.uri == uri {
                    document.change(uri.clone(), change.text.clone());
                }
            }
        }
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let text = params.text_document.text;

        let mut current = self.current_document.write().await;
        if current.is_none() {
            *current = Some(RwLock::new(Document::new(uri.clone(), text.clone())));
        }

        if let Some(ref mut document_lock) = *current {
            let mut document = document_lock.write().await;
            document.change(uri.clone(), text.clone());
        }

        self.documents
            .write()
            .await
            .insert(uri.clone(), text.clone());

        self.client
            .log_message(MessageType::INFO, format!("Opened: {}", uri))
            .await;
    }

    async fn completion(
        &self,
        params: CompletionParams,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;

        let documents = self.documents.read().await;
        let text = match documents.get(&uri) {
            Some(text) => text,
            None => return Ok(None),
        };

        let line = match text.lines().nth(position.line as usize) {
            Some(line) => line,
            None => return Ok(None),
        };

        // --------------------------------[EXPERIMENTAL] --------------------------------

        /*
            Set of experimental features not included in standard build.
            For more information, see https://github.com/Akzestia/cql-lsp
        */

        // let ssh_command_sequence = self.should_suggest_command_sequence(line, &position);

        // --------------------------------[EXPERIMENTAL] --------------------------------

        // --------------------------------[STABLE] --------------------------------

        /*
            Set of features included in standard build.
            For more information, see https://github.com/Akzestia/cql-lsp
        */

        // General
        let in_string = Self::is_in_string_literal(line, position.character);
        let ssh_keyspaces = self.should_suggest_keyspaces(line, &position);
        let ssh_graph_types = self.should_suggest_graph_engine_types(line, &position);
        let ssh_keywords = self.should_suggest_keywords(line, &position).await;
        let ssh_fields = self.should_suggest_fields(line, &position);
        let ssh_from = self.should_suggest_from(line, &position);
        let ssh_table_completions = self.should_suggest_table_completions(line, &position);
        let ssh_if_not_exists = self.should_suggest_if_not_exists(line, &position);
        let ssh_create_keywords = self.should_suggest_create_keywords(line, &position);
        let ssh_alter_keywords = self.should_suggest_alter_keywords(line, &position);

        // DROP kw
        let ssh_drop_keywords = self.should_suggest_drop_keywords(line, &position);
        let ssh_drop_keyspaces = self.should_suggest_drop_keyspaces(line, &position);
        let ssh_drop_tables = self.should_suggest_drop_tables(line, &position);
        // DROP Queries
        let ssh_drop_aggregate = self.should_suggest_drop_aggregate(line, &position);
        let ssh_drop_function = self.should_suggest_drop_function(line, &position);
        let ssh_drop_index = self.should_suggest_drop_indexes(line, &position);
        let ssh_drop_type = self.should_suggest_drop_types(line, &position);
        let ssh_drop_view = self.should_suggest_drop_views(line, &position);

        // Types
        let ssh_types = self
            .should_suggest_types_completions(line, &position, &uri)
            .await;
        let ssh_type_modifiers = self
            .should_suggest_type_modifiers(line, &position, &uri)
            .await;

        // --------------------------------[STABLE] --------------------------------

        if ssh_keyspaces {
            return if in_string {
                self.handle_in_string_keyspace_completion(line, &position)
                    .await
            } else {
                self.handle_out_of_string_keyspace_completion(line, &position)
                    .await
            };
        }

        if ssh_create_keywords {
            return self.handle_create_keywords();
        }

        if ssh_alter_keywords {
            return self.handle_alter_keywords();
        }

        if ssh_drop_keywords {
            return self.handle_drop_keywords();
        }

        if ssh_drop_keyspaces {
            return self.handle_drop_keyspace_completions(line, &position).await;
        }

        if ssh_drop_tables {
            return self.handle_table_completion(&position).await;
        }

        if ssh_drop_aggregate {
            return self.handle_drop_aggregate_completions().await;
        }

        if ssh_drop_function {
            return self.handle_drop_function_completions().await;
        }

        if ssh_drop_index {
            return self.handle_drop_index_completions().await;
        }

        if ssh_drop_type {
            return self.handle_drop_type_completions().await;
        }

        if ssh_drop_view {
            return self.handle_drop_view_completions().await;
        }

        if ssh_types {
            return self.handle_types_completion();
        }

        if ssh_type_modifiers {
            return self.handle_type_modifiers_completion(line);
        }

        if ssh_from {
            return self.handle_from_completion();
        }

        if ssh_if_not_exists {
            return self.handle_if_not_exists();
        }

        if ssh_fields {
            return self.handle_fields_completion(line, &position).await;
        }

        if ssh_table_completions {
            return self.handle_table_completion(&position).await;
        }

        if ssh_graph_types {
            return if in_string {
                self.handle_in_string_graph_engine_completion(line, &position)
                    .await
            } else {
                self.handle_out_of_string_graph_engine_completion().await
            };
        }

        if ssh_keywords && !in_string {
            return self.handle_keywords_completion();
        }

        Ok(Some(CompletionResponse::Array(vec![])))
    }
}
