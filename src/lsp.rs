use log::info;
use regex::Regex;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::consts::*;
use crate::cqlsh::{self, CqlSettings};

/*
    Note:

    The lsp code contains a HUGE amount of code so,
    for better navigation there some headers below:

    XAR-1: vec[] of keywords
    XAR-2: vec[] of types

*/

#[derive(Debug)]
pub struct Backend {
    pub client: Client,
    pub documents: RwLock<HashMap<Url, String>>,
    pub current_document: RwLock<Option<RwLock<Document>>>,
    pub config: CqlSettings,
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
    fn is_in_string_literal(line: &str, position: u32) -> bool {
        let prefix = match line.get(..position as usize) {
            Some(p) => p,
            None => return false,
        };

        let mut in_double_quotes = false;
        let mut in_single_quotes = false;
        let mut escape_next = false;

        for ch in prefix.chars() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => escape_next = true,
                '"' if !in_single_quotes => in_double_quotes = !in_double_quotes,
                '\'' if !in_double_quotes => in_single_quotes = !in_single_quotes,
                _ => {}
            }
        }

        in_double_quotes || in_single_quotes
    }

    async fn get_keyspaces(&self) -> Vec<String> {
        let items = cqlsh::query_keyspaces(&self.config).await;

        match items {
            Ok(r) => r.into_iter().collect(),
            Err(e) => {
                info!("{:?}", e.to_string());
                vec![]
            }
        }
    }

    // Needs rework
    fn should_suggest_keyspaces(&self, line: &str, position: u32) -> bool {
        let prefix = match line.get(..position as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim_end();
        if let Some(stripped) = trimmed_prefix.to_lowercase().strip_prefix("use") {
            let is_whole_word = trimmed_prefix.len() == "use".len()
                || !trimmed_prefix
                    .chars()
                    .nth("use".len())
                    .map(|c| c.is_alphanumeric())
                    .unwrap_or(false);

            if is_whole_word {
                let after_use = stripped.trim_start();
                return after_use.is_empty()
                    || after_use.starts_with('"')
                    || after_use.starts_with('\'');
            }
        }
        false
    }

    fn get_graph_engine_types(&self) -> Vec<String> {
        vec!["Core".to_string(), "Classic".to_string()]
    }

    fn should_suggest_graph_engine_types(&self, line: &str, position: u32) -> bool {
        let prefix = match line.get(..position as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim_end();
        let splitted: Vec<&str> = trimmed_prefix.split(' ').collect();

        if splitted.len() < 2 || (splitted[0] != "graph_engine" && splitted[1] != "=") {
            return false;
        }

        true
    }

    fn get_available_command_sequences(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        /*
            ### BASIC SEQUENCES

            $ Syntax Legend

            Ref Docs:
                DataStax HCD: https://docs.datastax.com/en/cql/hcd/reference/cql-reference-about.html
                Tree-Siter: https://github.com/Akzestia/tree-sitter-cql
                LSP: https://github.com/Akzestia/cql-lsp

            TK_NAME - $.table_keyspace_name
            IDENTIFIER - $.identifier
            SELECTORS - $.selectors

            $N position of cursor in snippet
            $N<TK_NAME> suggest $.table_keyspace_name in N posiion
            ; sequences that have semi-colun are end of the line completions

            ---[#NAME SKIPPED]--- Commands that doesn't need or have very complex
            sequence for completion

            # ALTER

            ALTER KEYSPACE $0<TK_NAME>
            ALTER MATERIALIZED VIEW $0<TK_NAME>
            ALTER ROLE $0<TK_NAME>
            ALTER TABLE $0<TK_NAME>
            ALTER TYPE $0<TK_NAME>
            ALTER USER $0<TK_NAME>

            -------------[#BATCH SKIPPED]-------------

            # COMMIT

            COMMIT SEARCH INDEX ON $0<TK_NAME> ;

            # CREATE

            CREATE AGGREGATE [IF NOT EXISTS] $0<TK_NAME>
            CREATE FUNCTION [IF NOT EXISTS] $0<TK_NAME>
            CREATE [CUSTOM] INDEX [IF NOT EXISTS] [IDENTIFIER] ON $0<TK_NAME>
            CREATE KEYSPACE [IF NOT EXISTS] $0<TK_NAME>
            CREATE MATERIALIZED VIEW [IF NOT EXISTS] $0<TK_NAME>
            CREATE ROLE [IF NOT EXISTS] $0<TK_NAME>
            CREATE SEARCH INDEX [IF NOT EXISTS] ON $0<TK_NAME>
            CREATE TABLE [IF NOT EXISTS] $0<TK_NAME>
            CREATE TYPE [IF NOT EXISTS] $0<TK_NAME>
            CREATE USER [IF NOT EXISTS] $0<TK_NAME>

            -------------[#DELETE SKIPPED]-------------

            # DROP

            DROP AGGREGATE [ IF EXISTS ] $0<TK_NAME>
            DROP FUNCTION [ IF EXISTS ] $0<TK_NAME>
            DROP INDEX [ IF EXISTS ] $0<TK_NAME>
            DROP KEYSPACE [ IF EXISTS ] $0<TK_NAME> ;
            DROP MATERIALIZED VIEW [ IF EXISTS ] $0<TK_NAME> ;
            DROP ROLE [ IF EXISTS ] $0<TK_NAME> ;
            DROP SEARCH INDEX ON $0<TK_NAME>
            DROP TABLE [ IF EXISTS ] $0<TK_NAME> ;
            DROP TYPE [ IF EXISTS ] $0<TK_NAME>;
            DROP USER [ IF EXISTS ] $0<TK_NAME>;

            # GRANT

            -------------[#INSERT SKIPPED]-------------

            # LIST

            LIST ALL PREMISSIONS $0
            LIST ROLES $0
            LIST USERS ;

            # REVOKE

            REVOKE $0<IDENTIFIER> FROM $1<IDENTIFIER> ;
            REVOKE ALL PREMISSIONS $0

            # SELECT [context_based_select=true]

            SELECT $1<SELECTORS> FROM $0<TK_NAME>
            SELECT $1<SELECTORS> FROM $0<TK_NAME> ;

            # TRUNCATE

            TRUNCATE TBALE $0<TK_NAME> ;

            -------------[#UPDATE SKIPPED]-------------

            # USE

            USE "$0<TK_NAME>";
            USE '$0<TK_NAME>';
        */

        let items = vec![
            CompletionItem {
                label: "ALTER".to_string(),
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
                label: "ALTER".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("ALTER MATERIALIZED VIEW cql command".to_string()),
                documentation: Some(Documentation::String(
                    "ALTER MATERIALIZED VIEW cql command".to_string(),
                )),
                insert_text: Some(r#"ALTER MATERIALIZED VIEW $0";"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ];

        Ok(Some(CompletionResponse::Array(items)))
    }

    fn should_suggest_command_sequence(&self, line: &str, position: u32) -> bool {
        false
    }

    async fn latest_keyspace(&self) -> Option<String> {
        let current = self.current_document.read().await;

        if let Some(ref document_lock) = *current {
            let document = document_lock.read().await;

            let re = Regex::new(r#"(?i)\buse\s+['"]([^'"]+)['"]\s*;"#).unwrap();

            let mut latest: Option<String> = None;

            for caps in re.captures_iter(&document.text) {
                if let Some(keyspace) = caps.get(1) {
                    latest = Some(keyspace.as_str().to_string());
                }
            }

            return latest;
        }

        None
    }

    async fn get_fields(&self, line: &str, position: u32) -> Vec<String> {
        let keyspace = self.latest_keyspace().await;

        let items = cqlsh::query_fields(&self.config, "system", "local").await;

        match items {
            Ok(r) => r,
            Err(e) => {
                info!("{:?}", e.to_string());
                vec![]
            }
        }
    }

    fn should_suggest_fields(&self, line: &str, position: u32) -> bool {
        let prefix = match line.get(..position as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let splitted: Vec<&str> = trimmed_prefix.split(' ').collect();

        info!("Splitted F: {:?}", splitted);

        if !splitted.contains(&"select") || splitted.contains(&"*") || splitted.contains(&"from") {
            info!("Splitted FAILURE");
            return false;
        }

        if splitted.len() > 2 && !splitted[splitted.len() - 2].contains(&",") {
            return false;
        }

        if trimmed_prefix.len() != prefix.len() && !splitted[splitted.len() - 1].contains(&",") {
            return false;
        }

        info!("Splitted SUCCESS");

        true
    }

    async fn handle_in_string_keyspace_completion(
        &self,
        line: &str,
        position: Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(prefix) = line.get(..position.character as usize) {
            if let Some(quote_pos) = prefix.rfind(|c| c == '"' || c == '\'') {
                let quote_char = prefix.chars().nth(quote_pos).unwrap_or('"');
                let typed_prefix = prefix.get(quote_pos + 1..).unwrap_or("");

                let suffix = line.get(position.character as usize..).unwrap_or("");
                let word_end = suffix
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(suffix.len());
                let has_closing_quote = suffix.starts_with(quote_char);
                let has_semicolon = suffix[has_closing_quote as usize..].starts_with(';');

                let mut items = Vec::new();

                for keyspace in self.get_keyspaces().await {
                    if keyspace.starts_with(typed_prefix) {
                        let insert_text = match (has_closing_quote, has_semicolon) {
                            (true, true) => keyspace.clone(),
                            (true, false) => format!("{}{};", keyspace, quote_char),
                            (false, true) => format!("{}{}", keyspace, quote_char),
                            (false, false) => format!("{}{};", keyspace, quote_char),
                        };

                        if has_closing_quote && has_semicolon == false {
                            let replace_end = position.character as usize
                                + word_end
                                + has_closing_quote as usize
                                + has_semicolon as usize;

                            let text_edit = TextEdit {
                                range: Range {
                                    start: Position {
                                        line: position.line,
                                        // +1 to avoid replacing prefix \"
                                        character: quote_pos as u32 + 1,
                                    },
                                    end: Position {
                                        line: position.line,
                                        character: replace_end as u32,
                                    },
                                },
                                new_text: insert_text,
                            };

                            items.push(CompletionItem {
                                label: keyspace.clone(),
                                kind: Some(CompletionItemKind::VALUE),
                                text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                                ..Default::default()
                            });
                        } else {
                            items.push(CompletionItem {
                                label: keyspace.clone(),
                                kind: Some(CompletionItemKind::VALUE),
                                insert_text: Some(insert_text),
                                insert_text_format: Some(InsertTextFormat::SNIPPET),
                                ..Default::default()
                            });
                        }
                    }
                }

                if !items.is_empty() {
                    return Ok(Some(CompletionResponse::Array(items)));
                }
            }
        }
        Ok(Some(CompletionResponse::Array(vec![])))
    }

    async fn handle_out_of_string_keyspace_completion(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        info!("Suggesting keyspace formats");

        let mut items = Vec::new();
        for keyspace in self.get_keyspaces().await {
            items.push(CompletionItem {
                label: keyspace.clone(),
                kind: Some(CompletionItemKind::VALUE),
                insert_text: Some(format!("\"{}\";", keyspace)),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            });
        }

        if !items.is_empty() {
            return Ok(Some(CompletionResponse::Array(items)));
        }
        Ok(Some(CompletionResponse::Array(vec![])))
    }

    fn handle_keywords_completion(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        info!("Offering keyword completions");

        return Ok(Some(CompletionResponse::Array(
            KEYWORDS.iter().cloned().collect(),
        )));
    }

    fn handle_types_completion(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        info!("Offering types completions");

        return Ok(Some(CompletionResponse::Array(
            TYPES.iter().cloned().collect(),
        )));
    }

    async fn handle_fields_completion(
        &self,
        line: &str,
        position: u32,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        info!("Offering fields completions");

        let mut items: Vec<CompletionItem> = Vec::new();

        for item in self.get_fields(line, position).await {
            info!("Suggested Items Str: {:?}", item);
            items.push(CompletionItem {
                label: item.clone(),
                kind: Some(CompletionItemKind::VALUE),
                ..Default::default()
            });
        }

        info!("Suggested Items collection: {:?}", items);

        return Ok(Some(CompletionResponse::Array(items)));
    }

    async fn handle_graph_engine_completion(
        &self,
        line: &str,
        position: u32,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        info!("Offering graph engine completions");

        let mut items: Vec<CompletionItem> = Vec::new();

        return Ok(Some(CompletionResponse::Array(items)));
    }

    fn get_document(&self, uri: &Url) -> Option<Document> {
        let documents = match self.documents.try_read() {
            Ok(docs) => docs,
            Err(_) => return None,
        };

        documents.get(uri).map(|text| Document {
            uri: uri.clone(),
            text: text.clone(),
        })
    }
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
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        info!("LSP initialized!");
        self.client
            .log_message(MessageType::INFO, "LSP initialized!")
            .await;
    }

    async fn shutdown(&self) -> tower_lsp::jsonrpc::Result<()> {
        info!("LSP shutdown!");
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
            info!("Document changed: {}", uri);
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

        info!("Opened document: {}", uri);
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

        let in_string = Self::is_in_string_literal(line, position.character);

        if self.should_suggest_keyspaces(line, position.character) {
            return if in_string {
                self.handle_in_string_keyspace_completion(line, position)
                    .await
            } else {
                self.handle_out_of_string_keyspace_completion().await
            };
        }

        if self.should_suggest_fields(line, position.character)
            && !self.should_suggest_keyspaces(line, position.character)
        {
            return self
                .handle_fields_completion(line, position.character)
                .await;
        }

        if !in_string
            && !self.should_suggest_fields(line, position.character)
            && !self.should_suggest_keyspaces(line, position.character)
        {
            return self.handle_keywords_completion();
        }

        Ok(Some(CompletionResponse::Array(vec![])))
    }
}
