use log::info;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use std::collections::HashMap;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Backend {
    pub client: Client,
    pub documents: RwLock<HashMap<Url, String>>,
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

    fn get_keyspaces(&self) -> Vec<String> {
        vec![
            "system".to_string(),
            "system_auth".to_string(),
            "system_schema".to_string(),
            "system_distributed".to_string(),
            "user_data".to_string(),
            "analytics".to_string(),
            "metrics".to_string(),
            "customer_profiles".to_string(),
        ]
    }

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

    fn handle_in_string_keyspace_completion(
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
                for keyspace in self.get_keyspaces() {
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

    fn handle_out_of_string_keyspace_completion(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        info!("Suggesting keyspace formats");

        let mut items = Vec::new();
        for keyspace in self.get_keyspaces() {
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

        let items = vec![
            CompletionItem {
                label: "USE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Upper case use keyword".to_string()),
                documentation: Some(Documentation::String("USE keyword".to_string())),
                insert_text: Some(r#"USE "$0";"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "use".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Lower case use keyword".to_string()),
                documentation: Some(Documentation::String("USE keyword.".to_string())),
                insert_text: Some(r#"use "$0";"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "SELECT".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Upper case select keyword".to_string()),
                documentation: Some(Documentation::String("SELECT keyword".to_string())),
                insert_text: Some(r#"SELECT $0"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "select".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Lower case select keyword".to_string()),
                documentation: Some(Documentation::String("SELECT keyword.".to_string())),
                insert_text: Some(r#"select $0"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "INSERT".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Upper case insert keyword".to_string()),
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
        ];

        return Ok(Some(CompletionResponse::Array(items)));
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

        if Self::should_suggest_keyspaces(self, line, position.character) {
            return if in_string {
                self.handle_in_string_keyspace_completion(line, position)
            } else {
                self.handle_out_of_string_keyspace_completion()
            };
        }

        if !in_string {
            return self.handle_keywords_completion();
        }

        Ok(Some(CompletionResponse::Array(vec![])))
    }
}
