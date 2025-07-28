use tower_lsp::lsp_types::*;

use crate::consts::*;
use crate::cqlsh::*;
use crate::lsp::Backend;

impl Backend {
    pub async fn handle_in_string_keyspace_completion(
        &self,
        line: &str,
        position: &Position,
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

    pub async fn handle_drop_keyspace_completions(
        &self,
        line: &str,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let mut items = Vec::new();
        for keyspace in self.get_keyspaces().await {
            let mut index = position.character as usize;
            while index > 0 {
                if line.chars().nth(index).unwrap_or_else(|| '_') == ' ' {
                    index += 1;
                    break;
                }
                index -= 1;
            }

            let text_edit = TextEdit {
                range: Range {
                    start: Position {
                        line: position.line,
                        character: index as u32,
                    },
                    end: Position {
                        line: position.line,
                        character: line.len() as u32,
                    },
                },
                new_text: format!("{};", keyspace),
            };

            items.push(CompletionItem {
                label: keyspace.clone(),
                kind: Some(CompletionItemKind::VALUE),
                text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                ..Default::default()
            });
        }

        if !items.is_empty() {
            return Ok(Some(CompletionResponse::Array(items)));
        }
        Ok(Some(CompletionResponse::Array(vec![])))
    }

    pub async fn handle_out_of_string_keyspace_completion(
        &self,
        line: &str,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let mut items = Vec::new();
        for keyspace in self.get_keyspaces().await {
            let mut index = position.character as usize;
            while index > 0 {
                if line.chars().nth(index).unwrap_or_else(|| '_') == ' ' {
                    index += 1;
                    break;
                }
                index -= 1;
            }

            let text_edit = TextEdit {
                range: Range {
                    start: Position {
                        line: position.line,
                        character: index as u32,
                    },
                    end: Position {
                        line: position.line,
                        character: line.len() as u32,
                    },
                },
                new_text: format!("\"{}\";", keyspace),
            };

            items.push(CompletionItem {
                label: keyspace.clone(),
                kind: Some(CompletionItemKind::VALUE),
                text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                ..Default::default()
            });
        }

        if !items.is_empty() {
            return Ok(Some(CompletionResponse::Array(items)));
        }
        Ok(Some(CompletionResponse::Array(vec![])))
    }

    pub fn handle_keywords_completion(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        return Ok(Some(CompletionResponse::Array(
            KEYWORDS.iter().cloned().collect(),
        )));
    }

    pub fn handle_types_completion(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        return Ok(Some(CompletionResponse::Array(
            TYPES.iter().cloned().collect(),
        )));
    }

    pub fn handle_type_modifiers_completion(
        &self,
        line: &str,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if line.to_lowercase().contains("primary") {
            return Ok(Some(CompletionResponse::Array(vec![
                CompletionItem {
                    label: "KEY".to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some("Upper case KEY type modifier".to_string()),
                    documentation: Some(Documentation::String("KEY type modifier".to_string())),
                    insert_text: Some(r#"KEY"#.to_string()),
                    insert_text_format: Some(InsertTextFormat::SNIPPET),
                    ..Default::default()
                },
                CompletionItem {
                    label: "key".to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some("Lower case key type modifier".to_string()),
                    documentation: Some(Documentation::String("key type modifier".to_string())),
                    insert_text: Some(r#"key"#.to_string()),
                    insert_text_format: Some(InsertTextFormat::SNIPPET),
                    ..Default::default()
                },
            ])));
        }

        return Ok(Some(CompletionResponse::Array(vec![
            CompletionItem {
                label: "PRIMARY KEY".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Upper case PRIMARY KEY type modifier".to_string()),
                documentation: Some(Documentation::String(
                    "PRIMARY KEY type modifier".to_string(),
                )),
                insert_text: Some(r#"PRIMARY KEY"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "primary key".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Lower case primary key type modifier".to_string()),
                documentation: Some(Documentation::String(
                    "primary key type modifier".to_string(),
                )),
                insert_text: Some(r#"primary key"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "STATIC".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Upper case STATIC type modifier".to_string()),
                documentation: Some(Documentation::String("STATIC type modifier".to_string())),
                insert_text: Some(r#"STATIC"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "static".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Lower case static type modifier".to_string()),
                documentation: Some(Documentation::String("static type modifier".to_string())),
                insert_text: Some(r#"static"#.to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ])));
    }

    pub async fn handle_fields_completion(
        &self,
        line: &str,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(response) = self
            .get_fields(line, position)
            .await
            .unwrap_or_else(|_| Some(CompletionResponse::Array(vec![])))
        {
            return Ok(Some(response));
        }

        return Ok(Some(CompletionResponse::Array(vec![])));
    }

    pub fn handle_from_completion(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        return Ok(Some(CompletionResponse::Array(vec![
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
        ])));
    }

    pub async fn handle_table_completion(
        &self,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(tables) = self
            .get_table_completions(position)
            .await
            .unwrap_or_else(|_| Some(CompletionResponse::Array(vec![])))
        {
            return Ok(Some(tables));
        }

        Ok(Some(CompletionResponse::Array(vec![])))
    }

    pub async fn handle_out_of_string_graph_engine_completion(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let mut items: Vec<CompletionItem> = Vec::new();

        for item in self.get_graph_engine_types() {
            items.push(CompletionItem {
                label: item.clone(),
                kind: Some(CompletionItemKind::VALUE),
                insert_text: Some(format!("'{}'", item)),
                ..Default::default()
            });
        }

        return Ok(Some(CompletionResponse::Array(items)));
    }

    pub async fn handle_in_string_graph_engine_completion(
        &self,
        line: &str,
        position: &Position,
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

                for type_ in self.get_graph_engine_types() {
                    if type_.starts_with(typed_prefix) {
                        let insert_text = match (has_closing_quote, has_semicolon) {
                            (true, true) => type_.clone(),
                            (true, false) => format!("{}{}", type_, quote_char),
                            (false, true) => format!("{}{}", type_, quote_char),
                            (false, false) => format!("{}{}", type_, quote_char),
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
                                label: type_.clone(),
                                kind: Some(CompletionItemKind::VALUE),
                                text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                                ..Default::default()
                            });
                        } else {
                            items.push(CompletionItem {
                                label: type_.clone(),
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

    pub fn handle_if_not_exists(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let items = vec![
            CompletionItem {
                label: "IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ];

        Ok(Some(CompletionResponse::Array(items)))
    }

    pub fn handle_create_keywords(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let items = vec![
            CompletionItem {
                label: "AGGREGATE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("AGGREGATE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "AGGREGATE IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("AGGREGATE IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "aggregate".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("aggregate $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "aggregate if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("aggregate if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "FUNCTION".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("FUNCTION $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "FUNCTION IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("FUNCTION IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "function".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("function $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "function if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("function if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "INDEX".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("INDEX $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "INDEX IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("INDEX IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "index".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("index $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "index if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("index if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "KEYSPACE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("KEYSPACE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "KEYSPACE IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("KEYSPACE IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "keyspace".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("keyspace $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "keyspace if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("keyspace if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "MATERIALIZED VIEW".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("MATERIALIZED VIEW $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "MATERIALIZED VIEW IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("MATERIALIZED VIEW IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "materialized view".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("materialized view $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "materialized view if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("materialized view if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "ROLE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("ROLE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "ROLE IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("ROLE IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "role".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("role $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "role if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("role if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "SEARCH INDEX".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("SEARCH INDEX $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "SEARCH INDEX IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("SEARCH INDEX IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "search index".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("search index $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "search index if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("search index if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TABLE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TABLE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TABLE IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TABLE IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "table".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("table $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "table if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("table if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TYPE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TYPE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TYPE IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TYPE IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "type".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("type $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "type if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("type if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "USER".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("USER $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "USER IF NOT EXISTS".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("USER IF NOT EXISTS $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "user".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("user $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "user if not exists".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("user if not exists $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ];

        Ok(Some(CompletionResponse::Array(items)))
    }

    pub fn handle_alter_keywords(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let items = vec![
            CompletionItem {
                label: "KEYSPACE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("KEYSPACE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "KEYSPACE WITH".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("KEYSPACE WITH $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "keyspace".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("keyspace $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "keyspace with".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("keyspace with $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "MATERIALIZED VIEW".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("MATERIALIZED VIEW $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "MATERIALIZED VIEW WITH".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("MATERIALIZED VIEW WITH $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "materialized view".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("materialized view $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "materialized view with".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("materialized view with $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "ROLE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("ROLE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "ROLE WITH".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("ROLE WITH $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "role".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("role $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "role with".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("role with $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TABLE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TABLE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TABLE WITH".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TABLE WITH $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "table".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("table $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "table with".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("table with $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TYPE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TYPE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TYPE WITH".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TYPE WITH $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "type".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("type $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "type with".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("type with $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "USER".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("USER $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "USER WITH".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("USER WITH $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "user".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("user $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "user with".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("user with $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ];

        Ok(Some(CompletionResponse::Array(items)))
    }

    pub fn handle_drop_keywords(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let items = vec![
            CompletionItem {
                label: "AGGREGATE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("AGGREGATE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "aggregate".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("aggregate $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "FUNCTION".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("FUNCTION $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "function".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("function $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "INDEX".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("INDEX $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "index".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("index $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "KEYSPACE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("KEYSPACE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "keyspace".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("keyspace $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "MATERIALIZED VIEW".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("MATERIALIZED VIEW $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "materialized view".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("materialized view $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "ROLE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("ROLE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "role".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("role $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "SEARCH INDEX".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("SEARCH INDEX $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "search index".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("search index $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TABLE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TABLE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "table".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("table $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "TYPE".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("TYPE $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "type".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("type $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "USER".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("USER $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "user".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                insert_text: Some("user $0".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ];

        Ok(Some(CompletionResponse::Array(items)))
    }

    pub async fn handle_drop_aggregate_completions(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let rq = query_aggregates(&self.config).await;

        match rq {
            Ok(r) => {
                let mut items = Vec::<CompletionItem>::new();

                for item in r {
                    items.push(CompletionItem {
                        label: format!("{}.{}", item.keyspace_name, item.aggregate_name),
                        kind: Some(CompletionItemKind::VALUE),
                        insert_text: Some(format!(
                            "{}.{}",
                            item.keyspace_name, item.aggregate_name
                        )),
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    });
                }

                return Ok(Some(CompletionResponse::Array(items)));
            }

            Err(_) => return Ok(Some(CompletionResponse::Array(vec![]))),
        }
    }

    pub async fn handle_drop_function_completions(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let rq = query_functions(&self.config).await;

        match rq {
            Ok(r) => {
                let mut items = Vec::<CompletionItem>::new();

                for item in r {
                    items.push(CompletionItem {
                        label: format!("{}.{}", item.keyspace_name, item.function_name),
                        kind: Some(CompletionItemKind::VALUE),
                        insert_text: Some(format!("{}.{}", item.keyspace_name, item.function_name)),
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    });
                }

                return Ok(Some(CompletionResponse::Array(items)));
            }

            Err(_) => return Ok(Some(CompletionResponse::Array(vec![]))),
        }
    }

    pub async fn handle_drop_index_completions(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let rq = query_indexes(&self.config).await;

        match rq {
            Ok(r) => {
                let mut items = Vec::<CompletionItem>::new();

                for item in r {
                    items.push(CompletionItem {
                        label: format!("{}.{}", item.keyspace_name, item.index_name),
                        kind: Some(CompletionItemKind::VALUE),
                        insert_text: Some(format!("{}.{}", item.keyspace_name, item.index_name)),
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    });
                }

                return Ok(Some(CompletionResponse::Array(items)));
            }

            Err(_) => return Ok(Some(CompletionResponse::Array(vec![]))),
        }
    }

    pub async fn handle_drop_type_completions(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let rq = query_types(&self.config).await;

        match rq {
            Ok(r) => {
                let mut items = Vec::<CompletionItem>::new();

                for item in r {
                    items.push(CompletionItem {
                        label: format!("{}.{}", item.keyspace_name, item.type_name),
                        kind: Some(CompletionItemKind::VALUE),
                        insert_text: Some(format!("{}.{}", item.keyspace_name, item.type_name)),
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    });
                }

                return Ok(Some(CompletionResponse::Array(items)));
            }

            Err(_) => return Ok(Some(CompletionResponse::Array(vec![]))),
        }
    }

    pub async fn handle_drop_view_completions(
        &self,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let rq = query_views(&self.config).await;

        match rq {
            Ok(r) => {
                let mut items = Vec::<CompletionItem>::new();

                for item in r {
                    items.push(CompletionItem {
                        label: format!("{}.{}", item.keyspace_name, item.view_name),
                        kind: Some(CompletionItemKind::VALUE),
                        insert_text: Some(format!("{}.{}", item.keyspace_name, item.view_name)),
                        insert_text_format: Some(InsertTextFormat::SNIPPET),
                        ..Default::default()
                    });
                }

                return Ok(Some(CompletionResponse::Array(items)));
            }

            Err(_) => return Ok(Some(CompletionResponse::Array(vec![]))),
        }
    }
}
