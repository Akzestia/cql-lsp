use log::{info, warn};

use crate::consts::*;
use crate::cqlsh::{self, Column};
use crate::lsp::Backend;
use tower_lsp::lsp_types::*;

impl Backend {
    pub fn is_use_keyspace_line(&self, s: &str) -> bool {
        // use "x";
        if s.len() < 8 {
            return false;
        }

        let input_str: Vec<char> = s.trim().chars().collect();

        let use_statement = String::from_iter(&input_str[0..=2]);

        if use_statement.to_lowercase() != "use" {
            return false;
        }

        if (input_str[3] != '\"'
            && input_str[input_str.len() - 2] != '\"'
            && input_str[input_str.len() - 1] != ';')
            || (input_str[3] != '\"'
                && input_str[input_str.len() - 2] != '\"'
                && input_str[input_str.len() - 1] != ';')
        {
            return false;
        }

        true
    }

    // Works
    pub async fn get_keyspaces(&self) -> Vec<String> {
        let items = cqlsh::query_keyspaces(&self.config).await;

        match items {
            Ok(r) => r.into_iter().collect(),
            Err(_) => {
                vec![]
            }
        }
    }

    // Works
    pub fn should_suggest_keyspaces(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        if let Some(semi_colon_pos) = line.find(&";") {
            if position.character > semi_colon_pos as u32 {
                return false;
            }
        }

        let mut index: usize = 0;
        let mut met_bracket = false;

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let split: Vec<&str> = trimmed_prefix.split(' ').collect();

        while index < position.character as usize {
            if met_bracket
                && (line.chars().nth(index).unwrap_or_else(|| '_') == '"'
                    || line.chars().nth(index).unwrap_or_else(|| '_') == '\'')
            {
                return false;
            }

            if !met_bracket
                && (line.chars().nth(index).unwrap_or_else(|| '_') == '"'
                    || line.chars().nth(index).unwrap_or_else(|| '_') == '\'')
            {
                met_bracket = true;
            }
            index += 1;
        }

        if !split.contains(&"use") {
            return false;
        }

        if split.len() > 1 && split[0] != "use" {
            return false;
        }

        for c in line.chars().enumerate() {
            if c.1 == ';' && c.0 < position.character as usize {
                return false;
            }
        }

        true
    }

    pub fn should_suggest_drop_keyspaces(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_keyspace_kw = lw.contains("keyspace");

        if !contains_drop_kw || !contains_keyspace_kw {
            return false;
        }

        if let Some(ksp_index) = lw.rfind("keyspace") {
            if position.character as usize <= ksp_index + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2 && split[0] == "drop" && split[1] == "keyspace" && !prefix.contains(";")
        {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_aggregate(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_aggregate_kw = lw.contains("aggregate");

        if !contains_drop_kw || !contains_aggregate_kw {
            return false;
        }

        if let Some(ksp_index) = lw.rfind("aggregate") {
            if position.character as usize <= ksp_index + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2
            && split[0] == "drop"
            && split[1] == "aggregate"
            && !prefix.contains(";")
        {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_function(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_function_kw = lw.contains("function");

        if !contains_drop_kw || !contains_function_kw {
            return false;
        }

        if let Some(ksp_function) = lw.rfind("function") {
            if position.character as usize <= ksp_function + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2 && split[0] == "drop" && split[1] == "function" && !prefix.contains(";")
        {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_indexes(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_index_kw = lw.contains("index");

        if !contains_drop_kw || !contains_index_kw {
            return false;
        }

        if let Some(ksp_index) = lw.rfind("index") {
            if position.character as usize <= ksp_index + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2 && split[0] == "drop" && split[1] == "index" && !prefix.contains(";") {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_types(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_type_kw = lw.contains("type");

        if !contains_drop_kw || !contains_type_kw {
            return false;
        }

        if let Some(ksp_type) = lw.rfind("type") {
            if position.character as usize <= ksp_type + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2 && split[0] == "drop" && split[1] == "type" && !prefix.contains(";") {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_views(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_view_kw = lw.contains("view");

        if !contains_drop_kw || !contains_view_kw {
            return false;
        }

        if let Some(ksp_view) = lw.rfind("view") {
            if position.character as usize <= ksp_view + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2 && split[0] == "drop" && split[1] == "view" && !prefix.contains(";") {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_tables(&self, line: &str, position: &Position) -> bool {
        let lw = line.to_lowercase();

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let contains_drop_kw = lw.contains("drop");
        let contains_keyspace_kw = lw.contains("table");

        if !contains_drop_kw || !contains_keyspace_kw {
            return false;
        }

        if let Some(ksp_index) = lw.rfind("table") {
            if position.character as usize <= ksp_index + 8 {
                return false;
            }
        }

        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() >= 2 && split[0] == "drop" && split[1] == "table" && !prefix.contains(";") {
            return true;
        }

        false
    }

    pub fn get_graph_engine_types(&self) -> Vec<String> {
        vec!["Core".to_string(), "Classic".to_string()]
    }

    // Works
    pub fn should_suggest_graph_engine_types(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
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

    pub fn get_available_command_sequences(
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

    pub fn should_suggest_command_sequence(&self, line: &str, position: &Position) -> bool {
        false
    }

    // Works
    pub async fn should_suggest_keywords(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        if prefix.contains("--")
            || prefix.contains("//")
            || prefix.contains("/*")
            || line.ends_with("*/")
        {
            return false;
        }

        if let Some(semi_colon_pos) = line.find(&";") {
            if position.character > semi_colon_pos as u32 {
                return false;
            }
        }

        let lw = line.to_lowercase();

        if lw.contains("use") {
            return false;
        }

        if lw.contains("select") && lw.contains("from") {
            if let Some(from_pos) = line.find(&";") {
                if position.character < (from_pos + 1) as u32 {
                    return false;
                }
            }
        }

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let split: Vec<&str> = trimmed_prefix.split(' ').collect();

        if split.len() > 0 && split[split.len() - 1].contains(";") {
            return false;
        }

        if split.len() >= 2
            && (split[split.len() - 1].contains("from") || split[split.len() - 2].contains("from"))
        {
            return false;
        }

        if line.contains("(") && !line.contains(")") {
            return false;
        }

        if line.contains("(") && line.contains(")") {
            let posx = line.find(&")").unwrap();

            if posx >= position.character as usize {
                return false;
            }
        }

        if lw.contains("drop")
            && (lw.contains("table")
                || lw.contains("index")
                || lw.contains("keyspace")
                || (lw.contains("materialized") && lw.contains("view"))
                || lw.contains("role")
                || (lw.contains("search") && lw.contains("index"))
                || lw.contains("type")
                || lw.contains("user")
                || lw.contains("function")
                || lw.contains("aggregate"))
            && split.len() >= 3
        {
            return false;
        }

        let current = self.current_document.read().await;

        if let Some(ref document_lock) = *current {
            let document = document_lock.read().await;
            let splitx: Vec<&str> = document.text.split('\n').collect();

            if self.is_line_in_multiline_comment_ref(line, position.line as usize, &splitx) {
                return false;
            }

            let mut index_up = position.line as usize;

            while index_up > 0 && index_up < splitx.len() {
                if (!splitx[index_up].contains("(")
                    && KEYWORDS_STRINGS_LWC.contains(&splitx[index_up].to_string()))
                    || splitx[index_up].contains(";")
                {
                    break;
                }

                if splitx[index_up].contains("(") {
                    return false;
                }

                index_up -= 1;
            }

            if index_up < splitx.len() && splitx[index_up].contains("(") {
                return false;
            }
        }

        if lw.contains("create") && lw.contains("if not exists") {
            let mut index = lw.rfind(&"exists").unwrap();
            index += 6;

            if position.character > index as u32
                && (split[split.len() - 1] == "exists" || split[split.len() - 2] == "exists")
            {
                return false;
            }
        }

        if (lw.contains("create") || lw.contains("alter")) && lw.contains("table") {
            let mut index = lw.rfind(&"table").unwrap();
            index += 5;

            if position.character > index as u32
                && (split[split.len() - 1] == "table" || split[split.len() - 2] == "table")
            {
                return false;
            }
        }

        if lw.contains("create") && lw.contains("aggregate") {
            let mut index = lw.rfind(&"aggregate").unwrap();
            index += 9;

            if position.character > index as u32
                && (split[split.len() - 1] == "aggregate" || split[split.len() - 2] == "aggregate")
            {
                return false;
            }
        }

        if lw.contains("create") && lw.contains("function") {
            let mut index = lw.rfind(&"function").unwrap();
            index += 8;

            if position.character > index as u32
                && (split[split.len() - 1] == "function" || split[split.len() - 2] == "function")
            {
                return false;
            }
        }

        if lw.contains("create") && lw.contains("index") {
            let mut index = lw.rfind(&"index").unwrap();
            index += 5;

            if position.character > index as u32
                && (split[split.len() - 1] == "index" || split[split.len() - 2] == "index")
            {
                return false;
            }
        }

        if (lw.contains("create") || lw.contains("alter")) && lw.contains("keyspace") {
            let mut keyspace = lw.rfind(&"keyspace").unwrap();
            keyspace += 8;

            if position.character > keyspace as u32
                && (split[split.len() - 1] == "keyspace" || split[split.len() - 2] == "keyspace")
            {
                return false;
            }
        }

        if (lw.contains("create") || lw.contains("alter")) && lw.contains("view") {
            let mut keyspace = lw.rfind(&"view").unwrap();
            keyspace += 4;

            if position.character > keyspace as u32
                && (split[split.len() - 1] == "view" || split[split.len() - 2] == "view")
            {
                return false;
            }
        }

        if (lw.contains("create") || lw.contains("alter")) && lw.contains("role") {
            let mut keyspace = lw.rfind(&"role").unwrap();
            keyspace += 4;

            if position.character > keyspace as u32
                && (split[split.len() - 1] == "role" || split[split.len() - 2] == "role")
            {
                return false;
            }
        }

        if (lw.contains("create") || lw.contains("alter")) && lw.contains("type") {
            let mut keyspace = lw.rfind(&"type").unwrap();
            keyspace += 4;

            if position.character > keyspace as u32
                && (split[split.len() - 1] == "type" || split[split.len() - 2] == "type")
            {
                return false;
            }
        }

        if (lw.contains("create") || lw.contains("alter")) && lw.contains("user") {
            let mut keyspace = lw.rfind(&"user").unwrap();
            keyspace += 4;

            if position.character > keyspace as u32
                && (split[split.len() - 1] == "user" || split[split.len() - 2] == "user")
            {
                return false;
            }
        }

        /*
            Todo

            Add more complex logic to prevent keywords being suggested inside expressions

            AND age = 23

            AND something * something >= something

            etc.
        */
        if split.len() >= 2
            && (split[split.len() - 1].contains("and") || split[split.len() - 2].contains("and"))
        {
            return false;
        }

        /*
            Todo

            Add more complex logic to prevent keywords being suggested inside expressions

            WHERE age = 23

            WHERE something * something >= something

            etc.
        */
        if split.len() >= 2
            && (split[split.len() - 1].contains("where")
                || split[split.len() - 2].contains("where"))
        {
            return false;
        }

        return true;
    }

    #[warn(unused_mut)]
    pub async fn latest_keyspace(&self, position: &Position) -> Option<String> {
        let current = self.current_document.read().await;

        if let Some(ref document_lock) = *current {
            let document = document_lock.read().await;

            let split: Vec<&str> = document.text.split('\n').collect();

            let mut keyspace_latest: String = "".to_string();
            let mut pos = 0;

            for str in split {
                let index = position.line;
                if index == pos {
                    if keyspace_latest.len() > 0 {
                        return Some(keyspace_latest);
                    }
                    return None;
                }
                pos += 1;

                if self.is_use_keyspace_line(str) {
                    let istr: Vec<char> = str.trim().chars().collect();

                    let extracted_ksp = String::from_iter(&istr[5..istr.len() - 2]);
                    keyspace_latest = extracted_ksp.clone();
                }
            }

            if keyspace_latest.len() > 0 {
                return Some(keyspace_latest);
            }
        }

        None
    }

    pub fn should_field_be_edit(&self, line: &str) -> bool {
        let lower_case = line.to_lowercase();
        let line_split: Vec<&str> = lower_case.split(' ').collect();

        if !line_split.contains(&"from") {
            return true;
        }

        let mut met_from_kw = false;

        for w in line_split {
            if met_from_kw {
                return !w.chars().any(|c| c.is_alphabetic());
            }

            if w == "from" {
                met_from_kw = true;
            }
        }

        true
    }

    pub fn get_start_offset(&self, line: &str, position: &Position) -> u32 {
        let mut index = position.character as usize;

        while index > 0 {
            if let Some(char) = line.chars().nth(index) {
                if char == ' ' {
                    return index as u32;
                }
            }

            index -= 1;
        }

        0
    }

    pub fn column_to_text_edit(&self, column: &Column, lates_keyspace: Option<&str>) -> String {
        let mut result_str: String;

        if let Some(keyspace) = lates_keyspace {
            if keyspace == column.keyspace_name {
                result_str = format!("{}, FROM {};", column.column_name, column.table_name);
            } else {
                result_str = format!(
                    "{}, FROM {}.{};",
                    column.column_name, column.keyspace_name, column.table_name
                );
            }
            return result_str;
        }
        result_str = format!(
            "{}, FROM {}.{};",
            column.column_name, column.keyspace_name, column.table_name
        );
        result_str
    }

    pub async fn get_fields(
        &self,
        line: &str,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let mut tbl_name = "".to_string();

        let lw_line = line.to_lowercase();

        if lw_line.contains("from") {
            let trimmed = lw_line.trim_end();
            let split: Vec<&str> = trimmed.split(' ').collect();
            if !split[split.len() - 1].contains("from") && split[split.len() - 1].len() > 1 {
                let ksp_tbl = split[split.len() - 1].replace(";", "");

                if ksp_tbl.contains(".") {
                    let keyspace_table: Vec<&str> = ksp_tbl.split('.').collect();
                    if keyspace_table.len() == 2 {
                        let ksp = keyspace_table[0];
                        let tbl = keyspace_table[1];

                        let mut items: Vec<Column> = Vec::new();

                        let result =
                            cqlsh::query_hard_scoped_fields(&self.config, &ksp, &tbl).await;
                        match result {
                            Ok(mut r) => {
                                items.append(&mut r);
                            }
                            Err(_) => {}
                        }

                        let mut result: Vec<CompletionItem> = Vec::new();

                        if self.should_field_be_edit(line) {
                            for item in items {
                                if lw_line.contains(&item.column_name.to_lowercase()) {
                                    continue;
                                }

                                let text_edit_str = self.column_to_text_edit(&item, Some(&ksp));

                                let text_edit = TextEdit {
                                    range: Range {
                                        start: Position {
                                            line: position.line,
                                            character: self.get_start_offset(line, position) + 1,
                                        },
                                        end: Position {
                                            line: position.line,
                                            // Insane wierd shit :D
                                            character: line.len() as u32,
                                        },
                                    },
                                    new_text: text_edit_str,
                                };

                                result.push(CompletionItem {
                                    label: format!(
                                        "{} | {}.{}",
                                        item.column_name, item.keyspace_name, item.table_name,
                                    ),
                                    kind: Some(CompletionItemKind::SNIPPET),
                                    text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                                    ..Default::default()
                                });
                            }
                        } else {
                            for item in items {
                                if lw_line.contains(&item.column_name.to_lowercase()) {
                                    continue;
                                }

                                result.push(CompletionItem {
                                    label: format!(
                                        "{} | {}.{}",
                                        item.column_name, item.keyspace_name, item.table_name,
                                    ),
                                    kind: Some(CompletionItemKind::FIELD),
                                    insert_text: Some(format!("{}", item.column_name)),
                                    ..Default::default()
                                });
                            }
                        }

                        let mut x: Vec<CompletionItem> =
                            CQL_NATIVE_FUNCTIONS.iter().cloned().collect();

                        result.append(&mut x);

                        return Ok(Some(CompletionResponse::Array(result)));
                    }
                } else {
                    tbl_name = ksp_tbl;
                }
            }
        }

        if let Some(keyspace) = self.latest_keyspace(position).await {
            let mut items: Vec<Column> = Vec::new();

            if tbl_name != "" {
                let result =
                    cqlsh::query_hard_scoped_fields(&self.config, &keyspace, &tbl_name).await;
                match result {
                    Ok(mut r) => {
                        items.append(&mut r);
                    }
                    Err(_) => {}
                }
            } else {
                items = cqlsh::query_keyspace_scoped_fields(&self.config, &keyspace)
                    .await
                    .unwrap_or_else(|_| vec![]);
            }

            let mut result: Vec<CompletionItem> = Vec::new();

            if self.should_field_be_edit(line) {
                for item in items {
                    if lw_line.contains(&item.column_name.to_lowercase()) {
                        continue;
                    }
                    let text_edit_str = self.column_to_text_edit(&item, Some(&keyspace));

                    let text_edit = TextEdit {
                        range: Range {
                            start: Position {
                                line: position.line,
                                character: self.get_start_offset(line, position) + 1,
                            },
                            end: Position {
                                line: position.line,
                                // Insane wierd shit :D
                                character: line.len() as u32,
                            },
                        },
                        new_text: text_edit_str,
                    };

                    result.push(CompletionItem {
                        label: format!(
                            "{} | {}.{}",
                            item.column_name, item.keyspace_name, item.table_name,
                        ),
                        kind: Some(CompletionItemKind::SNIPPET),
                        text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                        ..Default::default()
                    });
                }
            } else {
                for item in items {
                    if lw_line.contains(&item.column_name.to_lowercase()) {
                        continue;
                    }

                    result.push(CompletionItem {
                        label: format!(
                            "{} | {}.{}",
                            item.column_name, item.keyspace_name, item.table_name,
                        ),
                        kind: Some(CompletionItemKind::FIELD),
                        insert_text: Some(format!("{}", item.column_name)),
                        ..Default::default()
                    });
                }
            }

            let mut x: Vec<CompletionItem> = CQL_NATIVE_FUNCTIONS.iter().cloned().collect();

            result.append(&mut x);
            return Ok(Some(CompletionResponse::Array(result)));
        }

        /*
            Text Edit

            line.len() == position.character;
            SELECT id FROM ;
            SELECT name ;

            Insert Text

            ... FROM keyspace_name.table_name;
        */

        let items = cqlsh::query_g_fields(&self.config)
            .await
            .unwrap_or_else(|_| vec![]);

        let mut result: Vec<CompletionItem> = Vec::new();

        if self.should_field_be_edit(line) {
            for item in items {
                if lw_line.contains(&item.column_name.to_lowercase()) {
                    continue;
                }
                let text_edit_str = self.column_to_text_edit(&item, None);

                let text_edit = TextEdit {
                    range: Range {
                        start: Position {
                            line: position.line,
                            character: self.get_start_offset(line, position) + 1,
                        },
                        end: Position {
                            line: position.line,
                            character: line.len() as u32,
                        },
                    },
                    new_text: text_edit_str,
                };

                result.push(CompletionItem {
                    label: format!(
                        "{} | {}.{}",
                        item.column_name, item.keyspace_name, item.table_name,
                    ),
                    kind: Some(CompletionItemKind::SNIPPET),
                    text_edit: Some(CompletionTextEdit::Edit(text_edit)),
                    ..Default::default()
                });
            }
        } else {
            for item in items {
                if lw_line.contains(&item.column_name.to_lowercase()) {
                    continue;
                }
                result.push(CompletionItem {
                    label: format!(
                        "{} | {}.{}",
                        item.column_name, item.keyspace_name, item.table_name,
                    ),
                    kind: Some(CompletionItemKind::VALUE),
                    insert_text: Some(format!("{}", item.column_name)),
                    ..Default::default()
                });
            }
        }

        let mut x: Vec<CompletionItem> = CQL_NATIVE_FUNCTIONS.iter().cloned().collect();

        result.append(&mut x);
        Ok(Some(CompletionResponse::Array(result)))
    }

    // Works
    pub fn should_suggest_fields(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let splitted: Vec<&str> = trimmed_prefix.split(' ').collect();

        if !splitted.contains(&"select") || splitted.contains(&"*") || splitted.contains(&"from") {
            return false;
        }

        if splitted.contains(&"select") && splitted.len() == 1 {
            return true;
        }

        if splitted.len() > 2 && !splitted[splitted.len() - 2].contains(",") {
            return false;
        }

        if splitted.len() > 0
            && trimmed_prefix.len() != prefix.len()
            && !splitted[splitted.len() - 1].contains(",")
        {
            return false;
        }

        true
    }

    // Works
    pub fn should_suggest_from(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let splitted: Vec<&str> = trimmed_prefix.split(' ').collect();

        if !splitted.contains(&"select")
            || splitted.contains(&"from")
            || line.to_lowercase().contains("from")
        {
            return false;
        }

        if splitted.len() == 1
            && splitted.contains(&"select")
            && trimmed_prefix.len() != prefix.len()
        {
            return false;
        }

        if splitted.len() == 2
            && splitted.contains(&"select")
            && trimmed_prefix.len() == prefix.len()
        {
            return false;
        }

        if splitted.len() >= 3
            && splitted.contains(&"select")
            && splitted[splitted.len() - 1].contains(",")
        {
            return false;
        }

        true
    }

    pub async fn get_table_completions(
        &self,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(keyspace) = self.latest_keyspace(&position).await {
            let tables = cqlsh::query_keyspace_scoped_tables(&self.config, &keyspace)
                .await
                .unwrap_or_else(|_| vec![]);

            let tables_unscoped = cqlsh::query_g_tables(&self.config)
                .await
                .unwrap_or_else(|_| vec![]);

            let mut items = Vec::<CompletionItem>::new();

            for table in tables {
                items.push(CompletionItem {
                    label: table.table_name.clone(),
                    // Keyword to display scoped tables in different color
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some(format!("{}", table.united())),
                    insert_text: Some(format!(r#"{}"#, table.table_name)),
                    insert_text_format: Some(InsertTextFormat::SNIPPET),
                    ..Default::default()
                })
            }

            for tablex in tables_unscoped {
                items.push(CompletionItem {
                    label: tablex.united(),
                    kind: Some(CompletionItemKind::VARIABLE),
                    detail: Some(format!("{}", tablex.united())),
                    insert_text: Some(format!(r#"{}"#, tablex.united())),
                    insert_text_format: Some(InsertTextFormat::SNIPPET),
                    ..Default::default()
                })
            }

            return Ok(Some(CompletionResponse::Array(items)));
        }

        let tables = cqlsh::query_g_tables(&self.config)
            .await
            .unwrap_or_else(|_| vec![]);

        let mut items = Vec::<CompletionItem>::new();

        for table in tables {
            items.push(CompletionItem {
                label: table.united(),
                kind: Some(CompletionItemKind::VARIABLE),
                detail: Some(format!("{}", table.united())),
                insert_text: Some(format!(r#"{}"#, table.united())),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            })
        }

        return Ok(Some(CompletionResponse::Array(items)));
    }

    pub async fn is_inside_create_table_no_position(
        &self,
        line_index: usize,
        document_url: &Url,
    ) -> bool {
        let documents = self.documents.read().await;

        if let Some(document) = documents.get(document_url) {
            let lw_doc_text = document;
            let lines: Vec<&str> = lw_doc_text.split('\n').collect();

            let current_line = line_index;
            if current_line >= lines.len() {
                return false;
            }

            let mut found_create_table = false;
            let mut search_index = current_line;

            loop {
                let line_content = lines[search_index].to_lowercase();

                if (line_content.contains("create table")
                    || line_content.contains("create table if not exists"))
                    && line_content.contains("(")
                    && !line_content.contains(")")
                {
                    info!("Found CRT: {}", line_content);
                    found_create_table = true;
                    break;
                }

                if self.line_contains_cql_kw(&line_content) {
                    return false;
                }

                if search_index == 0 {
                    break;
                }
                search_index -= 1;
            }

            if !found_create_table {
                return false;
            }

            for i in (current_line + 1)..lines.len() {
                let line_content = lines[i];

                if self.line_contains_cql_kw(line_content) {
                    return false;
                }

                if line_content.contains(")") {
                    return true;
                }
            }
        }

        false
    }

    pub async fn is_inside_create_type_no_position(
        &self,
        line_index: usize,
        document_url: &Url,
    ) -> bool {
        let documents = self.documents.read().await;

        if let Some(document) = documents.get(document_url) {
            let lw_doc_text = document;
            let lines: Vec<&str> = lw_doc_text.split('\n').collect();

            let current_line = line_index;
            if current_line >= lines.len() {
                return false;
            }

            let mut found_create_table = false;
            let mut search_index = current_line;

            loop {
                let line_content = lines[search_index].to_lowercase();

                if (line_content.contains("create type")
                    || line_content.contains("create type if not exists"))
                    && line_content.contains("(")
                    && !line_content.contains(")")
                {
                    info!("Found CRT: {}", line_content);
                    found_create_table = true;
                    break;
                }

                if self.line_contains_cql_kw(&line_content) {
                    return false;
                }

                if search_index == 0 {
                    break;
                }
                search_index -= 1;
            }

            if !found_create_table {
                return false;
            }

            for i in (current_line + 1)..lines.len() {
                let line_content = lines[i];

                if self.line_contains_cql_kw(line_content) {
                    return false;
                }

                if line_content.contains(")") {
                    return true;
                }
            }
        }

        false
    }

    pub async fn is_inside_create_table(
        &self,
        line: &str,
        position: &Position,
        document_url: &Url,
    ) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };
        let lw = prefix.to_lowercase();
        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() < 2 {
            return false;
        }

        if split[0] == "create"
            && split[1] == "table"
            && line.contains("(")
            && line.contains(")")
            && (prefix.contains("(") && !prefix.contains(")"))
        {
            return true;
        }

        let documents = self.documents.read().await;

        if let Some(document) = documents.get(document_url) {
            let lw_doc_text = document;
            let lines: Vec<&str> = lw_doc_text.split('\n').collect();

            let current_line = position.line as usize;
            if current_line >= lines.len() {
                return false;
            }

            let mut found_create_table = false;
            let mut search_index = current_line;

            loop {
                let line_content = lines[search_index].to_lowercase();

                if (line_content.contains("create table")
                    || line_content.contains("create table if not exists"))
                    && line_content.contains("(")
                    && !line_content.contains(")")
                {
                    info!("Found CRT: {}", line_content);
                    found_create_table = true;
                    break;
                }

                if self.line_contains_cql_kw(&line_content) {
                    return false;
                }

                if search_index == 0 {
                    break;
                }
                search_index -= 1;
            }

            if !found_create_table {
                return false;
            }

            for i in (current_line + 1)..lines.len() {
                let line_content = lines[i];

                if self.line_contains_cql_kw(line_content) {
                    return false;
                }

                if line_content.contains(")") {
                    return true;
                }
            }
        }

        false
    }

    pub async fn should_suggest_types_completions(
        &self,
        line: &str,
        position: &Position,
        document_url: &Url,
    ) -> bool {
        if !self
            .is_inside_create_table(line, position, document_url)
            .await
        {
            return false;
        }

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim();
        let split: Vec<&str> = trimmed_prefix.split(' ').collect();

        match split.len() {
            0 => false,
            1 => prefix.ends_with(' '),
            2 => !prefix.ends_with(' '),
            _ => false,
        }
    }

    /*
        [field_name] [type] [type_modifier]

        name TEXT [modifier]
        name TEXT PRIVATE KEY
        name TEXT static
    */
    pub async fn should_suggest_type_modifiers(
        &self,
        line: &str,
        position: &Position,
        document_url: &Url,
    ) -> bool {
        if !self
            .is_inside_create_table(line, position, document_url)
            .await
        {
            return false;
        }

        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim().to_lowercase();
        let split: Vec<&str> = trimmed_prefix.split(' ').collect();

        match split.len() {
            0 => false,
            2 => prefix.ends_with(' ') && CQL_TYPES_LWC.contains(&split[1].to_string()),
            3 => {
                (!prefix.ends_with(' ') && CQL_TYPES_LWC.contains(&split[1].to_string()))
                    || (prefix.ends_with(' ')
                        && CQL_TYPES_LWC.contains(&split[1].to_string())
                        && split[2] == "primary")
            }
            4 => {
                !prefix.ends_with(' ')
                    && CQL_TYPES_LWC.contains(&split[1].to_string())
                    && split[2] == "primary"
            }
            _ => false,
        }
    }

    // Works
    pub fn should_suggest_table_completions(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };
        if let Some(semi_colon_pos) = line.find(&";") {
            if position.character > semi_colon_pos as u32 {
                return false;
            }
        }
        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let splitted: Vec<&str> = trimmed_prefix.split(' ').collect();

        if splitted.len() <= 2 && splitted[0].contains("update") {
            return true;
        }

        if splitted.len() >= 2
            && (splitted[splitted.len() - 2].contains("insert")
                || splitted[splitted.len() - 1].contains("into"))
        {
            return true;
        }

        if splitted.len() >= 2
            && ((splitted[0].contains("drop") && splitted[1].contains("table"))
                && ((splitted[splitted.len() - 2].contains("drop")
                    && splitted[splitted.len() - 1].contains("table"))
                    || (splitted.len() > 2
                        && splitted[splitted.len() - 3].contains("drop")
                        && splitted[splitted.len() - 2].contains("table")
                        && trimmed_prefix.len() == prefix.len())))
        {
            return true;
        }

        if splitted.len() >= 3
            && ((splitted[splitted.len() - 2].contains("insert")
                || splitted[splitted.len() - 1].contains("into"))
                || (splitted[splitted.len() - 3].contains("insert")
                    || splitted[splitted.len() - 2].contains("into")))
        {
            return true;
        }

        if !splitted.contains(&"select") && !splitted.contains(&"from") {
            return false;
        }
        if splitted.len() >= 2
            && !splitted[splitted.len() - 2].contains("from")
            && !splitted[splitted.len() - 1].contains("from")
        {
            return false;
        }
        if splitted.len() >= 2
            && splitted[splitted.len() - 2].contains("from")
            && trimmed_prefix.len() != prefix.len()
        {
            return false;
        }
        true
    }

    pub fn should_suggest_if_not_exists(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let lw = prefix.to_lowercase();
        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() < 2 {
            return false;
        }

        if split.contains(&"create")
            && ((split[split.len() - 1].to_lowercase() == "table"
                || split[split.len() - 2].to_lowercase() == "table")
                || (split[split.len() - 1].to_lowercase() == "view"
                    || split[split.len() - 2].to_lowercase() == "view")
                || (split[split.len() - 1].to_lowercase() == "keyspace"
                    || split[split.len() - 2].to_lowercase() == "keyspace")
                || (split[split.len() - 1].to_lowercase() == "aggregate"
                    || split[split.len() - 2].to_lowercase() == "aggregate")
                || (split[split.len() - 1].to_lowercase() == "function"
                    || split[split.len() - 2].to_lowercase() == "function")
                || (split[split.len() - 1].to_lowercase() == "index"
                    || split[split.len() - 2].to_lowercase() == "index")
                || (split[split.len() - 1].to_lowercase() == "role"
                    || split[split.len() - 2].to_lowercase() == "role")
                || (split[split.len() - 1].to_lowercase() == "type"
                    || split[split.len() - 2].to_lowercase() == "type")
                || (split[split.len() - 1].to_lowercase() == "user")
                || split[split.len() - 2].to_lowercase() == "user")
        {
            return true;
        }

        false
    }

    pub fn should_suggest_create_keywords(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let lw = prefix.to_lowercase();
        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() < 1 {
            return false;
        }

        if split[0] == "create" && split.len() <= 2 {
            return true;
        }

        false
    }

    pub fn should_suggest_alter_keywords(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let lw = prefix.to_lowercase();
        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() < 1 {
            return false;
        }

        if split[0] == "alter" && split.len() <= 2 {
            return true;
        }

        false
    }

    pub fn should_suggest_drop_keywords(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let lw = prefix.to_lowercase();
        let split: Vec<&str> = lw.split(' ').collect();

        if split.len() < 1 {
            return false;
        }

        if split[0] == "drop" && split.len() <= 2 {
            return true;
        }

        false
    }

    pub fn should_edit_select_statement(&self, line: &str, lines: &Vec<String>) -> bool {
        false
    }
}
