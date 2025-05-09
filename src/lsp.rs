use log::{debug, info, warn};

use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use std::collections::HashMap;
use tokio::sync::RwLock;

use crate::consts::*;
use crate::cqlsh::{self, Column, CqlSettings};

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
    // -----------------------------[Helper Functions]-----------------------------

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

    // -----------------------------[Formatting]-----------------------------

    fn remove_leading_spaces_wildcards(&self, line: &mut String) {
        let mut index = 0;
        let mut met_space = false;

        while index < line.len() {
            if !met_space && line.chars().nth(index).unwrap_or_else(|| '_') == ' ' {
                met_space = true;
            }

            if met_space && line.chars().nth(index).unwrap_or_else(|| '_') != ' ' {
                met_space = false;
            }

            if met_space
                && index != line.len() - 1
                && (line.chars().nth(index + 1).unwrap_or_else(|| '_') == ' '
                    || line.chars().nth(index + 1).unwrap_or_else(|| '_') == ';'
                    || line.chars().nth(index + 1).unwrap_or_else(|| '_') == ','
                    || line.chars().nth(index + 1).unwrap_or_else(|| '_') == ')'
                    || line.chars().nth(index + 1).unwrap_or_else(|| '_') == '>')
            {
                line.remove(index);
                met_space = false;
                if index >= 2 {
                    index -= 2;
                } else {
                    index -= 1;
                }
            }

            index += 1;
        }
    }

    fn remove_tailing_spaces_wildcards(&self, line: &mut String) {
        let mut index = 0;
        let mut met_wild_card = false;

        while index < line.len() {
            if !met_wild_card
                && (line.chars().nth(index).unwrap_or_else(|| '_') == '('
                    || line.chars().nth(index).unwrap_or_else(|| '_') == '<')
            {
                met_wild_card = true;
            }

            if met_wild_card
                && line.chars().nth(index).unwrap_or_else(|| '_') != '('
                && line.chars().nth(index).unwrap_or_else(|| '_') != '<'
            {
                met_wild_card = false;
            }

            if met_wild_card
                && index != line.len() - 1
                && line.chars().nth(index + 1).unwrap_or_else(|| '_') == ' '
            {
                line.remove(index + 1);
                met_wild_card = false;
                if index >= 2 {
                    index -= 2;
                } else {
                    index -= 1;
                }
            }

            index += 1;
        }
    }

    fn is_string_literal(&self, line: &str) -> bool {
        let mut met_v1_op_bracket = false;
        let mut met_v2_op_bracket = false;

        for char in line.chars() {
            if met_v1_op_bracket && char == '\'' {
                return true;
            }

            if met_v2_op_bracket && char == '"' {
                return true;
            }

            if !met_v1_op_bracket && char == '\'' {
                met_v1_op_bracket = true;
            }
            if !met_v2_op_bracket && char == '"' {
                met_v2_op_bracket = true;
            }

            if met_v1_op_bracket && char == '"' {
                return false;
            }

            if met_v2_op_bracket && char == '\'' {
                return false;
            }
        }

        false
    }

    fn fix_string_literals(&self, lines: &mut Vec<String>) {
        for line in lines.iter_mut() {
            let mut position = 0;
            while position < line.len() {
                if let Some(start) = line[position..].find('"').map(|p| p + position) {
                    if let Some(end) = line[start + 1..].find('"').map(|p| p + start + 1) {
                        let str = String::from(&line[start + 1..end]);
                        let trimmed = str.trim();
                        line.replace_range(start + 1..end, trimmed);
                        position = end + 1;
                    } else {
                        position = start + 1;
                    }
                } else if let Some(start) = line[position..].find('\'').map(|p| p + position) {
                    if let Some(end) = line[start + 1..].find('\'').map(|p| p + start + 1) {
                        let str = String::from(&line[start + 1..end]);
                        let trimmed = str.trim();
                        line.replace_range(start + 1..end, trimmed);
                        position = end + 1;
                    } else {
                        position = start + 1;
                    }
                } else {
                    break;
                }
            }
        }
    }

    /*
        Removes spaces before ;
    */
    fn fix_semi_colon(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        while index < lines.len() {
            self.remove_leading_spaces_wildcards(&mut lines[index]);
            self.remove_tailing_spaces_wildcards(&mut lines[index]);
            index += 1;
        }
    }

    /*
        Removes duplicates of ;
    */
    fn fix_duplicate_semi_colon(&self, line: &mut String) {
        let mut last_colon = false;
        let mut index = 0;

        /*
            The reason for using unwrap_or_else is
            that when line contains Japanese (non-standart range ASCII)
            the line.len() isn't represented correctly and will lead
            to out of bounds access
        */
        while index < line.len() {
            if !last_colon && line.chars().nth(index).unwrap_or_else(|| '_') == ';' {
                last_colon = true;
            } else if last_colon && line.chars().nth(index).unwrap_or_else(|| '_') == ';' {
                line.remove(index);
                last_colon = false;
                if index >= 2 {
                    index -= 2;
                } else {
                    index -= 1;
                }
            } else if line.chars().nth(index).unwrap_or_else(|| '_') != ';' {
                last_colon = false;
            }
            index += 1;
        }
    }

    // Removes any duplicate spaces
    fn fix_spacing(&self, line: &mut String) {
        let mut last_space = false;
        let mut index = 0;

        /*
            The reason for using unwrap_or_else is
            that when line contains Japanese (non-standart range ASCII)
            the line.len() isn't represented correctly and will lead
            to out of bounds access
        */
        while index < line.len() {
            if !last_space && line.chars().nth(index).unwrap_or_else(|| '_') == ' ' {
                last_space = true;
            } else if last_space && line.chars().nth(index).unwrap_or_else(|| '_') == ' ' {
                line.remove(index);
                last_space = false;
                if index >= 2 {
                    index -= 2;
                } else {
                    index -= 1;
                }
            } else if line.chars().nth(index).unwrap_or_else(|| '_') != ' ' {
                last_space = false;
            }
            index += 1;
        }
    }

    // Removes \n after \n or ( )
    fn fix_new_lines(&self, lines: &mut Vec<String>) {
        let mut index = 0;
        let mut last_new_line = false;
        let mut last_bracket = false;

        while index < lines.len() {
            if last_new_line && lines[index].len() == 0 {
                lines.remove(index);
                if index >= 2 {
                    index -= 2;
                } else if index > 0 {
                    index -= 1;
                }
            }

            if last_bracket && lines[index].len() == 0 {
                lines.remove(index);
                if index >= 2 {
                    index -= 2;
                } else if index > 0 {
                    index -= 1;
                }
            }

            if lines[index].len() == 0 {
                last_new_line = true;
            } else {
                last_new_line = false;
            }

            if lines[index].contains(&"(") {
                last_bracket = true;
            } else {
                last_bracket = false
            }

            index += 1;
        }
    }

    /*
        Removes all '\n' inside code_blocks
    */
    fn remove_new_lines_from_code_block(&self, lines: &mut Vec<String>) {
        let mut index = 0;
        let mut inside_code_block = false;

        while index < lines.len() {
            let line = lines[index].to_lowercase();

            if !inside_code_block && line.len() > 0 && !line.contains(&";") {
                inside_code_block = true;
            }

            if inside_code_block && line.contains(&";") {
                inside_code_block = false;
            }

            if inside_code_block && line.len() == 0 {
                lines.remove(index);
                if index >= 2 {
                    index -= 2;
                } else if index > 0 {
                    index -= 1;
                }
            }

            index += 1;
        }
    }

    /*
        Adds missing semi colon to the and of CQL command

        The list of Keywords that start CQL commands is strored inside
        CQL_KEYWORDS_LWC | LWC - lower_case
    */
    fn apply_semi_colon(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        while index < lines.len() {
            let line = lines[index].to_lowercase();

            if index + 1 != lines.len()
                && line.len() > 0
                && !line.contains(&";")
                && !line.contains(&"begin")
            {
                let lw = lines[index + 1].to_lowercase();
                let split: Vec<&str> = lw.split(' ').collect();
                if lines[index + 1].to_lowercase().len() == 0
                    || CQL_KEYWORDS_LWC.contains(&split[0].to_string())
                {
                    lines[index].push(';');
                }
            }

            if index == lines.len() - 1 && line.len() > 0 && !line.contains(&";") {
                lines[index].push(';');
            }

            index += 1;
        }
    }

    fn add_spacing_new_lines(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        while index < lines.len() {
            if index + 1 != lines.len()
                && (lines[index].contains(&";") || lines[index].to_lowercase().contains(&"begin"))
                && lines[index + 1].len() > 0
            {
                lines.insert(index + 1, "".to_string());
            }

            index += 1;
        }
    }

    fn add_spacing_after_comma(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        while index < lines.len() {
            for idx in 0..lines[index].len() {
                if idx + 1 != lines[index].len()
                    && lines[index].chars().nth(idx).unwrap_or_else(|| '_') == ','
                    && lines[index].chars().nth(idx + 1).unwrap_or_else(|| '_') != ' '
                {
                    lines[index].insert(idx + 1, ' ');
                }
            }

            index += 1;
        }
    }

    fn contains_styled_trigger_kw_create_table(&self, line: &str) -> bool {
        line.contains(&"create") && line.contains(&"table") && line.contains(&"(")
    }

    fn contains_styled_trigger_kw_insert_into(&self, line: &str) -> bool {
        line.contains(&"insert") && line.contains(&"(")
    }

    fn contains_styled_trigger_kw_values(&self, line: &str) -> bool {
        line.contains(&"values") && line.contains(&"(")
    }

    /*
        ISERT INTO ()
        CREATE TABLE ()
        VALUES ()

        Converets

        CREATE TABLE IF NOT EXISTS table_name ( name type, ...);

        Into

        CREATE TABLE IF NOT EXISTS table_name
        (
            name        type,
            long_name   type,
            sname       type

            PRIMARY KEY ()
        );

        etc.
    */
    fn style_format(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        let mut working_buf = Vec::<&str>::new();

        while index < lines.len() {
            if self.contains_styled_trigger_kw_create_table(&lines[index].to_lowercase()) {
                index += 1;
                continue;
            }
            if self.contains_styled_trigger_kw_insert_into(&lines[index].to_lowercase()) {
                index += 1;
                continue;
            }
            if self.contains_styled_trigger_kw_values(&lines[index].to_lowercase()) {
                index += 1;
                continue;
            }

            index += 1;
        }
    }

    async fn format_file(&self, lines: &Vec<&str>) -> Vec<TextEdit> {
        let mut edits = Vec::<TextEdit>::new();
        let mut working_vec: Vec<String> = lines.into_iter().map(|s| s.to_string()).collect();

        for index in 0..working_vec.len() {
            working_vec[index] = working_vec[index].trim().to_string();
            self.fix_spacing(&mut working_vec[index]);
            self.fix_duplicate_semi_colon(&mut working_vec[index]);
        }

        self.fix_semi_colon(&mut working_vec);
        self.fix_string_literals(&mut working_vec);
        self.fix_new_lines(&mut working_vec);
        self.remove_new_lines_from_code_block(&mut working_vec);
        self.apply_semi_colon(&mut working_vec);
        self.add_spacing_new_lines(&mut working_vec);
        self.add_spacing_after_comma(&mut working_vec);

        let idx = working_vec.len() - 1;

        for (index, line) in working_vec.into_iter().enumerate() {
            let end_char_pos: u32;

            if index >= lines.len() {
                end_char_pos = line.len() as u32;
            } else {
                end_char_pos = lines[index].len() as u32;
            }

            let text_edit = TextEdit {
                range: Range {
                    start: Position {
                        line: index as u32,
                        character: 0,
                    },
                    end: Position {
                        line: index as u32,
                        character: end_char_pos,
                    },
                },
                new_text: line,
            };

            edits.push(text_edit);
        }

        if idx < lines.len() {
            let text_edit = TextEdit {
                range: Range {
                    start: Position {
                        line: idx as u32,
                        character: lines[idx].len() as u32,
                    },
                    end: Position {
                        line: lines.len() as u32 - 1,
                        character: lines[lines.len() - 1].len() as u32,
                    },
                },
                new_text: "".to_string(),
            };
            edits.push(text_edit);
        }

        edits
    }

    // -----------------------------[Completions]-----------------------------

    fn is_use_keyspace_line(&self, s: &str) -> bool {
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
    async fn get_keyspaces(&self) -> Vec<String> {
        let items = cqlsh::query_keyspaces(&self.config).await;

        match items {
            Ok(r) => r.into_iter().collect(),
            Err(e) => {
                vec![]
            }
        }
    }

    // Works
    fn should_suggest_keyspaces(&self, line: &str, position: &Position) -> bool {
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

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let split: Vec<&str> = trimmed_prefix.split(' ').collect();

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

    fn get_graph_engine_types(&self) -> Vec<String> {
        vec!["Core".to_string(), "Classic".to_string()]
    }

    // Works
    fn should_suggest_graph_engine_types(&self, line: &str, position: &Position) -> bool {
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

    fn should_suggest_command_sequence(&self, line: &str, position: &Position) -> bool {
        false
    }

    // Works
    async fn should_suggest_keywords(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        if let Some(semi_colon_pos) = line.find(&";") {
            if position.character > semi_colon_pos as u32 {
                return false;
            }
        }

        if line.to_lowercase().contains(&"use") {
            return false;
        }

        if line.to_lowercase().contains(&"select") && line.to_lowercase().contains(&"from") {
            if let Some(from_pos) = line.find(&";") {
                if position.character < (from_pos + 1) as u32 {
                    return false;
                }
            }
        }

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let split: Vec<&str> = trimmed_prefix.split(' ').collect();

        if split.len() > 0 && split[split.len() - 1].contains(&";") {
            return false;
        }

        if split.len() >= 2
            && (split[split.len() - 1].contains(&"from")
                || split[split.len() - 2].contains(&"from"))
        {
            return false;
        }

        if line.contains(&"(") && !line.contains(&")") {
            return false;
        }

        if line.contains(&"(") && line.contains(&")") {
            let posx = line.rfind(&")").unwrap();

            if posx >= position.character as usize {
                return false;
            }
        }

        let current = self.current_document.read().await;

        if let Some(ref document_lock) = *current {
            let document = document_lock.read().await;
            let splitx: Vec<&str> = document.text.split('\n').collect();

            let mut index_up = position.line as usize;

            while index_up > 0 && index_up < splitx.len() {
                if (!splitx[index_up].contains(&"(")
                    && KEYWORDS_STRINGS_LWC.contains(&splitx[index_up].to_string()))
                    || splitx[index_up].contains(&";")
                {
                    break;
                }

                if splitx[index_up].contains(&"(") {
                    return false;
                }

                index_up -= 1;
            }

            if index_up < splitx.len() && splitx[index_up].contains(&"(") {
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
            && (split[split.len() - 1].contains(&"and") || split[split.len() - 2].contains(&"and"))
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
            && (split[split.len() - 1].contains(&"where")
                || split[split.len() - 2].contains(&"where"))
        {
            return false;
        }

        return true;
    }

    #[warn(unused_mut)]
    async fn latest_keyspace(&self, position: &Position) -> Option<String> {
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
                    let trimeed = str.replace(' ', "");
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

    fn should_field_be_edit(&self, line: &str) -> bool {
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

    fn get_start_offset(&self, line: &str, position: &Position) -> u32 {
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

    fn column_to_text_edit(
        &self,
        line: &str,
        column: &Column,
        lates_keyspace: Option<&str>,
    ) -> String {
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

    async fn get_fields(
        &self,
        line: &str,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        let mut tbl_name = "".to_string();

        let lw_line = line.to_lowercase();

        if lw_line.contains(&"from") {
            let trimmed = lw_line.trim_end();
            let split: Vec<&str> = trimmed.split(' ').collect();
            if !split[split.len() - 1].contains(&"from") && split[split.len() - 1].len() > 1 {
                let ksp_tbl = split[split.len() - 1].replace(";", "");

                if ksp_tbl.contains(&".") {
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

                                let text_edit_str =
                                    self.column_to_text_edit(line, &item, Some(&ksp));

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
                    let text_edit_str = self.column_to_text_edit(line, &item, Some(&keyspace));

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
            .unwrap_or_else(|e| vec![]);

        let mut result: Vec<CompletionItem> = Vec::new();

        if self.should_field_be_edit(line) {
            for item in items {
                if lw_line.contains(&item.column_name.to_lowercase()) {
                    continue;
                }
                let text_edit_str = self.column_to_text_edit(line, &item, None);

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
    fn should_suggest_fields(&self, line: &str, position: &Position) -> bool {
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

        if splitted.len() > 2 && !splitted[splitted.len() - 2].contains(&",") {
            return false;
        }

        if splitted.len() > 0
            && trimmed_prefix.len() != prefix.len()
            && !splitted[splitted.len() - 1].contains(&",")
        {
            return false;
        }

        true
    }

    // Works
    fn should_suggest_from(&self, line: &str, position: &Position) -> bool {
        let prefix = match line.get(..position.character as usize) {
            Some(p) => p,
            None => return false,
        };

        let trimmed_prefix = prefix.trim_end().to_lowercase();
        let splitted: Vec<&str> = trimmed_prefix.split(' ').collect();

        if !splitted.contains(&"select")
            || splitted.contains(&"from")
            || line.to_lowercase().contains(&"from")
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
            && splitted[splitted.len() - 1].contains(&",")
        {
            return false;
        }

        true
    }

    async fn get_table_completions(
        &self,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(keyspace) = self.latest_keyspace(&position).await {
            let tables = cqlsh::query_keyspace_scoped_tables(&self.config, &keyspace)
                .await
                .unwrap_or_else(|e| vec![]);

            let tables_unscoped = cqlsh::query_g_tables(&self.config)
                .await
                .unwrap_or_else(|e| vec![]);

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
            .unwrap_or_else(|e| vec![]);

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

    // Works
    fn should_suggest_table_completions(&self, line: &str, position: &Position) -> bool {
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

        if splitted.len() >= 2
            && (splitted[splitted.len() - 2].contains(&"insert")
                || splitted[splitted.len() - 1].contains(&"into"))
        {
            return true;
        }

        if splitted.len() >= 3
            && ((splitted[splitted.len() - 2].contains(&"insert")
                || splitted[splitted.len() - 1].contains(&"into"))
                || (splitted[splitted.len() - 3].contains(&"insert")
                    || splitted[splitted.len() - 2].contains(&"into")))
        {
            return true;
        }

        if !splitted.contains(&"select") && !splitted.contains(&"from") {
            return false;
        }
        if splitted.len() >= 2
            && !splitted[splitted.len() - 2].contains(&"from")
            && !splitted[splitted.len() - 1].contains(&"from")
        {
            return false;
        }
        if splitted.len() >= 2
            && splitted[splitted.len() - 2].contains(&"from")
            && trimmed_prefix.len() != prefix.len()
        {
            return false;
        }
        true
    }

    // -----------------------------[Handlers]-----------------------------

    async fn handle_in_string_keyspace_completion(
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

    async fn handle_out_of_string_keyspace_completion(
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

    fn handle_keywords_completion(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        return Ok(Some(CompletionResponse::Array(
            KEYWORDS.iter().cloned().collect(),
        )));
    }

    fn handle_types_completion(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        return Ok(Some(CompletionResponse::Array(
            TYPES.iter().cloned().collect(),
        )));
    }

    async fn handle_fields_completion(
        &self,
        line: &str,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(response) = self
            .get_fields(line, position)
            .await
            .unwrap_or_else(|e| Some(CompletionResponse::Array(vec![])))
        {
            return Ok(Some(response));
        }

        return Ok(Some(CompletionResponse::Array(vec![])));
    }

    fn handle_from_completion(&self) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
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

    async fn handle_table_completion(
        &self,
        position: &Position,
    ) -> tower_lsp::jsonrpc::Result<Option<CompletionResponse>> {
        if let Some(tables) = self
            .get_table_completions(position)
            .await
            .unwrap_or_else(|e| Some(CompletionResponse::Array(vec![])))
        {
            return Ok(Some(tables));
        }

        Ok(Some(CompletionResponse::Array(vec![])))
    }

    async fn handle_out_of_string_graph_engine_completion(
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

    async fn handle_in_string_graph_engine_completion(
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
            let mut pos = 0;

            for n in 0..lines.len() {
                pos += 1;
            }

            return Ok(Some(self.format_file(&lines).await));
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

    // Fixed document not being updated on change
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

        let in_string = Self::is_in_string_literal(line, position.character);
        let ssh_keyspaces = self.should_suggest_keyspaces(line, &position);
        let ssh_graph_types = self.should_suggest_graph_engine_types(line, &position);
        let ssh_command_sequence = self.should_suggest_command_sequence(line, &position);
        let ssh_keywords = self.should_suggest_keywords(line, &position).await;
        let ssh_fields = self.should_suggest_fields(line, &position);
        let ssh_from = self.should_suggest_from(line, &position);
        let ssh_table_completions = self.should_suggest_table_completions(line, &position);

        if ssh_keyspaces {
            return if in_string {
                self.handle_in_string_keyspace_completion(line, &position)
                    .await
            } else {
                self.handle_out_of_string_keyspace_completion(line, &position)
                    .await
            };
        }

        if ssh_from {
            return self.handle_from_completion();
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
