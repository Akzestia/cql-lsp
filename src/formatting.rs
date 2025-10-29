use log::info;
use tower_lsp::lsp_types::*;

use crate::{consts::*, lsp::Backend};

impl Backend {
    pub fn remove_leading_spaces_wildcards(&self, line: &mut String) {
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

    pub fn remove_tailing_spaces_wildcards(&self, line: &mut String) {
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

    pub async fn align_types_inside_create_statement(
        &self,
        lines: &mut Vec<String>,
        document_url: &Url,
    ) {
        let mut working_blocks: Vec<Vec<usize>> = Vec::new();
        let mut current_block: Vec<usize> = Vec::new();
        let mut in_table = false;
        let mut parenthesis_depth = 0;

        for (i, line) in lines.iter().enumerate() {
            let is_create_st_start = line.trim().to_lowercase().starts_with("create table")
                || line.trim().to_lowercase().starts_with("create type");
            let contains_cql_type = self.line_contains_cql_type(line);
            let has_open_brace = line.contains('(');
            let has_close_brace = line.contains(')');

            // Update parenthesis depth
            if has_open_brace {
                parenthesis_depth += line.matches('(').count();
            }
            if has_close_brace {
                parenthesis_depth -= line.matches(')').count();
            }

            // Start of a new table
            if is_create_st_start {
                // If we were already in a table, push the current block
                if in_table && !current_block.is_empty() {
                    working_blocks.push(current_block.clone());
                    current_block.clear();
                }
                in_table = true;
            }

            // If we're inside a table and the line contains CQL types, record it
            if in_table && contains_cql_type {
                current_block.push(i);
            }

            // End of table definition (parenthesis balanced)
            if in_table && parenthesis_depth == 0 && has_close_brace {
                if !current_block.is_empty() {
                    working_blocks.push(current_block.clone());
                    current_block.clear();
                }
                in_table = false;
                parenthesis_depth = 0; // Reset for next table
            }
        }

        // Handle any remaining block at the end
        if !current_block.is_empty() {
            working_blocks.push(current_block);
        }

        info!("Size of working blocks: {}", working_blocks.len());

        info!("Size of working blocks: {}", working_blocks.len());

        for vec in working_blocks.iter() {
            let mut max_offset_x = 0;
            let mut offsets: Vec<(usize, usize)> = Vec::new();

            for index in vec {
                let line = &lines[*index].to_lowercase();

                let split: Vec<&str> = line.split_whitespace().collect();
                let mut line_type = String::from("");

                for w in split {
                    if CQL_TYPES_LWC.contains(&w.to_lowercase().replace(",", "").trim().to_string())
                        || w.starts_with("set")
                        || w.starts_with("map")
                        || w.starts_with("list")
                        || w.starts_with("frozen")
                    {
                        line_type = w.to_string();
                        break;
                    }
                }

                if let Some(mut offset_x) = line.find(&line_type) {
                    info!("\n\nOFFSET: {}, LINE_TYPE: {}\n\n", offset_x, line_type);
                    offsets.push((*index, offset_x));
                    if max_offset_x < offset_x {
                        max_offset_x = offset_x;
                    }
                }
            }

            info!("Size of offsets: {}", offsets.len());

            /*
             * 0 -> index
             * 1 -> offset
             */
            for offset in offsets {
                if offset.1 < max_offset_x {
                    let mut working_line = String::from(&lines[offset.0]);
                    let diff = max_offset_x - offset.1;
                    info!("\n\nediting line: {}, {}\n\n", working_line, offset.1);

                    lines[offset.0].insert_str(offset.1 - 1, &" ".repeat(diff));
                }
            }
        }
    }

    pub fn add_tabs_to_cql_types(&self, lines: &mut Vec<String>) {
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }

            let mut found_type = None;
            for word in line.split_whitespace() {
                let clean_word = word.to_lowercase().replace(",", "").trim().to_string();
                if CQL_TYPES_LWC.contains(&clean_word) {
                    found_type = Some(word);
                    break;
                }
            }

            if let Some(typ) = found_type {
                if let Some(offset) = line.find(typ) {
                    if offset > 0 {
                        if !line[..offset]
                            .ends_with(&" ".repeat(self.formatting_config.type_alignment_offset))
                        {
                            line.insert_str(
                                offset,
                                &" ".repeat(self.formatting_config.type_alignment_offset),
                            );
                        }
                    }
                }
            }
        }
    }

    pub async fn add_tabs_to_args(&self, lines: &mut Vec<String>, document_url: &Url) {
        let mut indices = Vec::<usize>::new();

        for line in lines.iter().enumerate() {
            let is_inside_multiline_comment =
                self.is_line_in_multiline_comment(line.1, line.0, lines);
            let is_arg = self.is_line_inside_init_args(line.1, line.0, lines);
            let is_selector = self.is_line_inside_selectors(line.1, line.0, lines);
            let is_ml_comment_clause = self.is_multi_line_comment_clause(line.1);
            let trimed = line.1.trim_start();
            let mut is_pk = trimed.to_lowercase().starts_with("primary");
            let is_pk_create_table = self
                .is_inside_create_table_no_position(line.0, document_url)
                .await;

            info!("Line: {}\nIs inside: {}", line.1, is_pk_create_table);

            if !is_pk_create_table && is_pk {
                is_pk = false;
            }

            if (is_inside_multiline_comment
                || (is_arg && !is_inside_multiline_comment && !is_ml_comment_clause)
                || (is_selector && !is_inside_multiline_comment && !is_ml_comment_clause)
                || (is_pk && !is_inside_multiline_comment && !is_ml_comment_clause))
                && ((!line.1.to_lowercase().starts_with("create table")
                    || !line.1.to_lowercase().starts_with("create type"))
                    && (self
                        .is_inside_create_table_no_position(line.0, document_url)
                        .await
                        || self
                            .is_inside_create_type_no_position(line.0, document_url)
                            .await))
                && (!line.1.to_lowercase().starts_with("select")
                    && (self
                        .is_inside_create_table_no_position(line.0, document_url)
                        .await
                        || self
                            .is_inside_create_type_no_position(line.0, document_url)
                            .await))
                && (!line.1.to_lowercase().starts_with("as")
                    && (self
                        .is_inside_create_table_no_position(line.0, document_url)
                        .await
                        || self
                            .is_inside_create_type_no_position(line.0, document_url)
                            .await))
                && (!line.1.to_lowercase().starts_with("on")
                    && (self
                        .is_inside_create_table_no_position(line.0, document_url)
                        .await
                        || self
                            .is_inside_create_type_no_position(line.0, document_url)
                            .await))
                && (!line.1.to_lowercase().starts_with("where")
                    && (self
                        .is_inside_create_table_no_position(line.0, document_url)
                        .await
                        || self
                            .is_inside_create_type_no_position(line.0, document_url)
                            .await))
            {
                indices.push(line.0);
            }
        }

        for x in indices {
            lines[x].insert_str(0, "    ");
        }
    }

    pub fn fix_string_literals(&self, lines: &mut Vec<String>) {
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
    pub fn fix_semi_colon(&self, lines: &mut Vec<String>) {
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
    pub fn fix_duplicate_semi_colon(&self, line: &mut String) {
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
    pub fn fix_spacing(&self, line: &mut String) {
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
    pub fn fix_new_lines(&self, lines: &mut Vec<String>) {
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

            if lines[index].len() == 0
                && !self.is_line_in_multiline_comment(&lines[index], index, lines)
            {
                last_new_line = true;
            } else {
                last_new_line = false;
            }

            if lines[index].contains("(") {
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
    pub fn remove_new_lines_from_code_block(&self, lines: &mut Vec<String>) {
        let mut index = 0;
        let mut inside_code_block = false;

        while index < lines.len() {
            let line = lines[index].to_lowercase();

            if !inside_code_block && line.len() > 0 && !line.contains(";") {
                inside_code_block = true;
            }

            if inside_code_block && line.contains(";") {
                inside_code_block = false;
            }

            if inside_code_block
                && line.len() == 0
                && !self.is_line_in_multiline_comment(&line, index, lines)
            {
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
    pub fn apply_semi_colon(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        while index < lines.len() {
            let line = lines[index].to_lowercase();

            if index + 1 != lines.len()
                && line.len() > 0
                && !line.contains(";")
                && !line.contains("begin")
                && !line.contains("//")
                && !line.contains("--")
                && !line.contains("/*")
                && !line.contains("*/")
                && !line.ends_with("as")
                && !line.ends_with("with")
                && !self.is_line_in_multiline_comment(&line, index, lines)
            {
                let lw = lines[index + 1].to_lowercase();
                let split: Vec<&str> = lw.split(' ').collect();
                if lines[index + 1].to_lowercase().len() == 0
                    || CQL_KEYWORDS_LWC.contains(&split[0].to_string())
                {
                    lines[index].push(';');
                }
            }

            if index == lines.len() - 1
                && line.len() > 0
                && !line.contains(";")
                && !line.contains("begin")
                && !line.contains("//")
                && !line.contains("--")
                && !line.contains("/*")
                && !line.contains("*/")
                && !line.ends_with("as")
                && !line.ends_with("with")
                && !self.is_line_in_multiline_comment(&line, index, lines)
            {
                lines[index].push(';');
            }

            index += 1;
        }
    }

    pub fn add_spacing_new_lines(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        while index < lines.len() {
            if index + 1 != lines.len()
                && (lines[index].contains(";") || lines[index].to_lowercase().contains("begin"))
                && lines[index + 1].len() > 0
            {
                lines.insert(index + 1, "".to_string());
            }

            index += 1;
        }
    }

    pub fn add_new_line_before_pk(&self, lines: &mut Vec<String>) {
        let mut index = lines.len() - 1;

        let mut indicies: Vec<usize> = Vec::new();

        while index > 0 {
            if index - 1 != 0
                && (lines[index - 1]
                    .trim_start()
                    .to_lowercase()
                    .starts_with("primary"))
            {
                indicies.push(index - 1);
            }

            index -= 1;
        }

        for index in indicies.iter() {
            lines.insert(*index, "".to_string());
        }
    }

    pub fn add_comma_to_fields(&self, lines: &mut Vec<String>) {
        let mut index = 0;

        // while index < lines.len() {
        //     if index + 1 != lines.len()
        //         && self.is_inside_create_table(line, position)
        //         && (lines[index + 1]
        //             .trim_start()
        //             .to_lowercase()
        //             .starts_with("primary"))
        //     {
        //         indicies.push(index + 1);
        //     }

        //     index += 1;
        // }

        // for index in indicies.iter() {
        //     lines.insert(*index, "".to_string());
        // }
    }

    pub fn add_spacing_after_comma(&self, lines: &mut Vec<String>) {
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

    /*
        Hate this shit だよ xD
        Formats select statements in the following manner

        SELECT
        selector1,
        selector2,
        selector3,
        ...,
        selectorN,
        FROM [keyspace_name].table_name;
    */
    pub fn format_selectors(&self, lines: &mut Vec<String>) {
        let mut index = 0;
        let mut working_buf = Vec::<String>::new();

        while index < lines.len() {
            let lw = lines[index].to_lowercase();
            if lw.contains("select") && self.should_edit_select_statement(&lines[index], lines) {
                working_buf.clear();

                let mut idx = index;

                while idx < lines.len() {
                    if !lines[idx].to_lowercase().contains("from") && lines[idx].contains(";") {
                        return;
                    }

                    if lines[idx].to_lowercase().contains("from") {
                        let split: Vec<&str> = lines[idx].split(' ').collect();
                        for sp in split.into_iter() {
                            if sp.to_lowercase() == "from" {
                                break;
                            }
                            if sp.to_lowercase() != "select" {
                                let mut retained = sp.to_string();
                                retained.retain(|c| c != '\n' && c != '\r');
                                retained.push('\n');
                                working_buf.push(retained);
                            }
                        }

                        let from_pos = lines[idx].to_lowercase().rfind("from").unwrap();
                        working_buf.push(lines[idx][from_pos..].to_string());
                        break;
                    }

                    let split: Vec<&str> = lines[idx].split(' ').collect();

                    for sp in split.into_iter() {
                        if sp.to_lowercase() != "select" {
                            working_buf.push(sp.to_string());
                        }
                    }

                    idx += 1;
                }

                let mut start_idx = index + 1;
                for kw in working_buf.iter_mut() {
                    kw.retain(|c| c != '\n' && c != '\r');
                    kw.push('\n');

                    if start_idx < lines.len() {
                        lines.insert(start_idx, kw.clone());
                    } else {
                        lines.push(kw.clone());
                    }
                    start_idx += 1;
                }

                if lines[index].chars().nth(0).unwrap() == 'S' {
                    lines[index] = "SELECT".to_string();
                } else {
                    lines[index] = "select".to_string();
                }

                index += working_buf.len();
            } else {
                index += 1;
            }
        }
    }

    /*
        Hate this shit だよ xD
        Formats create table statements in the following manner

        CREATE TABLE [keyspace_name].table_name (
            short_name       type
            long_name_xxxxx  type
        );
    */
    pub fn format_table_fields(&self, lines: &mut Vec<String>) {}

    pub async fn format_file(&self, lines: &Vec<&str>, document_url: &Url) -> Vec<TextEdit> {
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
        // self.format_selectors(&mut working_vec);
        self.add_tabs_to_args(&mut working_vec, document_url).await;
        self.add_new_line_before_pk(&mut working_vec);
        self.add_tabs_to_cql_types(&mut working_vec);
        self.align_types_inside_create_statement(&mut working_vec, document_url)
            .await;

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
}
