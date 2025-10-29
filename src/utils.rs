use crate::consts::*;
use crate::lsp::Backend;
use log::info;

impl Backend {
    pub fn is_in_string_literal(line: &str, position: u32) -> bool {
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

    pub fn line_contains_cql_type(&self, line: &str) -> bool {
        let split: Vec<&str> = line.split_whitespace().collect();

        info!("{:?} Split", split);
        let mut is_type = false;

        for w in split {
            info!("{} ^^", w.to_lowercase().to_string());
            // Fix List<>, frozen<>, map<>, set<>
            if CQL_TYPES_LWC.contains(&w.to_lowercase().replace(",", "").trim().to_string())
                || w.starts_with("set")
                || w.starts_with("map")
                || w.starts_with("list")
                || w.starts_with("frozen")
            {
                is_type = true;
                break;
            }
        }

        return is_type;
    }

    pub fn line_contains_cql_kw(&self, line: &str) -> bool {
        let lw = line.to_lowercase();
        let split: Vec<&str> = lw.split(' ').collect();

        for kw in split {
            if CQL_KEYWORDS_LWC.contains(&kw.to_string()) {
                return false;
            }
        }

        false
    }

    pub fn is_line_inside_selectors(&self, line: &str, index: usize, lines: &Vec<String>) -> bool {
        if self.line_contains_cql_kw(line) || line.contains(";") || line.len() == 0 {
            return false;
        }

        if index == 0 || index == lines.len() - 1 {
            return false;
        }

        let lw = line.to_lowercase();

        if lw.contains("values") || lw.contains("from") {
            return false;
        }

        let mut index_up = index - 1;
        let mut index_down = index + 1;

        let mut top_bracket = false;
        let mut bottom_bracket = false;

        while index_up > 0 {
            let up_line = &lines[index_up].to_lowercase();
            if !top_bracket && up_line.contains("select") {
                top_bracket = true;
            }
            if !top_bracket {
                index_up -= 1;
            } else {
                break;
            }
        }

        let up_line = &lines[index_up].to_lowercase();
        if !top_bracket && up_line.contains("select") {
            top_bracket = true;
        }

        while index_down < lines.len() {
            let down_line = &lines[index_down].to_lowercase();
            if !bottom_bracket && down_line.contains("from") {
                bottom_bracket = true;
            }

            if !bottom_bracket && down_line.contains(";") {
                return false;
            }
            if !bottom_bracket {
                index_down += 1;
            } else {
                break;
            }
        }

        if top_bracket && bottom_bracket {
            return true;
        }

        false
    }

    pub fn is_multi_line_comment_clause(&self, line: &str) -> bool {
        if line.contains("/*") || line.contains("*/") {
            return true;
        }
        false
    }

    pub fn is_line_in_multiline_comment_ref(
        &self,
        line: &str,
        index: usize,
        lines: &Vec<&str>,
    ) -> bool {
        if index == 0 || index == lines.len() - 1 || line.contains("/*") || line.contains("*/") {
            return false;
        }

        let mut up_index = index - 1;
        let mut down_index = index + 1;

        let mut top_line = false;
        let mut bottom_line = false;

        while up_index > 0 {
            if !top_line && lines[up_index].contains("/*") {
                top_line = true;
            }

            if !top_line && lines[up_index].contains("*/") {
                return false;
            }

            if top_line {
                break;
            }
            up_index -= 1;
        }

        if !top_line && lines[up_index].contains("/*") {
            top_line = true;
        }

        if !top_line && lines[up_index].contains("*/") {
            return false;
        }

        while down_index < lines.len() {
            if !bottom_line && lines[down_index].contains("*/") {
                bottom_line = true;
            }

            if !bottom_line && lines[down_index].contains("/*") {
                return false;
            }

            if bottom_line {
                break;
            }

            down_index += 1;
        }

        if top_line && bottom_line {
            return true;
        }

        false
    }

    // Excluding /* && */
    pub fn is_line_in_multiline_comment(
        &self,
        line: &str,
        index: usize,
        lines: &Vec<String>,
    ) -> bool {
        if index == 0 || index == lines.len() - 1 || line.contains("/*") || line.contains("*/") {
            return false;
        }

        let mut up_index = index - 1;
        let mut down_index = index + 1;

        let mut top_line = false;
        let mut bottom_line = false;

        while up_index > 0 {
            if !top_line && lines[up_index].contains("/*") {
                top_line = true;
            }

            if !top_line && lines[up_index].contains("*/") {
                return false;
            }

            if top_line {
                break;
            }
            up_index -= 1;
        }

        if !top_line && lines[up_index].contains("/*") {
            top_line = true;
        }

        if !top_line && lines[up_index].contains("*/") {
            return false;
        }

        while down_index < lines.len() {
            if !bottom_line && lines[down_index].contains("*/") {
                bottom_line = true;
            }

            if !bottom_line && lines[down_index].contains("/*") {
                return false;
            }

            if bottom_line {
                break;
            }

            down_index += 1;
        }

        if top_line && bottom_line {
            return true;
        }

        false
    }

    pub fn is_line_inside_init_args(&self, line: &str, index: usize, lines: &Vec<String>) -> bool {
        if self.line_contains_cql_kw(line)
            || line.contains(";")
            || line.contains("{")
            || line.contains("}")
            || line.contains("(")
            || line.contains(")")
        {
            return false;
        }

        if index == 0 || index == lines.len() - 1 {
            return false;
        }

        let lw = line.to_lowercase();

        if lw.contains("values") || lw.contains("from") {
            return false;
        }

        let mut index_up = index - 1;
        let mut index_down = index + 1;

        let mut top_bracket = false;
        let mut bottom_bracket = false;

        while index_up > 0 {
            let up_line = &lines[index_up];
            if !top_bracket && (up_line.contains("{") || up_line.contains("(")) {
                top_bracket = true;
            }

            if !top_bracket && (up_line.contains("}") || up_line.contains(")")) {
                return false;
            }

            if !top_bracket && self.line_contains_cql_kw(up_line) {
                return false;
            }

            if top_bracket {
                break;
            }

            index_up -= 1;
        }

        let up_line = &lines[index_up];
        if !top_bracket && (up_line.contains("{") || up_line.contains("(")) {
            top_bracket = true;
        }

        if !top_bracket && (up_line.contains("}") || up_line.contains(")")) {
            return false;
        }

        if !top_bracket && self.line_contains_cql_kw(up_line) {
            return false;
        }

        while index_down < lines.len() {
            let down_line = &lines[index_down];

            if !bottom_bracket && (down_line.contains("}") || down_line.contains(")")) {
                bottom_bracket = true;
            }

            if !bottom_bracket && (down_line.contains("{") || down_line.contains("(")) {
                return false;
            }

            if !bottom_bracket && down_line.contains(";") {
                return false;
            }

            if !bottom_bracket && self.line_contains_cql_kw(down_line) {
                return false;
            }

            if bottom_bracket {
                break;
            }
            index_down += 1;
        }

        if top_bracket && bottom_bracket {
            return true;
        }

        false
    }
}
