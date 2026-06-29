//! Formula parser and SQL translator for pg_sheet.
//!
//! Parses Excel-like formulas (=SUM(B2:B10), =IF(A1>0, A1*2, 0)) and
//! translates them to SQL expressions where possible.
//!
//! Design:
//! - Formulas start with '='
//! - Cell references: A1, B2, AA10 (column letter + row number)
//! - Column references: {revenue}, {confidence} (curly braces = named column)
//! - Ranges: A1:A10
//! - Functions: SUM, AVG, IF, COALESCE, MIN, MAX, COUNT, ABS, ROUND, etc.
//! - Operators: +, -, *, /, ^, >, <, >=, <=, =, <>, &

use crate::types::{CellRange, CellRef, ParsedFormula};

/// Token types for the formula lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Number(f64),
    String(String),
    Bool(bool),
    CellRef(CellRef),
    Range(CellRef, CellRef),
    ColRef(String), // {column_name}
    Function(String),
    Operator(char),
    Comparison(String), // >=, <=, <>, ==
    Comma,
    LParen,
    RParen,
    Colon,
}

/// Tokenize a formula string (without the leading '=').
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\n' | '\r' => {
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            ':' => {
                tokens.push(Token::Colon);
                i += 1;
            }
            '+' | '-' | '*' | '/' | '^' | '&' => {
                tokens.push(Token::Operator(chars[i]));
                i += 1;
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Comparison(">=".to_string()));
                    i += 2;
                } else {
                    tokens.push(Token::Comparison(">".to_string()));
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Comparison("<=".to_string()));
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    tokens.push(Token::Comparison("<>".to_string()));
                    i += 2;
                } else {
                    tokens.push(Token::Comparison("<".to_string()));
                    i += 1;
                }
            }
            '=' => {
                tokens.push(Token::Comparison("=".to_string()));
                i += 1;
            }
            '"' => {
                // String literal
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '"' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("Unterminated string literal".to_string());
                }
                let s: String = chars[start..i].iter().collect();
                tokens.push(Token::String(s));
                i += 1; // skip closing quote
            }
            '{' => {
                // Column reference: {column_name}
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '}' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("Unterminated column reference {".to_string());
                }
                let name: String = chars[start..i].iter().collect();
                tokens.push(Token::ColRef(name.trim().to_string()));
                i += 1; // skip }
            }
            c if c.is_ascii_digit() || c == '.' => {
                // Number
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                match num_str.parse::<f64>() {
                    Ok(n) => tokens.push(Token::Number(n)),
                    Err(_) => return Err(format!("Invalid number: {}", num_str)),
                }
            }
            c if c.is_ascii_alphabetic() => {
                // Could be: function name, cell reference, TRUE/FALSE
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();

                match word.to_uppercase().as_str() {
                    "TRUE" => tokens.push(Token::Bool(true)),
                    "FALSE" => tokens.push(Token::Bool(false)),
                    _ => {
                        // Check if it's a cell reference (letters followed by digits)
                        if let Some(cell_ref) = parse_cell_ref_str(&word) {
                            // Peek ahead for range (A1:B10)
                            if i < chars.len() && chars[i] == ':' {
                                let colon_pos = i;
                                i += 1;
                                let range_start = i;
                                while i < chars.len()
                                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_')
                                {
                                    i += 1;
                                }
                                let end_word: String = chars[range_start..i].iter().collect();
                                if let Some(end_ref) = parse_cell_ref_str(&end_word) {
                                    tokens.push(Token::Range(cell_ref, end_ref));
                                } else {
                                    // Not a valid range, push cell ref and colon separately
                                    tokens.push(Token::CellRef(cell_ref));
                                    tokens.push(Token::Colon);
                                    i = colon_pos + 1;
                                }
                            } else {
                                tokens.push(Token::CellRef(cell_ref));
                            }
                        } else if i < chars.len() && chars[i] == '(' {
                            // Function name (followed by parenthesis)
                            tokens.push(Token::Function(word.to_uppercase()));
                        } else {
                            // Treat as column reference (unquoted)
                            tokens.push(Token::ColRef(word));
                        }
                    }
                }
            }
            c => {
                return Err(format!("Unexpected character: '{}'", c));
            }
        }
    }

    Ok(tokens)
}

/// Try to parse a string as a cell reference (e.g., "A1", "BC42").
fn parse_cell_ref_str(s: &str) -> Option<CellRef> {
    let chars: Vec<char> = s.chars().collect();

    // Must start with letters and end with digits
    let mut letter_end = 0;
    while letter_end < chars.len() && chars[letter_end].is_ascii_alphabetic() {
        letter_end += 1;
    }

    if letter_end == 0 || letter_end >= chars.len() {
        return None;
    }

    let col: String = chars[..letter_end].iter().collect();
    let row_str: String = chars[letter_end..].iter().collect();

    // All remaining chars must be digits
    if !row_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    match row_str.parse::<u32>() {
        Ok(row) if row > 0 => Some(CellRef::new(&col, row)),
        _ => None,
    }
}

/// Parse a formula string and extract metadata.
///
/// Returns a ParsedFormula with cell refs, column refs, functions, and
/// an optional SQL translation.
pub fn parse_formula(formula: &str) -> Result<ParsedFormula, String> {
    let text = formula.trim();
    if !text.starts_with('=') {
        return Err("Formula must start with '='".to_string());
    }

    let body = &text[1..];
    let tokens = tokenize(body)?;

    let mut cell_refs = Vec::new();
    let mut col_refs = Vec::new();
    let mut functions = Vec::new();

    for token in &tokens {
        match token {
            Token::CellRef(r) => {
                cell_refs.push(r.clone());
            }
            Token::Range(start, end) => {
                cell_refs.push(start.clone());
                cell_refs.push(end.clone());
            }
            Token::ColRef(name) if !col_refs.contains(name) => {
                col_refs.push(name.clone());
            }
            Token::Function(name) if !functions.contains(name) => {
                functions.push(name.clone());
            }
            _ => {}
        }
    }

    // Try SQL translation
    let (sql, sql_translatable) = translate_to_sql(&tokens, body);

    Ok(ParsedFormula {
        text: text.to_string(),
        cell_refs,
        col_refs,
        functions,
        sql,
        sql_translatable,
    })
}

/// Map of Excel functions to SQL equivalents.
fn function_to_sql(name: &str) -> Option<&'static str> {
    match name {
        // Aggregates
        "SUM" => Some("SUM"),
        "AVG" | "AVERAGE" => Some("AVG"),
        "MIN" => Some("LEAST"),
        "MAX" => Some("GREATEST"),
        "COUNT" => Some("COUNT"),
        "STDEV" | "STDEVP" => Some("STDDEV"),
        "VAR" | "VARP" => Some("VARIANCE"),

        // Math
        "ABS" => Some("ABS"),
        "ROUND" => Some("ROUND"),
        "FLOOR" => Some("FLOOR"),
        "CEILING" | "CEIL" => Some("CEIL"),
        "SQRT" => Some("SQRT"),
        "POWER" | "POW" => Some("POWER"),
        "MOD" => Some("MOD"),
        "LN" => Some("LN"),
        "LOG" | "LOG10" => Some("LOG"),
        "EXP" => Some("EXP"),
        "SIGN" => Some("SIGN"),
        "PI" => Some("PI()"),

        // Conditional
        "IF" => Some("CASE"),  // special handling
        "IFS" => Some("CASE"), // special handling
        "COALESCE" => Some("COALESCE"),

        // String
        "UPPER" => Some("UPPER"),
        "LOWER" => Some("LOWER"),
        "TRIM" => Some("TRIM"),
        "LEN" | "LENGTH" => Some("LENGTH"),
        "LEFT" => Some("LEFT"),
        "RIGHT" => Some("RIGHT"),
        "CONCATENATE" | "CONCAT" => Some("CONCAT"),
        "SUBSTITUTE" => Some("REPLACE"),

        // Date
        "NOW" => Some("NOW()"),
        "TODAY" => Some("CURRENT_DATE"),
        "YEAR" => Some("EXTRACT(YEAR FROM"),
        "MONTH" => Some("EXTRACT(MONTH FROM"),
        "DAY" => Some("EXTRACT(DAY FROM"),

        // Null handling
        "IFERROR" | "IFNA" => Some("COALESCE"), // approximate
        "ISBLANK" => Some("IS NULL"),

        _ => None,
    }
}

/// Attempt to translate a token stream to a SQL expression.
///
/// Returns (Some(sql), true) if fully translatable, (None, false) otherwise.
fn translate_to_sql(tokens: &[Token], _original: &str) -> (Option<String>, bool) {
    // For column-reference-only formulas (no cell refs like A1:B2),
    // we can translate directly to SQL expressions.
    let has_cell_refs = tokens
        .iter()
        .any(|t| matches!(t, Token::CellRef(_) | Token::Range(_, _)));

    if has_cell_refs {
        // Cell-ref formulas need HyperFormula on the client.
        // We store them but can't evaluate server-side without row context.
        return (None, false);
    }

    // Try to build SQL from column references and functions
    let mut sql = String::new();
    let mut i = 0;

    while i < tokens.len() {
        match &tokens[i] {
            Token::Number(n) => {
                if *n == (*n as i64) as f64 {
                    sql.push_str(&(*n as i64).to_string());
                } else {
                    sql.push_str(&n.to_string());
                }
            }
            Token::String(s) => {
                // SQL string literal (escape single quotes)
                sql.push('\'');
                sql.push_str(&s.replace('\'', "''"));
                sql.push('\'');
            }
            Token::Bool(b) => {
                sql.push_str(if *b { "TRUE" } else { "FALSE" });
            }
            Token::ColRef(name) => {
                // Column reference → quoted identifier
                sql.push('"');
                sql.push_str(name);
                sql.push('"');
            }
            Token::Operator(op) => {
                match op {
                    '&' => sql.push_str(" || "), // Excel concat → SQL concat
                    '^' => sql.push_str(" ^ "),
                    _ => {
                        sql.push(' ');
                        sql.push(*op);
                        sql.push(' ');
                    }
                }
            }
            Token::Comparison(op) => {
                sql.push(' ');
                if op == "=" {
                    sql.push('=');
                } else {
                    sql.push_str(op);
                }
                sql.push(' ');
            }
            Token::Function(name) => {
                if name == "IF" {
                    // IF(cond, then, else) → CASE WHEN cond THEN then ELSE else END
                    if let Some(result) = translate_if_function(tokens, &mut i) {
                        sql.push_str(&result);
                        continue; // i already advanced
                    } else {
                        return (None, false);
                    }
                } else if let Some(sql_func) = function_to_sql(name) {
                    if sql_func.ends_with('(') {
                        // Functions like EXTRACT(YEAR FROM ...)
                        sql.push_str(sql_func);
                    } else if sql_func.contains("()") {
                        // Nullary functions like PI(), NOW()
                        sql.push_str(sql_func);
                        // Skip the () in tokens
                        if i + 2 < tokens.len()
                            && tokens[i + 1] == Token::LParen
                            && tokens[i + 2] == Token::RParen
                        {
                            i += 3;
                            continue;
                        }
                    } else {
                        sql.push_str(sql_func);
                    }
                } else {
                    // Unknown function — can't translate
                    return (None, false);
                }
            }
            Token::LParen => sql.push('('),
            Token::RParen => sql.push(')'),
            Token::Comma => sql.push_str(", "),
            Token::Colon => sql.push(':'),
            Token::CellRef(_) | Token::Range(_, _) => {
                // Should not reach here (checked above)
                return (None, false);
            }
        }
        i += 1;
    }

    if sql.is_empty() {
        (None, false)
    } else {
        (Some(sql.trim().to_string()), true)
    }
}

/// Translate IF(cond, then, else) to CASE WHEN ... THEN ... ELSE ... END.
///
/// Advances `pos` past the entire IF(...) expression.
fn translate_if_function(tokens: &[Token], pos: &mut usize) -> Option<String> {
    // tokens[pos] == Function("IF")
    // Expect: IF ( cond , then , else )
    let start = *pos;
    *pos += 1; // skip IF

    if *pos >= tokens.len() || tokens[*pos] != Token::LParen {
        *pos = start;
        return None;
    }
    *pos += 1; // skip (

    // Collect three arguments separated by commas, respecting nesting
    let mut args: Vec<Vec<Token>> = Vec::new();
    let mut current_arg: Vec<Token> = Vec::new();
    let mut depth = 1;

    while *pos < tokens.len() && depth > 0 {
        match &tokens[*pos] {
            Token::LParen => {
                depth += 1;
                current_arg.push(tokens[*pos].clone());
            }
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    args.push(current_arg.clone());
                } else {
                    current_arg.push(tokens[*pos].clone());
                }
            }
            Token::Comma if depth == 1 => {
                args.push(current_arg.clone());
                current_arg = Vec::new();
            }
            _ => {
                current_arg.push(tokens[*pos].clone());
            }
        }
        *pos += 1;
    }

    if args.len() < 2 || args.len() > 3 {
        *pos = start;
        return None;
    }

    let (cond_sql, _) = translate_to_sql(&args[0], "");
    let (then_sql, _) = translate_to_sql(&args[1], "");

    let cond = cond_sql?;
    let then = then_sql?;

    let else_part = if args.len() == 3 {
        let (else_sql, _) = translate_to_sql(&args[2], "");
        format!(" ELSE {}", else_sql?)
    } else {
        String::new()
    };

    Some(format!("CASE WHEN {} THEN {}{} END", cond, then, else_part))
}

/// Extract cell references from a parsed formula (for dependency tracking).
pub fn extract_dependencies(formula: &ParsedFormula) -> Vec<(String, u32)> {
    formula
        .cell_refs
        .iter()
        .map(|r| (r.col.clone(), r.row))
        .collect()
}

/// Expand a range (A1:A5) into individual cell references.
pub fn expand_range(range: &CellRange) -> Vec<CellRef> {
    let mut refs = Vec::new();
    let start_col = range.start.col_index();
    let end_col = range.end.col_index();
    let start_row = range.start.row;
    let end_row = range.end.row;

    for col_idx in start_col..=end_col {
        let col = index_to_col_letter(col_idx);
        for row in start_row..=end_row {
            refs.push(CellRef::new(&col, row));
        }
    }
    refs
}

/// Convert a 0-based column index to letter(s): 0=A, 1=B, ..., 25=Z, 26=AA.
fn index_to_col_letter(mut idx: u32) -> String {
    let mut result = String::new();
    loop {
        result.insert(0, (b'A' + (idx % 26) as u8) as char);
        if idx < 26 {
            break;
        }
        idx = idx / 26 - 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple_math() {
        let tokens = tokenize("1 + 2 * 3").unwrap();
        assert_eq!(tokens.len(), 5);
        assert!(matches!(tokens[0], Token::Number(n) if n == 1.0));
        assert!(matches!(tokens[1], Token::Operator('+')));
    }

    #[test]
    fn test_tokenize_cell_ref() {
        let tokens = tokenize("A1 + B2").unwrap();
        assert_eq!(tokens.len(), 3);
        assert!(matches!(&tokens[0], Token::CellRef(r) if r.col == "A" && r.row == 1));
        assert!(matches!(&tokens[2], Token::CellRef(r) if r.col == "B" && r.row == 2));
    }

    #[test]
    fn test_tokenize_range() {
        let tokens = tokenize("SUM(A1:A10)").unwrap();
        assert_eq!(tokens.len(), 4);
        assert!(matches!(&tokens[0], Token::Function(name) if name == "SUM"));
        assert!(matches!(&tokens[2], Token::Range(s, e) if s.row == 1 && e.row == 10));
    }

    #[test]
    fn test_tokenize_col_ref() {
        let tokens = tokenize("{revenue} * {confidence}").unwrap();
        assert_eq!(tokens.len(), 3);
        assert!(matches!(&tokens[0], Token::ColRef(name) if name == "revenue"));
        assert!(matches!(&tokens[2], Token::ColRef(name) if name == "confidence"));
    }

    #[test]
    fn test_parse_formula_col_refs() {
        let parsed = parse_formula("={revenue} * {confidence}").unwrap();
        assert!(parsed.col_refs.contains(&"revenue".to_string()));
        assert!(parsed.col_refs.contains(&"confidence".to_string()));
        assert!(parsed.cell_refs.is_empty());
        assert!(parsed.sql_translatable);
        assert_eq!(parsed.sql.unwrap(), "\"revenue\" * \"confidence\"");
    }

    #[test]
    fn test_parse_formula_if() {
        let parsed = parse_formula("=IF({confidence} > 0.5, {revenue}, 0)").unwrap();
        assert!(parsed.sql_translatable);
        assert!(parsed.sql.unwrap().contains("CASE WHEN"));
    }

    #[test]
    fn test_parse_formula_cell_refs_not_translatable() {
        let parsed = parse_formula("=A1 + B2").unwrap();
        assert!(!parsed.sql_translatable);
        assert!(parsed.sql.is_none());
        assert_eq!(parsed.cell_refs.len(), 2);
    }

    #[test]
    fn test_cell_ref_col_index() {
        assert_eq!(CellRef::new("A", 1).col_index(), 0);
        assert_eq!(CellRef::new("B", 1).col_index(), 1);
        assert_eq!(CellRef::new("Z", 1).col_index(), 25);
        assert_eq!(CellRef::new("AA", 1).col_index(), 26);
    }

    #[test]
    fn test_expand_range() {
        let range = CellRange {
            start: CellRef::new("A", 1),
            end: CellRef::new("B", 3),
        };
        let refs = expand_range(&range);
        assert_eq!(refs.len(), 6); // A1,A2,A3,B1,B2,B3
    }

    #[test]
    fn test_index_to_col_letter() {
        assert_eq!(index_to_col_letter(0), "A");
        assert_eq!(index_to_col_letter(25), "Z");
        assert_eq!(index_to_col_letter(26), "AA");
    }
}
