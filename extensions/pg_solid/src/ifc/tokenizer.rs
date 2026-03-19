/// Token types produced by the STEP file tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    EntityRef(u64),
    StringLiteral(String),
    Integer(i64),
    Real(f64),
    Enum(String),
    Keyword(String),
    Unset,
    Derived,
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Equals,
}

/// Stateful tokenizer for STEP physical file format (ISO 10303-21).
pub struct Tokenizer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.input.get(self.pos).copied();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
            // Skip /* ... */ comments
            if self.pos + 1 < self.input.len()
                && self.input[self.pos] == b'/'
                && self.input[self.pos + 1] == b'*'
            {
                self.pos += 2;
                while self.pos + 1 < self.input.len() {
                    if self.input[self.pos] == b'*' && self.input[self.pos + 1] == b'/' {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    pub fn next_token(&mut self) -> Result<Option<Token>, String> {
        self.skip_whitespace_and_comments();

        match self.peek() {
            None => Ok(None),
            Some(b'#') => self.read_entity_ref(),
            Some(b'\'') => self.read_string(),
            Some(b'.') => self.read_enum(),
            Some(b'$') => {
                self.advance();
                Ok(Some(Token::Unset))
            }
            Some(b'*') => {
                self.advance();
                Ok(Some(Token::Derived))
            }
            Some(b'(') => {
                self.advance();
                Ok(Some(Token::LeftParen))
            }
            Some(b')') => {
                self.advance();
                Ok(Some(Token::RightParen))
            }
            Some(b',') => {
                self.advance();
                Ok(Some(Token::Comma))
            }
            Some(b';') => {
                self.advance();
                Ok(Some(Token::Semicolon))
            }
            Some(b'=') => {
                self.advance();
                Ok(Some(Token::Equals))
            }
            Some(b) if b == b'-' || b == b'+' || b.is_ascii_digit() => self.read_number(),
            Some(b) if b.is_ascii_alphabetic() || b == b'_' => self.read_keyword(),
            Some(b) => Err(format!(
                "unexpected byte 0x{:02x} ('{}') at position {}",
                b, b as char, self.pos
            )),
        }
    }

    fn read_entity_ref(&mut self) -> Result<Option<Token>, String> {
        self.advance(); // skip '#'
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(format!("expected digits after '#' at position {}", start));
        }
        let s = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|e| format!("invalid utf8 in entity ref: {e}"))?;
        let id: u64 = s
            .parse()
            .map_err(|e| format!("invalid entity ref number '{s}': {e}"))?;
        Ok(Some(Token::EntityRef(id)))
    }

    fn read_string(&mut self) -> Result<Option<Token>, String> {
        self.advance(); // skip opening '
        let mut result = String::new();
        loop {
            match self.advance() {
                None => return Err("unterminated string literal".into()),
                Some(b'\'') => {
                    // Check for '' escape
                    if self.peek() == Some(b'\'') {
                        self.advance();
                        result.push('\'');
                    } else {
                        break;
                    }
                }
                Some(b'\\') => {
                    // IFC string escapes: \X\HH, \X2\HHHH\X0\, \S\char
                    match self.peek() {
                        Some(b'X') => {
                            self.advance(); // X
                            if self.peek() == Some(b'2') {
                                // \X2\HHHH...\X0\ — extended Unicode
                                self.advance(); // 2
                                if self.peek() == Some(b'\\') {
                                    self.advance(); // backslash
                                }
                                let mut hex_str = String::new();
                                loop {
                                    match self.peek() {
                                        Some(b'\\') => {
                                            self.advance();
                                            // Check for \X0\ terminator
                                            if self.peek() == Some(b'X') {
                                                self.advance();
                                                if self.peek() == Some(b'0') {
                                                    self.advance();
                                                    if self.peek() == Some(b'\\') {
                                                        self.advance();
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                        Some(b) if b.is_ascii_hexdigit() => {
                                            result.push(b as char);
                                            hex_str.push(b as char);
                                            self.advance();
                                        }
                                        _ => break,
                                    }
                                }
                                // Decode hex pairs as UTF-16
                                let mut i = 0;
                                while i + 3 < hex_str.len() {
                                    if let Ok(code) = u16::from_str_radix(&hex_str[i..i + 4], 16) {
                                        if let Some(ch) = char::from_u32(code as u32) {
                                            result.push(ch);
                                        }
                                    }
                                    i += 4;
                                }
                            } else if self.peek() == Some(b'\\') {
                                // \X\HH — single byte hex
                                self.advance(); // backslash
                                let mut hex = String::new();
                                for _ in 0..2 {
                                    if let Some(b) = self.peek() {
                                        if b.is_ascii_hexdigit() {
                                            hex.push(b as char);
                                            self.advance();
                                        }
                                    }
                                }
                                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                                    result.push(byte as char);
                                }
                            } else {
                                result.push('\\');
                                result.push('X');
                            }
                        }
                        Some(b'S') => {
                            self.advance(); // S
                            if self.peek() == Some(b'\\') {
                                self.advance(); // backslash
                                if let Some(b) = self.advance() {
                                    result.push((b + 128) as char);
                                }
                            } else {
                                result.push('\\');
                                result.push('S');
                            }
                        }
                        _ => {
                            result.push('\\');
                        }
                    }
                }
                Some(b) => {
                    result.push(b as char);
                }
            }
        }
        Ok(Some(Token::StringLiteral(result)))
    }

    fn read_number(&mut self) -> Result<Option<Token>, String> {
        let start = self.pos;
        // Optional sign
        if matches!(self.peek(), Some(b'-') | Some(b'+')) {
            self.advance();
        }
        // Integer part
        while let Some(b) = self.peek() {
            if b.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }
        let mut is_real = false;
        // Decimal part
        if self.peek() == Some(b'.') {
            // Check it's not an enum starting (e.g., after a number that isn't real)
            // Actually in STEP, a number followed by . is always a real
            is_real = true;
            self.advance(); // skip '.'
            while let Some(b) = self.peek() {
                if b.is_ascii_digit() {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        // Exponent
        if matches!(self.peek(), Some(b'E') | Some(b'e')) {
            is_real = true;
            self.advance();
            if matches!(self.peek(), Some(b'-') | Some(b'+')) {
                self.advance();
            }
            while let Some(b) = self.peek() {
                if b.is_ascii_digit() {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let s = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|e| format!("invalid utf8 in number: {e}"))?;

        if is_real {
            let v: f64 = s.parse().map_err(|e| format!("invalid real '{s}': {e}"))?;
            Ok(Some(Token::Real(v)))
        } else {
            let v: i64 = s
                .parse()
                .map_err(|e| format!("invalid integer '{s}': {e}"))?;
            Ok(Some(Token::Integer(v)))
        }
    }

    fn read_enum(&mut self) -> Result<Option<Token>, String> {
        self.advance(); // skip opening '.'
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b == b'.' {
                break;
            }
            self.advance();
        }
        let s = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|e| format!("invalid utf8 in enum: {e}"))?
            .to_string();
        self.advance(); // skip closing '.'
                        // Convert .T. and .F. to booleans
        match s.as_str() {
            "T" | "TRUE" => Ok(Some(Token::Enum("T".into()))),
            "F" | "FALSE" => Ok(Some(Token::Enum("F".into()))),
            _ => Ok(Some(Token::Enum(s))),
        }
    }

    fn read_keyword(&mut self) -> Result<Option<Token>, String> {
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b.is_ascii_alphanumeric() || b == b'_' {
                self.advance();
            } else {
                break;
            }
        }
        let s = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|e| format!("invalid utf8 in keyword: {e}"))?
            .to_uppercase();
        Ok(Some(Token::Keyword(s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(input: &[u8]) -> Vec<Token> {
        let mut t = Tokenizer::new(input);
        let mut result = Vec::new();
        while let Ok(Some(tok)) = t.next_token() {
            result.push(tok);
        }
        result
    }

    #[test]
    fn test_entity_ref() {
        assert_eq!(tokens(b"#123"), vec![Token::EntityRef(123)]);
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(
            tokens(b"'Wall-01'"),
            vec![Token::StringLiteral("Wall-01".into())]
        );
    }

    #[test]
    fn test_escaped_string() {
        assert_eq!(
            tokens(b"'It''s fine'"),
            vec![Token::StringLiteral("It's fine".into())]
        );
    }

    #[test]
    fn test_real() {
        assert_eq!(tokens(b"3.14159"), vec![Token::Real(3.14159)]);
    }

    #[test]
    fn test_negative_real() {
        assert_eq!(tokens(b"-2.5"), vec![Token::Real(-2.5)]);
    }

    #[test]
    fn test_scientific_notation() {
        assert_eq!(tokens(b"1.5E-3"), vec![Token::Real(0.0015)]);
    }

    #[test]
    fn test_integer() {
        assert_eq!(tokens(b"42"), vec![Token::Integer(42)]);
    }

    #[test]
    fn test_negative_integer() {
        assert_eq!(tokens(b"-7"), vec![Token::Integer(-7)]);
    }

    #[test]
    fn test_enum() {
        assert_eq!(tokens(b".ELEMENT."), vec![Token::Enum("ELEMENT".into())]);
    }

    #[test]
    fn test_bool_enums() {
        assert_eq!(tokens(b".T."), vec![Token::Enum("T".into())]);
        assert_eq!(tokens(b".F."), vec![Token::Enum("F".into())]);
    }

    #[test]
    fn test_unset_derived() {
        assert_eq!(
            tokens(b"$,*"),
            vec![Token::Unset, Token::Comma, Token::Derived]
        );
    }

    #[test]
    fn test_keyword() {
        assert_eq!(tokens(b"IFCWALL"), vec![Token::Keyword("IFCWALL".into())]);
    }

    #[test]
    fn test_keyword_case_insensitive() {
        assert_eq!(tokens(b"IfcWall"), vec![Token::Keyword("IFCWALL".into())]);
    }

    #[test]
    fn test_punctuation() {
        assert_eq!(
            tokens(b"()=;,"),
            vec![
                Token::LeftParen,
                Token::RightParen,
                Token::Equals,
                Token::Semicolon,
                Token::Comma,
            ]
        );
    }

    #[test]
    fn test_full_entity() {
        let input = b"#1 = IFCCARTESIANPOINT((0.0, 0.0, 0.0));";
        let toks = tokens(input);
        assert_eq!(toks[0], Token::EntityRef(1));
        assert_eq!(toks[1], Token::Equals);
        assert_eq!(toks[2], Token::Keyword("IFCCARTESIANPOINT".into()));
        assert_eq!(toks[3], Token::LeftParen);
        assert_eq!(toks[4], Token::LeftParen);
        assert_eq!(toks[5], Token::Real(0.0));
        assert_eq!(toks[6], Token::Comma);
        assert_eq!(toks[7], Token::Real(0.0));
        assert_eq!(toks[8], Token::Comma);
        assert_eq!(toks[9], Token::Real(0.0));
        assert_eq!(toks[10], Token::RightParen);
        assert_eq!(toks[11], Token::RightParen);
        assert_eq!(toks[12], Token::Semicolon);
    }

    #[test]
    fn test_comment_skipping() {
        let input = b"#1 /* comment */ = IFCWALL('guid');";
        let toks = tokens(input);
        assert_eq!(toks[0], Token::EntityRef(1));
        assert_eq!(toks[1], Token::Equals);
        assert_eq!(toks[2], Token::Keyword("IFCWALL".into()));
    }

    #[test]
    fn test_whitespace_handling() {
        let input = b"  #1  =  IFCWALL  (  'name'  )  ;  ";
        let toks = tokens(input);
        assert_eq!(toks.len(), 7);
    }

    #[test]
    fn test_hex_escape() {
        let input = b"'\\X\\41'";
        let toks = tokens(input);
        assert_eq!(toks[0], Token::StringLiteral("A".into()));
    }
}
