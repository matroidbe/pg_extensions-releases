/// Compound unit expression parser.
///
/// Parses expressions like "kg", "m/s", "kW*h", "kg/m³", "m^2"
/// into a list of (unit_ref, exponent) pairs.
///
/// Grammar:
///   expression = term (('/' | '*' | '·') term)*
///   term = unit_ref ('^' number | '²' | '³')?
///   unit_ref = identifier (matched against DB name or symbol)
///
/// A parsed unit component: (unit_reference, exponent).
#[derive(Debug, Clone, PartialEq)]
pub struct UnitTerm {
    pub unit_ref: String,
    pub exponent: i16,
}

/// Parse a unit expression into its component terms.
///
/// Examples:
/// - "kg" → [("kg", 1)]
/// - "m/s" → [("m", 1), ("s", -1)]
/// - "kg/m³" → [("kg", 1), ("m", -3)]
/// - "kW*h" → [("kW", 1), ("h", 1)]
/// - "m^2" → [("m", 2)]
pub fn parse_unit_expression(expr: &str) -> Result<Vec<UnitTerm>, String> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err("empty unit expression".to_string());
    }

    let mut terms = Vec::new();
    let mut current_sign: i16 = 1; // 1 for multiply, -1 for divide
    let mut pos = 0;
    let chars: Vec<char> = expr.chars().collect();
    let len = chars.len();

    while pos < len {
        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        // Parse unit reference (ASCII alphanumeric + underscore)
        // Exclude Unicode superscripts (², ³) which are exponent markers
        let start = pos;
        while pos < len && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_') {
            pos += 1;
        }

        if pos == start {
            return Err(format!(
                "expected unit name at position {}, got '{}'",
                pos,
                chars.get(pos).unwrap_or(&' ')
            ));
        }

        let unit_ref: String = chars[start..pos].iter().collect();
        let mut exponent: i16 = 1;

        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }

        // Check for exponent: ^N, ², ³
        if pos < len {
            match chars[pos] {
                '^' => {
                    pos += 1;
                    let neg = pos < len && chars[pos] == '-';
                    if neg {
                        pos += 1;
                    }
                    let exp_start = pos;
                    while pos < len && chars[pos].is_ascii_digit() {
                        pos += 1;
                    }
                    if pos == exp_start {
                        return Err(format!(
                            "expected exponent number after '^' at position {}",
                            pos
                        ));
                    }
                    let exp_str: String = chars[exp_start..pos].iter().collect();
                    exponent = exp_str
                        .parse::<i16>()
                        .map_err(|_| format!("invalid exponent: {}", exp_str))?;
                    if neg {
                        exponent = -exponent;
                    }
                }
                '\u{00B2}' => {
                    // ²
                    exponent = 2;
                    pos += 1;
                }
                '\u{00B3}' => {
                    // ³
                    exponent = 3;
                    pos += 1;
                }
                _ => {}
            }
        }

        terms.push(UnitTerm {
            unit_ref,
            exponent: exponent * current_sign,
        });

        // Skip whitespace
        while pos < len && chars[pos].is_whitespace() {
            pos += 1;
        }

        // Check for operator
        if pos < len {
            match chars[pos] {
                '/' => {
                    current_sign = -1;
                    pos += 1;
                }
                '*' | '\u{00B7}' => {
                    // * or ·
                    current_sign = 1;
                    pos += 1;
                }
                _ => {
                    return Err(format!(
                        "unexpected character '{}' at position {}",
                        chars[pos], pos
                    ));
                }
            }
        }
    }

    if terms.is_empty() {
        return Err("no unit terms parsed".to_string());
    }

    Ok(terms)
}

/// Check if a unit expression is a simple (single) unit reference.
pub fn is_simple_unit(expr: &str) -> bool {
    matches!(parse_unit_expression(expr), Ok(ref terms) if terms.len() == 1 && terms[0].exponent == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_unit() {
        let terms = parse_unit_expression("kg").unwrap();
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].unit_ref, "kg");
        assert_eq!(terms[0].exponent, 1);
    }

    #[test]
    fn test_parse_division() {
        let terms = parse_unit_expression("m/s").unwrap();
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].unit_ref, "m");
        assert_eq!(terms[0].exponent, 1);
        assert_eq!(terms[1].unit_ref, "s");
        assert_eq!(terms[1].exponent, -1);
    }

    #[test]
    fn test_parse_multiplication() {
        let terms = parse_unit_expression("kW*h").unwrap();
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].unit_ref, "kW");
        assert_eq!(terms[0].exponent, 1);
        assert_eq!(terms[1].unit_ref, "h");
        assert_eq!(terms[1].exponent, 1);
    }

    #[test]
    fn test_parse_power_caret() {
        let terms = parse_unit_expression("m^2").unwrap();
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].unit_ref, "m");
        assert_eq!(terms[0].exponent, 2);
    }

    #[test]
    fn test_parse_unicode_superscript() {
        let terms = parse_unit_expression("m²").unwrap();
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].unit_ref, "m");
        assert_eq!(terms[0].exponent, 2);
    }

    #[test]
    fn test_parse_compound_with_power() {
        let terms = parse_unit_expression("kg/m³").unwrap();
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].unit_ref, "kg");
        assert_eq!(terms[0].exponent, 1);
        assert_eq!(terms[1].unit_ref, "m");
        assert_eq!(terms[1].exponent, -3);
    }

    #[test]
    fn test_parse_empty_error() {
        assert!(parse_unit_expression("").is_err());
    }

    #[test]
    fn test_parse_negative_exponent() {
        let terms = parse_unit_expression("s^-2").unwrap();
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].unit_ref, "s");
        assert_eq!(terms[0].exponent, -2);
    }

    #[test]
    fn test_is_simple_unit() {
        assert!(is_simple_unit("kg"));
        assert!(is_simple_unit("meter"));
        assert!(!is_simple_unit("m/s"));
        assert!(!is_simple_unit("m²"));
    }
}
