use std::collections::HashMap;

use super::entity::{IfcEntity, IfcValue};
use super::tokenizer::{Token, Tokenizer};

/// Header information from the IFC file.
#[derive(Debug, Default)]
pub struct IfcHeader {
    pub schema_identifiers: Vec<String>,
}

/// Parse result: header + entity map.
#[allow(dead_code)]
pub struct ParseResult {
    pub header: IfcHeader,
    pub entities: HashMap<u64, IfcEntity>,
}

/// Parse a complete IFC file from bytes.
pub fn parse(input: &[u8]) -> Result<ParseResult, String> {
    let mut tokenizer = Tokenizer::new(input);
    let mut header = IfcHeader::default();
    let mut entities = HashMap::new();
    let mut in_data = false;
    let mut in_header = false;

    while let Some(tok) = tokenizer.next_token()? {
        match &tok {
            Token::Keyword(kw) => match kw.as_str() {
                "ISO_10303_21" | "ISO" => {
                    // Skip the file start marker and any trailing tokens on that line
                    skip_to_semicolon(&mut tokenizer)?;
                }
                "HEADER" => {
                    skip_to_semicolon(&mut tokenizer)?;
                    in_header = true;
                }
                "DATA" => {
                    skip_to_semicolon(&mut tokenizer)?;
                    in_data = true;
                    in_header = false;
                }
                "ENDSEC" => {
                    skip_to_semicolon(&mut tokenizer)?;
                    if in_data {
                        in_data = false;
                    }
                    in_header = false;
                }
                "END_ISO_10303_21" | "END" => {
                    break;
                }
                "FILE_SCHEMA" if in_header => {
                    header.schema_identifiers = parse_file_schema(&mut tokenizer)?;
                }
                _ if in_header => {
                    // Skip other header entities
                    skip_to_semicolon(&mut tokenizer)?;
                }
                _ => {}
            },
            Token::EntityRef(id) if in_data => {
                let entity = parse_entity_instance(&mut tokenizer, *id)?;
                entities.insert(*id, entity);
            }
            _ => {
                // Skip unexpected tokens (e.g., "-" in "ISO-10303-21;")
            }
        }
    }

    Ok(ParseResult { header, entities })
}

/// Parse FILE_SCHEMA(('IFC2X3')); — extract schema identifiers.
fn parse_file_schema(tokenizer: &mut Tokenizer) -> Result<Vec<String>, String> {
    let mut schemas = Vec::new();
    // Expect ( ( 'schema1', 'schema2' ) ) ;
    let mut depth = 0;
    loop {
        match tokenizer.next_token()? {
            Some(Token::LeftParen) => depth += 1,
            Some(Token::RightParen) => {
                depth -= 1;
                if depth <= 0 {
                    // Skip to semicolon
                    skip_to_semicolon(tokenizer)?;
                    break;
                }
            }
            Some(Token::StringLiteral(s)) => schemas.push(s),
            Some(Token::Semicolon) => break,
            None => break,
            _ => {}
        }
    }
    Ok(schemas)
}

/// Skip tokens until we hit a semicolon (inclusive).
fn skip_to_semicolon(tokenizer: &mut Tokenizer) -> Result<(), String> {
    loop {
        match tokenizer.next_token()? {
            Some(Token::Semicolon) | None => return Ok(()),
            _ => {}
        }
    }
}

/// Parse `= TYPENAME(attr1, attr2, ...);` after the entity ref was already consumed.
fn parse_entity_instance(tokenizer: &mut Tokenizer, id: u64) -> Result<IfcEntity, String> {
    // Expect '='
    match tokenizer.next_token()? {
        Some(Token::Equals) => {}
        other => return Err(format!("expected '=' after #{id}, got {other:?}")),
    }

    // Expect type name keyword
    let type_name = match tokenizer.next_token()? {
        Some(Token::Keyword(name)) => name,
        other => return Err(format!("expected type name after #{id} =, got {other:?}")),
    };

    // Expect '('
    match tokenizer.next_token()? {
        Some(Token::LeftParen) => {}
        other => {
            return Err(format!(
                "expected '(' after #{id} = {type_name}, got {other:?}"
            ))
        }
    }

    // Parse attributes until ')'
    let attributes = parse_attribute_list(tokenizer)?;

    // Expect ';'
    match tokenizer.next_token()? {
        Some(Token::Semicolon) => {}
        other => {
            return Err(format!(
                "expected ';' after #{id} = {type_name}(...), got {other:?}"
            ))
        }
    }

    Ok(IfcEntity {
        id,
        type_name,
        attributes,
    })
}

/// Parse comma-separated attributes until ')'. The opening '(' has been consumed.
fn parse_attribute_list(tokenizer: &mut Tokenizer) -> Result<Vec<IfcValue>, String> {
    let mut attrs = Vec::new();

    // Check for empty list
    // We need to peek by trying to parse
    loop {
        let tok = tokenizer.next_token()?;
        match tok {
            Some(Token::RightParen) => return Ok(attrs),
            Some(t) => {
                let value = parse_value_from_token(t, tokenizer)?;
                attrs.push(value);

                // Expect ',' or ')'
                match tokenizer.next_token()? {
                    Some(Token::Comma) => continue,
                    Some(Token::RightParen) => return Ok(attrs),
                    other => {
                        return Err(format!(
                            "expected ',' or ')' in attribute list, got {other:?}"
                        ))
                    }
                }
            }
            None => return Err("unexpected end of input in attribute list".into()),
        }
    }
}

/// Parse a single attribute value, given the first token already consumed.
fn parse_value_from_token(token: Token, tokenizer: &mut Tokenizer) -> Result<IfcValue, String> {
    match token {
        Token::EntityRef(id) => Ok(IfcValue::Ref(id)),
        Token::StringLiteral(s) => Ok(IfcValue::String(s)),
        Token::Integer(v) => Ok(IfcValue::Integer(v)),
        Token::Real(v) => Ok(IfcValue::Real(v)),
        Token::Enum(s) => {
            if s == "T" || s == "TRUE" {
                Ok(IfcValue::Bool(true))
            } else if s == "F" || s == "FALSE" {
                Ok(IfcValue::Bool(false))
            } else {
                Ok(IfcValue::Enum(s))
            }
        }
        Token::Unset => Ok(IfcValue::Null),
        Token::Derived => Ok(IfcValue::Derived),
        Token::LeftParen => {
            // Nested list
            let items = parse_attribute_list(tokenizer)?;
            Ok(IfcValue::List(items))
        }
        Token::Keyword(name) => {
            // Typed value: IFCLABEL('text') or IFCREAL(1.0)
            match tokenizer.next_token()? {
                Some(Token::LeftParen) => {
                    let inner_attrs = parse_attribute_list(tokenizer)?;
                    if inner_attrs.len() == 1 {
                        Ok(IfcValue::Typed(
                            name,
                            Box::new(inner_attrs.into_iter().next().unwrap()),
                        ))
                    } else {
                        // Multiple args — store as typed list
                        Ok(IfcValue::Typed(name, Box::new(IfcValue::List(inner_attrs))))
                    }
                }
                other => Err(format!(
                    "expected '(' after typed value keyword {name}, got {other:?}"
                )),
            }
        }
        other => Err(format!("unexpected token in attribute: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_IFC: &[u8] = b"ISO-10303-21;
HEADER;
FILE_DESCRIPTION((''), '2;1');
FILE_NAME('test.ifc', '2024-01-01', (''), (''), '', '', '');
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCCARTESIANPOINT((0.0, 0.0, 0.0));
#2 = IFCDIRECTION((1.0, 0.0, 0.0));
#3 = IFCDIRECTION((0.0, 0.0, 1.0));
#10 = IFCAXIS2PLACEMENT3D(#1, #3, #2);
ENDSEC;
END-ISO-10303-21;";

    #[test]
    fn test_parse_minimal_ifc() {
        let result = parse(MINIMAL_IFC).unwrap();
        assert_eq!(result.entities.len(), 4);
        assert!(result.entities.contains_key(&1));
        assert!(result.entities.contains_key(&2));
        assert!(result.entities.contains_key(&3));
        assert!(result.entities.contains_key(&10));
    }

    #[test]
    fn test_parse_schema_identifier() {
        let result = parse(MINIMAL_IFC).unwrap();
        assert_eq!(result.header.schema_identifiers, vec!["IFC2X3"]);
    }

    #[test]
    fn test_parse_cartesian_point() {
        let result = parse(MINIMAL_IFC).unwrap();
        let pt = &result.entities[&1];
        assert_eq!(pt.type_name, "IFCCARTESIANPOINT");
        assert_eq!(pt.attributes.len(), 1); // single list attribute
        let coords = pt.attributes[0].as_list().unwrap();
        assert_eq!(coords.len(), 3);
        assert_eq!(coords[0].as_f64(), Some(0.0));
    }

    #[test]
    fn test_parse_axis2placement() {
        let result = parse(MINIMAL_IFC).unwrap();
        let p = &result.entities[&10];
        assert_eq!(p.type_name, "IFCAXIS2PLACEMENT3D");
        assert_eq!(p.attributes.len(), 3);
        assert_eq!(p.attributes[0].as_ref(), Some(1)); // Location = #1
        assert_eq!(p.attributes[1].as_ref(), Some(3)); // Axis = #3
        assert_eq!(p.attributes[2].as_ref(), Some(2)); // RefDirection = #2
    }

    #[test]
    fn test_parse_null_and_derived() {
        let input = b"ISO-10303-21;
HEADER;
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCWALL('guid', $, 'name', $, *, #2, #3, $);
ENDSEC;
END-ISO-10303-21;";
        let result = parse(input).unwrap();
        let wall = &result.entities[&1];
        assert_eq!(wall.type_name, "IFCWALL");
        assert_eq!(wall.attributes[0].as_str(), Some("guid"));
        assert!(wall.attributes[1].is_null());
        assert_eq!(wall.attributes[2].as_str(), Some("name"));
        assert!(wall.attributes[3].is_null());
        assert_eq!(wall.attributes[4], IfcValue::Derived);
        assert_eq!(wall.attributes[5].as_ref(), Some(2));
    }

    #[test]
    fn test_parse_typed_value() {
        let input = b"ISO-10303-21;
HEADER;
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCPROPERTYSINGLEVALUE('FireRating', $, IFCLABEL('2HR'), $);
ENDSEC;
END-ISO-10303-21;";
        let result = parse(input).unwrap();
        let prop = &result.entities[&1];
        assert_eq!(prop.type_name, "IFCPROPERTYSINGLEVALUE");
        assert_eq!(prop.attributes[0].as_str(), Some("FireRating"));
        match &prop.attributes[2] {
            IfcValue::Typed(name, inner) => {
                assert_eq!(name, "IFCLABEL");
                assert_eq!(inner.as_str(), Some("2HR"));
            }
            other => panic!("expected Typed, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_nested_list() {
        let input = b"ISO-10303-21;
HEADER;
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCTEST(((1, 2), (3, 4)));
ENDSEC;
END-ISO-10303-21;";
        let result = parse(input).unwrap();
        let ent = &result.entities[&1];
        let outer = ent.attributes[0].as_list().unwrap();
        assert_eq!(outer.len(), 2);
        let inner1 = outer[0].as_list().unwrap();
        assert_eq!(inner1.len(), 2);
        assert_eq!(inner1[0].as_i64(), Some(1));
    }

    #[test]
    fn test_parse_empty_list() {
        let input = b"ISO-10303-21;
HEADER;
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCTEST(());
ENDSEC;
END-ISO-10303-21;";
        let result = parse(input).unwrap();
        let ent = &result.entities[&1];
        let list = ent.attributes[0].as_list().unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_parse_multiline_entity() {
        let input = b"ISO-10303-21;
HEADER;
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCWALL(
    'guid',
    $,
    'name',
    $,
    $,
    #2,
    #3,
    $
);
ENDSEC;
END-ISO-10303-21;";
        let result = parse(input).unwrap();
        let wall = &result.entities[&1];
        assert_eq!(wall.type_name, "IFCWALL");
        assert_eq!(wall.attributes.len(), 8);
    }
}
