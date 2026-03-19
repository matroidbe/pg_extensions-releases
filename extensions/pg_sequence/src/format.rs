//! Pure Rust format template engine for document number generation.
//! No SPI dependency — fully unit-testable.

/// Context for template variable resolution.
pub struct FormatContext {
    pub prefix: String,
    pub suffix: String,
    pub counter: i64,
    pub counter_pad: usize,
    pub year: String,
    pub month: String,
    pub day: String,
    pub scope: String,
}

/// Format a template string by replacing `{variable}` tokens.
///
/// Supported variables: prefix, suffix, counter, year, month, day, scope.
/// Unknown variables produce an error.
pub fn format_template(template: &str, ctx: &FormatContext) -> Result<String, String> {
    let mut result = String::with_capacity(template.len() + 32);
    let mut chars = template.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '{' {
            let mut var_name = String::new();
            let mut found_close = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    found_close = true;
                    break;
                }
                var_name.push(inner);
            }
            if !found_close {
                return Err(format!("unclosed template variable '{{{var_name}'"));
            }
            let value = resolve_variable(&var_name, ctx)?;
            result.push_str(&value);
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

fn resolve_variable(name: &str, ctx: &FormatContext) -> Result<String, String> {
    match name {
        "prefix" => Ok(ctx.prefix.clone()),
        "suffix" => Ok(ctx.suffix.clone()),
        "counter" => Ok(format!("{:0>width$}", ctx.counter, width = ctx.counter_pad)),
        "year" => Ok(ctx.year.clone()),
        "month" => Ok(ctx.month.clone()),
        "day" => Ok(ctx.day.clone()),
        "scope" => Ok(ctx.scope.clone()),
        _ => Err(format!("unknown template variable '{{{name}}}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_ctx() -> FormatContext {
        FormatContext {
            prefix: "INV-".to_string(),
            suffix: "".to_string(),
            counter: 42,
            counter_pad: 5,
            year: "2026".to_string(),
            month: "03".to_string(),
            day: "14".to_string(),
            scope: "".to_string(),
        }
    }

    #[test]
    fn test_simple_prefix_counter() {
        let ctx = default_ctx();
        let result = format_template("{prefix}{counter}", &ctx).unwrap();
        assert_eq!(result, "INV-00042");
    }

    #[test]
    fn test_full_format_with_year() {
        let ctx = default_ctx();
        let result = format_template("INV-{year}-{counter}", &ctx).unwrap();
        assert_eq!(result, "INV-2026-00042");
    }

    #[test]
    fn test_all_variables() {
        let ctx = FormatContext {
            prefix: "PO".to_string(),
            suffix: "-END".to_string(),
            counter: 1,
            counter_pad: 4,
            year: "2026".to_string(),
            month: "03".to_string(),
            day: "14".to_string(),
            scope: "WH-A".to_string(),
        };
        let result = format_template(
            "{prefix}/{scope}/{year}{month}{day}-{counter}{suffix}",
            &ctx,
        )
        .unwrap();
        assert_eq!(result, "PO/WH-A/20260314-0001-END");
    }

    #[test]
    fn test_unknown_variable_error() {
        let ctx = default_ctx();
        let result = format_template("{prefix}{unknown}", &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown template variable"));
    }

    #[test]
    fn test_unclosed_brace_error() {
        let ctx = default_ctx();
        let result = format_template("{prefix}{counter", &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unclosed"));
    }

    #[test]
    fn test_counter_padding() {
        let mut ctx = default_ctx();
        ctx.counter = 1;
        ctx.counter_pad = 8;
        let result = format_template("{counter}", &ctx).unwrap();
        assert_eq!(result, "00000001");
    }

    #[test]
    fn test_counter_no_padding() {
        let mut ctx = default_ctx();
        ctx.counter = 12345;
        ctx.counter_pad = 1;
        let result = format_template("{counter}", &ctx).unwrap();
        assert_eq!(result, "12345");
    }

    #[test]
    fn test_literal_text_passthrough() {
        let ctx = default_ctx();
        let result = format_template("STATIC-TEXT", &ctx).unwrap();
        assert_eq!(result, "STATIC-TEXT");
    }

    #[test]
    fn test_scope_in_format() {
        let mut ctx = default_ctx();
        ctx.scope = "DEPT-A".to_string();
        let result = format_template("{scope}/DO-{counter}", &ctx).unwrap();
        assert_eq!(result, "DEPT-A/DO-00042");
    }

    #[test]
    fn test_empty_template() {
        let ctx = default_ctx();
        let result = format_template("", &ctx).unwrap();
        assert_eq!(result, "");
    }
}
