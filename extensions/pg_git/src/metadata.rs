//! File metadata extraction.
//!
//! Pure Rust module — no pgrx dependencies, fully unit-testable.
//! Extracts mime type, encoding, language, size, and line count from file content.

/// Extracted metadata for a file.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub mime_type: String,
    pub size_bytes: i64,
    pub encoding: String,
    pub language: Option<String>,
    pub line_count: Option<i32>,
}

/// Extract metadata from a file path and its content bytes.
pub fn extract_metadata(path: &str, content: &[u8]) -> FileMetadata {
    let mime_type = guess_mime_type(path);
    let size_bytes = content.len() as i64;
    let is_text = std::str::from_utf8(content).is_ok();
    let encoding = if is_text { "utf-8" } else { "binary" }.to_string();
    let language = detect_language(path);
    let line_count = if is_text {
        Some(content.iter().filter(|&&b| b == b'\n').count() as i32)
    } else {
        None
    };

    FileMetadata {
        mime_type,
        size_bytes,
        encoding,
        language,
        line_count,
    }
}

/// Extract title from markdown content (first `# ` heading).
pub fn extract_title(path: &str, content: &[u8]) -> Option<String> {
    if !path.ends_with(".md") && !path.ends_with(".markdown") {
        return None;
    }
    let text = std::str::from_utf8(content).ok()?;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(heading) = trimmed.strip_prefix("# ") {
            let title = heading.trim();
            if !title.is_empty() {
                return Some(title.to_string());
            }
        }
    }
    None
}

fn guess_mime_type(path: &str) -> String {
    mime_guess::from_path(path)
        .first()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

fn detect_language(path: &str) -> Option<String> {
    let ext = path.rsplit('.').next()?;
    let lang = match ext.to_lowercase().as_str() {
        "md" | "markdown" => "markdown",
        "rs" => "rust",
        "py" => "python",
        "js" => "javascript",
        "ts" => "typescript",
        "tsx" => "typescript",
        "jsx" => "javascript",
        "rb" => "ruby",
        "go" => "go",
        "java" => "java",
        "c" => "c",
        "cpp" | "cc" | "cxx" => "c++",
        "h" | "hpp" => "c-header",
        "sql" => "sql",
        "sh" | "bash" | "zsh" => "shell",
        "json" => "json",
        "yaml" | "yml" => "yaml",
        "toml" => "toml",
        "xml" => "xml",
        "html" | "htm" => "html",
        "css" => "css",
        "txt" => "text",
        "csv" => "csv",
        "dockerfile" => "dockerfile",
        _ => return None,
    };
    Some(lang.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_markdown_metadata() {
        let meta = extract_metadata("docs/readme.md", b"# Hello\n\nWorld\n");
        assert_eq!(meta.mime_type, "text/markdown");
        assert_eq!(meta.encoding, "utf-8");
        assert_eq!(meta.language.as_deref(), Some("markdown"));
        assert_eq!(meta.line_count, Some(3));
        assert_eq!(meta.size_bytes, 15);
    }

    #[test]
    fn test_rust_metadata() {
        let content = b"fn main() {\n    println!(\"hello\");\n}\n";
        let meta = extract_metadata("src/main.rs", content);
        assert_eq!(meta.mime_type, "text/x-rust");
        assert_eq!(meta.encoding, "utf-8");
        assert_eq!(meta.language.as_deref(), Some("rust"));
        assert_eq!(meta.line_count, Some(3));
    }

    #[test]
    fn test_binary_metadata() {
        let content = &[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]; // JPEG header bytes
        let meta = extract_metadata("photo.jpg", content);
        assert_eq!(meta.mime_type, "image/jpeg");
        assert_eq!(meta.encoding, "binary");
        assert_eq!(meta.language, None);
        assert_eq!(meta.line_count, None);
    }

    #[test]
    fn test_unknown_extension() {
        let meta = extract_metadata("data.qzx", b"some data\n");
        assert_eq!(meta.mime_type, "application/octet-stream");
        assert_eq!(meta.encoding, "utf-8");
        assert_eq!(meta.language, None);
        assert_eq!(meta.line_count, Some(1));
    }

    #[test]
    fn test_empty_file() {
        let meta = extract_metadata("empty.md", b"");
        assert_eq!(meta.size_bytes, 0);
        assert_eq!(meta.encoding, "utf-8");
        assert_eq!(meta.line_count, Some(0));
    }

    #[test]
    fn test_extract_title_markdown() {
        assert_eq!(
            extract_title("readme.md", b"# Hello World\n\nContent"),
            Some("Hello World".to_string())
        );
    }

    #[test]
    fn test_extract_title_no_heading() {
        assert_eq!(extract_title("readme.md", b"No heading here\n"), None);
    }

    #[test]
    fn test_extract_title_non_markdown() {
        assert_eq!(extract_title("code.rs", b"# This is a comment\n"), None);
    }

    #[test]
    fn test_extract_title_heading_not_first_line() {
        assert_eq!(
            extract_title("readme.md", b"Some text\n\n# Title Here\n"),
            Some("Title Here".to_string())
        );
    }
}
