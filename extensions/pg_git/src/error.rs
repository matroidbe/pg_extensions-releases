use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgGitError {
    #[error("Repository '{0}' not found")]
    RepoNotFound(String),

    #[error("Repository '{0}' already exists")]
    RepoAlreadyExists(String),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Git error: {0}")]
    Git(String),

    #[error("Path error: {0}")]
    InvalidPath(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SPI error: {0}")]
    Spi(String),

    #[error("Binary file cannot be displayed as text: {0}")]
    BinaryFile(String),

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}
