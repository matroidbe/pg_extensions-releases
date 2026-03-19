use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgImageError {
    #[error("Image decode error: {0}")]
    DecodeError(String),

    #[error("Image encode error: {0}")]
    EncodeError(String),

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("EXIF parse error: {0}")]
    ExifError(String),

    #[error("No EXIF data found")]
    NoExifData,

    #[error("GPS data not available")]
    NoGpsData,

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("ONNX runtime error: {0}")]
    OnnxError(String),

    #[error("Tokenizer error: {0}")]
    TokenizerError(String),
}
