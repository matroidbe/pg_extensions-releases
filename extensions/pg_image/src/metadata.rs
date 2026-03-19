use crate::decode;
use crate::error::PgImageError;

/// Image width in pixels (header-only, fast).
pub fn width(data: &[u8]) -> Result<i32, PgImageError> {
    let (w, _) = decode::read_dimensions(data)?;
    Ok(w as i32)
}

/// Image height in pixels (header-only, fast).
pub fn height(data: &[u8]) -> Result<i32, PgImageError> {
    let (_, h) = decode::read_dimensions(data)?;
    Ok(h as i32)
}

/// Image format name (header-only, fast).
pub fn format(data: &[u8]) -> Result<String, PgImageError> {
    let fmt = decode::detect_format(data)?;
    Ok(decode::format_name(fmt).to_string())
}

/// Color space name (requires decoder init, but not full pixel decode).
pub fn colorspace(data: &[u8]) -> Result<String, PgImageError> {
    let ct = decode::read_color_type(data)?;
    Ok(decode::colorspace_name(ct).to_string())
}

/// Number of color channels.
pub fn channels(data: &[u8]) -> Result<i32, PgImageError> {
    let ct = decode::read_color_type(data)?;
    Ok(decode::color_channels(ct) as i32)
}

/// Bits per channel.
pub fn bit_depth(data: &[u8]) -> Result<i32, PgImageError> {
    let ct = decode::read_color_type(data)?;
    Ok(decode::color_bit_depth(ct) as i32)
}

/// Encoded image size in bytes.
pub fn size_bytes(data: &[u8]) -> i64 {
    data.len() as i64
}

/// All metadata as a JSON object.
pub fn metadata_json(data: &[u8]) -> Result<serde_json::Value, PgImageError> {
    let (w, h) = decode::read_dimensions(data)?;
    let fmt = decode::detect_format(data)?;
    let ct = decode::read_color_type(data)?;

    Ok(serde_json::json!({
        "width": w,
        "height": h,
        "format": decode::format_name(fmt),
        "colorspace": decode::colorspace_name(ct),
        "channels": decode::color_channels(ct),
        "bit_depth": decode::color_bit_depth(ct),
        "size_bytes": data.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal valid 1x1 red RGB PNG (69 bytes) — correct CRCs
    const TINY_PNG: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90,
        0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0xF8,
        0xCF, 0xC0, 0x00, 0x00, 0x03, 0x01, 0x01, 0x00, 0xC9, 0xFE, 0x92, 0xEF, 0x00, 0x00, 0x00,
        0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];

    #[test]
    fn test_width() {
        assert_eq!(width(TINY_PNG).unwrap(), 1);
    }

    #[test]
    fn test_height() {
        assert_eq!(height(TINY_PNG).unwrap(), 1);
    }

    #[test]
    fn test_format() {
        assert_eq!(format(TINY_PNG).unwrap(), "png");
    }

    #[test]
    fn test_colorspace() {
        assert_eq!(colorspace(TINY_PNG).unwrap(), "rgb");
    }

    #[test]
    fn test_channels() {
        assert_eq!(channels(TINY_PNG).unwrap(), 3);
    }

    #[test]
    fn test_bit_depth() {
        assert_eq!(bit_depth(TINY_PNG).unwrap(), 8);
    }

    #[test]
    fn test_size_bytes() {
        assert_eq!(size_bytes(TINY_PNG), TINY_PNG.len() as i64);
    }

    #[test]
    fn test_metadata_json() {
        let json = metadata_json(TINY_PNG).unwrap();
        assert_eq!(json["width"], 1);
        assert_eq!(json["height"], 1);
        assert_eq!(json["format"], "png");
        assert_eq!(json["colorspace"], "rgb");
        assert_eq!(json["channels"], 3);
        assert_eq!(json["bit_depth"], 8);
    }

    #[test]
    fn test_invalid_data() {
        assert!(width(&[0x00]).is_err());
        assert!(height(&[0x00]).is_err());
        assert!(format(&[0x00]).is_err());
    }
}
