use image::{ColorType, DynamicImage, ImageFormat, ImageReader};
use std::io::Cursor;

use crate::error::PgImageError;

/// Detect image format from magic bytes without decoding.
pub fn detect_format(data: &[u8]) -> Result<ImageFormat, PgImageError> {
    ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))?
        .format()
        .ok_or_else(|| PgImageError::UnsupportedFormat("unknown format".into()))
}

/// Read image dimensions from header only (no full pixel decode).
pub fn read_dimensions(data: &[u8]) -> Result<(u32, u32), PgImageError> {
    ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))?
        .into_dimensions()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))
}

/// Read ColorType from decoder without full pixel decode.
pub fn read_color_type(data: &[u8]) -> Result<ColorType, PgImageError> {
    use image::ImageDecoder;
    let reader = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))?;
    let decoder = reader
        .into_decoder()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))?;
    Ok(decoder.color_type())
}

/// Full decode of image bytes into DynamicImage.
pub fn decode_image(data: &[u8]) -> Result<DynamicImage, PgImageError> {
    ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))?
        .decode()
        .map_err(|e| PgImageError::DecodeError(e.to_string()))
}

/// Encode a DynamicImage to bytes in the given format.
pub fn encode_image(img: &DynamicImage, format: ImageFormat) -> Result<Vec<u8>, PgImageError> {
    let mut buf = Cursor::new(Vec::new());
    img.write_to(&mut buf, format)
        .map_err(|e| PgImageError::EncodeError(e.to_string()))?;
    Ok(buf.into_inner())
}

/// Parse a format name string to ImageFormat.
pub fn parse_format(name: &str) -> Result<ImageFormat, PgImageError> {
    match name.to_lowercase().as_str() {
        "jpeg" | "jpg" => Ok(ImageFormat::Jpeg),
        "png" => Ok(ImageFormat::Png),
        "webp" => Ok(ImageFormat::WebP),
        "gif" => Ok(ImageFormat::Gif),
        "tiff" | "tif" => Ok(ImageFormat::Tiff),
        "bmp" => Ok(ImageFormat::Bmp),
        other => Err(PgImageError::UnsupportedFormat(other.to_string())),
    }
}

/// Convert ImageFormat to human-readable name.
pub fn format_name(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Jpeg => "jpeg",
        ImageFormat::Png => "png",
        ImageFormat::WebP => "webp",
        ImageFormat::Gif => "gif",
        ImageFormat::Tiff => "tiff",
        ImageFormat::Bmp => "bmp",
        _ => "unknown",
    }
}

/// Convert ColorType to human-readable colorspace name.
pub fn colorspace_name(ct: ColorType) -> &'static str {
    match ct {
        ColorType::L8 | ColorType::L16 => "grayscale",
        ColorType::La8 | ColorType::La16 => "grayscale+alpha",
        ColorType::Rgb8 | ColorType::Rgb16 | ColorType::Rgb32F => "rgb",
        ColorType::Rgba8 | ColorType::Rgba16 | ColorType::Rgba32F => "rgba",
        _ => "unknown",
    }
}

/// Get number of channels from ColorType.
pub fn color_channels(ct: ColorType) -> u8 {
    ct.channel_count()
}

/// Get bits per channel from ColorType.
pub fn color_bit_depth(ct: ColorType) -> u8 {
    ct.bytes_per_pixel() / ct.channel_count() * 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_format_roundtrip() {
        assert_eq!(format_name(parse_format("jpeg").unwrap()), "jpeg");
        assert_eq!(format_name(parse_format("jpg").unwrap()), "jpeg");
        assert_eq!(format_name(parse_format("png").unwrap()), "png");
        assert_eq!(format_name(parse_format("webp").unwrap()), "webp");
        assert_eq!(format_name(parse_format("tiff").unwrap()), "tiff");
        assert_eq!(format_name(parse_format("bmp").unwrap()), "bmp");
    }

    #[test]
    fn test_parse_format_case_insensitive() {
        assert!(parse_format("JPEG").is_ok());
        assert!(parse_format("Png").is_ok());
    }

    #[test]
    fn test_parse_format_unknown() {
        assert!(parse_format("avif").is_err());
        assert!(parse_format("xyz").is_err());
    }

    #[test]
    fn test_colorspace_name() {
        assert_eq!(colorspace_name(ColorType::Rgb8), "rgb");
        assert_eq!(colorspace_name(ColorType::Rgba8), "rgba");
        assert_eq!(colorspace_name(ColorType::L8), "grayscale");
        assert_eq!(colorspace_name(ColorType::La8), "grayscale+alpha");
    }

    #[test]
    fn test_color_channels() {
        assert_eq!(color_channels(ColorType::Rgb8), 3);
        assert_eq!(color_channels(ColorType::Rgba8), 4);
        assert_eq!(color_channels(ColorType::L8), 1);
        assert_eq!(color_channels(ColorType::La8), 2);
    }

    #[test]
    fn test_color_bit_depth() {
        assert_eq!(color_bit_depth(ColorType::Rgb8), 8);
        assert_eq!(color_bit_depth(ColorType::Rgb16), 16);
        assert_eq!(color_bit_depth(ColorType::L8), 8);
    }

    // Minimal valid 1x1 red PNG (69 bytes) — generated with correct CRCs
    const TINY_PNG: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90,
        0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0xF8,
        0xCF, 0xC0, 0x00, 0x00, 0x03, 0x01, 0x01, 0x00, 0xC9, 0xFE, 0x92, 0xEF, 0x00, 0x00, 0x00,
        0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];

    #[test]
    fn test_detect_format_png() {
        let fmt = detect_format(TINY_PNG).unwrap();
        assert_eq!(fmt, ImageFormat::Png);
    }

    #[test]
    fn test_read_dimensions_png() {
        let (w, h) = read_dimensions(TINY_PNG).unwrap();
        assert_eq!(w, 1);
        assert_eq!(h, 1);
    }

    #[test]
    fn test_read_color_type_png() {
        let ct = read_color_type(TINY_PNG).unwrap();
        assert_eq!(ct, ColorType::Rgb8);
    }

    #[test]
    fn test_decode_image_png() {
        let img = decode_image(TINY_PNG).unwrap();
        assert_eq!(img.width(), 1);
        assert_eq!(img.height(), 1);
    }

    #[test]
    fn test_encode_roundtrip() {
        let img = decode_image(TINY_PNG).unwrap();
        let encoded = encode_image(&img, ImageFormat::Png).unwrap();
        let (w, h) = read_dimensions(&encoded).unwrap();
        assert_eq!(w, 1);
        assert_eq!(h, 1);
    }

    #[test]
    fn test_invalid_data() {
        assert!(detect_format(&[0x00, 0x01, 0x02]).is_err());
        assert!(read_dimensions(&[0x00, 0x01, 0x02]).is_err());
        assert!(decode_image(&[0x00, 0x01, 0x02]).is_err());
    }
}
