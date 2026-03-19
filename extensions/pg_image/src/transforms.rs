use image::{DynamicImage, ImageFormat};

use crate::decode;
use crate::error::PgImageError;
use crate::exif;

/// Resize image to exact dimensions using Lanczos3 filter.
pub fn resize(data: &[u8], width: i32, height: i32) -> Result<Vec<u8>, PgImageError> {
    if width <= 0 || height <= 0 {
        return Err(PgImageError::InvalidParameter(
            "width and height must be > 0".into(),
        ));
    }

    let format = decode::detect_format(data)?;
    let img = decode::decode_image(data)?;
    let resized = img.resize_exact(
        width as u32,
        height as u32,
        image::imageops::FilterType::Lanczos3,
    );
    encode_with_fallback(&resized, format)
}

/// Create thumbnail preserving aspect ratio.
pub fn thumbnail(data: &[u8], max_width: i32, max_height: i32) -> Result<Vec<u8>, PgImageError> {
    if max_width <= 0 || max_height <= 0 {
        return Err(PgImageError::InvalidParameter(
            "max_width and max_height must be > 0".into(),
        ));
    }

    let format = decode::detect_format(data)?;
    let img = decode::decode_image(data)?;
    let thumb = img.thumbnail(max_width as u32, max_height as u32);
    encode_with_fallback(&thumb, format)
}

/// Crop a region from the image.
pub fn crop(data: &[u8], x: i32, y: i32, width: i32, height: i32) -> Result<Vec<u8>, PgImageError> {
    if x < 0 || y < 0 || width <= 0 || height <= 0 {
        return Err(PgImageError::InvalidParameter(
            "crop coordinates must be >= 0 and dimensions > 0".into(),
        ));
    }

    let format = decode::detect_format(data)?;
    let mut img = decode::decode_image(data)?;

    let (img_w, img_h) = (img.width(), img.height());
    let x = x as u32;
    let y = y as u32;
    let w = width as u32;
    let h = height as u32;

    if x + w > img_w || y + h > img_h {
        return Err(PgImageError::InvalidParameter(format!(
            "crop region ({}+{}, {}+{}) exceeds image dimensions ({}x{})",
            x, w, y, h, img_w, img_h
        )));
    }

    let cropped = img.crop(x, y, w, h);
    encode_with_fallback(&cropped, format)
}

/// Convert image to a different format.
pub fn convert_format(data: &[u8], format_name: &str) -> Result<Vec<u8>, PgImageError> {
    let target_format = decode::parse_format(format_name)?;
    let img = decode::decode_image(data)?;
    decode::encode_image(&img, target_format)
}

/// Convert image to grayscale.
pub fn grayscale(data: &[u8]) -> Result<Vec<u8>, PgImageError> {
    let format = decode::detect_format(data)?;
    let img = decode::decode_image(data)?;
    let gray = img.grayscale();
    encode_with_fallback(&gray, format)
}

/// Rotate image by 90, 180, or 270 degrees.
pub fn rotate(data: &[u8], degrees: i32) -> Result<Vec<u8>, PgImageError> {
    let format = decode::detect_format(data)?;
    let img = decode::decode_image(data)?;

    let rotated = match degrees {
        90 => img.rotate90(),
        180 => img.rotate180(),
        270 => img.rotate270(),
        0 => img,
        _ => {
            return Err(PgImageError::InvalidParameter(
                "degrees must be 0, 90, 180, or 270".into(),
            ));
        }
    };

    encode_with_fallback(&rotated, format)
}

/// Apply EXIF orientation and return correctly oriented image.
pub fn auto_orient(data: &[u8]) -> Result<Vec<u8>, PgImageError> {
    let format = decode::detect_format(data)?;
    let img = decode::decode_image(data)?;

    let orientation = exif::orientation(data).unwrap_or(1);
    let oriented = apply_orientation(img, orientation);

    encode_with_fallback(&oriented, format)
}

/// Apply EXIF orientation transform.
/// See: https://sirv.com/help/articles/rotate-photos-to-be-upright/
fn apply_orientation(img: DynamicImage, orientation: i32) -> DynamicImage {
    match orientation {
        1 => img,                     // Normal
        2 => img.fliph(),             // Flip horizontal
        3 => img.rotate180(),         // Rotate 180
        4 => img.flipv(),             // Flip vertical
        5 => img.rotate270().fliph(), // Rotate 270 + flip horizontal
        6 => img.rotate90(),          // Rotate 90
        7 => img.rotate90().fliph(),  // Rotate 90 + flip horizontal
        8 => img.rotate270(),         // Rotate 270
        _ => img,                     // Unknown, return as-is
    }
}

/// Encode image, falling back to PNG if the original format doesn't support writing.
fn encode_with_fallback(img: &DynamicImage, format: ImageFormat) -> Result<Vec<u8>, PgImageError> {
    match decode::encode_image(img, format) {
        Ok(data) => Ok(data),
        Err(_) => {
            // Fall back to PNG for formats that don't support writing
            decode::encode_image(img, ImageFormat::Png)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal valid 1x1 RGB PNG
    const TINY_PNG: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90,
        0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08, 0xD7, 0x63, 0xF8,
        0xCF, 0xC0, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0xE2, 0x21, 0xBC, 0x33, 0x00, 0x00, 0x00,
        0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];

    fn make_test_png(width: u32, height: u32) -> Vec<u8> {
        let img = DynamicImage::new_rgb8(width, height);
        let mut buf = std::io::Cursor::new(Vec::new());
        img.write_to(&mut buf, ImageFormat::Png).unwrap();
        buf.into_inner()
    }

    #[test]
    fn test_resize() {
        let data = make_test_png(100, 50);
        let result = resize(&data, 10, 5).unwrap();
        let (w, h) = decode::read_dimensions(&result).unwrap();
        assert_eq!(w, 10);
        assert_eq!(h, 5);
    }

    #[test]
    fn test_resize_invalid_dimensions() {
        assert!(resize(TINY_PNG, 0, 10).is_err());
        assert!(resize(TINY_PNG, 10, -1).is_err());
    }

    #[test]
    fn test_thumbnail_preserves_aspect_ratio() {
        let data = make_test_png(200, 100);
        let result = thumbnail(&data, 50, 50).unwrap();
        let (w, h) = decode::read_dimensions(&result).unwrap();
        // 200x100 fit into 50x50 → 50x25
        assert_eq!(w, 50);
        assert_eq!(h, 25);
    }

    #[test]
    fn test_crop() {
        let data = make_test_png(100, 100);
        let result = crop(&data, 10, 10, 50, 50).unwrap();
        let (w, h) = decode::read_dimensions(&result).unwrap();
        assert_eq!(w, 50);
        assert_eq!(h, 50);
    }

    #[test]
    fn test_crop_out_of_bounds() {
        let data = make_test_png(100, 100);
        assert!(crop(&data, 80, 80, 50, 50).is_err());
    }

    #[test]
    fn test_convert_format() {
        let png_data = make_test_png(10, 10);
        let jpeg_data = convert_format(&png_data, "jpeg").unwrap();
        let fmt = decode::detect_format(&jpeg_data).unwrap();
        assert_eq!(fmt, ImageFormat::Jpeg);
    }

    #[test]
    fn test_grayscale() {
        let data = make_test_png(10, 10);
        let result = grayscale(&data).unwrap();
        // Should still be a valid image
        let (w, h) = decode::read_dimensions(&result).unwrap();
        assert_eq!(w, 10);
        assert_eq!(h, 10);
    }

    #[test]
    fn test_rotate_90() {
        let data = make_test_png(100, 50);
        let result = rotate(&data, 90).unwrap();
        let (w, h) = decode::read_dimensions(&result).unwrap();
        // 100x50 rotated 90° → 50x100
        assert_eq!(w, 50);
        assert_eq!(h, 100);
    }

    #[test]
    fn test_rotate_invalid() {
        assert!(rotate(TINY_PNG, 45).is_err());
    }

    #[test]
    fn test_auto_orient_no_exif() {
        let data = make_test_png(10, 10);
        // No EXIF → returns same dimensions
        let result = auto_orient(&data).unwrap();
        let (w, h) = decode::read_dimensions(&result).unwrap();
        assert_eq!(w, 10);
        assert_eq!(h, 10);
    }

    #[test]
    fn test_apply_orientation() {
        let img = DynamicImage::new_rgb8(100, 50);

        // Orientation 1: no change
        let o1 = apply_orientation(img.clone(), 1);
        assert_eq!(o1.width(), 100);
        assert_eq!(o1.height(), 50);

        // Orientation 6: rotate 90 → swaps dimensions
        let o6 = apply_orientation(img.clone(), 6);
        assert_eq!(o6.width(), 50);
        assert_eq!(o6.height(), 100);

        // Orientation 3: rotate 180 → same dimensions
        let o3 = apply_orientation(img, 3);
        assert_eq!(o3.width(), 100);
        assert_eq!(o3.height(), 50);
    }
}
