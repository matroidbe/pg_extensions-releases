//! RasterBuffer → PNG bytes. Mirrors the encode pattern used by
//! `pg_image/src/decode.rs::encode_image` — build an in-memory
//! `Cursor<Vec<u8>>`, call `image::DynamicImage::write_to` with
//! `ImageFormat::Png`, return the inner buffer.
//!
//! Single format for v1 (PNG with full alpha). JPEG / WebP can be
//! added later by mirroring pg_image's feature flags.

use std::io::Cursor;

use image::{ImageBuffer, ImageFormat, Rgba};

use super::rasterise::RasterBuffer;

/// Encode an RGBA `RasterBuffer` as PNG bytes.
///
/// Returns an `Err` only if the underlying `image` encoder errors —
/// which in practice happens for zero-sized images (caught upstream).
pub fn encode_png(buf: &RasterBuffer) -> Result<Vec<u8>, String> {
    if buf.width == 0 || buf.height == 0 {
        return Err(format!(
            "encode_png: refusing zero-sized image ({}x{})",
            buf.width, buf.height
        ));
    }
    let expected = (buf.width as usize) * (buf.height as usize) * 4;
    if buf.pixels.len() != expected {
        return Err(format!(
            "encode_png: pixel buffer length {} != expected {} for {}x{}",
            buf.pixels.len(),
            expected,
            buf.width,
            buf.height
        ));
    }
    let img: ImageBuffer<Rgba<u8>, Vec<u8>> =
        ImageBuffer::from_raw(buf.width, buf.height, buf.pixels.clone())
            .ok_or_else(|| "encode_png: ImageBuffer::from_raw failed".to_string())?;
    let mut out = Cursor::new(Vec::new());
    img.write_to(&mut out, ImageFormat::Png)
        .map_err(|e| format!("encode_png: {e}"))?;
    Ok(out.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_emits_png_magic() {
        let buf = RasterBuffer::new(4, 4, [255, 0, 0, 255]);
        let png = encode_png(&buf).unwrap();
        // PNG file signature is 89 50 4E 47 0D 0A 1A 0A.
        assert_eq!(
            &png[0..8],
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]
        );
        // IHDR chunk type immediately follows the signature + 4-byte length.
        assert_eq!(&png[12..16], b"IHDR");
    }

    #[test]
    fn encode_round_trip_dimensions() {
        let buf = RasterBuffer::new(32, 16, [0, 0, 0, 0]);
        let png = encode_png(&buf).unwrap();
        // Decode it back through the same crate to confirm dimensions.
        let decoded = image::load_from_memory(&png).unwrap();
        assert_eq!(decoded.width(), 32);
        assert_eq!(decoded.height(), 16);
    }

    #[test]
    fn encode_zero_size_errors() {
        let buf = RasterBuffer::new(0, 0, [0, 0, 0, 0]);
        assert!(encode_png(&buf).is_err());
    }
}
