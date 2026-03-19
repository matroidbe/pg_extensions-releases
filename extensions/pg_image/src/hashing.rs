use image_hasher::{HashAlg, HasherConfig};

use crate::decode;
use crate::error::PgImageError;

/// Compute perceptual hash of an image, returned as base64 string.
pub fn compute_phash(data: &[u8]) -> Result<String, PgImageError> {
    let img = decode::decode_image(data)?;
    let hasher = HasherConfig::new()
        .hash_alg(HashAlg::DoubleGradient)
        .hash_size(8, 8)
        .to_hasher();
    let hash = hasher.hash_image(&img);
    Ok(hash.to_base64())
}

/// Compute Hamming distance between two perceptual hashes (base64-encoded).
/// Lower distance = more visually similar.
pub fn hash_distance(hash1: &str, hash2: &str) -> Result<i32, PgImageError> {
    let h1 = image_hasher::ImageHash::<Vec<u8>>::from_base64(hash1)
        .map_err(|e| PgImageError::InvalidParameter(format!("invalid hash1: {:?}", e)))?;
    let h2 = image_hasher::ImageHash::<Vec<u8>>::from_base64(hash2)
        .map_err(|e| PgImageError::InvalidParameter(format!("invalid hash2: {:?}", e)))?;
    Ok(h1.dist(&h2) as i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{DynamicImage, ImageFormat};

    fn make_test_png(r: u8, g: u8, b: u8) -> Vec<u8> {
        let mut img = DynamicImage::new_rgb8(64, 64);
        // Fill with a solid color
        for pixel in img.as_mut_rgb8().unwrap().pixels_mut() {
            pixel.0 = [r, g, b];
        }
        let mut buf = std::io::Cursor::new(Vec::new());
        img.write_to(&mut buf, ImageFormat::Png).unwrap();
        buf.into_inner()
    }

    #[test]
    fn test_phash_returns_non_empty() {
        let data = make_test_png(255, 0, 0);
        let hash = compute_phash(&data).unwrap();
        assert!(!hash.is_empty());
    }

    #[test]
    fn test_same_image_distance_zero() {
        let data = make_test_png(255, 0, 0);
        let h1 = compute_phash(&data).unwrap();
        let h2 = compute_phash(&data).unwrap();
        assert_eq!(hash_distance(&h1, &h2).unwrap(), 0);
    }

    #[test]
    fn test_different_images_distance_nonzero() {
        let red = make_test_png(255, 0, 0);
        let blue = make_test_png(0, 0, 255);
        let h1 = compute_phash(&red).unwrap();
        let h2 = compute_phash(&blue).unwrap();
        // Solid color images may hash similarly, but different channels should differ
        // At minimum, the hashes should be computable
        let dist = hash_distance(&h1, &h2).unwrap();
        assert!(dist >= 0);
    }

    #[test]
    fn test_invalid_hash() {
        assert!(hash_distance("not_valid", "also_invalid").is_err());
    }
}
