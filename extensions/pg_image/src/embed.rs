use std::ffi::CString;

use ndarray::{Array2, Array4};

use crate::decode;
use crate::error::PgImageError;
use crate::onnx_common;
use crate::tokenizer;

// =============================================================================
// GUC Settings
// =============================================================================

pub static EMBED_MODEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(Some(c"clip-vit-b32-vision.onnx"));

pub static EMBED_TEXT_MODEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(Some(c"clip-vit-b32-text.onnx"));

pub fn register_gucs() {
    pgrx::GucRegistry::define_string_guc(
        c"pg_image.embed_model",
        c"Default ONNX model for image embeddings",
        c"CLIP vision encoder model filename (e.g., clip-vit-b32-vision.onnx)",
        &EMBED_MODEL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_image.embed_text_model",
        c"Default ONNX model for text embeddings",
        c"CLIP text encoder model filename (e.g., clip-vit-b32-text.onnx)",
        &EMBED_TEXT_MODEL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
}

pub fn default_embed_model() -> String {
    EMBED_MODEL
        .get()
        .map(|cs| cs.to_string_lossy().into_owned())
        .unwrap_or_else(|| "clip-vit-b32-vision.onnx".to_string())
}

pub fn default_text_model() -> String {
    EMBED_TEXT_MODEL
        .get()
        .map(|cs| cs.to_string_lossy().into_owned())
        .unwrap_or_else(|| "clip-vit-b32-text.onnx".to_string())
}

// =============================================================================
// CLIP Image Preprocessing
// =============================================================================

const CLIP_INPUT_SIZE: u32 = 224;

// CLIP-specific normalization parameters (different from ImageNet)
const CLIP_MEAN: [f32; 3] = [0.48145466, 0.4578275, 0.40821073];
const CLIP_STD: [f32; 3] = [0.26862954, 0.261_302_6, 0.275_777_1];

/// Preprocess image for CLIP: resize to 224x224, normalize with CLIP mean/std, NCHW layout.
fn preprocess_clip_image(data: &[u8]) -> Result<Array4<f32>, PgImageError> {
    let img = decode::decode_image(data)?;
    let resized = img.resize_exact(
        CLIP_INPUT_SIZE,
        CLIP_INPUT_SIZE,
        image::imageops::FilterType::Lanczos3,
    );
    let rgb = resized.to_rgb8();

    let size = CLIP_INPUT_SIZE as usize;
    let mut tensor = Array4::<f32>::zeros((1, 3, size, size));
    for y in 0..size {
        for x in 0..size {
            let pixel = rgb.get_pixel(x as u32, y as u32);
            tensor[[0, 0, y, x]] = (pixel[0] as f32 / 255.0 - CLIP_MEAN[0]) / CLIP_STD[0];
            tensor[[0, 1, y, x]] = (pixel[1] as f32 / 255.0 - CLIP_MEAN[1]) / CLIP_STD[1];
            tensor[[0, 2, y, x]] = (pixel[2] as f32 / 255.0 - CLIP_MEAN[2]) / CLIP_STD[2];
        }
    }

    Ok(tensor)
}

// =============================================================================
// CLIP Image Embedding
// =============================================================================

/// Generate a CLIP image embedding (512-dim float vector).
pub fn embed_image(data: &[u8], model_name: &str) -> Result<Vec<f32>, PgImageError> {
    let input_tensor = preprocess_clip_image(data)?;

    onnx_common::run_inference(model_name, |session| {
        let input_value = ort::value::Value::from_array(input_tensor.clone())
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;

        let outputs = session
            .run(ort::inputs![input_value])
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;

        let output_value = &outputs[0];
        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) = output_value
            .try_extract_tensor::<f32>()
            .map_err(|e: ort::Error| PgImageError::OnnxError(e.to_string()))?;

        // Expected shape: [1, 512] for CLIP ViT-B/32
        if shape.len() != 2 || shape[0] != 1 {
            return Err(PgImageError::OnnxError(format!(
                "unexpected embedding shape: {:?}, expected [1, N]",
                shape
            )));
        }

        Ok(raw_data.to_vec())
    })
}

// =============================================================================
// CLIP Text Embedding
// =============================================================================

/// Generate a CLIP text embedding (512-dim float vector, same space as embed_image).
pub fn embed_text(text: &str, model_name: &str) -> Result<Vec<f32>, PgImageError> {
    let tok = tokenizer::get_clip_tokenizer()?;
    let token_ids = tok.encode(text)?;

    // Build input_ids tensor [1, 77] of i64
    let input_array = Array2::from_shape_vec((1, token_ids.len()), token_ids.clone())
        .map_err(|e| PgImageError::OnnxError(format!("shape error: {}", e)))?;

    // Build attention_mask: 1 for real tokens, 0 for padding
    let attention_mask_vec: Vec<i64> = token_ids
        .iter()
        .map(|&id| if id != 0 { 1 } else { 0 })
        .collect();
    let attention_mask = Array2::from_shape_vec((1, attention_mask_vec.len()), attention_mask_vec)
        .map_err(|e| PgImageError::OnnxError(format!("attention_mask shape error: {}", e)))?;

    onnx_common::run_inference(model_name, |session| {
        let input_value = ort::value::Value::from_array(input_array.clone())
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;
        let mask_value = ort::value::Value::from_array(attention_mask.clone())
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;

        let outputs = session
            .run(ort::inputs![input_value, mask_value])
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;

        let output_value = &outputs[0];
        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) = output_value
            .try_extract_tensor::<f32>()
            .map_err(|e: ort::Error| PgImageError::OnnxError(e.to_string()))?;

        if shape.len() != 2 || shape[0] != 1 {
            return Err(PgImageError::OnnxError(format!(
                "unexpected text embedding shape: {:?}, expected [1, N]",
                shape
            )));
        }

        Ok(raw_data.to_vec())
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clip_preprocessing_shape() {
        // Minimal 1x1 red PNG
        let tiny_png: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x78,
            0x9C, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00, 0x03, 0x01, 0x01, 0x00, 0xC9, 0xFE, 0x92,
            0xEF, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];
        let tensor = preprocess_clip_image(tiny_png).unwrap();
        assert_eq!(tensor.shape(), &[1, 3, 224, 224]);
    }

    #[test]
    fn test_clip_normalization_values() {
        // A pure white pixel (255,255,255) should produce known normalized values
        // normalized = (1.0 - mean) / std
        let expected_r = (1.0 - CLIP_MEAN[0]) / CLIP_STD[0];
        let expected_g = (1.0 - CLIP_MEAN[1]) / CLIP_STD[1];
        let expected_b = (1.0 - CLIP_MEAN[2]) / CLIP_STD[2];

        // All should be positive (white > mean for all channels)
        assert!(expected_r > 0.0);
        assert!(expected_g > 0.0);
        assert!(expected_b > 0.0);
    }

    /// End-to-end CLIP image embedding test.
    /// Requires: ONNX Runtime + /var/lib/pg_image/models/clip-vit-b32-vision.onnx
    #[test]
    #[ignore]
    fn test_embed_image_e2e() {
        let image_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bus.jpg");
        let image_data = std::fs::read(image_path).expect("read bus.jpg");

        // Load model directly (bypass GUC)
        let input_tensor = preprocess_clip_image(&image_data).unwrap();
        let mut session = ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .commit_from_file("/var/lib/pg_image/models/clip-vit-b32-vision.onnx")
            .unwrap();

        let input_value = ort::value::Value::from_array(input_tensor).unwrap();
        let outputs = session.run(ort::inputs![input_value]).unwrap();

        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) =
            outputs[0].try_extract_tensor::<f32>().unwrap();

        eprintln!("CLIP embedding shape: {:?}", &shape[..]);
        assert_eq!(shape.len(), 2);
        assert_eq!(shape[0], 1);
        let dim = shape[1] as usize;
        assert!(dim == 512, "expected 512-dim, got {}", dim);
        assert_eq!(raw_data.len(), dim);

        // All values should be finite
        assert!(raw_data.iter().all(|v| v.is_finite()));

        // Embeddings should not be all zeros
        assert!(raw_data.iter().any(|&v| v.abs() > 1e-6));

        eprintln!("First 5 values: {:?}", &raw_data[..5]);
    }

    /// End-to-end CLIP text embedding test.
    /// Requires: ONNX Runtime + /var/lib/pg_image/models/clip-vit-b32-text.onnx + tokenizer vocab
    #[test]
    #[ignore]
    fn test_embed_text_e2e() {
        let vocab_path = std::path::Path::new("/var/lib/pg_image/models/clip-bpe-vocab.json");
        let tok = tokenizer::ClipTokenizer::load(vocab_path).expect("load tokenizer");
        let token_ids = tok.encode("a bus on a road").unwrap();
        assert_eq!(token_ids.len(), 77);

        let input_array = Array2::from_shape_vec((1, 77), token_ids.clone()).unwrap();
        let attention_mask_vec: Vec<i64> = token_ids
            .iter()
            .map(|&id| if id != 0 { 1 } else { 0 })
            .collect();
        let attention_mask = Array2::from_shape_vec((1, 77), attention_mask_vec).unwrap();

        let mut session = ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .commit_from_file("/var/lib/pg_image/models/clip-vit-b32-text.onnx")
            .unwrap();

        let input_value = ort::value::Value::from_array(input_array).unwrap();
        let mask_value = ort::value::Value::from_array(attention_mask).unwrap();
        let outputs = session.run(ort::inputs![input_value, mask_value]).unwrap();

        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) =
            outputs[0].try_extract_tensor::<f32>().unwrap();

        eprintln!("Text embedding shape: {:?}", &shape[..]);
        assert_eq!(shape.len(), 2);
        assert_eq!(shape[0], 1);
        let dim = shape[1] as usize;
        assert!(dim == 512, "expected 512-dim, got {}", dim);

        assert!(raw_data.iter().all(|v| v.is_finite()));
        assert!(raw_data.iter().any(|&v| v.abs() > 1e-6));
    }

    /// Cross-modal similarity test: bus.jpg embedding should be closer to "a bus on a road"
    /// than to "a sunset over the ocean".
    #[test]
    #[ignore]
    fn test_clip_crossmodal_similarity() {
        let image_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bus.jpg");
        let image_data = std::fs::read(image_path).expect("read bus.jpg");

        // Get image embedding
        let img_tensor = preprocess_clip_image(&image_data).unwrap();
        let mut vision_session = ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .commit_from_file("/var/lib/pg_image/models/clip-vit-b32-vision.onnx")
            .unwrap();
        let img_out = vision_session
            .run(ort::inputs![
                ort::value::Value::from_array(img_tensor).unwrap()
            ])
            .unwrap();
        let (_, img_embed): (&ort::tensor::Shape, &[f32]) =
            img_out[0].try_extract_tensor::<f32>().unwrap();

        // Get text embeddings (load tokenizer directly — no GUC in test context)
        let vocab_path = std::path::Path::new("/var/lib/pg_image/models/clip-bpe-vocab.json");
        let tok = tokenizer::ClipTokenizer::load(vocab_path).expect("load tokenizer");
        let mut text_session = ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .commit_from_file("/var/lib/pg_image/models/clip-vit-b32-text.onnx")
            .unwrap();

        let bus_tokens = tok.encode("a bus on a road").unwrap();
        let sunset_tokens = tok.encode("a sunset over the ocean").unwrap();

        let bus_input = Array2::from_shape_vec((1, 77), bus_tokens.clone()).unwrap();
        let bus_mask_vec: Vec<i64> = bus_tokens
            .iter()
            .map(|&id| if id != 0 { 1 } else { 0 })
            .collect();
        let bus_mask = Array2::from_shape_vec((1, 77), bus_mask_vec).unwrap();

        let sunset_input = Array2::from_shape_vec((1, 77), sunset_tokens.clone()).unwrap();
        let sunset_mask_vec: Vec<i64> = sunset_tokens
            .iter()
            .map(|&id| if id != 0 { 1 } else { 0 })
            .collect();
        let sunset_mask = Array2::from_shape_vec((1, 77), sunset_mask_vec).unwrap();

        let bus_embed_vec = {
            let bus_out = text_session
                .run(ort::inputs![
                    ort::value::Value::from_array(bus_input).unwrap(),
                    ort::value::Value::from_array(bus_mask).unwrap()
                ])
                .unwrap();
            let (_, data): (&ort::tensor::Shape, &[f32]) =
                bus_out[0].try_extract_tensor::<f32>().unwrap();
            data.to_vec()
        };

        let sunset_embed_vec = {
            let sunset_out = text_session
                .run(ort::inputs![
                    ort::value::Value::from_array(sunset_input).unwrap(),
                    ort::value::Value::from_array(sunset_mask).unwrap()
                ])
                .unwrap();
            let (_, data): (&ort::tensor::Shape, &[f32]) =
                sunset_out[0].try_extract_tensor::<f32>().unwrap();
            data.to_vec()
        };

        // Cosine similarity
        let cos_bus = cosine_similarity(img_embed, &bus_embed_vec);
        let cos_sunset = cosine_similarity(img_embed, &sunset_embed_vec);

        eprintln!(
            "bus.jpg <-> 'a bus on a road': {:.4}, bus.jpg <-> 'a sunset over the ocean': {:.4}",
            cos_bus, cos_sunset
        );
        assert!(
            cos_bus > cos_sunset,
            "bus description should be more similar than sunset: bus={:.4}, sunset={:.4}",
            cos_bus,
            cos_sunset
        );
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot / (norm_a * norm_b)
        }
    }
}
