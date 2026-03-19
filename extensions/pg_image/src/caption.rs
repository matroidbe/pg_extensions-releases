use std::ffi::CString;

use ndarray::{Array4, ArrayD};
use ort::session::SessionInputs;

use crate::decode;
use crate::error::PgImageError;
use crate::onnx_common;
use crate::tokenizer;

// =============================================================================
// GUC Settings
// =============================================================================

pub static CAPTION_ENCODER_MODEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(Some(c"vit-gpt2-encoder.onnx"));

pub static CAPTION_DECODER_MODEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(Some(c"vit-gpt2-decoder.onnx"));

pub static CAPTION_MAX_TOKENS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(30);

pub fn register_gucs() {
    pgrx::GucRegistry::define_string_guc(
        c"pg_image.caption_encoder_model",
        c"ViT-GPT2 encoder ONNX model filename",
        c"Vision encoder model for image captioning",
        &CAPTION_ENCODER_MODEL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_image.caption_decoder_model",
        c"ViT-GPT2 decoder ONNX model filename",
        c"Text decoder model for image captioning",
        &CAPTION_DECODER_MODEL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_image.caption_max_tokens",
        c"Maximum number of tokens to generate for captions",
        c"Limits caption length (default 30 tokens)",
        &CAPTION_MAX_TOKENS,
        1,
        200,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
}

pub fn default_encoder_model() -> String {
    CAPTION_ENCODER_MODEL
        .get()
        .map(|cs| cs.to_string_lossy().into_owned())
        .unwrap_or_else(|| "vit-gpt2-encoder.onnx".to_string())
}

pub fn default_decoder_model() -> String {
    CAPTION_DECODER_MODEL
        .get()
        .map(|cs| cs.to_string_lossy().into_owned())
        .unwrap_or_else(|| "vit-gpt2-decoder.onnx".to_string())
}

// =============================================================================
// ViT Preprocessing (ImageNet normalization, 224x224)
// =============================================================================

const VIT_INPUT_SIZE: u32 = 224;
const IMAGENET_MEAN: [f32; 3] = [0.485, 0.456, 0.406];
const IMAGENET_STD: [f32; 3] = [0.229, 0.224, 0.225];

fn preprocess_vit(data: &[u8]) -> Result<Array4<f32>, PgImageError> {
    let img = decode::decode_image(data)?;
    let resized = img.resize_exact(
        VIT_INPUT_SIZE,
        VIT_INPUT_SIZE,
        image::imageops::FilterType::Lanczos3,
    );
    let rgb = resized.to_rgb8();

    let size = VIT_INPUT_SIZE as usize;
    let mut tensor = Array4::<f32>::zeros((1, 3, size, size));
    for y in 0..size {
        for x in 0..size {
            let pixel = rgb.get_pixel(x as u32, y as u32);
            tensor[[0, 0, y, x]] = (pixel[0] as f32 / 255.0 - IMAGENET_MEAN[0]) / IMAGENET_STD[0];
            tensor[[0, 1, y, x]] = (pixel[1] as f32 / 255.0 - IMAGENET_MEAN[1]) / IMAGENET_STD[1];
            tensor[[0, 2, y, x]] = (pixel[2] as f32 / 255.0 - IMAGENET_MEAN[2]) / IMAGENET_STD[2];
        }
    }
    Ok(tensor)
}

// =============================================================================
// Caption Generation
// =============================================================================

const GPT2_BOS_TOKEN: i64 = 50256;
const NUM_LAYERS: usize = 12;
const NUM_HEADS: usize = 12;
const HEAD_DIM: usize = 64;

fn ort_err(e: impl std::fmt::Display) -> PgImageError {
    PgImageError::OnnxError(e.to_string())
}

/// Greedy argmax over a logits slice.
fn argmax(logits: &[f32]) -> i64 {
    let mut best_id = 0i64;
    let mut best_val = f32::NEG_INFINITY;
    for (i, &val) in logits.iter().enumerate() {
        if val > best_val {
            best_val = val;
            best_id = i as i64;
        }
    }
    best_id
}

/// Generate a text caption for an image using ViT-GPT2.
///
/// 1. Run the ViT encoder to get image features
/// 2. Run the GPT2 decoder autoregressively with KV-cache
/// 3. Decode tokens back to text
pub fn caption(
    data: &[u8],
    encoder_model: &str,
    decoder_model: &str,
    max_tokens: usize,
) -> Result<String, PgImageError> {
    let pixel_values = preprocess_vit(data)?;

    // Step 1: Run encoder → hidden states [1, enc_seq, 768]
    let (enc_dims, enc_data) = onnx_common::run_inference(encoder_model, |session| {
        let input_value = ort::value::Value::from_array(pixel_values.clone()).map_err(ort_err)?;

        let outputs = session.run(ort::inputs![input_value]).map_err(ort_err)?;

        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) =
            outputs[0].try_extract_tensor::<f32>().map_err(ort_err)?;

        let dims: Vec<usize> = shape.iter().map(|&d| d as usize).collect();
        Ok((dims, raw_data.to_vec()))
    })?;

    // Step 2: Autoregressive decoding with KV-cache
    //
    // The merged decoder model expects:
    //   input_ids: [1, seq] int64
    //   encoder_hidden_states: [1, enc_seq, 768] float32
    //   past_key_values.{0..11}.key: [1, 12, past_seq, 64] float32
    //   past_key_values.{0..11}.value: [1, 12, past_seq, 64] float32
    //   use_cache_branch: [1] bool
    //
    // First call: past_seq=0, use_cache_branch=false, input_ids=[BOS]
    // Subsequent: past_seq=accumulated, use_cache_branch=true, input_ids=[last_token]

    let mut token_ids: Vec<i64> = vec![GPT2_BOS_TOKEN];

    // Initialize empty KV cache: [1, 12, 0, 64] per layer
    let mut past_keys: Vec<Vec<f32>> = vec![vec![]; NUM_LAYERS];
    let mut past_values: Vec<Vec<f32>> = vec![vec![]; NUM_LAYERS];
    let mut past_seq_len: usize = 0;

    for step in 0..max_tokens {
        let next_token = onnx_common::run_inference(decoder_model, |session| {
            // input_ids: first step = full sequence, subsequent = just last token
            let ids_data = if step == 0 {
                token_ids.clone()
            } else {
                vec![*token_ids.last().unwrap()]
            };
            let ids_seq = ids_data.len();

            let input_ids = ndarray::Array2::from_shape_vec((1, ids_seq), ids_data)
                .map_err(|e| PgImageError::OnnxError(format!("input_ids shape: {}", e)))?;

            let enc_tensor = ArrayD::from_shape_vec(enc_dims.clone(), enc_data.clone())
                .map_err(|e| PgImageError::OnnxError(format!("enc shape: {}", e)))?;

            // Build ONNX inputs map with named inputs
            let mut inputs: Vec<(String, ort::value::DynValue)> = Vec::new();

            inputs.push((
                "input_ids".to_string(),
                ort::value::Value::from_array(input_ids)
                    .map_err(ort_err)?
                    .into_dyn(),
            ));
            inputs.push((
                "encoder_hidden_states".to_string(),
                ort::value::Value::from_array(enc_tensor)
                    .map_err(ort_err)?
                    .into_dyn(),
            ));

            // Past KV cache
            for layer in 0..NUM_LAYERS {
                let key_tensor = ArrayD::from_shape_vec(
                    vec![1, NUM_HEADS, past_seq_len, HEAD_DIM],
                    past_keys[layer].clone(),
                )
                .map_err(|e| PgImageError::OnnxError(format!("past_key shape: {}", e)))?;

                let value_tensor = ArrayD::from_shape_vec(
                    vec![1, NUM_HEADS, past_seq_len, HEAD_DIM],
                    past_values[layer].clone(),
                )
                .map_err(|e| PgImageError::OnnxError(format!("past_value shape: {}", e)))?;

                inputs.push((
                    format!("past_key_values.{}.key", layer),
                    ort::value::Value::from_array(key_tensor)
                        .map_err(ort_err)?
                        .into_dyn(),
                ));
                inputs.push((
                    format!("past_key_values.{}.value", layer),
                    ort::value::Value::from_array(value_tensor)
                        .map_err(ort_err)?
                        .into_dyn(),
                ));
            }

            // use_cache_branch: false on first step, true after
            let use_cache = ndarray::Array1::from_vec(vec![step > 0]);
            inputs.push((
                "use_cache_branch".to_string(),
                ort::value::Value::from_array(use_cache)
                    .map_err(ort_err)?
                    .into_dyn(),
            ));

            let outputs = session.run(SessionInputs::from(inputs)).map_err(ort_err)?;

            // Output 0: logits [1, seq, 50257]
            let (shape, logits): (&ort::tensor::Shape, &[f32]) =
                outputs[0].try_extract_tensor::<f32>().map_err(ort_err)?;

            if shape.len() != 3 {
                return Err(PgImageError::OnnxError(format!(
                    "unexpected decoder logits shape: {:?}",
                    shape
                )));
            }

            let vocab_size = shape[2] as usize;
            let out_seq = shape[1] as usize;
            let offset = (out_seq - 1) * vocab_size;
            let next_id = argmax(&logits[offset..offset + vocab_size]);

            // Extract present KV cache from outputs[1..49]
            // present.{0..11}.key, present.{0..11}.value (alternating)
            for layer in 0..NUM_LAYERS {
                let key_idx = 1 + layer * 2;
                let val_idx = 2 + layer * 2;

                let (key_shape, key_data): (&ort::tensor::Shape, &[f32]) = outputs[key_idx]
                    .try_extract_tensor::<f32>()
                    .map_err(ort_err)?;
                let (_, val_data): (&ort::tensor::Shape, &[f32]) = outputs[val_idx]
                    .try_extract_tensor::<f32>()
                    .map_err(ort_err)?;

                past_keys[layer] = key_data.to_vec();
                past_values[layer] = val_data.to_vec();

                // Update past_seq_len from the first layer's key shape
                if layer == 0 {
                    // shape: [1, 12, new_past_seq, 64]
                    past_seq_len = key_shape[2] as usize;
                }
            }

            Ok(next_id)
        })?;

        if next_token == GPT2_BOS_TOKEN {
            break;
        }
        token_ids.push(next_token);
    }

    // Step 3: Decode tokens to text
    let decoder = tokenizer::get_gpt2_decoder()?;
    let text = decoder.decode(&token_ids[1..]); // skip BOS
    Ok(text)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vit_preprocessing_shape() {
        let tiny_png: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x78,
            0x9C, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00, 0x03, 0x01, 0x01, 0x00, 0xC9, 0xFE, 0x92,
            0xEF, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];
        let tensor = preprocess_vit(tiny_png).unwrap();
        assert_eq!(tensor.shape(), &[1, 3, 224, 224]);
    }

    #[test]
    fn test_imagenet_normalization_range() {
        // Black pixel (0,0,0) normalized: (0.0 - mean) / std
        // Should produce negative values for all channels
        let r = (0.0 - IMAGENET_MEAN[0]) / IMAGENET_STD[0];
        let g = (0.0 - IMAGENET_MEAN[1]) / IMAGENET_STD[1];
        let b = (0.0 - IMAGENET_MEAN[2]) / IMAGENET_STD[2];
        assert!(r < 0.0);
        assert!(g < 0.0);
        assert!(b < 0.0);
    }

    #[test]
    fn test_argmax_basic() {
        let logits = [0.1f32, 0.9, 0.3, 0.2];
        assert_eq!(argmax(&logits), 1);
    }

    /// End-to-end caption test.
    /// Requires: ONNX Runtime + encoder/decoder models + GPT2 vocab
    #[test]
    #[ignore]
    fn test_caption_e2e() {
        let image_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bus.jpg");
        let image_data = std::fs::read(image_path).expect("read bus.jpg");

        // Preprocess
        let pixel_values = preprocess_vit(&image_data).unwrap();

        // Run encoder
        let mut enc_session = ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .commit_from_file("/var/lib/pg_image/models/vit-gpt2-encoder.onnx")
            .unwrap();

        let enc_input = ort::value::Value::from_array(pixel_values).unwrap();
        let enc_outputs = enc_session.run(ort::inputs![enc_input]).unwrap();
        let (enc_shape, enc_data): (&ort::tensor::Shape, &[f32]) =
            enc_outputs[0].try_extract_tensor::<f32>().unwrap();
        let enc_dims: Vec<usize> = enc_shape.iter().map(|&d| d as usize).collect();
        let enc_data = enc_data.to_vec();

        eprintln!("Encoder output shape: {:?}", enc_dims);

        // Run decoder autoregressively
        let mut dec_session = ort::session::Session::builder()
            .unwrap()
            .with_intra_threads(1)
            .unwrap()
            .commit_from_file("/var/lib/pg_image/models/vit-gpt2-decoder.onnx")
            .unwrap();

        let mut token_ids: Vec<i64> = vec![GPT2_BOS_TOKEN];
        let mut past_keys: Vec<Vec<f32>> = vec![vec![]; NUM_LAYERS];
        let mut past_values: Vec<Vec<f32>> = vec![vec![]; NUM_LAYERS];
        let mut past_seq_len: usize = 0;

        for step in 0..20 {
            let ids_data = if step == 0 {
                token_ids.clone()
            } else {
                vec![*token_ids.last().unwrap()]
            };

            let input_ids = ndarray::Array2::from_shape_vec((1, ids_data.len()), ids_data).unwrap();
            let enc_tensor = ArrayD::from_shape_vec(enc_dims.clone(), enc_data.clone()).unwrap();

            let mut inputs: Vec<(String, ort::value::DynValue)> = Vec::new();
            inputs.push((
                "input_ids".into(),
                ort::value::Value::from_array(input_ids).unwrap().into_dyn(),
            ));
            inputs.push((
                "encoder_hidden_states".into(),
                ort::value::Value::from_array(enc_tensor)
                    .unwrap()
                    .into_dyn(),
            ));

            for layer in 0..NUM_LAYERS {
                let kt = ArrayD::from_shape_vec(
                    vec![1, NUM_HEADS, past_seq_len, HEAD_DIM],
                    past_keys[layer].clone(),
                )
                .unwrap();
                let vt = ArrayD::from_shape_vec(
                    vec![1, NUM_HEADS, past_seq_len, HEAD_DIM],
                    past_values[layer].clone(),
                )
                .unwrap();
                inputs.push((
                    format!("past_key_values.{}.key", layer),
                    ort::value::Value::from_array(kt).unwrap().into_dyn(),
                ));
                inputs.push((
                    format!("past_key_values.{}.value", layer),
                    ort::value::Value::from_array(vt).unwrap().into_dyn(),
                ));
            }

            let use_cache = ndarray::Array1::from_vec(vec![step > 0]);
            inputs.push((
                "use_cache_branch".into(),
                ort::value::Value::from_array(use_cache).unwrap().into_dyn(),
            ));

            let outputs = dec_session.run(SessionInputs::from(inputs)).unwrap();

            let (shape, logits): (&ort::tensor::Shape, &[f32]) =
                outputs[0].try_extract_tensor::<f32>().unwrap();
            let vocab_size = shape[2] as usize;
            let out_seq = shape[1] as usize;
            let offset = (out_seq - 1) * vocab_size;
            let next_id = argmax(&logits[offset..offset + vocab_size]);

            for layer in 0..NUM_LAYERS {
                let key_idx = 1 + layer * 2;
                let val_idx = 2 + layer * 2;
                let (key_shape, key_data): (&ort::tensor::Shape, &[f32]) =
                    outputs[key_idx].try_extract_tensor::<f32>().unwrap();
                let (_, val_data): (&ort::tensor::Shape, &[f32]) =
                    outputs[val_idx].try_extract_tensor::<f32>().unwrap();
                past_keys[layer] = key_data.to_vec();
                past_values[layer] = val_data.to_vec();
                if layer == 0 {
                    past_seq_len = key_shape[2] as usize;
                }
            }

            if next_id == GPT2_BOS_TOKEN {
                break;
            }
            token_ids.push(next_id);
        }

        // Decode tokens
        let vocab_path = std::path::Path::new("/var/lib/pg_image/models/gpt2-vocab.json");
        let decoder = tokenizer::Gpt2Decoder::load(vocab_path).expect("load GPT2 vocab");
        let text = decoder.decode(&token_ids[1..]);
        eprintln!("Caption: {}", text);

        assert!(!text.is_empty(), "caption should not be empty");
        // Caption should contain reasonable English words
        assert!(text.len() > 5, "caption too short: {}", text);
    }
}
