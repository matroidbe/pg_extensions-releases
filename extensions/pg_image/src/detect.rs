use std::ffi::CString;

use ndarray::{Array2, Array4};

use crate::decode;
use crate::error::PgImageError;
use crate::onnx_common;

// =============================================================================
// GUC Settings
// =============================================================================

pub static MODEL_DIR: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub static DEFAULT_MODEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(Some(c"yolov8n.onnx"));

pub static DEFAULT_CONFIDENCE: pgrx::GucSetting<f64> = pgrx::GucSetting::<f64>::new(0.5);

pub static DEFAULT_IOU_THRESHOLD: pgrx::GucSetting<f64> = pgrx::GucSetting::<f64>::new(0.45);

pub fn register_gucs() {
    pgrx::GucRegistry::define_string_guc(
        c"pg_image.model_dir",
        c"Directory containing ONNX model files",
        c"Path to directory with .onnx model files (e.g., yolov8n.onnx)",
        &MODEL_DIR,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_image.default_model",
        c"Default ONNX model filename for img_detect",
        c"Name of the .onnx file to use when model parameter is not specified",
        &DEFAULT_MODEL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_float_guc(
        c"pg_image.detect_confidence",
        c"Default confidence threshold for object detection",
        c"Detections below this confidence are filtered out (0.0 to 1.0)",
        &DEFAULT_CONFIDENCE,
        0.0,
        1.0,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_float_guc(
        c"pg_image.detect_iou_threshold",
        c"IoU threshold for non-maximum suppression",
        c"Overlapping boxes with IoU above this are suppressed (0.0 to 1.0)",
        &DEFAULT_IOU_THRESHOLD,
        0.0,
        1.0,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
}

/// Get the default model name from GUC, falling back to "yolov8n.onnx".
pub fn default_model_name() -> String {
    DEFAULT_MODEL
        .get()
        .map(|cs| cs.to_string_lossy().into_owned())
        .unwrap_or_else(|| "yolov8n.onnx".to_string())
}

// =============================================================================
// Detection Result
// =============================================================================

#[derive(Debug, Clone)]
pub struct Detection {
    pub label: String,
    pub confidence: f64,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

// =============================================================================
// YOLO Input Size
// =============================================================================

const YOLO_INPUT_SIZE: u32 = 640;

// =============================================================================
// COCO 80 Class Labels
// =============================================================================

const COCO_LABELS: [&str; 80] = [
    "person",
    "bicycle",
    "car",
    "motorcycle",
    "airplane",
    "bus",
    "train",
    "truck",
    "boat",
    "traffic light",
    "fire hydrant",
    "stop sign",
    "parking meter",
    "bench",
    "bird",
    "cat",
    "dog",
    "horse",
    "sheep",
    "cow",
    "elephant",
    "bear",
    "zebra",
    "giraffe",
    "backpack",
    "umbrella",
    "handbag",
    "tie",
    "suitcase",
    "frisbee",
    "skis",
    "snowboard",
    "sports ball",
    "kite",
    "baseball bat",
    "baseball glove",
    "skateboard",
    "surfboard",
    "tennis racket",
    "bottle",
    "wine glass",
    "cup",
    "fork",
    "knife",
    "spoon",
    "bowl",
    "banana",
    "apple",
    "sandwich",
    "orange",
    "broccoli",
    "carrot",
    "hot dog",
    "pizza",
    "donut",
    "cake",
    "chair",
    "couch",
    "potted plant",
    "bed",
    "dining table",
    "toilet",
    "tv",
    "laptop",
    "mouse",
    "remote",
    "keyboard",
    "cell phone",
    "microwave",
    "oven",
    "toaster",
    "sink",
    "refrigerator",
    "book",
    "clock",
    "vase",
    "scissors",
    "teddy bear",
    "hair drier",
    "toothbrush",
];

// =============================================================================
// Preprocessing
// =============================================================================

/// Preprocess image for YOLO: resize to 640x640, normalize to [0,1], CHW layout.
fn preprocess(data: &[u8]) -> Result<(Array4<f32>, u32, u32), PgImageError> {
    let img = decode::decode_image(data)?;
    let (orig_w, orig_h) = (img.width(), img.height());

    let resized = img.resize_exact(
        YOLO_INPUT_SIZE,
        YOLO_INPUT_SIZE,
        image::imageops::FilterType::Triangle,
    );
    let rgb = resized.to_rgb8();

    // Convert to NCHW float32 tensor, normalized to [0, 1]
    let mut tensor =
        Array4::<f32>::zeros((1, 3, YOLO_INPUT_SIZE as usize, YOLO_INPUT_SIZE as usize));
    for y in 0..YOLO_INPUT_SIZE as usize {
        for x in 0..YOLO_INPUT_SIZE as usize {
            let pixel = rgb.get_pixel(x as u32, y as u32);
            tensor[[0, 0, y, x]] = pixel[0] as f32 / 255.0;
            tensor[[0, 1, y, x]] = pixel[1] as f32 / 255.0;
            tensor[[0, 2, y, x]] = pixel[2] as f32 / 255.0;
        }
    }

    Ok((tensor, orig_w, orig_h))
}

// =============================================================================
// Postprocessing
// =============================================================================

/// Postprocess YOLOv8 output: extract detections and apply NMS.
///
/// YOLOv8 output shape: [1, 84, 8400]
///   - 84 = 4 (cx, cy, w, h) + 80 (class scores)
///   - 8400 = number of candidate boxes
fn postprocess(
    output: &Array2<f32>,
    orig_w: u32,
    orig_h: u32,
    conf_threshold: f64,
    iou_threshold: f64,
) -> Vec<Detection> {
    let num_detections = output.shape()[1];
    let num_classes = output.shape()[0] - 4;

    let scale_x = orig_w as f64 / YOLO_INPUT_SIZE as f64;
    let scale_y = orig_h as f64 / YOLO_INPUT_SIZE as f64;

    let mut raw_detections: Vec<(Detection, f64)> = Vec::new();

    for i in 0..num_detections {
        let mut best_class = 0;
        let mut best_score = 0.0f32;
        for c in 0..num_classes {
            let score = output[[4 + c, i]];
            if score > best_score {
                best_score = score;
                best_class = c;
            }
        }

        if (best_score as f64) < conf_threshold {
            continue;
        }

        // YOLOv8: center x, center y, width, height
        let cx = output[[0, i]] as f64 * scale_x;
        let cy = output[[1, i]] as f64 * scale_y;
        let w = output[[2, i]] as f64 * scale_x;
        let h = output[[3, i]] as f64 * scale_y;

        let x = cx - w / 2.0;
        let y = cy - h / 2.0;

        let label = if best_class < COCO_LABELS.len() {
            COCO_LABELS[best_class].to_string()
        } else {
            format!("class_{}", best_class)
        };

        raw_detections.push((
            Detection {
                label,
                confidence: best_score as f64,
                x: x.max(0.0),
                y: y.max(0.0),
                width: w.min(orig_w as f64 - x.max(0.0)),
                height: h.min(orig_h as f64 - y.max(0.0)),
            },
            best_score as f64,
        ));
    }

    raw_detections.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    nms(raw_detections, iou_threshold)
}

/// Non-maximum suppression.
fn nms(detections: Vec<(Detection, f64)>, iou_threshold: f64) -> Vec<Detection> {
    let mut keep: Vec<Detection> = Vec::new();
    let mut suppressed = vec![false; detections.len()];

    for i in 0..detections.len() {
        if suppressed[i] {
            continue;
        }
        let det_i = &detections[i].0;
        keep.push(det_i.clone());

        for j in (i + 1)..detections.len() {
            if suppressed[j] {
                continue;
            }
            let det_j = &detections[j].0;
            if det_i.label == det_j.label && iou(det_i, det_j) > iou_threshold {
                suppressed[j] = true;
            }
        }
    }

    keep
}

/// Intersection over Union.
fn iou(a: &Detection, b: &Detection) -> f64 {
    let x1 = a.x.max(b.x);
    let y1 = a.y.max(b.y);
    let x2 = (a.x + a.width).min(b.x + b.width);
    let y2 = (a.y + a.height).min(b.y + b.height);

    let intersection = (x2 - x1).max(0.0) * (y2 - y1).max(0.0);
    let area_a = a.width * a.height;
    let area_b = b.width * b.height;
    let union = area_a + area_b - intersection;

    if union <= 0.0 {
        0.0
    } else {
        intersection / union
    }
}

// =============================================================================
// Main Detection Function
// =============================================================================

/// Run YOLO object detection on image bytes.
pub fn detect(
    data: &[u8],
    model_name: &str,
    conf_threshold: f64,
    iou_threshold: f64,
) -> Result<Vec<Detection>, PgImageError> {
    // Preprocess outside the cache lock
    let (input_tensor, orig_w, orig_h) = preprocess(data)?;

    onnx_common::run_inference(model_name, |session| {
        let input_value = ort::value::Value::from_array(input_tensor.clone())
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;

        let outputs = session
            .run(ort::inputs![input_value])
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;

        // Extract output tensor: YOLOv8 output [1, 84, 8400]
        let output_value = &outputs[0];
        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) = output_value
            .try_extract_tensor::<f32>()
            .map_err(|e: ort::Error| PgImageError::OnnxError(e.to_string()))?;

        if shape.len() != 3 || shape[0] != 1 {
            return Err(PgImageError::OnnxError(format!(
                "unexpected output shape: {:?}, expected [1, N, M]",
                shape
            )));
        }
        let rows = shape[1] as usize;
        let cols = shape[2] as usize;

        let output_2d = Array2::from_shape_vec((rows, cols), raw_data.to_vec())
            .map_err(|e| PgImageError::OnnxError(format!("shape error: {}", e)))?;

        Ok(postprocess(
            &output_2d,
            orig_w,
            orig_h,
            conf_threshold,
            iou_threshold,
        ))
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iou_identical_boxes() {
        let a = Detection {
            label: "person".into(),
            confidence: 0.9,
            x: 10.0,
            y: 10.0,
            width: 100.0,
            height: 100.0,
        };
        assert!((iou(&a, &a) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_iou_no_overlap() {
        let a = Detection {
            label: "person".into(),
            confidence: 0.9,
            x: 0.0,
            y: 0.0,
            width: 10.0,
            height: 10.0,
        };
        let b = Detection {
            label: "person".into(),
            confidence: 0.8,
            x: 20.0,
            y: 20.0,
            width: 10.0,
            height: 10.0,
        };
        assert_eq!(iou(&a, &b), 0.0);
    }

    #[test]
    fn test_iou_partial_overlap() {
        let a = Detection {
            label: "person".into(),
            confidence: 0.9,
            x: 0.0,
            y: 0.0,
            width: 10.0,
            height: 10.0,
        };
        let b = Detection {
            label: "person".into(),
            confidence: 0.8,
            x: 5.0,
            y: 5.0,
            width: 10.0,
            height: 10.0,
        };
        assert!((iou(&a, &b) - 25.0 / 175.0).abs() < 1e-6);
    }

    #[test]
    fn test_nms_suppresses_overlapping() {
        let detections = vec![
            (
                Detection {
                    label: "person".into(),
                    confidence: 0.9,
                    x: 10.0,
                    y: 10.0,
                    width: 100.0,
                    height: 100.0,
                },
                0.9,
            ),
            (
                Detection {
                    label: "person".into(),
                    confidence: 0.7,
                    x: 15.0,
                    y: 15.0,
                    width: 100.0,
                    height: 100.0,
                },
                0.7,
            ),
        ];
        let result = nms(detections, 0.5);
        assert_eq!(result.len(), 1);
        assert!((result[0].confidence - 0.9).abs() < 1e-6);
    }

    #[test]
    fn test_nms_keeps_different_classes() {
        let detections = vec![
            (
                Detection {
                    label: "person".into(),
                    confidence: 0.9,
                    x: 10.0,
                    y: 10.0,
                    width: 100.0,
                    height: 100.0,
                },
                0.9,
            ),
            (
                Detection {
                    label: "car".into(),
                    confidence: 0.8,
                    x: 10.0,
                    y: 10.0,
                    width: 100.0,
                    height: 100.0,
                },
                0.8,
            ),
        ];
        let result = nms(detections, 0.5);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coco_labels_count() {
        assert_eq!(COCO_LABELS.len(), 80);
    }

    #[test]
    fn test_postprocess_empty_output() {
        let output = Array2::<f32>::zeros((84, 0));
        let results = postprocess(&output, 640, 480, 0.5, 0.45);
        assert!(results.is_empty());
    }

    #[test]
    fn test_postprocess_below_threshold() {
        let mut output = Array2::<f32>::zeros((84, 1));
        output[[0, 0]] = 320.0;
        output[[1, 0]] = 240.0;
        output[[2, 0]] = 50.0;
        output[[3, 0]] = 50.0;
        for c in 4..84 {
            output[[c, 0]] = 0.1;
        }
        let results = postprocess(&output, 640, 480, 0.5, 0.45);
        assert!(results.is_empty());
    }

    #[test]
    fn test_postprocess_above_threshold() {
        let mut output = Array2::<f32>::zeros((84, 1));
        output[[0, 0]] = 320.0;
        output[[1, 0]] = 240.0;
        output[[2, 0]] = 50.0;
        output[[3, 0]] = 50.0;
        output[[4, 0]] = 0.9;
        let results = postprocess(&output, 640, 480, 0.5, 0.45);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].label, "person");
        assert!((results[0].confidence - 0.9).abs() < 1e-6);
    }

    /// End-to-end test: loads the real YOLOv8n model and runs detection on bus.jpg.
    /// Requires: ONNX Runtime installed + /var/lib/pg_image/models/yolov8n.onnx + fixture image.
    /// Run with: cargo test -p pg_image --lib test_detect_e2e -- --ignored
    #[test]
    #[ignore]
    fn test_detect_e2e_bus_image() {
        let model_path = "/var/lib/pg_image/models/yolov8n.onnx";
        let image_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bus.jpg");

        let image_data = std::fs::read(image_path).expect("failed to read bus.jpg");

        // Preprocess
        let (input_tensor, orig_w, orig_h) = preprocess(&image_data).expect("preprocess failed");
        assert_eq!(orig_w, 810); // bus.jpg is 810x1080
        assert_eq!(orig_h, 1080);

        // Load model directly (bypasses GUC-based model_path())
        let mut session = ort::session::Session::builder()
            .expect("session builder")
            .with_intra_threads(1)
            .expect("intra threads")
            .commit_from_file(model_path)
            .expect("load model");

        // Run inference
        let input_value = ort::value::Value::from_array(input_tensor).expect("from_array");

        let outputs = session
            .run(ort::inputs![input_value])
            .expect("run inference");

        // Extract output
        let output_value = &outputs[0];
        let (shape, raw_data): (&ort::tensor::Shape, &[f32]) = output_value
            .try_extract_tensor::<f32>()
            .expect("extract tensor");

        assert_eq!(shape.len(), 3);
        assert_eq!(shape[0], 1);

        let rows = shape[1] as usize;
        let cols = shape[2] as usize;
        let output_2d =
            Array2::from_shape_vec((rows, cols), raw_data.to_vec()).expect("reshape output");

        // Postprocess
        let detections = postprocess(&output_2d, orig_w, orig_h, 0.25, 0.45);

        // Verify we got meaningful detections
        assert!(!detections.is_empty(), "should detect objects in bus.jpg");

        // Print detections for visual inspection
        eprintln!("Detected {} objects:", detections.len());
        for d in &detections {
            eprintln!(
                "  {} ({:.3}) at ({:.0}, {:.0}) {}x{}",
                d.label, d.confidence, d.x, d.y, d.width as i32, d.height as i32
            );
        }

        // bus.jpg should contain persons and a bus
        let labels: Vec<&str> = detections.iter().map(|d| d.label.as_str()).collect();
        assert!(
            labels.contains(&"person"),
            "should detect at least one person, got: {:?}",
            labels
        );
        assert!(
            labels.contains(&"bus"),
            "should detect a bus, got: {:?}",
            labels
        );

        // Verify bounding box coordinates are sane
        for d in &detections {
            assert!(d.x >= 0.0, "x should be >= 0");
            assert!(d.y >= 0.0, "y should be >= 0");
            assert!(d.width > 0.0, "width should be > 0");
            assert!(d.height > 0.0, "height should be > 0");
            assert!(
                d.x + d.width <= orig_w as f64 + 1.0,
                "box should be within image width"
            );
            assert!(
                d.y + d.height <= orig_h as f64 + 1.0,
                "box should be within image height"
            );
            assert!(
                d.confidence > 0.0 && d.confidence <= 1.0,
                "confidence should be in (0, 1]"
            );
        }
    }
}
