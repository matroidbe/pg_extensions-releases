//! pg_image: Image processing for PostgreSQL
//!
//! Extract metadata, EXIF data, GPS coordinates, apply transforms,
//! compute perceptual hashes, and detect objects — all from SQL on bytea columns.

pub mod caption;
pub mod decode;
pub mod detect;
pub mod embed;
pub mod error;
pub mod exif;
pub mod gps;
pub mod hashing;
pub mod metadata;
pub mod onnx_common;
pub mod tokenizer;
pub mod transforms;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    detect::register_gucs();
    embed::register_gucs();
    caption::register_gucs();
}

// =============================================================================
// Extension Documentation
// =============================================================================

#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Metadata Functions (header-only fast path where possible)
// =============================================================================

/// Image width in pixels.
#[pg_extern(immutable, parallel_safe, strict, name = "img_width")]
fn img_width(data: &[u8]) -> i32 {
    match metadata::width(data) {
        Ok(w) => w,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Image height in pixels.
#[pg_extern(immutable, parallel_safe, strict, name = "img_height")]
fn img_height(data: &[u8]) -> i32 {
    match metadata::height(data) {
        Ok(h) => h,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Image format name (jpeg, png, webp, gif, tiff, bmp).
#[pg_extern(immutable, parallel_safe, strict, name = "img_format")]
fn img_format(data: &[u8]) -> String {
    match metadata::format(data) {
        Ok(f) => f,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Color space name (rgb, rgba, grayscale, grayscale+alpha).
#[pg_extern(immutable, parallel_safe, strict, name = "img_colorspace")]
fn img_colorspace(data: &[u8]) -> String {
    match metadata::colorspace(data) {
        Ok(cs) => cs,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Number of color channels (1, 2, 3, or 4).
#[pg_extern(immutable, parallel_safe, strict, name = "img_channels")]
fn img_channels(data: &[u8]) -> i32 {
    match metadata::channels(data) {
        Ok(c) => c,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Bits per channel (8 or 16).
#[pg_extern(immutable, parallel_safe, strict, name = "img_bit_depth")]
fn img_bit_depth(data: &[u8]) -> i32 {
    match metadata::bit_depth(data) {
        Ok(d) => d,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Encoded image size in bytes.
#[pg_extern(immutable, parallel_safe, strict, name = "img_size_bytes")]
fn img_size_bytes(data: &[u8]) -> i64 {
    metadata::size_bytes(data)
}

/// All image metadata as a JSON object.
#[pg_extern(immutable, parallel_safe, strict, name = "img_metadata")]
fn img_metadata(data: &[u8]) -> pgrx::JsonB {
    match metadata::metadata_json(data) {
        Ok(json) => pgrx::JsonB(json),
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// EXIF Functions
// =============================================================================

/// All EXIF metadata as a JSON object. Returns NULL if no EXIF data.
#[pg_extern(immutable, parallel_safe, name = "img_exif")]
fn img_exif(data: Option<&[u8]>) -> Option<pgrx::JsonB> {
    data.and_then(|d| exif::extract_all(d).ok().map(pgrx::JsonB))
}

/// Single EXIF tag value by name. Returns NULL if tag not found.
#[pg_extern(immutable, parallel_safe, name = "img_exif_value")]
fn img_exif_value(data: Option<&[u8]>, tag: &str) -> Option<String> {
    data.and_then(|d| exif::get_tag_value(d, tag))
}

/// EXIF DateTimeOriginal as timestamptz (assumes UTC). Returns NULL if not present.
#[pg_extern(immutable, parallel_safe, name = "img_timestamp")]
fn img_timestamp(data: Option<&[u8]>) -> Option<pgrx::datum::TimestampWithTimeZone> {
    data.and_then(exif::parse_timestamp)
}

/// Camera manufacturer from EXIF. Returns NULL if not present.
#[pg_extern(immutable, parallel_safe, name = "img_camera_make")]
fn img_camera_make(data: Option<&[u8]>) -> Option<String> {
    data.and_then(exif::camera_make)
}

/// Camera model from EXIF. Returns NULL if not present.
#[pg_extern(immutable, parallel_safe, name = "img_camera_model")]
fn img_camera_model(data: Option<&[u8]>) -> Option<String> {
    data.and_then(exif::camera_model)
}

/// EXIF orientation tag (1-8). Returns NULL if not present.
#[pg_extern(immutable, parallel_safe, name = "img_orientation")]
fn img_orientation(data: Option<&[u8]>) -> Option<i32> {
    data.and_then(exif::orientation)
}

// =============================================================================
// GPS Functions
// =============================================================================

/// GPS latitude in decimal degrees. Returns NULL if no GPS data.
#[pg_extern(immutable, parallel_safe, name = "img_gps_lat")]
fn img_gps_lat(data: Option<&[u8]>) -> Option<f64> {
    data.and_then(gps::extract_lat)
}

/// GPS longitude in decimal degrees. Returns NULL if no GPS data.
#[pg_extern(immutable, parallel_safe, name = "img_gps_lon")]
fn img_gps_lon(data: Option<&[u8]>) -> Option<f64> {
    data.and_then(gps::extract_lon)
}

/// GPS altitude in meters. Returns NULL if no GPS data.
#[pg_extern(immutable, parallel_safe, name = "img_gps_altitude")]
fn img_gps_altitude(data: Option<&[u8]>) -> Option<f64> {
    data.and_then(gps::extract_altitude)
}

// PostGIS bridge functions — only created if PostGIS is installed
pgrx::extension_sql!(
    r#"
DO $bridge$
BEGIN
    -- Only create PostGIS bridge functions if PostGIS is available
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        EXECUTE $fn$
            CREATE OR REPLACE FUNCTION pgimg.img_gps_point(data bytea) RETURNS geometry AS $$
                SELECT CASE
                    WHEN pgimg.img_gps_lon(data) IS NOT NULL AND pgimg.img_gps_lat(data) IS NOT NULL
                    THEN ST_SetSRID(ST_MakePoint(pgimg.img_gps_lon(data), pgimg.img_gps_lat(data)), 4326)
                    ELSE NULL
                END;
            $$ LANGUAGE SQL IMMUTABLE STRICT;
        $fn$;

        EXECUTE $fn$
            CREATE OR REPLACE FUNCTION pgimg.img_gps_pointz(data bytea) RETURNS geometry AS $$
                SELECT CASE
                    WHEN pgimg.img_gps_lon(data) IS NOT NULL AND pgimg.img_gps_lat(data) IS NOT NULL
                    THEN ST_SetSRID(ST_MakePoint(
                        pgimg.img_gps_lon(data),
                        pgimg.img_gps_lat(data),
                        COALESCE(pgimg.img_gps_altitude(data), 0)
                    ), 4326)
                    ELSE NULL
                END;
            $$ LANGUAGE SQL IMMUTABLE STRICT;
        $fn$;

        RAISE NOTICE 'pg_image: PostGIS bridge functions created (img_gps_point, img_gps_pointz)';
    ELSE
        RAISE NOTICE 'pg_image: PostGIS not found, skipping bridge functions. Install PostGIS and run CREATE EXTENSION pg_image again to enable.';
    END IF;
END
$bridge$;
"#,
    name = "postgis_bridge",
    finalize
);

// =============================================================================
// Transform Functions
// =============================================================================

/// Resize image to exact dimensions.
#[pg_extern(immutable, parallel_safe, strict, name = "img_resize")]
fn img_resize(data: &[u8], width: i32, height: i32) -> Vec<u8> {
    match transforms::resize(data, width, height) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Create thumbnail preserving aspect ratio (fits within max_width x max_height).
#[pg_extern(immutable, parallel_safe, strict, name = "img_thumbnail")]
fn img_thumbnail(data: &[u8], max_width: i32, max_height: i32) -> Vec<u8> {
    match transforms::thumbnail(data, max_width, max_height) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Crop a region from the image.
#[pg_extern(immutable, parallel_safe, strict, name = "img_crop")]
fn img_crop(data: &[u8], x: i32, y: i32, width: i32, height: i32) -> Vec<u8> {
    match transforms::crop(data, x, y, width, height) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Convert image to a different format (jpeg, png, webp, tiff, bmp).
#[pg_extern(immutable, parallel_safe, strict, name = "img_convert")]
fn img_convert(data: &[u8], format: &str) -> Vec<u8> {
    match transforms::convert_format(data, format) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Convert image to grayscale.
#[pg_extern(immutable, parallel_safe, strict, name = "img_grayscale")]
fn img_grayscale(data: &[u8]) -> Vec<u8> {
    match transforms::grayscale(data) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Rotate image by 90, 180, or 270 degrees.
#[pg_extern(immutable, parallel_safe, strict, name = "img_rotate")]
fn img_rotate(data: &[u8], degrees: i32) -> Vec<u8> {
    match transforms::rotate(data, degrees) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Apply EXIF orientation and return correctly oriented image.
#[pg_extern(immutable, parallel_safe, strict, name = "img_auto_orient")]
fn img_auto_orient(data: &[u8]) -> Vec<u8> {
    match transforms::auto_orient(data) {
        Ok(out) => out,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Extract raw RGB pixel bytes (for ML pipelines).
#[pg_extern(immutable, parallel_safe, strict, name = "img_to_rgb")]
fn img_to_rgb(data: &[u8]) -> Vec<u8> {
    match decode::decode_image(data) {
        Ok(img) => img.to_rgb8().into_raw(),
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Hashing Functions
// =============================================================================

/// Compute perceptual hash (base64-encoded). Useful for deduplication.
#[pg_extern(immutable, parallel_safe, strict, name = "img_pixel_hash")]
fn img_pixel_hash(data: &[u8]) -> String {
    match hashing::compute_phash(data) {
        Ok(hash) => hash,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Hamming distance between two perceptual hashes. Lower = more similar.
#[pg_extern(immutable, parallel_safe, strict, name = "img_pixel_hash_distance")]
fn img_pixel_hash_distance(hash1: &str, hash2: &str) -> i32 {
    match hashing::hash_distance(hash1, hash2) {
        Ok(d) => d,
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Object Detection (ONNX)
// =============================================================================

/// Detect objects in an image using a YOLO ONNX model.
/// Returns a set of rows with label, confidence, and bounding box coordinates.
///
/// Requires ONNX Runtime installed and a model file in pg_image.model_dir.
///
/// Usage:
///   SELECT * FROM pgimg.img_detect(photo);
///   SELECT * FROM pgimg.img_detect(photo, 'yolov8s.onnx');
///   SELECT * FROM pgimg.img_detect(photo, 'yolov8n.onnx', 0.7);
#[pg_extern(name = "img_detect")]
fn img_detect(
    data: &[u8],
    model: default!(Option<&str>, "NULL"),
    confidence: default!(f64, 0.0),
) -> TableIterator<
    'static,
    (
        name!(label, String),
        name!(confidence, f64),
        name!(x, f64),
        name!(y, f64),
        name!(width, f64),
        name!(height, f64),
    ),
> {
    let default_model = detect::default_model_name();
    let model_name = model.unwrap_or(&default_model);

    let conf_threshold = if confidence > 0.0 {
        confidence
    } else {
        detect::DEFAULT_CONFIDENCE.get()
    };
    let iou_threshold = detect::DEFAULT_IOU_THRESHOLD.get();

    let detections = match detect::detect(data, model_name, conf_threshold, iou_threshold) {
        Ok(d) => d,
        Err(e) => pgrx::error!("{}", e),
    };

    let rows: Vec<_> = detections
        .into_iter()
        .map(|d| (d.label, d.confidence, d.x, d.y, d.width, d.height))
        .collect();

    TableIterator::new(rows)
}

// =============================================================================
// Embedding Functions (CLIP)
// =============================================================================

/// Generate CLIP image embedding (returns float4[] with 512 elements).
///
/// Usage:
///   SELECT pgimg.img_embed(photo) FROM products;
///   SELECT pgimg.img_embed(photo)::vector(512) FROM products;  -- with pgvector
#[pg_extern(name = "img_embed")]
fn img_embed(data: &[u8], model: default!(Option<&str>, "NULL")) -> Vec<f32> {
    let default_model = embed::default_embed_model();
    let model_name = model.unwrap_or(&default_model);

    match embed::embed_image(data, model_name) {
        Ok(v) => v,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Generate CLIP text embedding (returns float4[] with 512 elements, same space as img_embed).
///
/// Usage:
///   SELECT pgimg.img_embed_text('a red bicycle');
///   SELECT * FROM products ORDER BY pgimg.img_embed(photo)::vector(512) <=>
///     pgimg.img_embed_text('red bicycle')::vector(512) LIMIT 10;
#[pg_extern(name = "img_embed_text")]
fn img_embed_text(text: &str, model: default!(Option<&str>, "NULL")) -> Vec<f32> {
    let default_model = embed::default_text_model();
    let model_name = model.unwrap_or(&default_model);

    match embed::embed_text(text, model_name) {
        Ok(v) => v,
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Captioning Functions (ViT-GPT2)
// =============================================================================

/// Generate a text caption for an image using ViT-GPT2.
///
/// Usage:
///   SELECT pgimg.img_caption(photo) FROM products;
///   SELECT pgimg.img_caption(photo, 50) FROM products;  -- max 50 tokens
#[pg_extern(name = "img_caption")]
fn img_caption(data: &[u8], max_tokens: default!(i32, 0)) -> String {
    let encoder = caption::default_encoder_model();
    let decoder = caption::default_decoder_model();
    let max = if max_tokens > 0 {
        max_tokens as usize
    } else {
        caption::CAPTION_MAX_TOKENS.get() as usize
    };

    match caption::caption(data, &encoder, &decoder, max) {
        Ok(text) => text,
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    // Minimal valid 1x1 red RGB PNG as hex string for SQL (69 bytes, correct CRCs)
    const TINY_PNG_HEX: &str = r"\x89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";

    #[pg_test]
    fn test_img_width() {
        let result = Spi::get_one::<i32>(&format!(
            "SELECT pgimg.img_width('{}'::bytea)",
            TINY_PNG_HEX
        ));
        assert_eq!(result.unwrap().unwrap(), 1);
    }

    #[pg_test]
    fn test_img_height() {
        let result = Spi::get_one::<i32>(&format!(
            "SELECT pgimg.img_height('{}'::bytea)",
            TINY_PNG_HEX
        ));
        assert_eq!(result.unwrap().unwrap(), 1);
    }

    #[pg_test]
    fn test_img_format() {
        let result = Spi::get_one::<String>(&format!(
            "SELECT pgimg.img_format('{}'::bytea)",
            TINY_PNG_HEX
        ));
        assert_eq!(result.unwrap().unwrap(), "png");
    }

    #[pg_test]
    fn test_img_colorspace() {
        let result = Spi::get_one::<String>(&format!(
            "SELECT pgimg.img_colorspace('{}'::bytea)",
            TINY_PNG_HEX
        ));
        assert_eq!(result.unwrap().unwrap(), "rgb");
    }

    #[pg_test]
    fn test_img_channels() {
        let result = Spi::get_one::<i32>(&format!(
            "SELECT pgimg.img_channels('{}'::bytea)",
            TINY_PNG_HEX
        ));
        assert_eq!(result.unwrap().unwrap(), 3);
    }

    #[pg_test]
    fn test_img_metadata_returns_jsonb() {
        let result = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT pgimg.img_metadata('{}'::bytea)",
            TINY_PNG_HEX
        ));
        let json = result.unwrap().unwrap();
        assert_eq!(json.0["width"], 1);
        assert_eq!(json.0["format"], "png");
    }

    #[pg_test]
    fn test_img_size_bytes() {
        let result = Spi::get_one::<i64>(&format!(
            "SELECT pgimg.img_size_bytes('{}'::bytea)",
            TINY_PNG_HEX
        ));
        assert!(result.unwrap().unwrap() > 0);
    }

    #[pg_test]
    fn test_null_returns_null_for_exif() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgimg.img_exif(NULL::bytea)");
        assert!(result.unwrap().is_none());
    }

    #[pg_test]
    fn test_null_returns_null_for_gps() {
        let result = Spi::get_one::<f64>("SELECT pgimg.img_gps_lat(NULL::bytea)");
        assert!(result.unwrap().is_none());
    }

    // =========================================================================
    // End-to-end object detection tests (require ONNX Runtime + model file)
    // Run with: cargo pgrx test pg16 -- --ignored
    // =========================================================================

    /// Helper: read bus.jpg fixture and return as Postgres bytea hex literal.
    fn bus_jpg_bytea_literal() -> String {
        let image_bytes = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bus.jpg"
        ))
        .expect("read bus.jpg fixture");
        let hex: String = image_bytes.iter().map(|b| format!("{:02x}", b)).collect();
        format!(r"\x{}", hex)
    }

    /// Set model_dir GUC for tests (default is None, must be configured).
    fn setup_model_dir() {
        Spi::run("SET pg_image.model_dir = '/var/lib/pg_image/models'").unwrap();
    }

    #[pg_test]
    #[ignore] // requires ONNX Runtime + /var/lib/pg_image/models/yolov8n.onnx
    fn test_img_detect_e2e_sql() {
        setup_model_dir();
        let bytea_lit = bus_jpg_bytea_literal();

        // Run detection into temp table (single inference, multiple assertions)
        Spi::run(&format!(
            "CREATE TEMP TABLE _dets AS \
             SELECT * FROM pgimg.img_detect('{}'::bytea, 'yolov8n.onnx', 0.25)",
            bytea_lit
        ))
        .unwrap();

        // Should detect at least 3 objects (4 persons + 1 bus in reference)
        let count = Spi::get_one::<i64>("SELECT count(*) FROM _dets")
            .unwrap()
            .unwrap();
        assert!(count >= 3, "expected >= 3 detections, got {}", count);

        // Should find persons
        let person_count = Spi::get_one::<i64>("SELECT count(*) FROM _dets WHERE label = 'person'")
            .unwrap()
            .unwrap();
        assert!(
            person_count >= 2,
            "expected >= 2 persons, got {}",
            person_count
        );

        // Should find a bus
        let bus_count = Spi::get_one::<i64>("SELECT count(*) FROM _dets WHERE label = 'bus'")
            .unwrap()
            .unwrap();
        assert_eq!(bus_count, 1, "expected 1 bus, got {}", bus_count);

        // Confidence values must be in (0, 1]
        let bad_conf = Spi::get_one::<i64>(
            "SELECT count(*) FROM _dets WHERE confidence <= 0 OR confidence > 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(bad_conf, 0, "all confidences should be in (0, 1]");

        // Bounding boxes must have positive coordinates and dimensions
        let bad_boxes = Spi::get_one::<i64>(
            "SELECT count(*) FROM _dets WHERE x < 0 OR y < 0 OR width <= 0 OR height <= 0",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            bad_boxes, 0,
            "all bounding boxes should have positive coords/dimensions"
        );
    }

    #[pg_test]
    #[ignore] // requires ONNX Runtime + /var/lib/pg_image/models/yolov8n.onnx
    fn test_img_detect_default_params() {
        setup_model_dir();
        let bytea_lit = bus_jpg_bytea_literal();

        // Call with default model and default confidence (uses GUC values)
        let count = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM pgimg.img_detect('{}'::bytea)",
            bytea_lit
        ))
        .unwrap()
        .unwrap();
        assert!(count > 0, "default params should detect objects, got 0");
    }

    #[pg_test]
    #[ignore] // requires ONNX Runtime + /var/lib/pg_image/models/yolov8n.onnx
    fn test_img_detect_lateral_join_pattern() {
        setup_model_dir();
        let bytea_lit = bus_jpg_bytea_literal();

        // Create table with image data
        Spi::run("CREATE TEMP TABLE _images (id int, photo bytea)").unwrap();
        Spi::run(&format!(
            "INSERT INTO _images VALUES (1, '{}'::bytea)",
            bytea_lit
        ))
        .unwrap();

        // Test the LATERAL join pattern (real-world usage)
        let count = Spi::get_one::<i64>(
            "SELECT count(*) \
             FROM _images i, \
             LATERAL pgimg.img_detect(i.photo, 'yolov8n.onnx', 0.25) d",
        )
        .unwrap()
        .unwrap();
        assert!(
            count >= 3,
            "LATERAL join should detect >= 3 objects, got {}",
            count
        );

        // Verify we can combine detection with metadata functions
        let width = Spi::get_one::<i32>("SELECT pgimg.img_width(photo) FROM _images WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(width, 810, "bus.jpg is 810px wide");
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
