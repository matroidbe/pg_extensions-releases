# pg_image - Image Processing for PostgreSQL

Extract metadata, EXIF data, GPS coordinates, apply transforms, compute perceptual hashes, and detect objects with ONNX — all from SQL on `bytea` columns.

## Installation

```bash
# Build and install extension
PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make install

# In PostgreSQL
CREATE EXTENSION pg_image;
```

All metadata, EXIF, GPS, transform, and hashing functions work immediately.

### Enable Object Detection (optional)

Object detection requires ONNX Runtime + a YOLO model. The Makefile handles everything including permissions for system PostgreSQL:

```bash
# One command: installs ONNX Runtime, exports YOLO model, configures GUC
PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make setup-onnx

# Or all-in-one (extension + ONNX)
PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make install-full
```

Models are stored in `$(pg_config --sharedir)/pg_image/` (same pattern as pg_ml). For system PostgreSQL, ownership is set to `postgres:postgres` automatically.

Use a different YOLO variant:

```bash
PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config DEFAULT_MODEL=yolov8s make setup-model
```

Check status:

```bash
make status
```

## Quick Start

```sql
-- Get image dimensions and format
SELECT pgimg.img_width(photo), pgimg.img_height(photo), pgimg.img_format(photo)
FROM products;

-- All metadata as JSON
SELECT pgimg.img_metadata(photo) FROM products;

-- EXIF data
SELECT pgimg.img_camera_make(photo), pgimg.img_camera_model(photo)
FROM photos WHERE pgimg.img_exif(photo) IS NOT NULL;

-- GPS coordinates (with PostGIS)
SELECT pgimg.img_gps_point(photo) AS location
FROM drone_frames;

-- Create thumbnails
SELECT pgimg.img_thumbnail(photo, 128, 128) AS thumb FROM products;

-- Detect objects with YOLO
SELECT * FROM pgimg.img_detect(photo);

-- Find near-duplicate images
SELECT a.id, b.id,
       pgimg.img_pixel_hash_distance(
           pgimg.img_pixel_hash(a.photo),
           pgimg.img_pixel_hash(b.photo)
       ) AS distance
FROM images a, images b
WHERE a.id < b.id;
```

## API Reference

### Metadata

| Function | Returns | Description |
|----------|---------|-------------|
| `img_width(bytea)` | `integer` | Width in pixels |
| `img_height(bytea)` | `integer` | Height in pixels |
| `img_format(bytea)` | `text` | Format: jpeg, png, webp, gif, tiff, bmp |
| `img_colorspace(bytea)` | `text` | Color space: rgb, rgba, grayscale |
| `img_channels(bytea)` | `integer` | Number of channels |
| `img_bit_depth(bytea)` | `integer` | Bits per channel |
| `img_size_bytes(bytea)` | `bigint` | Encoded size |
| `img_metadata(bytea)` | `jsonb` | All metadata as JSON |

### EXIF

| Function | Returns | Description |
|----------|---------|-------------|
| `img_exif(bytea)` | `jsonb` | All EXIF as JSON (NULL if none) |
| `img_exif_value(bytea, text)` | `text` | Single EXIF tag value |
| `img_timestamp(bytea)` | `timestamptz` | DateTimeOriginal |
| `img_camera_make(bytea)` | `text` | Camera manufacturer |
| `img_camera_model(bytea)` | `text` | Camera model |
| `img_orientation(bytea)` | `integer` | Orientation (1-8) |

### GPS

| Function | Returns | Description |
|----------|---------|-------------|
| `img_gps_lat(bytea)` | `float8` | Latitude (decimal degrees) |
| `img_gps_lon(bytea)` | `float8` | Longitude (decimal degrees) |
| `img_gps_altitude(bytea)` | `float8` | Altitude (meters) |
| `img_gps_point(bytea)` | `geometry` | PostGIS Point (requires PostGIS) |
| `img_gps_pointz(bytea)` | `geometry` | PostGIS PointZ (requires PostGIS) |

### Transforms

| Function | Returns | Description |
|----------|---------|-------------|
| `img_resize(bytea, int, int)` | `bytea` | Resize to exact dimensions |
| `img_thumbnail(bytea, int, int)` | `bytea` | Thumbnail preserving aspect ratio |
| `img_crop(bytea, int, int, int, int)` | `bytea` | Crop region (x, y, w, h) |
| `img_convert(bytea, text)` | `bytea` | Convert format |
| `img_grayscale(bytea)` | `bytea` | Convert to grayscale |
| `img_rotate(bytea, int)` | `bytea` | Rotate 90/180/270 |
| `img_auto_orient(bytea)` | `bytea` | Apply EXIF orientation |
| `img_to_rgb(bytea)` | `bytea` | Raw RGB pixel bytes |

### Hashing

| Function | Returns | Description |
|----------|---------|-------------|
| `img_pixel_hash(bytea)` | `text` | Perceptual hash (base64) |
| `img_pixel_hash_distance(text, text)` | `integer` | Hamming distance between hashes |

### Object Detection

| Function | Returns | Description |
|----------|---------|-------------|
| `img_detect(bytea, text, float8)` | `SETOF (label, confidence, x, y, width, height)` | YOLO object detection via ONNX |

The model parameter defaults to `pg_image.default_model` GUC (yolov8n.onnx). The confidence parameter defaults to `pg_image.detect_confidence` GUC (0.5).

## Configuration

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_image.model_dir` | *(set by `make setup-onnx`)* | Directory containing ONNX model files |
| `pg_image.default_model` | `yolov8n.onnx` | Default model for `img_detect` |
| `pg_image.detect_confidence` | `0.5` | Default confidence threshold (0.0-1.0) |
| `pg_image.detect_iou_threshold` | `0.45` | NMS IoU threshold (0.0-1.0) |

`make setup-onnx` writes `pg_image.model_dir` to `postgresql.conf` automatically. To configure manually:

```sql
ALTER SYSTEM SET pg_image.model_dir = '/usr/share/postgresql/16/pg_image';
SELECT pg_reload_conf();

-- Lower confidence for more detections
SET pg_image.detect_confidence = 0.3;
```

## Integration Examples

### PostGIS: Geolocated Object Detection

```sql
-- Detect objects in drone frames with GPS coordinates
SELECT d.label, d.confidence,
       d.x, d.y, d.width, d.height,
       pgimg.img_gps_point(f.raw_image) AS location
FROM drone_frames f,
     LATERAL pgimg.img_detect(f.raw_image) d
WHERE d.confidence > 0.8;
```

### pgvector: Visual Search Pipeline

```sql
-- Create thumbnails and extract RGB for embedding
SELECT id,
       pgimg.img_thumbnail(photo, 224, 224) AS resized,
       pgimg.img_to_rgb(pgimg.img_thumbnail(photo, 224, 224)) AS rgb_bytes
FROM products;
```

### Image Deduplication

```sql
-- Find near-duplicate images using perceptual hashing
SELECT a.id AS image_a, b.id AS image_b,
       pgimg.img_pixel_hash_distance(
           pgimg.img_pixel_hash(a.photo),
           pgimg.img_pixel_hash(b.photo)
       ) AS distance
FROM images a
JOIN images b ON a.id < b.id
WHERE pgimg.img_pixel_hash_distance(
    pgimg.img_pixel_hash(a.photo),
    pgimg.img_pixel_hash(b.photo)
) < 10;
```

### EXIF-based Photo Organization

```sql
-- Photos by camera, sorted by timestamp
SELECT pgimg.img_camera_model(photo) AS camera,
       pgimg.img_timestamp(photo) AS taken_at,
       pgimg.img_width(photo) || 'x' || pgimg.img_height(photo) AS resolution
FROM photos
WHERE pgimg.img_timestamp(photo) IS NOT NULL
ORDER BY pgimg.img_timestamp(photo) DESC;
```

## Supported Formats

JPEG, PNG, WebP, GIF, TIFF, BMP

## Dependencies

| Crate | Purpose |
|-------|---------|
| `image` 0.25 | Decoding, encoding, transforms |
| `kamadak-exif` 0.6 | EXIF metadata parsing |
| `image_hasher` 3 | Perceptual hashing |
| `ort` 2.0 | ONNX Runtime inference |
| `ndarray` 0.17 | Tensor manipulation |
