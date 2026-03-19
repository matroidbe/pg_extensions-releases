use std::io::Cursor;

use crate::error::PgImageError;

/// Parse EXIF data from image bytes.
fn parse_exif(data: &[u8]) -> Result<exif::Exif, PgImageError> {
    let reader = exif::Reader::new();
    reader
        .read_from_container(&mut Cursor::new(data))
        .map_err(|e| PgImageError::ExifError(e.to_string()))
}

/// Extract all EXIF fields as a JSON object.
pub fn extract_all(data: &[u8]) -> Result<serde_json::Value, PgImageError> {
    let exif_data = parse_exif(data)?;
    let mut map = serde_json::Map::new();

    for field in exif_data.fields() {
        let tag_name = format!("{}", field.tag);
        let value = field_value_to_json(&field.value);
        map.insert(tag_name, value);
    }

    Ok(serde_json::Value::Object(map))
}

/// Get a single EXIF tag value by name.
pub fn get_tag_value(data: &[u8], tag_name: &str) -> Option<String> {
    let exif_data = parse_exif(data).ok()?;
    let tag_lower = tag_name.to_lowercase();

    for field in exif_data.fields() {
        let name = format!("{}", field.tag);
        if name.to_lowercase() == tag_lower {
            return Some(field.display_value().with_unit(&exif_data).to_string());
        }
    }
    None
}

/// Parse EXIF DateTimeOriginal as TimestampWithTimeZone.
pub fn parse_timestamp(data: &[u8]) -> Option<pgrx::datum::TimestampWithTimeZone> {
    let exif_data = parse_exif(data).ok()?;

    // Try DateTimeOriginal first, then DateTimeDigitized, then DateTime
    let dt_field = exif_data
        .get_field(exif::Tag::DateTimeOriginal, exif::In::PRIMARY)
        .or_else(|| exif_data.get_field(exif::Tag::DateTimeDigitized, exif::In::PRIMARY))
        .or_else(|| exif_data.get_field(exif::Tag::DateTime, exif::In::PRIMARY))?;

    let dt_str = match &dt_field.value {
        exif::Value::Ascii(ref vecs) if !vecs.is_empty() => {
            String::from_utf8_lossy(&vecs[0]).to_string()
        }
        _ => return None,
    };

    // EXIF format: "YYYY:MM:DD HH:MM:SS" -> convert to "YYYY-MM-DD HH:MM:SS"
    if dt_str.len() < 19 {
        return None;
    }
    let iso_str = format!(
        "{}-{}-{} {}",
        &dt_str[0..4],
        &dt_str[5..7],
        &dt_str[8..10],
        &dt_str[11..19]
    );

    // Use SPI to leverage PostgreSQL's timestamp parsing
    let sql = format!("SELECT '{} UTC'::timestamptz", iso_str);
    pgrx::Spi::get_one::<pgrx::datum::TimestampWithTimeZone>(&sql)
        .ok()
        .flatten()
}

/// Camera manufacturer from EXIF Make tag.
pub fn camera_make(data: &[u8]) -> Option<String> {
    let exif_data = parse_exif(data).ok()?;
    let field = exif_data.get_field(exif::Tag::Make, exif::In::PRIMARY)?;
    Some(
        field
            .display_value()
            .to_string()
            .trim_matches('"')
            .to_string(),
    )
}

/// Camera model from EXIF Model tag.
pub fn camera_model(data: &[u8]) -> Option<String> {
    let exif_data = parse_exif(data).ok()?;
    let field = exif_data.get_field(exif::Tag::Model, exif::In::PRIMARY)?;
    Some(
        field
            .display_value()
            .to_string()
            .trim_matches('"')
            .to_string(),
    )
}

/// EXIF Orientation tag (1-8).
pub fn orientation(data: &[u8]) -> Option<i32> {
    let exif_data = parse_exif(data).ok()?;
    let field = exif_data.get_field(exif::Tag::Orientation, exif::In::PRIMARY)?;
    field.value.get_uint(0).map(|v| v as i32)
}

/// Convert an EXIF Value to a serde_json::Value.
fn field_value_to_json(value: &exif::Value) -> serde_json::Value {
    match value {
        exif::Value::Byte(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::Ascii(vecs) => {
            let strings: Vec<String> = vecs
                .iter()
                .map(|v| String::from_utf8_lossy(v).to_string())
                .collect();
            if strings.len() == 1 {
                serde_json::json!(strings[0])
            } else {
                serde_json::json!(strings)
            }
        }
        exif::Value::Short(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::Long(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::Rational(v) => {
            let floats: Vec<f64> = v.iter().map(|r| r.num as f64 / r.denom as f64).collect();
            if floats.len() == 1 {
                serde_json::json!(floats[0])
            } else {
                serde_json::json!(floats)
            }
        }
        exif::Value::SByte(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::SShort(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::SLong(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::SRational(v) => {
            let floats: Vec<f64> = v.iter().map(|r| r.num as f64 / r.denom as f64).collect();
            if floats.len() == 1 {
                serde_json::json!(floats[0])
            } else {
                serde_json::json!(floats)
            }
        }
        exif::Value::Float(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::Double(v) => {
            if v.len() == 1 {
                serde_json::json!(v[0])
            } else {
                serde_json::json!(v)
            }
        }
        exif::Value::Undefined(bytes, _) => {
            serde_json::json!(format!("0x{}", hex_encode(bytes)))
        }
        _ => serde_json::Value::Null,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
