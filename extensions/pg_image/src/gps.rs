use std::io::Cursor;

/// Convert EXIF GPS DMS (degrees/minutes/seconds as Rational) to decimal degrees.
///
/// GPS coordinates in EXIF are stored as three Rational values:
///   [0] = degrees, [1] = minutes, [2] = seconds
/// Each Rational has num/denom fields.
fn dms_to_decimal(rationals: &[exif::Rational], ref_dir: &str) -> Option<f64> {
    if rationals.len() < 3 || rationals.iter().any(|r| r.denom == 0) {
        return None;
    }

    let degrees = rationals[0].num as f64 / rationals[0].denom as f64;
    let minutes = rationals[1].num as f64 / rationals[1].denom as f64;
    let seconds = rationals[2].num as f64 / rationals[2].denom as f64;

    let mut decimal = degrees + minutes / 60.0 + seconds / 3600.0;

    // South and West are negative
    if ref_dir == "S" || ref_dir == "W" {
        decimal = -decimal;
    }

    Some(decimal)
}

/// Parse EXIF data from bytes.
fn parse_exif(data: &[u8]) -> Option<exif::Exif> {
    let reader = exif::Reader::new();
    reader.read_from_container(&mut Cursor::new(data)).ok()
}

/// Get GPS reference direction as string ("N", "S", "E", "W").
fn get_gps_ref(exif_data: &exif::Exif, tag: exif::Tag) -> Option<String> {
    let field = exif_data.get_field(tag, exif::In::PRIMARY)?;
    match &field.value {
        exif::Value::Ascii(vecs) if !vecs.is_empty() => {
            Some(String::from_utf8_lossy(&vecs[0]).trim().to_string())
        }
        _ => None,
    }
}

/// Get GPS coordinate rationals.
fn get_gps_rationals(exif_data: &exif::Exif, tag: exif::Tag) -> Option<Vec<exif::Rational>> {
    let field = exif_data.get_field(tag, exif::In::PRIMARY)?;
    match &field.value {
        exif::Value::Rational(rationals) if rationals.len() >= 3 => Some(rationals.clone()),
        _ => None,
    }
}

/// Extract GPS latitude in decimal degrees. Returns None if no GPS data.
pub fn extract_lat(data: &[u8]) -> Option<f64> {
    let exif_data = parse_exif(data)?;
    let rationals = get_gps_rationals(&exif_data, exif::Tag::GPSLatitude)?;
    let ref_dir =
        get_gps_ref(&exif_data, exif::Tag::GPSLatitudeRef).unwrap_or_else(|| "N".to_string());
    dms_to_decimal(&rationals, &ref_dir)
}

/// Extract GPS longitude in decimal degrees. Returns None if no GPS data.
pub fn extract_lon(data: &[u8]) -> Option<f64> {
    let exif_data = parse_exif(data)?;
    let rationals = get_gps_rationals(&exif_data, exif::Tag::GPSLongitude)?;
    let ref_dir =
        get_gps_ref(&exif_data, exif::Tag::GPSLongitudeRef).unwrap_or_else(|| "E".to_string());
    dms_to_decimal(&rationals, &ref_dir)
}

/// Extract GPS altitude in meters. Returns None if no altitude data.
pub fn extract_altitude(data: &[u8]) -> Option<f64> {
    let exif_data = parse_exif(data)?;
    let field = exif_data.get_field(exif::Tag::GPSAltitude, exif::In::PRIMARY)?;
    match &field.value {
        exif::Value::Rational(rationals) if !rationals.is_empty() && rationals[0].denom != 0 => {
            let alt = rationals[0].num as f64 / rationals[0].denom as f64;
            // Check GPSAltitudeRef: 0 = above sea level, 1 = below
            let below = exif_data
                .get_field(exif::Tag::GPSAltitudeRef, exif::In::PRIMARY)
                .and_then(|f| f.value.get_uint(0))
                .unwrap_or(0)
                == 1;
            Some(if below { -alt } else { alt })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dms_to_decimal_north_east() {
        // Eiffel Tower: 48°51'30.2"N, 2°17'40.2"E
        let lat_rationals = vec![
            exif::Rational { num: 48, denom: 1 },
            exif::Rational { num: 51, denom: 1 },
            exif::Rational {
                num: 302,
                denom: 10,
            },
        ];
        let lat = dms_to_decimal(&lat_rationals, "N").unwrap();
        assert!((lat - 48.8584).abs() < 0.001);

        let lon_rationals = vec![
            exif::Rational { num: 2, denom: 1 },
            exif::Rational { num: 17, denom: 1 },
            exif::Rational {
                num: 402,
                denom: 10,
            },
        ];
        let lon = dms_to_decimal(&lon_rationals, "E").unwrap();
        assert!((lon - 2.2945).abs() < 0.001);
    }

    #[test]
    fn test_dms_to_decimal_south_west() {
        // Rio de Janeiro: ~22°54'30"S, 43°10'12"W
        let lat_rationals = vec![
            exif::Rational { num: 22, denom: 1 },
            exif::Rational { num: 54, denom: 1 },
            exif::Rational { num: 30, denom: 1 },
        ];
        let lat = dms_to_decimal(&lat_rationals, "S").unwrap();
        assert!(lat < 0.0);
        assert!((lat + 22.9083).abs() < 0.001);

        let lon_rationals = vec![
            exif::Rational { num: 43, denom: 1 },
            exif::Rational { num: 10, denom: 1 },
            exif::Rational { num: 12, denom: 1 },
        ];
        let lon = dms_to_decimal(&lon_rationals, "W").unwrap();
        assert!(lon < 0.0);
        assert!((lon + 43.17).abs() < 0.001);
    }

    #[test]
    fn test_dms_to_decimal_zero_denom() {
        let rationals = vec![
            exif::Rational { num: 48, denom: 1 },
            exif::Rational { num: 51, denom: 0 }, // invalid
            exif::Rational { num: 30, denom: 1 },
        ];
        assert!(dms_to_decimal(&rationals, "N").is_none());
    }

    #[test]
    fn test_dms_to_decimal_insufficient_values() {
        let rationals = vec![
            exif::Rational { num: 48, denom: 1 },
            exif::Rational { num: 51, denom: 1 },
        ];
        assert!(dms_to_decimal(&rationals, "N").is_none());
    }

    #[test]
    fn test_extract_lat_no_exif() {
        // PNG has no EXIF data
        let tiny_png: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0xE2, 0x21, 0xBC,
            0x33, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];
        assert!(extract_lat(tiny_png).is_none());
        assert!(extract_lon(tiny_png).is_none());
        assert!(extract_altitude(tiny_png).is_none());
    }
}
