//! Parquet parser — one record per row, via Arrow.
//!
//! Parquet is the format of the raw zone (immutable files that Eidos engines
//! ingest and replay from), so this is the parser `ingest from opendal` uses for
//! it. Rows are rendered with `arrow-json`, which keeps the types a downstream
//! command can cast without guessing:
//!
//! - integers and floats → JSON numbers
//! - timestamps and dates → ISO-8601 strings
//! - binary (e.g. a GeoParquet WKB geometry) → lowercase hex, which PostgreSQL
//!   reads with `decode(value, 'hex')` → `ST_GeomFromWKB(...)`
//! - nested structs and lists → JSON objects and arrays
//!
//! Parser config (all optional):
//!
//! ```json
//! { "explicit_nulls": true,          // emit null for a null cell (default true)
//!   "columns": ["gauge", "level_m"]  // read only these top-level columns
//! }
//! ```
//!
//! The whole file is decoded in memory, like every other parser here; raw-zone
//! files are one arrival each, so they stay small.

use crate::connector::sdk::{ParseContext, Parser};
use arrow_json::writer::JsonArray;
use arrow_json::WriterBuilder;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use serde_json::Value;

pub struct ParquetParser {
    explicit_nulls: bool,
    columns: Option<Vec<String>>,
}

impl ParquetParser {
    pub fn new() -> Self {
        Self {
            explicit_nulls: true,
            columns: None,
        }
    }

    pub fn from_config(config: &Value) -> Result<Self, String> {
        let mut parser = Self::new();
        if config.is_null() {
            return Ok(parser);
        }
        if let Some(v) = config.get("explicit_nulls") {
            parser.explicit_nulls = v
                .as_bool()
                .ok_or_else(|| "parquet: explicit_nulls must be a boolean".to_string())?;
        }
        if let Some(v) = config.get("columns") {
            let cols = v
                .as_array()
                .ok_or_else(|| "parquet: columns must be an array of names".to_string())?
                .iter()
                .map(|c| {
                    c.as_str()
                        .map(String::from)
                        .ok_or_else(|| "parquet: columns must be strings".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            parser.columns = Some(cols);
        }
        Ok(parser)
    }
}

impl Default for ParquetParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser for ParquetParser {
    fn parse(&self, bytes: Bytes, context: &ParseContext) -> Result<Vec<Value>, String> {
        let file = context.filename.as_deref().unwrap_or("<payload>");
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| format!("parquet: '{}' is not a readable Parquet file: {}", file, e))?;

        if let Some(columns) = &self.columns {
            let schema = builder.schema().clone();
            let mut indices = Vec::with_capacity(columns.len());
            for name in columns {
                let idx = schema
                    .index_of(name)
                    .map_err(|_| format!("parquet: column '{}' not found in '{}'", name, file))?;
                indices.push(idx);
            }
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }

        let reader = builder
            .build()
            .map_err(|e| format!("parquet: cannot read '{}': {}", file, e))?;

        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(self.explicit_nulls)
                .build::<_, JsonArray>(&mut buf);
            for batch in reader {
                let batch =
                    batch.map_err(|e| format!("parquet: decode of '{}' failed: {}", file, e))?;
                writer
                    .write(&batch)
                    .map_err(|e| format!("parquet: row rendering for '{}' failed: {}", file, e))?;
            }
            writer
                .finish()
                .map_err(|e| format!("parquet: row rendering for '{}' failed: {}", file, e))?;
        }

        // A file with zero row groups writes nothing at all, not `[]`.
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        match serde_json::from_slice::<Value>(&buf)
            .map_err(|e| format!("parquet: internal JSON for '{}' invalid: {}", file, e))?
        {
            Value::Array(rows) => Ok(rows),
            other => Ok(vec![other]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, BinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    /// A small raw-zone-shaped file: gauge readings with a nullable column,
    /// a timestamp and a WKB point.
    fn gauge_file() -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("gauge", DataType::Utf8, false),
            Field::new("level_m", DataType::Float64, false),
            Field::new("discharge_m3s", DataType::Float64, true),
            Field::new("seq", DataType::Int64, false),
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("geom", DataType::Binary, false),
        ]));
        // WKB for POINT(5.05 50.985), little-endian.
        let mut wkb = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        wkb.extend_from_slice(&5.05f64.to_le_bytes());
        wkb.extend_from_slice(&50.985f64.to_le_bytes());

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["DEM-DIE", "DEM-AAR"])),
            Arc::new(Float64Array::from(vec![3.41, 2.87])),
            Arc::new(Float64Array::from(vec![Some(42.5), None])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_791_446_400_000_000, // 2026-10-08T08:00:00
                1_791_450_000_000_000, // 2026-10-08T09:00:00
            ])),
            Arc::new(BinaryArray::from(vec![wkb.as_slice(), wkb.as_slice()])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    fn ctx() -> ParseContext {
        ParseContext {
            filename: Some("hic-waterlevels/arrival_date=2026-10-08/a.parquet".into()),
            source_uri: None,
        }
    }

    #[test]
    fn one_record_per_row_with_types_kept() {
        let rows = ParquetParser::new().parse(gauge_file(), &ctx()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["gauge"], "DEM-DIE");
        assert_eq!(rows[0]["level_m"], 3.41);
        assert_eq!(rows[0]["seq"], 1);
        assert_eq!(rows[0]["discharge_m3s"], 42.5);
        assert!(rows[0]["observed_at"]
            .as_str()
            .unwrap()
            .starts_with("2026-10-08T08:00:00"));
    }

    #[test]
    fn null_cells_are_explicit_by_default() {
        let rows = ParquetParser::new().parse(gauge_file(), &ctx()).unwrap();
        assert!(rows[1].as_object().unwrap().contains_key("discharge_m3s"));
        assert!(rows[1]["discharge_m3s"].is_null());
    }

    #[test]
    fn null_cells_can_be_omitted() {
        let cfg = serde_json::json!({"explicit_nulls": false});
        let rows = ParquetParser::from_config(&cfg)
            .unwrap()
            .parse(gauge_file(), &ctx())
            .unwrap();
        assert!(!rows[1].as_object().unwrap().contains_key("discharge_m3s"));
    }

    #[test]
    fn binary_geometry_is_hex_wkb() {
        let rows = ParquetParser::new().parse(gauge_file(), &ctx()).unwrap();
        let hex = rows[0]["geom"]
            .as_str()
            .expect("binary renders as a string");
        // byte order 01, type 1 (Point) — what ST_GeomFromWKB(decode(hex,'hex')) reads.
        assert!(hex.starts_with("0101000000"), "got {}", hex);
        assert_eq!(hex.len(), 42); // 21 bytes
    }

    #[test]
    fn projection_reads_only_named_columns() {
        let cfg = serde_json::json!({"columns": ["gauge", "level_m"]});
        let rows = ParquetParser::from_config(&cfg)
            .unwrap()
            .parse(gauge_file(), &ctx())
            .unwrap();
        let keys: Vec<_> = rows[0].as_object().unwrap().keys().cloned().collect();
        assert_eq!(keys, vec!["gauge".to_string(), "level_m".to_string()]);
    }

    #[test]
    fn unknown_projection_column_names_the_file() {
        let cfg = serde_json::json!({"columns": ["nope"]});
        let err = ParquetParser::from_config(&cfg)
            .unwrap()
            .parse(gauge_file(), &ctx())
            .unwrap_err();
        assert!(
            err.contains("'nope'") && err.contains("a.parquet"),
            "{}",
            err
        );
    }

    #[test]
    fn not_parquet_is_a_clear_error() {
        let err = ParquetParser::new()
            .parse(Bytes::from_static(b"gauge,level\nDEM,1\n"), &ctx())
            .unwrap_err();
        assert!(err.contains("not a readable Parquet file"), "{}", err);
    }

    #[test]
    fn bad_config_is_rejected() {
        assert!(ParquetParser::from_config(&serde_json::json!({"columns": "gauge"})).is_err());
        assert!(ParquetParser::from_config(&serde_json::json!({"explicit_nulls": 1})).is_err());
    }
}
