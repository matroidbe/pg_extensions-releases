//! `pgx.fetch_vec()` — vector / tensor / RGB query SRF for composite
//! variables. The composite is declared via
//! `pgx.register_variable(..., components := ARRAY[...])`; each entry
//! points at an existing scalar variable on the same dataset. Calling
//! `fetch_vec` calls the per-component `fetch_impl` under the hood and
//! zips the resulting cells by `(lat, lon, level, time)` into a single
//! row whose `values float8[]` is in component-position order.

use crate::srf::fetch::fetch_impl;
use pgrx::prelude::*;
use std::collections::HashMap;

/// One row returned by `pgx.fetch_vec()`.
pub type FetchVecRow = (
    Option<f64>,                                // lat
    Option<f64>,                                // lon
    Option<f64>,                                // level
    Option<pgrx::datum::TimestampWithTimeZone>, // time
    Vec<f64>,                                   // values, in component position order
);

/// Convert the four optional coords to a hashable key. NaN-safe: any
/// NaN coord becomes a sentinel (we don't expect NaN coords from
/// well-behaved Zarr stores, but the catalog is permissive).
type CoordKey = (u64, u64, u64, i64);

fn key_of(
    lat: Option<f64>,
    lon: Option<f64>,
    level: Option<f64>,
    time: Option<pgrx::datum::TimestampWithTimeZone>,
) -> CoordKey {
    fn ob(v: Option<f64>) -> u64 {
        match v {
            Some(x) if x.is_nan() => u64::MAX,
            Some(x) => x.to_bits(),
            None => 0,
        }
    }
    // TimestampWithTimeZone uses microseconds since the PG epoch
    // (2000-01-01) internally — fine as a hash key (we just need a
    // stable i64 from the timestamp; absolute scale doesn't matter
    // for equality).
    let t = time.map(|t| t.into()).unwrap_or(i64::MIN);
    (ob(lat), ob(lon), ob(level), t)
}

pub fn fetch_vec_impl(
    dataset: &str,
    variable: &str,
    at_time: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    level_from: Option<f64>,
    level_to: Option<f64>,
    max_cells: i32,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
) -> Vec<FetchVecRow> {
    if dataset.is_empty() || variable.is_empty() {
        pgrx::error!("pgx.fetch_vec: dataset and variable are required");
    }
    if max_cells <= 0 {
        pgrx::error!(
            "pgx.fetch_vec: max_cells must be positive (got {})",
            max_cells
        );
    }

    let components = lookup_components(dataset, variable);
    if components.is_empty() {
        pgrx::error!(
            "pgx.fetch_vec: '{}' is not a vector variable — register it via \
             `pgx.register_variable(..., components := ARRAY[...])` first, \
             or use `pgx.fetch` for scalar reads.",
            variable
        );
    }
    let n_components = components.len();

    // For each component, fetch its cells and merge by coord.
    let mut by_coord: HashMap<CoordKey, MergedCell> = HashMap::new();
    for (idx, comp_name) in components.iter().enumerate() {
        let rows = fetch_impl(
            dataset, comp_name, at_time, bbox_wkt, level_from, level_to, max_cells, time_from,
            time_to,
        );
        for (lat, lon, level, time, value) in rows {
            let key = key_of(lat, lon, level, time);
            let entry = by_coord.entry(key).or_insert_with(|| MergedCell {
                lat,
                lon,
                level,
                time,
                values: vec![f64::NAN; n_components],
            });
            entry.values[idx] = value;
        }
    }

    by_coord
        .into_values()
        .map(|c| (c.lat, c.lon, c.level, c.time, c.values))
        .collect()
}

struct MergedCell {
    lat: Option<f64>,
    lon: Option<f64>,
    level: Option<f64>,
    time: Option<pgrx::datum::TimestampWithTimeZone>,
    values: Vec<f64>,
}

/// Return the ordered list of component variable NAMES for a composite
/// variable. Empty when the variable isn't a composite (i.e., has no
/// rows in `pgx.variable_components`).
fn lookup_components(dataset: &str, variable: &str) -> Vec<String> {
    let sql = r#"
        SELECT vc.component_name
        FROM   pgx.variable_components vc
        JOIN   pgx.variables  v_composite ON v_composite.id = vc.composite_variable_id
        JOIN   pgx.datasets   d           ON d.id = v_composite.dataset_id
        WHERE  d.name = $1 AND v_composite.name = $2
        ORDER  BY vc.position
    "#;
    Spi::connect(|client| {
        let table = client.select(sql, None, &[dataset.into(), variable.into()])?;
        let mut out = Vec::new();
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            if !name.is_empty() {
                out.push(name);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_default()
}
