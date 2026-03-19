use pgrx::prelude::*;

use crate::parser::{is_simple_unit, parse_unit_expression};

/// Resolved unit information from the database.
#[derive(Debug, Clone)]
pub struct ResolvedUnit {
    pub factor: f64,
    pub offset: f64,
    pub dims: [i16; 7],
    pub category_id: Option<i32>,
}

/// Combined unit info after parsing and resolving a compound expression.
#[derive(Debug, Clone)]
struct CombinedUnit {
    factor: f64,
    offset: f64,
    dims: [i16; 7],
    category_id: Option<i32>,
    is_simple: bool,
}

/// Convert a numeric value from one unit (or compound expression) to another.
/// Both sides must have matching dimensions.
///
/// Examples:
///   SELECT pguom.convert(1.0, 'kg', 'lb');        -- simple
///   SELECT pguom.convert(100.0, 'degC', 'degF');   -- temperature
///   SELECT pguom.convert(1.0, 'kW/m3', 'W/L');     -- compound
#[pg_extern(immutable)]
pub fn convert(value: f64, from_unit: &str, to_unit: &str) -> f64 {
    if from_unit == to_unit {
        return value;
    }

    let from = resolve_expression(from_unit);
    let to = resolve_expression(to_unit);

    // Check dimension compatibility
    if from.dims != to.dims {
        pgrx::error!(
            "pg_uom: incompatible dimensions: '{}' and '{}' have different physical dimensions",
            from_unit,
            to_unit
        );
    }

    // For dimensionless units with same zero dimensions, check category match
    if from.dims == [0; 7] {
        if let (Some(fc), Some(tc)) = (from.category_id, to.category_id) {
            if fc != tc {
                pgrx::error!(
                    "pg_uom: cannot convert between different dimensionless categories: '{}' and '{}'",
                    from_unit,
                    to_unit
                );
            }
        }
    }

    // Affine conversion for simple units (temperature)
    if from.is_simple && to.is_simple && (from.offset != 0.0 || to.offset != 0.0) {
        let base = value * from.factor + from.offset;
        return (base - to.offset) / to.factor;
    }

    // Linear conversion (compound or non-affine)
    value * from.factor / to.factor
}

/// Get the raw conversion factor between two unit expressions.
#[pg_extern(immutable)]
pub fn conversion_factor(from_unit: &str, to_unit: &str) -> f64 {
    if from_unit == to_unit {
        return 1.0;
    }

    let from = resolve_expression(from_unit);
    let to = resolve_expression(to_unit);

    if from.dims != to.dims {
        pgrx::error!(
            "pg_uom: incompatible dimensions for conversion factor: '{}' and '{}'",
            from_unit,
            to_unit
        );
    }

    from.factor / to.factor
}

/// Find all units with the same dimensions as the given unit.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn compatible_units(
    unit_ref: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(symbol, String),
        name!(category, Option<String>),
        name!(factor, f64),
    ),
> {
    let resolved = resolve_single_unit(unit_ref);
    let dims = resolved.dims;

    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT u.name, u.symbol, c.name AS category, u.factor
                FROM pguom.unit u
                LEFT JOIN pguom.category c ON u.category_id = c.id
                WHERE u.dim_mass = $1 AND u.dim_length = $2 AND u.dim_time = $3
                  AND u.dim_temp = $4 AND u.dim_amount = $5 AND u.dim_current = $6
                  AND u.dim_luminosity = $7
                ORDER BY u.factor
                "#,
                None,
                &[
                    dims[0].into(),
                    dims[1].into(),
                    dims[2].into(),
                    dims[3].into(),
                    dims[4].into(),
                    dims[5].into(),
                    dims[6].into(),
                ],
            )
            .unwrap();

        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let symbol: String = row.get_by_name("symbol").unwrap().unwrap();
            let category: Option<String> = row.get_by_name("category").unwrap();
            let factor: f64 = row.get_by_name("factor").unwrap().unwrap();
            rows.push((name, symbol, category, factor));
        }
    });

    TableIterator::new(rows)
}

/// Show the dimension formula for a unit expression (e.g., "kg·m⁻³").
#[pg_extern(immutable)]
pub fn unit_dimensions(unit_ref: &str) -> String {
    let combined = resolve_expression(unit_ref);
    format_dimensions(&combined.dims)
}

/// Format dimension vector as human-readable string (public for queries module).
pub fn format_dimensions_pub(dims: &[i16; 7]) -> String {
    format_dimensions(dims)
}

/// Format dimension vector as human-readable string.
fn format_dimensions(dims: &[i16; 7]) -> String {
    let names = ["kg", "m", "s", "K", "mol", "A", "cd"];
    let superscripts = [
        "", "\u{00B9}", "\u{00B2}", "\u{00B3}", "\u{2074}", "\u{2075}", "\u{2076}", "\u{2077}",
        "\u{2078}", "\u{2079}",
    ];

    let mut parts = Vec::new();
    for (i, &exp) in dims.iter().enumerate() {
        if exp == 0 {
            continue;
        }
        if exp == 1 {
            parts.push(names[i].to_string());
        } else if exp > 0 && (exp as usize) < superscripts.len() {
            parts.push(format!("{}{}", names[i], superscripts[exp as usize]));
        } else {
            // Negative or large exponents
            let abs = exp.unsigned_abs() as usize;
            let sup = if abs < superscripts.len() {
                superscripts[abs].to_string()
            } else {
                format!("^{}", exp.abs())
            };
            if exp < 0 {
                parts.push(format!("{}\u{207B}{}", names[i], sup));
            } else {
                parts.push(format!("{}{}", names[i], sup));
            }
        }
    }

    if parts.is_empty() {
        "dimensionless".to_string()
    } else {
        parts.join("\u{00B7}")
    }
}

/// Resolve a single unit reference (name or symbol) from the database.
pub fn resolve_single_unit(unit_ref: &str) -> ResolvedUnit {
    Spi::connect(|client| {
        let mut result = client
            .select(
                r#"
                SELECT factor, "offset", category_id,
                       dim_mass, dim_length, dim_time, dim_temp,
                       dim_amount, dim_current, dim_luminosity
                FROM pguom.unit
                WHERE name = $1 OR symbol = $1
                LIMIT 1
                "#,
                None,
                &[unit_ref.into()],
            )
            .unwrap();

        match result.next() {
            Some(row) => {
                let factor: f64 = row.get_by_name("factor").unwrap().unwrap();
                let offset: Option<f64> = row.get_by_name("offset").unwrap();
                let category_id: Option<i32> = row.get_by_name("category_id").unwrap();
                let dm: Option<i16> = row.get_by_name("dim_mass").unwrap();
                let dl: Option<i16> = row.get_by_name("dim_length").unwrap();
                let dt: Option<i16> = row.get_by_name("dim_time").unwrap();
                let dte: Option<i16> = row.get_by_name("dim_temp").unwrap();
                let da: Option<i16> = row.get_by_name("dim_amount").unwrap();
                let dc: Option<i16> = row.get_by_name("dim_current").unwrap();
                let dlu: Option<i16> = row.get_by_name("dim_luminosity").unwrap();
                ResolvedUnit {
                    factor,
                    offset: offset.unwrap_or(0.0),
                    dims: [
                        dm.unwrap_or(0),
                        dl.unwrap_or(0),
                        dt.unwrap_or(0),
                        dte.unwrap_or(0),
                        da.unwrap_or(0),
                        dc.unwrap_or(0),
                        dlu.unwrap_or(0),
                    ],
                    category_id,
                }
            }
            None => pgrx::error!("pg_uom: unit '{}' does not exist", unit_ref),
        }
    })
}

/// Resolve a unit expression (simple or compound) into combined factor + dimensions.
fn resolve_expression(expr: &str) -> CombinedUnit {
    let simple = is_simple_unit(expr);

    // Try as simple unit first (common case, single DB lookup)
    if simple {
        let resolved = resolve_single_unit(expr);
        return CombinedUnit {
            factor: resolved.factor,
            offset: resolved.offset,
            dims: resolved.dims,
            category_id: resolved.category_id,
            is_simple: true,
        };
    }

    // Parse compound expression
    let terms = parse_unit_expression(expr).unwrap_or_else(|e| {
        pgrx::error!("pg_uom: failed to parse unit expression '{}': {}", expr, e);
    });

    let mut combined_factor: f64 = 1.0;
    let mut combined_dims = [0i16; 7];

    for term in &terms {
        let resolved = resolve_single_unit(&term.unit_ref);

        // Compound units cannot use offset (temperature in compounds doesn't make physical sense)
        if resolved.offset != 0.0 {
            pgrx::error!(
                "pg_uom: unit '{}' has an offset and cannot be used in compound expressions",
                term.unit_ref
            );
        }

        combined_factor *= resolved.factor.powi(term.exponent as i32);
        for (i, dim) in combined_dims.iter_mut().enumerate() {
            *dim += resolved.dims[i] * term.exponent;
        }
    }

    CombinedUnit {
        factor: combined_factor,
        offset: 0.0,
        dims: combined_dims,
        category_id: None,
        is_simple: false,
    }
}
