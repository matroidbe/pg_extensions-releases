use pgrx::prelude::*;

/// Create a new unit with explicit dimension vector.
#[allow(clippy::too_many_arguments)]
#[pg_extern]
pub fn create_unit(
    name: &str,
    symbol: &str,
    factor: f64,
    dim_mass: default!(Option<i32>, "0"),
    dim_length: default!(Option<i32>, "0"),
    dim_time: default!(Option<i32>, "0"),
    dim_temp: default!(Option<i32>, "0"),
    dim_amount: default!(Option<i32>, "0"),
    dim_current: default!(Option<i32>, "0"),
    dim_luminosity: default!(Option<i32>, "0"),
    category_name: default!(Option<&str>, "NULL"),
    offset: default!(Option<f64>, "0"),
    description: default!(Option<&str>, "NULL"),
) -> i32 {
    if factor <= 0.0 {
        pgrx::error!("pg_uom: factor must be positive, got {}", factor);
    }

    let category_id: Option<i32> = category_name.map(get_category_id);

    // Cast i32 → i16 for SMALLINT columns
    let dm = dim_mass.unwrap_or(0) as i16;
    let dl = dim_length.unwrap_or(0) as i16;
    let dt = dim_time.unwrap_or(0) as i16;
    let dte = dim_temp.unwrap_or(0) as i16;
    let da = dim_amount.unwrap_or(0) as i16;
    let dc = dim_current.unwrap_or(0) as i16;
    let dlu = dim_luminosity.unwrap_or(0) as i16;

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pguom.unit (
            category_id, name, symbol, factor, "offset",
            dim_mass, dim_length, dim_time, dim_temp,
            dim_amount, dim_current, dim_luminosity,
            description
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING id
        "#,
        &[
            category_id.into(),
            name.into(),
            symbol.into(),
            factor.into(),
            offset.unwrap_or(0.0).into(),
            dm.into(),
            dl.into(),
            dt.into(),
            dte.into(),
            da.into(),
            dc.into(),
            dlu.into(),
            description.into(),
        ],
    )
    .expect("failed to create unit")
    .expect("no id returned from unit insert")
}

/// Drop a unit by name.
#[pg_extern]
pub fn drop_unit(unit_name: &str) {
    let deleted = Spi::get_one_with_args::<String>(
        "DELETE FROM pguom.unit WHERE name = $1 RETURNING name",
        &[unit_name.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("pg_uom: unit '{}' does not exist", unit_name),
    }
}

/// Get category ID by name.
fn get_category_id(name: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT id FROM pguom.category WHERE name = $1",
        &[name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| {
        pgrx::error!("pg_uom: category '{}' does not exist", name);
    })
}
