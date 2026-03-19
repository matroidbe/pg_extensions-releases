use pgrx::prelude::*;

/// List all units, optionally filtered by category.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn list_units(
    category_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(symbol, String),
        name!(category, Option<String>),
        name!(factor, f64),
        name!(dimensions, String),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = match category_name {
            Some(cat) => client
                .select(
                    r#"
                    SELECT u.name, u.symbol, c.name AS category, u.factor,
                           u.dim_mass, u.dim_length, u.dim_time, u.dim_temp,
                           u.dim_amount, u.dim_current, u.dim_luminosity
                    FROM pguom.unit u
                    LEFT JOIN pguom.category c ON u.category_id = c.id
                    WHERE c.name = $1
                    ORDER BY u.factor
                    "#,
                    None,
                    &[cat.into()],
                )
                .unwrap(),
            None => client
                .select(
                    r#"
                    SELECT u.name, u.symbol, c.name AS category, u.factor,
                           u.dim_mass, u.dim_length, u.dim_time, u.dim_temp,
                           u.dim_amount, u.dim_current, u.dim_luminosity
                    FROM pguom.unit u
                    LEFT JOIN pguom.category c ON u.category_id = c.id
                    ORDER BY COALESCE(c.name, 'zzz'), u.factor
                    "#,
                    None,
                    &[],
                )
                .unwrap(),
        };

        for row in result {
            let dm: Option<i16> = row.get_by_name("dim_mass").unwrap();
            let dl: Option<i16> = row.get_by_name("dim_length").unwrap();
            let dt: Option<i16> = row.get_by_name("dim_time").unwrap();
            let dte: Option<i16> = row.get_by_name("dim_temp").unwrap();
            let da: Option<i16> = row.get_by_name("dim_amount").unwrap();
            let dc: Option<i16> = row.get_by_name("dim_current").unwrap();
            let dlu: Option<i16> = row.get_by_name("dim_luminosity").unwrap();
            let dims = [
                dm.unwrap_or(0),
                dl.unwrap_or(0),
                dt.unwrap_or(0),
                dte.unwrap_or(0),
                da.unwrap_or(0),
                dc.unwrap_or(0),
                dlu.unwrap_or(0),
            ];
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let symbol: String = row.get_by_name("symbol").unwrap().unwrap();
            let category: Option<String> = row.get_by_name("category").unwrap();
            let factor: f64 = row.get_by_name("factor").unwrap().unwrap();
            rows.push((
                name,
                symbol,
                category,
                factor,
                crate::convert::format_dimensions_pub(&dims),
            ));
        }
    });

    TableIterator::new(rows)
}

/// List all categories with unit counts.
#[pg_extern]
pub fn list_categories() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(description, Option<String>),
        name!(unit_count, i64),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT c.name, c.description, COUNT(u.id) AS unit_count
                FROM pguom.category c
                LEFT JOIN pguom.unit u ON u.category_id = c.id
                GROUP BY c.id, c.name, c.description
                ORDER BY c.name
                "#,
                None,
                &[],
            )
            .unwrap();

        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let description: Option<String> = row.get_by_name("description").unwrap();
            let unit_count: Option<i64> = row.get_by_name("unit_count").unwrap();
            rows.push((name, description, unit_count.unwrap_or(0)));
        }
    });

    TableIterator::new(rows)
}
