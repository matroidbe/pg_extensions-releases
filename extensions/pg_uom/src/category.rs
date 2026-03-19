use pgrx::prelude::*;

/// Create a new unit category.
#[pg_extern]
pub fn create_category(name: &str, description: default!(Option<&str>, "NULL")) -> i32 {
    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pguom.category (name, description)
        VALUES ($1, $2)
        RETURNING id
        "#,
        &[name.into(), description.into()],
    )
    .expect("failed to create category")
    .expect("no id returned from category insert")
}

/// Drop a category. Units in this category will have their category_id set to NULL.
#[pg_extern]
pub fn drop_category(name: &str) {
    let deleted = Spi::get_one_with_args::<String>(
        "DELETE FROM pguom.category WHERE name = $1 RETURNING name",
        &[name.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("pg_uom: category '{}' does not exist", name),
    }
}
