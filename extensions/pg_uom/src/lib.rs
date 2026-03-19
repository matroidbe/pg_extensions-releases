mod category;
mod convert;
pub mod parser;
mod queries;
mod schema;
mod seed;
mod unit;

pub use category::*;
pub use convert::*;
pub use queries::*;
pub use seed::*;
pub use unit::*;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings
// =============================================================================

pub static PG_UOM_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_uom.enabled",
        c"Enable pg_uom unit conversion functions",
        c"When false, convert() returns the input value unchanged.",
        &PG_UOM_ENABLED,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::log!("pg_uom: initialized");
}

// =============================================================================
// Integration Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn set_search_path() {
        Spi::run("SET search_path TO pguom, public").unwrap();
    }

    fn seed() {
        set_search_path();
        Spi::run("SELECT pguom.seed_si()").unwrap();
    }

    // =========================================================================
    // Phase 1: Schema validation
    // =========================================================================

    #[pg_test]
    fn test_category_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pguom' AND table_name = 'category')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_unit_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pguom' AND table_name = 'unit')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_unit_table_has_dimension_columns() {
        set_search_path();
        let cols = Spi::get_one::<i64>(
            r#"SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'pguom' AND table_name = 'unit'
               AND column_name::text LIKE 'dim_%'"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(cols, 7); // mass, length, time, temp, amount, current, luminosity
    }

    // =========================================================================
    // Phase 2: Seed Data
    // =========================================================================

    #[pg_test]
    fn test_seed_si_creates_categories() {
        seed();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pguom.category")
            .unwrap()
            .unwrap();
        assert!(count >= 17); // 17 categories
    }

    #[pg_test]
    fn test_seed_si_creates_mass_units() {
        seed();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pguom.unit u JOIN pguom.category c ON u.category_id = c.id WHERE c.name = 'mass'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 6); // kg, g, mg, t, lb, oz
    }

    #[pg_test]
    fn test_seed_si_creates_length_units() {
        seed();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pguom.unit u JOIN pguom.category c ON u.category_id = c.id WHERE c.name = 'length'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 8); // m, mm, cm, km, in, ft, yd, mi
    }

    #[pg_test]
    fn test_seed_si_returns_count() {
        set_search_path();
        let count = Spi::get_one::<i32>("SELECT pguom.seed_si()")
            .unwrap()
            .unwrap();
        assert!(count >= 55); // should be 55+ units
    }

    #[pg_test]
    fn test_seed_si_idempotent() {
        seed();
        // Second call should not fail (ON CONFLICT DO NOTHING)
        Spi::run("SELECT pguom.seed_si()").unwrap();
    }

    // =========================================================================
    // Phase 2: Core Conversion — Simple
    // =========================================================================

    #[pg_test]
    fn test_convert_kg_to_lb() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'kg', 'lb')")
            .unwrap()
            .unwrap();
        // 1 kg ≈ 2.20462 lb
        assert!((result - 2.20462).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_lb_to_kg() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'lb', 'kg')")
            .unwrap()
            .unwrap();
        // 1 lb ≈ 0.45359 kg
        assert!((result - 0.45359).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_meter_to_foot() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'm', 'ft')")
            .unwrap()
            .unwrap();
        // 1 m ≈ 3.28084 ft
        assert!((result - 3.28084).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_mile_to_km() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'mi', 'km')")
            .unwrap()
            .unwrap();
        // 1 mi ≈ 1.60934 km
        assert!((result - 1.60934).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_hour_to_minute() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'h', 'min')")
            .unwrap()
            .unwrap();
        assert!((result - 60.0).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_dozen_to_each() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'dz', 'ea')")
            .unwrap()
            .unwrap();
        assert!((result - 12.0).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_by_symbol() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1000.0, 'g', 'kg')")
            .unwrap()
            .unwrap();
        assert!((result - 1.0).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_same_unit() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(42.0, 'kg', 'kg')")
            .unwrap()
            .unwrap();
        assert!((result - 42.0).abs() < 0.0001);
    }

    #[pg_test]
    fn test_convert_zero_value() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(0.0, 'kg', 'lb')")
            .unwrap()
            .unwrap();
        assert!((result - 0.0).abs() < 0.0001);
    }

    // =========================================================================
    // Phase 2: Temperature conversion (affine)
    // =========================================================================

    #[pg_test]
    fn test_convert_celsius_to_fahrenheit() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(100.0, 'degC', 'degF')")
            .unwrap()
            .unwrap();
        // 100°C = 212°F
        assert!((result - 212.0).abs() < 0.1);
    }

    #[pg_test]
    fn test_convert_fahrenheit_to_celsius() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(32.0, 'degF', 'degC')")
            .unwrap()
            .unwrap();
        // 32°F = 0°C
        assert!((result - 0.0).abs() < 0.1);
    }

    #[pg_test]
    fn test_convert_celsius_to_kelvin() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(0.0, 'degC', 'K')")
            .unwrap()
            .unwrap();
        // 0°C = 273.15 K
        assert!((result - 273.15).abs() < 0.1);
    }

    // =========================================================================
    // Phase 2: Error cases
    // =========================================================================

    #[pg_test]
    #[should_panic(expected = "incompatible dimensions")]
    fn test_convert_cross_dimension_error() {
        seed();
        // kg is mass, m is length — different dimensions
        Spi::run("SELECT pguom.convert(1.0, 'kg', 'm')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_convert_nonexistent_unit_error() {
        seed();
        Spi::run("SELECT pguom.convert(1.0, 'nonexistent', 'kg')").unwrap();
    }

    // =========================================================================
    // Phase 2: Compound unit conversion
    // =========================================================================

    #[pg_test]
    fn test_convert_compound_m_per_s_to_km_per_h() {
        seed();
        // 1 m/s = 3.6 km/h
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'm/s', 'km/h')")
            .unwrap()
            .unwrap();
        assert!((result - 3.6).abs() < 0.01);
    }

    #[pg_test]
    fn test_convert_compound_kg_per_m3() {
        seed();
        // 1000 kg/m³ = 1 g/mL = 1 g/cm³ ... but we don't have cm³
        // Let's test: 1 kg/m³ in lb/ft³
        // 1 kg/m³ ≈ 0.06243 lb/ft³
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'kg/m³', 'lb/ft³')")
            .unwrap()
            .unwrap();
        assert!((result - 0.06243).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_compound_kw_to_w() {
        seed();
        // Using named compound: kW → W
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'kW', 'W')")
            .unwrap()
            .unwrap();
        assert!((result - 1000.0).abs() < 0.001);
    }

    #[pg_test]
    fn test_convert_compound_bar_to_psi() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'bar', 'psi')")
            .unwrap()
            .unwrap();
        // 1 bar ≈ 14.5038 psi
        assert!((result - 14.5038).abs() < 0.01);
    }

    #[pg_test]
    fn test_conversion_factor_simple() {
        seed();
        let factor = Spi::get_one::<f64>("SELECT pguom.conversion_factor('km', 'm')")
            .unwrap()
            .unwrap();
        assert!((factor - 1000.0).abs() < 0.001);
    }

    // =========================================================================
    // Phase 3: CRUD
    // =========================================================================

    #[pg_test]
    fn test_create_category_returns_id() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pguom.create_category('test_cat', 'A test category')")
            .unwrap()
            .unwrap();
        assert!(id > 0);
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_create_category_duplicate_error() {
        set_search_path();
        Spi::run("SELECT pguom.create_category('dup_cat')").unwrap();
        Spi::run("SELECT pguom.create_category('dup_cat')").unwrap();
    }

    #[pg_test]
    fn test_drop_category() {
        set_search_path();
        Spi::run("SELECT pguom.create_category('to_drop')").unwrap();
        Spi::run("SELECT pguom.drop_category('to_drop')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pguom.category WHERE name = 'to_drop')",
        )
        .unwrap()
        .unwrap();
        assert!(!exists);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_drop_category_nonexistent_error() {
        set_search_path();
        Spi::run("SELECT pguom.drop_category('no_such_cat')").unwrap();
    }

    #[pg_test]
    fn test_create_unit_returns_id() {
        seed();
        let id = Spi::get_one::<i32>(
            "SELECT pguom.create_unit('custom_unit', 'cu', 2.5, 1, 0, 0, 0, 0, 0, 0, 'mass', 0, 'A custom mass unit')",
        )
        .unwrap()
        .unwrap();
        assert!(id > 0);
    }

    #[pg_test]
    #[should_panic(expected = "factor must be positive")]
    fn test_create_unit_negative_factor_error() {
        set_search_path();
        Spi::run("SELECT pguom.create_unit('bad_unit', 'bad', -1.0)").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_create_unit_duplicate_name_error() {
        seed();
        Spi::run("SELECT pguom.create_unit('kilogram', 'dup_sym', 1.0)").unwrap();
    }

    #[pg_test]
    fn test_create_custom_unit_and_convert() {
        seed();
        // Create a custom mass unit: 1 stone = 6.35029 kg
        Spi::run("SELECT pguom.create_unit('stone', 'st', 6.35029, 1, 0, 0, 0, 0, 0, 0, 'mass')")
            .unwrap();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1.0, 'st', 'kg')")
            .unwrap()
            .unwrap();
        assert!((result - 6.35029).abs() < 0.001);
    }

    #[pg_test]
    fn test_drop_unit() {
        seed();
        Spi::run("SELECT pguom.create_unit('temp_unit', 'tmp', 1.0, 1, 0, 0, 0, 0, 0, 0, 'mass')")
            .unwrap();
        Spi::run("SELECT pguom.drop_unit('temp_unit')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pguom.unit WHERE name = 'temp_unit')",
        )
        .unwrap()
        .unwrap();
        assert!(!exists);
    }

    // =========================================================================
    // Phase 3: Queries
    // =========================================================================

    #[pg_test]
    fn test_list_units_all() {
        seed();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pguom.list_units()")
            .unwrap()
            .unwrap();
        assert!(count >= 55);
    }

    #[pg_test]
    fn test_list_units_filtered() {
        seed();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pguom.list_units('mass')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 6);
    }

    #[pg_test]
    fn test_list_categories() {
        seed();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pguom.list_categories()")
            .unwrap()
            .unwrap();
        assert!(count >= 17);
    }

    #[pg_test]
    fn test_compatible_units() {
        seed();
        // All mass units should be compatible with kg
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pguom.compatible_units('kg')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 6); // kg, g, mg, t, lb, oz
    }

    #[pg_test]
    fn test_unit_dimensions_simple() {
        seed();
        let dims = Spi::get_one::<String>("SELECT pguom.unit_dimensions('kg')")
            .unwrap()
            .unwrap();
        assert_eq!(dims, "kg");
    }

    #[pg_test]
    fn test_unit_dimensions_compound() {
        seed();
        let dims = Spi::get_one::<String>("SELECT pguom.unit_dimensions('N')")
            .unwrap()
            .unwrap();
        // Newton = kg·m·s⁻²
        assert!(dims.contains("kg"));
        assert!(dims.contains("m"));
        assert!(dims.contains("s"));
    }

    // =========================================================================
    // Phase 4: Edge cases
    // =========================================================================

    #[pg_test]
    fn test_convert_large_value() {
        seed();
        let result = Spi::get_one::<f64>("SELECT pguom.convert(1e15, 'mg', 'kg')")
            .unwrap()
            .unwrap();
        // 1e15 mg = 1e9 kg
        assert!((result - 1e9).abs() < 1.0);
    }

    #[pg_test]
    #[should_panic(expected = "offset")]
    fn test_compound_with_temperature_error() {
        seed();
        // Temperature can't be used in compound expressions
        Spi::run("SELECT pguom.convert(1.0, 'degC/s', 'K/s')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "dimensionless")]
    fn test_convert_cross_dimensionless_category() {
        seed();
        // Create a 'percentage' category with a 'percent' unit (dimensionless)
        Spi::run("SELECT pguom.create_category('percentage', 'Percentage units')").unwrap();
        Spi::run(
            "SELECT pguom.create_unit('percent', 'pct', 0.01, 0, 0, 0, 0, 0, 0, 0, 'percentage')",
        )
        .unwrap();
        // Should not be able to convert dozen (count) to percent (percentage)
        Spi::run("SELECT pguom.convert(1.0, 'dz', 'pct')").unwrap();
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
