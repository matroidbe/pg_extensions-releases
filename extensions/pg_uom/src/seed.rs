use pgrx::prelude::*;

// uom crate imports for verified conversion factors
// Note: avoid glob import of uom::si::f64 to prevent `Time` clash with pgrx
use uom::si::f64::{
    Area, ElectricCurrent, ElectricPotential, ElectricalResistance, Energy, Force, Frequency,
    Length, Mass, Power, Pressure, ThermodynamicTemperature, Velocity, Volume,
};
// Alias uom's Time to avoid clash with pgrx::Time
use uom::si::f64::Time as UomTime;

/// Seed all SI base units and common derived units using verified factors from the `uom` crate.
/// Returns the number of units seeded.
#[pg_extern]
pub fn seed_si() -> i32 {
    let mut count: i32 = 0;

    // Create categories
    for (name, desc) in &[
        ("mass", "Weight and mass units"),
        ("length", "Distance and length units"),
        ("volume", "Volume and capacity units"),
        ("time", "Time duration units"),
        ("area", "Area and surface units"),
        ("temperature", "Temperature units"),
        ("force", "Force units"),
        ("energy", "Energy and work units"),
        ("power", "Power units"),
        ("pressure", "Pressure units"),
        ("frequency", "Frequency units"),
        ("velocity", "Speed and velocity units"),
        ("acceleration", "Acceleration units"),
        ("electric_current", "Electric current units"),
        ("electric_potential", "Electric potential/voltage units"),
        ("electric_resistance", "Electric resistance units"),
        ("count", "Countable/discrete units"),
    ] {
        Spi::run_with_args(
            "INSERT INTO pguom.category (name, description) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING",
            &[(*name).into(), (*desc).into()],
        )
        .expect("failed to create category");
    }

    // Helper to insert a unit
    fn insert_unit(
        cat: &str,
        name: &str,
        symbol: &str,
        factor: f64,
        offset: f64,
        dims: [i16; 7],
        desc: &str,
    ) {
        Spi::run_with_args(
            r#"
            INSERT INTO pguom.unit (category_id, name, symbol, factor, "offset",
                dim_mass, dim_length, dim_time, dim_temp, dim_amount, dim_current, dim_luminosity,
                description)
            VALUES (
                (SELECT id FROM pguom.category WHERE name = $1),
                $2, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12,
                $13
            )
            ON CONFLICT (name) DO NOTHING
            "#,
            &[
                cat.into(),
                name.into(),
                symbol.into(),
                factor.into(),
                offset.into(),
                dims[0].into(),
                dims[1].into(),
                dims[2].into(),
                dims[3].into(),
                dims[4].into(),
                dims[5].into(),
                dims[6].into(),
                desc.into(),
            ],
        )
        .expect("failed to insert unit");
    }

    // Dimension vectors: [mass, length, time, temp, amount, current, luminosity]
    let mass = [1, 0, 0, 0, 0, 0, 0_i16];
    let length = [0, 1, 0, 0, 0, 0, 0_i16];
    let volume = [0, 3, 0, 0, 0, 0, 0_i16]; // m³ = length³
    let time = [0, 0, 1, 0, 0, 0, 0_i16];
    let area = [0, 2, 0, 0, 0, 0, 0_i16]; // m² = length²
    let temp = [0, 0, 0, 1, 0, 0, 0_i16];
    let force = [1, 1, -2, 0, 0, 0, 0_i16]; // kg·m/s²
    let energy = [1, 2, -2, 0, 0, 0, 0_i16]; // kg·m²/s²
    let power = [1, 2, -3, 0, 0, 0, 0_i16]; // kg·m²/s³
    let pressure = [1, -1, -2, 0, 0, 0, 0_i16]; // kg/(m·s²)
    let frequency = [0, 0, -1, 0, 0, 0, 0_i16]; // 1/s
    let velocity = [0, 1, -1, 0, 0, 0, 0_i16]; // m/s
    let acceleration = [0, 1, -2, 0, 0, 0, 0_i16]; // m/s²
    let current = [0, 0, 0, 0, 0, 1, 0_i16]; // A
    let voltage = [1, 2, -3, 0, 0, -1, 0_i16]; // kg·m²/(s³·A)
    let resistance = [1, 2, -3, 0, 0, -2, 0_i16]; // kg·m²/(s³·A²)
    let dimensionless = [0, 0, 0, 0, 0, 0, 0_i16];

    // =========================================================================
    // Mass units — factors from uom crate
    // =========================================================================
    let kg_factor = 1.0; // SI base
    insert_unit(
        "mass",
        "kilogram",
        "kg",
        kg_factor,
        0.0,
        mass,
        "SI base unit of mass",
    );

    let g_factor = Mass::new::<uom::si::mass::kilogram>(1.0).get::<uom::si::mass::gram>();
    insert_unit(
        "mass",
        "gram",
        "g",
        1.0 / g_factor,
        0.0,
        mass,
        "1/1000 of a kilogram",
    );

    let mg_factor = Mass::new::<uom::si::mass::kilogram>(1.0).get::<uom::si::mass::milligram>();
    insert_unit(
        "mass",
        "milligram",
        "mg",
        1.0 / mg_factor,
        0.0,
        mass,
        "1/1000 of a gram",
    );

    let t_factor = Mass::new::<uom::si::mass::megagram>(1.0).get::<uom::si::mass::kilogram>();
    insert_unit(
        "mass",
        "tonne",
        "t",
        t_factor,
        0.0,
        mass,
        "Metric ton = 1000 kg",
    );

    let lb_factor = Mass::new::<uom::si::mass::pound>(1.0).get::<uom::si::mass::kilogram>();
    insert_unit(
        "mass",
        "pound",
        "lb",
        lb_factor,
        0.0,
        mass,
        "Avoirdupois pound",
    );

    let oz_factor = Mass::new::<uom::si::mass::ounce>(1.0).get::<uom::si::mass::kilogram>();
    insert_unit(
        "mass",
        "ounce",
        "oz",
        oz_factor,
        0.0,
        mass,
        "Avoirdupois ounce",
    );

    count += 6;

    // =========================================================================
    // Length units
    // =========================================================================
    insert_unit(
        "length",
        "meter",
        "m",
        1.0,
        0.0,
        length,
        "SI base unit of length",
    );

    let mm = Length::new::<uom::si::length::millimeter>(1.0).get::<uom::si::length::meter>();
    insert_unit(
        "length",
        "millimeter",
        "mm",
        mm,
        0.0,
        length,
        "1/1000 of a meter",
    );

    let cm = Length::new::<uom::si::length::centimeter>(1.0).get::<uom::si::length::meter>();
    insert_unit(
        "length",
        "centimeter",
        "cm",
        cm,
        0.0,
        length,
        "1/100 of a meter",
    );

    let km = Length::new::<uom::si::length::kilometer>(1.0).get::<uom::si::length::meter>();
    insert_unit("length", "kilometer", "km", km, 0.0, length, "1000 meters");

    let inch = Length::new::<uom::si::length::inch>(1.0).get::<uom::si::length::meter>();
    insert_unit("length", "inch", "in", inch, 0.0, length, "Imperial inch");

    let ft = Length::new::<uom::si::length::foot>(1.0).get::<uom::si::length::meter>();
    insert_unit("length", "foot", "ft", ft, 0.0, length, "Imperial foot");

    let yd = Length::new::<uom::si::length::yard>(1.0).get::<uom::si::length::meter>();
    insert_unit("length", "yard", "yd", yd, 0.0, length, "Imperial yard");

    let mi = Length::new::<uom::si::length::mile>(1.0).get::<uom::si::length::meter>();
    insert_unit("length", "mile", "mi", mi, 0.0, length, "Imperial mile");

    count += 8;

    // =========================================================================
    // Volume units (dimension = length³, so dim_length = 3)
    // =========================================================================
    let l_factor = Volume::new::<uom::si::volume::liter>(1.0).get::<uom::si::volume::cubic_meter>();
    insert_unit(
        "volume",
        "liter",
        "L",
        l_factor,
        0.0,
        volume,
        "1/1000 of a cubic meter",
    );

    let ml = Volume::new::<uom::si::volume::milliliter>(1.0).get::<uom::si::volume::cubic_meter>();
    insert_unit(
        "volume",
        "milliliter",
        "mL",
        ml,
        0.0,
        volume,
        "1/1000 of a liter",
    );

    insert_unit(
        "volume",
        "cubic_meter",
        "m3",
        1.0,
        0.0,
        volume,
        "SI derived unit of volume",
    );

    let gal = Volume::new::<uom::si::volume::gallon>(1.0).get::<uom::si::volume::cubic_meter>();
    insert_unit(
        "volume",
        "gallon_us",
        "gal",
        gal,
        0.0,
        volume,
        "US liquid gallon",
    );

    count += 4;

    // =========================================================================
    // Time units
    // =========================================================================
    insert_unit(
        "time",
        "second",
        "s",
        1.0,
        0.0,
        time,
        "SI base unit of time",
    );

    let min_f = UomTime::new::<uom::si::time::minute>(1.0).get::<uom::si::time::second>();
    insert_unit("time", "minute", "min", min_f, 0.0, time, "60 seconds");

    let h_f = UomTime::new::<uom::si::time::hour>(1.0).get::<uom::si::time::second>();
    insert_unit("time", "hour", "h", h_f, 0.0, time, "3600 seconds");

    let d_f = UomTime::new::<uom::si::time::day>(1.0).get::<uom::si::time::second>();
    insert_unit("time", "day", "d", d_f, 0.0, time, "86400 seconds");

    count += 4;

    // =========================================================================
    // Area units (dimension = length², so dim_length = 2)
    // =========================================================================
    insert_unit(
        "area",
        "square_meter",
        "m2",
        1.0,
        0.0,
        area,
        "SI derived unit of area",
    );

    let sqkm =
        Area::new::<uom::si::area::square_kilometer>(1.0).get::<uom::si::area::square_meter>();
    insert_unit(
        "area",
        "square_kilometer",
        "km2",
        sqkm,
        0.0,
        area,
        "1,000,000 square meters",
    );

    let ha = Area::new::<uom::si::area::hectare>(1.0).get::<uom::si::area::square_meter>();
    insert_unit(
        "area",
        "hectare",
        "ha",
        ha,
        0.0,
        area,
        "10,000 square meters",
    );

    let ac = Area::new::<uom::si::area::acre>(1.0).get::<uom::si::area::square_meter>();
    insert_unit("area", "acre", "ac", ac, 0.0, area, "Imperial acre");

    let sqft = Area::new::<uom::si::area::square_foot>(1.0).get::<uom::si::area::square_meter>();
    insert_unit(
        "area",
        "square_foot",
        "ft2",
        sqft,
        0.0,
        area,
        "Imperial square foot",
    );

    count += 5;

    // =========================================================================
    // Temperature units (affine conversion via offset)
    // kelvin is SI base: factor=1, offset=0
    // celsius: K = C * 1.0 + 273.15 → factor=1.0, offset=273.15
    // fahrenheit: K = F * 5/9 + 255.3722... → factor=5/9, offset=255.3722...
    // =========================================================================
    insert_unit(
        "temperature",
        "kelvin",
        "K",
        1.0,
        0.0,
        temp,
        "SI base unit of temperature",
    );

    // Verify via uom: 0°C = 273.15 K
    let celsius_offset =
        ThermodynamicTemperature::new::<uom::si::thermodynamic_temperature::degree_celsius>(0.0)
            .get::<uom::si::thermodynamic_temperature::kelvin>();
    insert_unit(
        "temperature",
        "celsius",
        "degC",
        1.0,
        celsius_offset,
        temp,
        "Degrees Celsius",
    );

    // Verify via uom: 0°F in K
    let f_zero_k =
        ThermodynamicTemperature::new::<uom::si::thermodynamic_temperature::degree_fahrenheit>(0.0)
            .get::<uom::si::thermodynamic_temperature::kelvin>();
    // 1°F change = 5/9 K change
    let f_one_k =
        ThermodynamicTemperature::new::<uom::si::thermodynamic_temperature::degree_fahrenheit>(1.0)
            .get::<uom::si::thermodynamic_temperature::kelvin>();
    let f_factor = f_one_k - f_zero_k; // 5/9
    insert_unit(
        "temperature",
        "fahrenheit",
        "degF",
        f_factor,
        f_zero_k,
        temp,
        "Degrees Fahrenheit",
    );

    count += 3;

    // =========================================================================
    // Force units (kg·m/s²)
    // =========================================================================
    insert_unit(
        "force",
        "newton",
        "N",
        1.0,
        0.0,
        force,
        "SI derived unit of force",
    );

    let lbf = Force::new::<uom::si::force::pound_force>(1.0).get::<uom::si::force::newton>();
    insert_unit(
        "force",
        "pound_force",
        "lbf",
        lbf,
        0.0,
        force,
        "Pound-force",
    );

    count += 2;

    // =========================================================================
    // Energy units (kg·m²/s²)
    // =========================================================================
    insert_unit(
        "energy",
        "joule",
        "J",
        1.0,
        0.0,
        energy,
        "SI derived unit of energy",
    );

    let kwh = Energy::new::<uom::si::energy::kilowatt_hour>(1.0).get::<uom::si::energy::joule>();
    insert_unit(
        "energy",
        "kilowatt_hour",
        "kWh",
        kwh,
        0.0,
        energy,
        "3,600,000 joules",
    );

    let cal = Energy::new::<uom::si::energy::calorie>(1.0).get::<uom::si::energy::joule>();
    insert_unit(
        "energy",
        "calorie",
        "cal",
        cal,
        0.0,
        energy,
        "Thermochemical calorie",
    );

    let btu = Energy::new::<uom::si::energy::btu>(1.0).get::<uom::si::energy::joule>();
    insert_unit(
        "energy",
        "btu",
        "BTU",
        btu,
        0.0,
        energy,
        "British thermal unit",
    );

    count += 4;

    // =========================================================================
    // Power units (kg·m²/s³)
    // =========================================================================
    insert_unit(
        "power",
        "watt",
        "W",
        1.0,
        0.0,
        power,
        "SI derived unit of power",
    );

    let kw = Power::new::<uom::si::power::kilowatt>(1.0).get::<uom::si::power::watt>();
    insert_unit("power", "kilowatt", "kW", kw, 0.0, power, "1000 watts");

    let hp = Power::new::<uom::si::power::horsepower>(1.0).get::<uom::si::power::watt>();
    insert_unit(
        "power",
        "horsepower",
        "hp",
        hp,
        0.0,
        power,
        "Mechanical horsepower",
    );

    count += 3;

    // =========================================================================
    // Pressure units (kg/(m·s²))
    // =========================================================================
    insert_unit(
        "pressure",
        "pascal",
        "Pa",
        1.0,
        0.0,
        pressure,
        "SI derived unit of pressure",
    );

    let bar_f = Pressure::new::<uom::si::pressure::bar>(1.0).get::<uom::si::pressure::pascal>();
    insert_unit(
        "pressure",
        "bar",
        "bar",
        bar_f,
        0.0,
        pressure,
        "100,000 pascals",
    );

    let psi_f = Pressure::new::<uom::si::pressure::pound_force_per_square_inch>(1.0)
        .get::<uom::si::pressure::pascal>();
    insert_unit(
        "pressure",
        "psi",
        "psi",
        psi_f,
        0.0,
        pressure,
        "Pounds per square inch",
    );

    let atm =
        Pressure::new::<uom::si::pressure::atmosphere>(1.0).get::<uom::si::pressure::pascal>();
    insert_unit(
        "pressure",
        "atmosphere",
        "atm",
        atm,
        0.0,
        pressure,
        "Standard atmosphere",
    );

    count += 4;

    // =========================================================================
    // Frequency units (1/s)
    // =========================================================================
    insert_unit(
        "frequency",
        "hertz",
        "Hz",
        1.0,
        0.0,
        frequency,
        "SI derived unit of frequency",
    );

    let khz =
        Frequency::new::<uom::si::frequency::kilohertz>(1.0).get::<uom::si::frequency::hertz>();
    insert_unit(
        "frequency",
        "kilohertz",
        "kHz",
        khz,
        0.0,
        frequency,
        "1000 hertz",
    );

    let mhz =
        Frequency::new::<uom::si::frequency::megahertz>(1.0).get::<uom::si::frequency::hertz>();
    insert_unit(
        "frequency",
        "megahertz",
        "MHz",
        mhz,
        0.0,
        frequency,
        "1,000,000 hertz",
    );

    count += 3;

    // =========================================================================
    // Velocity units (m/s)
    // =========================================================================
    insert_unit(
        "velocity",
        "meter_per_second",
        "m_s",
        1.0,
        0.0,
        velocity,
        "SI derived unit of velocity",
    );

    let kmh = Velocity::new::<uom::si::velocity::kilometer_per_hour>(1.0)
        .get::<uom::si::velocity::meter_per_second>();
    insert_unit(
        "velocity",
        "kilometer_per_hour",
        "km_h",
        kmh,
        0.0,
        velocity,
        "km/h",
    );

    let mph = Velocity::new::<uom::si::velocity::mile_per_hour>(1.0)
        .get::<uom::si::velocity::meter_per_second>();
    insert_unit(
        "velocity",
        "mile_per_hour",
        "mph",
        mph,
        0.0,
        velocity,
        "Miles per hour",
    );

    count += 3;

    // =========================================================================
    // Acceleration units (m/s²)
    // =========================================================================
    insert_unit(
        "acceleration",
        "meter_per_second_squared",
        "m_s2",
        1.0,
        0.0,
        acceleration,
        "SI derived unit of acceleration",
    );
    count += 1;

    // =========================================================================
    // Electric current units (A)
    // =========================================================================
    insert_unit(
        "electric_current",
        "ampere",
        "A",
        1.0,
        0.0,
        current,
        "SI base unit of electric current",
    );

    let ma = ElectricCurrent::new::<uom::si::electric_current::milliampere>(1.0)
        .get::<uom::si::electric_current::ampere>();
    insert_unit(
        "electric_current",
        "milliampere",
        "mA",
        ma,
        0.0,
        current,
        "1/1000 of an ampere",
    );

    count += 2;

    // =========================================================================
    // Electric potential units (V = kg·m²/(s³·A))
    // =========================================================================
    insert_unit(
        "electric_potential",
        "volt",
        "V",
        1.0,
        0.0,
        voltage,
        "SI derived unit of voltage",
    );

    let kv = ElectricPotential::new::<uom::si::electric_potential::kilovolt>(1.0)
        .get::<uom::si::electric_potential::volt>();
    insert_unit(
        "electric_potential",
        "kilovolt",
        "kV",
        kv,
        0.0,
        voltage,
        "1000 volts",
    );

    let mv = ElectricPotential::new::<uom::si::electric_potential::millivolt>(1.0)
        .get::<uom::si::electric_potential::volt>();
    insert_unit(
        "electric_potential",
        "millivolt",
        "mV",
        mv,
        0.0,
        voltage,
        "1/1000 of a volt",
    );

    count += 3;

    // =========================================================================
    // Electric resistance units (Ω = kg·m²/(s³·A²))
    // =========================================================================
    insert_unit(
        "electric_resistance",
        "ohm",
        "ohm",
        1.0,
        0.0,
        resistance,
        "SI derived unit of resistance",
    );

    let kohm = ElectricalResistance::new::<uom::si::electrical_resistance::kiloohm>(1.0)
        .get::<uom::si::electrical_resistance::ohm>();
    insert_unit(
        "electric_resistance",
        "kiloohm",
        "kohm",
        kohm,
        0.0,
        resistance,
        "1000 ohms",
    );

    count += 2;

    // =========================================================================
    // Dimensionless / count units
    // =========================================================================
    insert_unit(
        "count",
        "each",
        "ea",
        1.0,
        0.0,
        dimensionless,
        "Single unit / piece",
    );
    insert_unit("count", "pair", "pr", 2.0, 0.0, dimensionless, "2 units");
    insert_unit("count", "dozen", "dz", 12.0, 0.0, dimensionless, "12 units");
    insert_unit(
        "count",
        "gross",
        "gro",
        144.0,
        0.0,
        dimensionless,
        "12 dozen = 144 units",
    );
    insert_unit(
        "count",
        "hundred",
        "hd",
        100.0,
        0.0,
        dimensionless,
        "100 units",
    );
    insert_unit(
        "count",
        "thousand",
        "th",
        1000.0,
        0.0,
        dimensionless,
        "1000 units",
    );

    count += 6;

    count
}
