use pgrx::prelude::*;

/// Seed common ISO 4217 currencies. Idempotent (ON CONFLICT DO NOTHING).
/// Returns the number of currencies after seeding.
#[pg_extern]
pub fn seed_iso() -> i32 {
    // (code, name, symbol, decimal_places)
    let currencies: &[(&str, &str, &str, i16)] = &[
        // Major currencies
        ("USD", "US Dollar", "$", 2),
        ("EUR", "Euro", "€", 2),
        ("GBP", "British Pound", "£", 2),
        ("JPY", "Japanese Yen", "¥", 0),
        ("CHF", "Swiss Franc", "CHF", 2),
        ("CAD", "Canadian Dollar", "CA$", 2),
        ("AUD", "Australian Dollar", "A$", 2),
        ("NZD", "New Zealand Dollar", "NZ$", 2),
        // Asia
        ("CNY", "Chinese Yuan", "¥", 2),
        ("HKD", "Hong Kong Dollar", "HK$", 2),
        ("SGD", "Singapore Dollar", "S$", 2),
        ("TWD", "Taiwan Dollar", "NT$", 2),
        ("KRW", "South Korean Won", "₩", 0),
        ("INR", "Indian Rupee", "₹", 2),
        ("THB", "Thai Baht", "฿", 2),
        ("IDR", "Indonesian Rupiah", "Rp", 2),
        ("PHP", "Philippine Peso", "₱", 2),
        ("MYR", "Malaysian Ringgit", "RM", 2),
        ("VND", "Vietnamese Dong", "₫", 0),
        // Middle East
        ("AED", "UAE Dirham", "د.إ", 2),
        ("SAR", "Saudi Riyal", "﷼", 2),
        ("KWD", "Kuwaiti Dinar", "د.ك", 3),
        ("BHD", "Bahraini Dinar", ".د.ب", 3),
        ("OMR", "Omani Rial", "﷼", 3),
        ("QAR", "Qatari Riyal", "﷼", 2),
        ("ILS", "Israeli Shekel", "₪", 2),
        ("TRY", "Turkish Lira", "₺", 2),
        ("EGP", "Egyptian Pound", "£", 2),
        // Europe (non-EUR)
        ("NOK", "Norwegian Krone", "kr", 2),
        ("SEK", "Swedish Krona", "kr", 2),
        ("DKK", "Danish Krone", "kr", 2),
        ("PLN", "Polish Zloty", "zł", 2),
        ("CZK", "Czech Koruna", "Kč", 2),
        ("HUF", "Hungarian Forint", "Ft", 2),
        ("RON", "Romanian Leu", "lei", 2),
        ("BGN", "Bulgarian Lev", "лв", 2),
        ("HRK", "Croatian Kuna", "kn", 2),
        ("RUB", "Russian Ruble", "₽", 2),
        ("UAH", "Ukrainian Hryvnia", "₴", 2),
        // Americas
        ("BRL", "Brazilian Real", "R$", 2),
        ("MXN", "Mexican Peso", "MX$", 2),
        ("ARS", "Argentine Peso", "$", 2),
        ("CLP", "Chilean Peso", "$", 0),
        ("COP", "Colombian Peso", "$", 2),
        ("PEN", "Peruvian Sol", "S/.", 2),
        // Africa
        ("ZAR", "South African Rand", "R", 2),
        ("NGN", "Nigerian Naira", "₦", 2),
        ("KES", "Kenyan Shilling", "KSh", 2),
    ];

    for (code, name, symbol, decimal_places) in currencies {
        Spi::run_with_args(
            r#"
            INSERT INTO pgcurrency.currency (code, name, symbol, decimal_places)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code) DO NOTHING
            "#,
            &[
                (*code).into(),
                (*name).into(),
                (*symbol).into(),
                (*decimal_places).into(),
            ],
        )
        .expect("failed to seed currency");
    }

    Spi::get_one::<i64>("SELECT COUNT(*)::int8 FROM pgcurrency.currency")
        .unwrap()
        .unwrap() as i32
}
