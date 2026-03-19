use pgrx::prelude::*;

/// Seed common ERP document sequences. Idempotent — skips existing sequences.
#[pg_extern]
pub fn seed_common() {
    let sequences = [
        (
            "invoice",
            "INV-",
            "",
            "{prefix}{year}-{counter}",
            5,
            1i64,
            1,
            "yearly",
            false,
            "Sales invoices",
        ),
        (
            "purchase_order",
            "PO-",
            "",
            "{prefix}{year}-{counter}",
            5,
            1,
            1,
            "yearly",
            false,
            "Purchase orders",
        ),
        (
            "sales_order",
            "SO-",
            "",
            "{prefix}{year}-{counter}",
            5,
            1,
            1,
            "yearly",
            false,
            "Sales orders",
        ),
        (
            "delivery_order",
            "DO-",
            "",
            "{prefix}{counter}",
            6,
            1,
            1,
            "never",
            false,
            "Delivery orders",
        ),
        (
            "return",
            "RTV-",
            "",
            "{prefix}{year}{month}-{counter}",
            4,
            1,
            1,
            "monthly",
            false,
            "Return to vendor",
        ),
    ];

    for (name, prefix, suffix, format, pad, start, inc, policy, gf, desc) in &sequences {
        let exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgsequence.sequence_def WHERE name = $1)",
            &[(*name).into()],
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if exists {
            continue;
        }

        Spi::run_with_args(
            r#"
            INSERT INTO pgsequence.sequence_def
                (name, prefix, suffix, format, counter_pad, start_value, increment, reset_policy, gap_free, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            &[
                (*name).into(),
                (*prefix).into(),
                (*suffix).into(),
                (*format).into(),
                (*pad).into(),
                (*start).into(),
                (*inc).into(),
                (*policy).into(),
                (*gf).into(),
                (*desc).into(),
            ],
        )
        .expect("failed to seed sequence");
    }
}
