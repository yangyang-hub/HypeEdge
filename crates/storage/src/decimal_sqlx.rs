//! Conversion helpers between the domain [`Decimal`] and `bigdecimal::BigDecimal`.
//!
//! Postgres columns are `NUMERIC(38,18)`. Row structs carry
//! [`BigDecimal`] (which sqlx encodes/decodes exactly via its `bigdecimal`
//! feature); the boundary converts to/from our fixed-point `Decimal`. The DB
//! path is network-bound, so the string conversion is negligible; exactness
//! matters more than speed here.

use bigdecimal::{BigDecimal, num_bigint::BigInt};
use hypeedge_domain::decimal::Decimal;

/// Convert a domain `Decimal` to `BigDecimal` for binding. Uses the exact raw
/// value (raw/10^18), never the API-normalized Display.
pub fn dec_to_bd(d: Decimal) -> BigDecimal {
    BigDecimal::new(BigInt::from(d.raw()), 18)
}

/// Convert a `BigDecimal` read from Postgres back to a domain `Decimal`.
/// Fails if the value has more than 38 significant digits or 18 fractional
/// digits (i.e. does not fit `NUMERIC(38,18)`).
pub fn bd_to_dec(bd: BigDecimal) -> Result<Decimal, hypeedge_domain::decimal::DecimalError> {
    // BigDecimal to_string is exact; only formatting may switch to exponent
    // notation for extreme magnitudes, which the lenient parser accepts.
    Decimal::from_str_lenient(&bd.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_is_exact() {
        for s in [
            "0",
            "1",
            "-1",
            "0.0015",
            "123.450000000000000000",
            "99999999999999999999.999999999999999999",
            "-100000000000000000000",
        ] {
            let d = Decimal::from_str_strict(s).unwrap();
            let bd = dec_to_bd(d);
            assert_eq!(d, bd_to_dec(bd).unwrap(), "roundtrip {s}");
        }
    }
}
