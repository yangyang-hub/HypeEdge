//! Golden corpus parity test: replay the canonical `decimal_string()` outputs
//! recorded from the (now-removed) Python backend through our fixed-point
//! `Decimal`.
//!
//! The corpus lives in `tests/fixtures/golden_decimal.jsonl`, recorded from
//! the Python backend before it was deleted. Each row is
//! `{"input": "...", "canonical": "..."|null, "strict": bool}`.
//!
//! `strict` rows go through `from_str_strict` and must reproduce `canonical`
//! exactly; non-strict rows are float-`str()` coercion outputs and go through
//! `from_str_lenient` (mirroring how Python's `DecimalValue` accepts floats).

use hypeedge_domain::decimal::Decimal;
use hypeedge_domain::decimal::DecimalError;

#[test]
fn golden_decimal_corpus_parity() {
    let corpus_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/golden_decimal.jsonl"
    );
    let content = std::fs::read_to_string(corpus_path).expect("corpus file should exist");
    let mut checked = 0usize;
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        let input = row["input"].as_str().unwrap();
        let strict = row["strict"].as_bool().unwrap_or(false);
        let canonical = row["canonical"].as_str();

        let result = if strict {
            Decimal::from_str_strict(input)
        } else {
            Decimal::from_str_lenient(input)
        };

        match (canonical, result) {
            (Some(expected), Ok(parsed)) => {
                let actual = parsed.to_string();
                assert_eq!(
                    actual, expected,
                    "row {idx}: input={input:?} strict={strict} expected canonical {expected:?}, got {actual:?}"
                );
            }
            (None, Err(_)) => {
                // Python rejected it; we rejected too — parity satisfied
                // regardless of the specific reason.
            }
            (Some(_), Err(e)) => panic!("row {idx}: input={input:?} failed to parse: {e}"),
            (None, Ok(parsed)) => {
                panic!("row {idx}: input={input:?} parsed to {parsed} but Python rejected it")
            }
        }
        checked += 1;
    }
    assert!(checked > 0, "corpus must not be empty");
    eprintln!("golden decimal corpus: {checked} rows verified");
}

/// Arithmetic regression corpus for the H-DM1..3 boundary fixes. Every
/// assertion here must hold for the fixed-point `Decimal` regardless of the
/// canonical-string corpus above (which only covers parse + Display).
#[test]
fn boundary_arithmetic_corpus() {
    // --- H-DM1: div must not flip the sign at i128::MIN ---
    // MIN / 1 == MIN exactly (magnitude 2^127 fits after sign application).
    assert_eq!(Decimal::MIN.div(Decimal::ONE), Decimal::MIN);
    assert_eq!(Decimal::MIN.div(Decimal::ONE).raw(), i128::MIN);
    // MIN / 2 == i128::MIN / 2 (exact, truncation toward zero not needed).
    assert_eq!(Decimal::MIN.div(Decimal::from_i128(2)).raw(), i128::MIN / 2);
    // MIN / -2 must be positive.
    assert_eq!(
        Decimal::MIN.div(Decimal::from_i128(-2)).raw(),
        -(i128::MIN / 2)
    );
    assert!(Decimal::MIN.div(Decimal::from_i128(-2)).is_positive());
    // MAX / 1 == MAX.
    assert_eq!(Decimal::MAX.div(Decimal::ONE), Decimal::MAX);
    // Sign of ordinary divisions is unchanged.
    assert!(
        Decimal::from_i128(7)
            .div(Decimal::from_i128(-3))
            .is_negative()
    );
    assert!(
        Decimal::from_i128(-7)
            .div(Decimal::from_i128(-3))
            .is_positive()
    );

    // --- H-DM2: abs() is never negative, MIN saturates to MAX ---
    assert!(Decimal::MIN.abs().raw() >= 0);
    assert_eq!(Decimal::MIN.abs(), Decimal::MAX);
    assert_eq!(Decimal::MAX.abs(), Decimal::MAX);
    assert_eq!(Decimal::ZERO.abs(), Decimal::ZERO);
    // round_to_places(MIN) must not panic/wrap and keeps sign + integrality.
    let r = Decimal::MIN.round_to_places(2);
    assert!(r.raw() <= 0);
    assert_eq!(r.raw() % 10_000_000_000_000_000, 0);
    assert!(Decimal::MAX.round_to_places(2).raw() > 0);
    // Half-up rounding on ordinary values is unchanged.
    assert_eq!(
        Decimal::from_str_strict("1.005")
            .unwrap()
            .round_to_places(2)
            .to_string(),
        "1.01"
    );
    assert_eq!(
        Decimal::from_str_strict("-1.005")
            .unwrap()
            .round_to_places(2)
            .to_string(),
        "-1.01"
    );

    // --- H-DM3: floor/ceil at the extremes saturate instead of overflowing ---
    assert!(Decimal::MIN.floor().raw() <= Decimal::MIN.raw());
    assert!(Decimal::MAX.ceil().raw() >= Decimal::MAX.raw());
    assert_eq!(Decimal::MIN.floor(), Decimal::MIN); // saturated floor
    assert_eq!(Decimal::MAX.ceil(), Decimal::MAX); // saturated ceil
    assert_eq!(
        Decimal::from_str_strict("-100.5")
            .unwrap()
            .floor()
            .to_string(),
        "-101"
    );
    assert_eq!(
        Decimal::from_str_strict("100.5")
            .unwrap()
            .ceil()
            .to_string(),
        "101"
    );
    assert_eq!(
        Decimal::from_str_strict("100.5")
            .unwrap()
            .floor()
            .to_string(),
        "100"
    );
    assert_eq!(
        Decimal::from_str_strict("-100.5")
            .unwrap()
            .ceil()
            .to_string(),
        "-100"
    );

    // --- Rounding modes (documented contracts) ---
    let v = Decimal::from_str_strict("1.234567890123456789").unwrap();
    assert_eq!(v.quantize(4).to_string(), "1.2345");
    assert_eq!(v.quantize(8).to_string(), "1.23456789");
    assert_eq!(v.round_to_places(8).to_string(), "1.23456789");
    assert_eq!(
        Decimal::from_str_strict("0.000000000000000005")
            .unwrap()
            .round_to_places(18)
            .to_string(),
        "0.000000000000000005"
    );
    // Python-normalize (28 sig digits, ROUND_HALF_EVEN) at the boundary.
    let long = Decimal::from_str_strict("99999999999999999999.999999999999999999").unwrap();
    assert_eq!(
        long.round_python_prec28().to_string(),
        "100000000000000000000"
    );

    // --- Serde round-trip contract (28-sig normalize, lossy; exact string lossless) ---
    let big = Decimal::from_str_strict("99999999999999999999.999999999999999999").unwrap();
    let s = serde_json::to_string(&big).unwrap();
    assert_eq!(s, "\"100000000000000000000\"");
    let back: Decimal = serde_json::from_str(&s).unwrap();
    assert_eq!(back.to_string(), "100000000000000000000");
    assert_ne!(back, big, "serde round-trip normalizes by contract");
    assert_eq!(
        big.to_exact_string(),
        "99999999999999999999.999999999999999999"
    );
    let v29 = Decimal::from_str_strict("12345678901234567890.123456789").unwrap();
    assert_eq!(
        Decimal::from_str_strict(&v29.to_exact_string()).unwrap(),
        v29
    );

    // --- Large exponents: rejected fast, never a shift-loop DoS ---
    for bad in [
        "1e2147483647",
        "-1e2147483647",
        "1e-2147483648",
        "1e50",
        "1e-50",
    ] {
        assert_eq!(
            Decimal::from_str_lenient(bad),
            Err(DecimalError::OutOfRange),
            "absurd exponent {bad:?} must be rejected"
        );
    }
    // A 38-significant-digit lenient input still parses (no regression from
    // the exponent cap: exponent 0 is within bounds).
    let wide = "99999999999999999999.999999999999999999";
    assert!(Decimal::from_str_lenient(wide).is_ok());
}
