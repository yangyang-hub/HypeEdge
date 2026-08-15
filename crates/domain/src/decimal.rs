//! Exact decimal domain values: a fixed-point `i128` scaled by `10^-18`.
//!
//! This mirrors Python's `DecimalValue`/`Price`/`Size`/`Usd`/`Pct` in
//! `src/hypeedge/core/types.py` and the JSON decimal contract in
//! `src/hypeedge/api/schemas.py` (`decimal_string`, `_parse_decimal_string`).
//!
//! ## Why `i128` at scale 18 (and not `rust_decimal` / `bigdecimal`)
//!
//! Postgres columns are `NUMERIC(38,18)` (up to 20 integer digits + 18
//! fractional digits = 38 significant digits). `rust_decimal` stores a 96-bit
//! mantissa (28–29 significant digits) and therefore **cannot** represent the
//! full legal range. `bigdecimal` matches Python's arbitrary precision but
//! allocates on every operation — the same pressure we are removing from the
//! hot path. A raw `i128` scaled by `10^18` holds `|value| ≤ 2^127/10^18 ≈
//! 1.70×10^20`, i.e. every `NUMERIC(38,18)` value exactly, and add/sub are a
//! single register op. Multiply/divide overflow `i128`, so they go through an
//! `I256` intermediate (four limbs, no heap).
//!
//! ## Parity notes vs. Python `decimal`
//!
//! Python's default context is `prec=28, ROUND_HALF_EVEN` and `Decimal` keeps
//! a *dynamic* exponent. This type keeps a *fixed* scale of 18. Consequences:
//! - add/sub are exact for any values whose result fits `NUMERIC(38,18)`; the
//!   two models agree whenever both operands have ≤ 18 fractional digits
//!   (every value that crosses the JSON API boundary does).
//! - mul/div round to 18 fractional digits with **ROUND_HALF_UP** (away from
//!   zero). Python rounds to 28 *significant* digits with ROUND_HALF_EVEN.
//!   For the trading hot paths (price×size, amount×rate, …) operands have ≤ 8
//!   fractional digits, products ≤ 16, so no rounding occurs and the models
//!   agree bit-for-bit. Only results with > 18 fractional digits differ; those
//!   are normally `quantize`d to the instrument precision immediately.
//! - [`Decimal::round_python_prec28`] exists for the rare place where a result
//!   must reproduce Python's 28-significant-digit ROUND_HALF_EVEN exactly.

use std::fmt;
use std::ops::{Add, AddAssign, Div, Mul, Neg, Sub};

use ethnum::I256;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Fractional digits of the fixed-point representation (`10^-18`).
pub const SCALE: u32 = 18;
/// `10^18` as a raw value.
pub const SCALE_I128: i128 = 1_000_000_000_000_000_000;

/// Maximum number of significant digits the API contract accepts (matches
/// `_parse_decimal_string`'s `NUMERIC(38,18)` check).
pub const MAX_SIGNIFICANT_DIGITS: usize = 38;

/// An exact decimal value = `raw / 10^18`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    raw: i128,
}

impl Default for Decimal {
    fn default() -> Self {
        Self::ZERO
    }
}

impl Decimal {
    pub const ZERO: Decimal = Decimal { raw: 0 };
    pub const ONE: Decimal = Decimal { raw: SCALE_I128 };
    /// Largest representable value ≈ 1.70×10^20.
    pub const MAX: Decimal = Decimal { raw: i128::MAX };
    pub const MIN: Decimal = Decimal { raw: i128::MIN };
    /// Smallest positive representable value = `10^-18`.
    pub const EPSILON: Decimal = Decimal { raw: 1 };

    /// Upper bound on `|exponent|` accepted by the lenient parser. Any
    /// exponent beyond this cannot produce an in-range `NUMERIC(38,18)` value
    /// (positive exponents yield > 38 significant digits and are rejected by
    /// [`Decimal::from_parts`]; negative exponents beyond 18 fractional digits
    /// truncate to zero). The bound also keeps `apply_exponent`'s shift loop
    /// linear in the input length instead of exponential in the exponent.
    pub const MAX_LENIENT_EXPONENT: u32 = 40;

    /// Create from a raw scaled value (`value / 10^18`).
    pub const fn from_raw(raw: i128) -> Decimal {
        Decimal { raw }
    }

    /// The raw scaled value.
    pub const fn raw(&self) -> i128 {
        self.raw
    }

    /// Create from an integer (exact).
    pub const fn from_i128(v: i128) -> Decimal {
        match v.checked_mul(SCALE_I128) {
            Some(raw) => Decimal { raw },
            None => panic!("Decimal integer overflow"),
        }
    }

    /// Create from an integer without overflow checking (debug asserts only).
    pub const fn from_i128_unchecked(v: i128) -> Decimal {
        Decimal {
            raw: v * SCALE_I128,
        }
    }

    /// Convert a raw value at a smaller scale to scale 18, truncating any
    /// fractional part that does not fit (`scale ≤ 18`).
    ///
    /// `scale = 0` means integer, `scale = 8` means `raw/10^8`, etc.
    pub const fn from_scaled(raw: i128, scale: u32) -> Decimal {
        if scale == SCALE {
            Decimal { raw }
        } else if scale < SCALE {
            Decimal {
                raw: raw * pow10_i128(SCALE - scale),
            }
        } else {
            Decimal {
                raw: raw / pow10_i128(scale - SCALE),
            }
        }
    }

    /// Construct from a float **exactly as Python's `Decimal(str(f))` does**:
    /// the float is formatted with Rust's shortest round-trip formatter and
    /// parsed. This is what makes `DecimalValue(0.1) == 0.1` in Python.
    pub fn from_f64(f: f64) -> Result<Decimal, DecimalError> {
        if !f.is_finite() {
            return Err(DecimalError::NotFinite);
        }
        // `format!("{}", 0.1)` -> "0.1"; `1000.5` -> "1000.5"; large/small
        // magnitudes use exponent form (e.g. `1e20`), which is why the lenient
        // parser accepts exponents.
        Self::from_str_lenient(&format!("{f}"))
    }

    /// Strict parse: the API contract. Rejects exponent notation, leading
    /// zeros, `.5`/`5.` shapes, values with > 38 significant digits, and
    /// values with > 18 fractional digits. Mirrors `_parse_decimal_string`.
    pub fn from_str_strict(s: &str) -> Result<Decimal, DecimalError> {
        let (neg, int_part, frac_part) = parse_decimal_string(s)?;
        let decimal = Self::from_parts(int_part, frac_part)?;
        Ok(if neg { -decimal } else { decimal })
    }

    /// Lenient parse: accepts exponent notation (e.g. `1e-07`, `1.5E+3`) and
    /// arbitrary leading zeros. Also accepts a leading `+` and `.5`-style
    /// shapes that the strict parser rejects — this is deliberate: lenient is
    /// the internal coercion entry (float `str()` output, pre-normalized
    /// values), strict is the API gate. Still enforces `NUMERIC(38,18)` bounds
    /// and caps `|exponent|` at [`MAX_LENIENT_EXPONENT`] so a hostile string
    /// like `1e2147483647` cannot drive an unbounded shift loop.
    pub fn from_str_lenient(s: &str) -> Result<Decimal, DecimalError> {
        let s = s.trim();
        let (neg, mantissa, exp) = parse_lenient_decimal(s)?;
        let (int_part, frac_part) = apply_exponent(mantissa, exp)?;
        // Lenient inputs (float-string coercion, pre-normalized values) may
        // carry more than 18 fractional digits; truncate the excess rather
        // than rejecting (the strict parser is the API gate).
        let frac = if frac_part.is_empty() {
            None
        } else {
            Some(&frac_part[..frac_part.len().min(SCALE as usize)])
        };
        let decimal = Self::from_parts(&int_part, frac)?;
        Ok(if neg { -decimal } else { decimal })
    }

    /// Assemble from digit strings: integer part and optional fractional part
    /// (fractional digits beyond 18 are truncated, mirroring the strict
    /// parser's rejection of > 18 fractional digits being handled by caller).
    fn from_parts(int_part: &str, frac_part: Option<&str>) -> Result<Decimal, DecimalError> {
        let int_part = int_part.trim_start_matches('0');
        let int_part = if int_part.is_empty() { "0" } else { int_part };

        let mut significant = int_part.len();
        if let Some(frac) = frac_part {
            if frac.len() > SCALE as usize {
                return Err(DecimalError::TooManyFractionalDigits);
            }
            significant += frac.trim_end_matches('0').len();
        }
        if significant > MAX_SIGNIFICANT_DIGITS {
            return Err(DecimalError::TooManySignificantDigits);
        }

        let int_raw: i128 = int_part.parse().map_err(|_| DecimalError::OutOfRange)?;
        let int_raw = int_raw
            .checked_mul(SCALE_I128)
            .ok_or(DecimalError::OutOfRange)?;
        let frac_raw: i128 = match frac_part {
            Some(frac) => {
                let mut padded = frac.to_string();
                padded.push_str(&"0".repeat(SCALE as usize - frac.len()));
                padded.parse().map_err(|_| DecimalError::OutOfRange)?
            }
            None => 0,
        };
        Ok(Decimal {
            raw: int_raw + frac_raw,
        })
    }

    /// A value with the given coefficient and exponent (`coeff × 10^exp`),
    /// mirroring `Decimal((digits, exponent))` semantics.
    pub fn with_exponent(coeff: i128, exp: i32) -> Result<Decimal, DecimalError> {
        if exp >= 0 {
            let raw = coeff
                .checked_mul(pow10_i128(exp as u32))
                .ok_or(DecimalError::OutOfRange)?;
            Ok(Decimal { raw })
        } else {
            let e = exp.unsigned_abs();
            if e > SCALE {
                // scale down beyond 18 places: must truncate; reject if it
                // would lose precision silently — caller chooses.
                return Ok(Decimal {
                    raw: coeff / pow10_i128(e),
                });
            }
            let raw = coeff
                .checked_mul(pow10_i128(SCALE - e))
                .ok_or(DecimalError::OutOfRange)?;
            Ok(Decimal { raw })
        }
    }

    /// Decompose into (coefficient, exponent) such that `self = coeff × 10^exp`
    /// with the minimal exponent ≥ -18 that keeps `coeff` integral.
    pub fn as_tuple(&self) -> (i128, i32) {
        let mut raw = self.raw;
        let mut exp = -18i32;
        // Remove trailing zeros from the raw scale to normalize the exponent.
        while raw != 0 && raw % 10 == 0 && exp < 0 {
            raw /= 10;
            exp += 1;
        }
        (raw, exp)
    }

    // --- predicates ---

    pub const fn is_zero(&self) -> bool {
        self.raw == 0
    }
    pub const fn is_negative(&self) -> bool {
        self.raw < 0
    }
    pub const fn is_positive(&self) -> bool {
        self.raw > 0
    }
    pub const fn signum(&self) -> i8 {
        match self.raw {
            0 => 0,
            n if n < 0 => -1,
            _ => 1,
        }
    }
    pub const fn abs(&self) -> Decimal {
        // `i128::MIN` has no positive counterpart: `wrapping_abs` would return
        // a negative value (breaking the `abs() >= 0` contract) and a panicking
        // `abs` would crash at the boundary. Saturate to `MAX` instead so the
        // magnitude never wraps and the result is always non-negative.
        Decimal {
            raw: match self.raw.checked_abs() {
                Some(v) => v,
                None => i128::MAX,
            },
        }
    }
    pub const fn max(self, other: Decimal) -> Decimal {
        if self.raw >= other.raw { self } else { other }
    }
    pub const fn min(self, other: Decimal) -> Decimal {
        if self.raw <= other.raw { self } else { other }
    }

    /// Clamp to `[min, max]`.
    pub const fn clamp(self, min: Decimal, max: Decimal) -> Decimal {
        if self.raw < min.raw {
            min
        } else if self.raw > max.raw {
            max
        } else {
            self
        }
    }

    /// Floor to the nearest integer (toward negative infinity).
    ///
    /// Computed in `I256` and saturated: at `i128::MIN` the true floor is one
    /// unit of `SCALE` below the representable range, so the result clamps to
    /// `MIN` rather than panicking or wrapping.
    pub fn floor(&self) -> Decimal {
        let int = I256::from(self.raw / SCALE_I128);
        let frac = self.raw % SCALE_I128;
        let base = if frac < 0 { int - I256::ONE } else { int };
        Decimal {
            raw: signed_to_i128(base * I256::from(SCALE_I128)),
        }
    }

    /// Ceil to the nearest integer (toward positive infinity).
    ///
    /// Computed in `I256` and saturated: at `i128::MAX` the true ceiling is one
    /// unit of `SCALE` above the representable range, so the result clamps to
    /// `MAX` rather than panicking or wrapping.
    pub fn ceil(&self) -> Decimal {
        let int = I256::from(self.raw / SCALE_I128);
        let frac = self.raw % SCALE_I128;
        let base = if frac > 0 { int + I256::ONE } else { int };
        Decimal {
            raw: signed_to_i128(base * I256::from(SCALE_I128)),
        }
    }

    /// Step down: `floor(value/step) * step` (mirrors `_to_step(ROUND_FLOOR)`).
    pub fn floor_to_step(&self, step: Decimal) -> Decimal {
        (self.div(step)).floor().mul(step)
    }

    /// Step up: `ceil(value/step) * step` (mirrors `_to_step(ROUND_CEILING)`).
    pub fn ceil_to_step(&self, step: Decimal) -> Decimal {
        (self.div(step)).ceil().mul(step)
    }

    /// Multiply and round to scale 18 with ROUND_HALF_UP (away from zero),
    /// computed in `I256` so no intermediate overflows.
    ///
    /// Name mirrors Python's `DecimalValue.__mul__`; the `Mul` trait delegates
    /// here.
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, rhs: Decimal) -> Decimal {
        let a = I256::from(self.raw);
        let b = I256::from(rhs.raw);
        let prod = a * b;
        // Round half away from zero: add sign-adjusted half of the divisor.
        let half = if prod.is_negative() {
            -I256::from(SCALE_I128 / 2)
        } else {
            I256::from(SCALE_I128 / 2)
        };
        let scaled = (prod + half) / I256::from(SCALE_I128);
        Decimal {
            raw: signed_to_i128(scaled),
        }
    }

    /// Divide and round to scale 18 with ROUND_HALF_UP. Panics on division by
    /// zero (caller validates).
    ///
    /// Name mirrors Python's `DecimalValue.__truediv__`; the `Div` trait
    /// delegates here.
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, rhs: Decimal) -> Decimal {
        assert!(!rhs.is_zero(), "Decimal division by zero");
        let neg = (self.raw < 0) ^ (rhs.raw < 0);
        // `unsigned_abs()` yields a `u128`; converting through `I256::from`
        // preserves the full magnitude. (Casting `unsigned_abs() as i128`
        // would wrap `2^127` — the magnitude of `i128::MIN` — back to a
        // negative value and silently flip the quotient's sign.)
        let num = I256::from(self.raw.unsigned_abs()) * I256::from(SCALE_I128);
        let den = I256::from(rhs.raw.unsigned_abs());
        let q = (num + den / I256::from(2)) / den;
        // Apply the sign in `I256` *before* the saturated narrowing: for
        // `MIN / 1` the magnitude `2^127` alone exceeds `i128::MAX`, but the
        // true (negative) quotient `-2^127` is exactly representable.
        let q = if neg { -q } else { q };
        Decimal {
            raw: signed_to_i128(q),
        }
    }

    /// Round to a given number of fractional digits (truncation toward zero
    /// when `n < 18` keeps the lower digits; this is "quantize toward zero").
    pub fn quantize(&self, places: u32) -> Decimal {
        if places >= SCALE {
            return *self;
        }
        let shift = SCALE - places;
        let div = pow10_i128(shift);
        // Truncate toward zero.
        let truncated = self.raw / div * div;
        Decimal { raw: truncated }
    }

    /// Round to the nearest value at `places` fractional digits using
    /// ROUND_HALF_UP (away from zero).
    pub fn round_to_places(&self, places: u32) -> Decimal {
        if places >= SCALE {
            return *self;
        }
        let shift = SCALE - places;
        let div = pow10_i128(shift);
        let half = div / 2;
        let mut raw = self.raw;
        let sign = if raw < 0 { -1i128 } else { 1i128 };
        // `i128::abs()` panics on `MIN`; saturate the magnitude first so the
        // half-up add below cannot wrap either (`MIN`/`MAX` magnitudes round
        // to themselves, which is the best representable answer).
        let mag = raw.checked_abs().unwrap_or(i128::MAX).saturating_add(half);
        raw = mag / div * div * sign;
        Decimal { raw }
    }

    /// Reproduce Python's default `Decimal` context: round to `prec`
    /// significant digits with ROUND_HALF_EVEN. Needed for byte-level parity
    /// where the Python codebase's implicit `prec=28` rounding surfaces.
    pub fn round_python_prec28(&self) -> Decimal {
        self.round_significant_half_even(28)
    }

    fn round_significant_half_even(&self, prec: u32) -> Decimal {
        if self.raw == 0 || prec == 0 {
            return *self;
        }
        let (coeff, exp) = self.as_tuple();
        let coeff = coeff.unsigned_abs();
        let digits = significant_digits(coeff);
        if digits <= prec as usize {
            return *self;
        }
        let drop = (digits - prec as usize) as u32;
        let div = pow10_i128(drop);
        let q = coeff / div as u128;
        let rem = coeff % div as u128;
        let halfway = div as u128 / 2;
        let round_up = match rem.cmp(&halfway) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => q % 2 == 1, // half-even
        };
        let mut rounded = q;
        if round_up {
            rounded += 1;
        }
        let rounded = rounded as i128;
        let exp = exp + drop as i32;
        let sign = if self.is_negative() { -1i128 } else { 1i128 };
        // Rebuild at scale 18: rounded × 10^exp.
        if exp >= 0 {
            let raw = (rounded * pow10_i128(exp as u32) * sign) as i128;
            Decimal { raw }
        } else {
            let e = exp.unsigned_abs();
            let raw = (rounded * sign) as i128;
            Decimal {
                raw: raw * pow10_i128(SCALE - e),
            }
        }
    }
}

/// Number of significant decimal digits of a non-zero u128.
fn significant_digits(v: u128) -> usize {
    if v == 0 {
        return 0;
    }
    let mut n = 0;
    let mut x = v;
    while x > 0 {
        x /= 10;
        n += 1;
    }
    n
}

/// Deterministic `pow10` for `u32 ≤ 38`, panicking beyond (never called with
/// values that overflow the underlying type in practice).
const fn pow10_i128(e: u32) -> i128 {
    let mut r = 1i128;
    let mut i = 0;
    while i < e {
        r *= 10;
        i += 1;
    }
    r
}

/// I256 -> i128. For valid `NUMERIC(38,18)` inputs the scaled result always
/// fits i128; this clamps defensively in the pathological case.
fn signed_to_i128(v: I256) -> i128 {
    let x = v.as_i128();
    if I256::from(x) == v {
        x
    } else if v.is_negative() {
        i128::MIN
    } else {
        i128::MAX
    }
}

// --- parsing helpers ---

/// Split a strict decimal string into `(negative, int_part, frac_part)`.
fn parse_decimal_string(s: &str) -> Result<(bool, &str, Option<&str>), DecimalError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(DecimalError::InvalidFormat);
    }
    let (body, neg) = match s.strip_prefix('-') {
        Some(rest) => (rest, true),
        None => (s, false),
    };
    // The pattern allows "0" or "1-9..." integer and optional ".digits".
    let (int_part, frac_part) = regex_body_match(body).ok_or(DecimalError::InvalidFormat)?;
    Ok((neg, int_part, frac_part))
}

/// Minimal regex match for `^(0|[1-9][0-9]*)(\.([0-9]+))?$` on the body.
/// Returns `(int_part, frac_part)`.
fn regex_body_match(body: &str) -> Option<(&str, Option<&str>)> {
    let mut chars = body.chars();
    let first = chars.next()?;
    if !first.is_ascii_digit() {
        return None;
    }
    if first == '0' {
        // must be exactly "0" (or "0.xxx")
        match chars.next() {
            None => return Some(("0", None)),
            Some('.') => {
                let frac = &body[2..];
                if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                return Some(("0", Some(frac)));
            }
            Some(_) => return None, // leading zero like "05"
        }
    }
    // first is 1-9
    let mut idx = 1;
    let mut int_end = 1;
    let mut frac: Option<&str> = None;
    for c in chars {
        match c {
            '0'..='9' => {
                int_end = idx + 1;
            }
            '.' => {
                let rest = &body[idx + 1..];
                if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                frac = Some(rest);
                break;
            }
            _ => return None,
        }
        idx += 1;
    }
    Some((&body[..int_end], frac))
}

/// Lenient parse: `[sign] digits [. digits] [e[sign]digits]`.
/// Returns `(negative, mantissa_without_sign, exponent)`.
fn parse_lenient_decimal(s: &str) -> Result<(bool, &str, i32), DecimalError> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    // Split exponent.
    let (mantissa, exp) = match s.find(['e', 'E']) {
        Some(i) => {
            let exp_str = &s[i + 1..];
            let exp: i32 = exp_str.parse().map_err(|_| DecimalError::InvalidFormat)?;
            // Reject absurd exponents up front: `apply_exponent` shifts digit
            // by digit, so a string like `1e2147483647` would otherwise loop
            // ~2^31 times (CPU DoS). No exponent beyond ±40 can produce an
            // in-range value anyway (see [`Decimal::MAX_LENIENT_EXPONENT`]).
            if exp.unsigned_abs() > Decimal::MAX_LENIENT_EXPONENT {
                return Err(DecimalError::OutOfRange);
            }
            (&s[..i], exp)
        }
        None => (s, 0),
    };
    if mantissa.is_empty() {
        return Err(DecimalError::InvalidFormat);
    }
    // Validate mantissa digits.
    let mut seen_dot = false;
    for (_, c) in mantissa.char_indices() {
        match c {
            '.' if !seen_dot => seen_dot = true,
            '.' => return Err(DecimalError::InvalidFormat),
            '0'..='9' => {}
            _ => return Err(DecimalError::InvalidFormat),
        }
    }
    Ok((neg, mantissa, exp))
}

/// Apply a decimal exponent to a mantissa string, producing int + frac parts.
fn apply_exponent(mantissa: &str, exp: i32) -> Result<(String, String), DecimalError> {
    // Split mantissa into integer and fraction parts.
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    let int_part = int_part.trim_start_matches('0');
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let frac_part = frac_part.trim_end_matches('0');
    let frac_part = if frac_part.is_empty() { "" } else { frac_part };

    // Value = (int + frac/10^k) × 10^exp.
    if exp >= 0 {
        // Shift decimal point right.
        let mut int = int_part.to_string();
        let mut frac = frac_part.to_string();
        for _ in 0..exp {
            if frac.is_empty() {
                int.push('0');
            } else {
                int.push(frac.remove(0));
            }
        }
        Ok((int, frac))
    } else {
        // Shift decimal point left.
        let mut int = int_part.to_string();
        let mut frac = frac_part.to_string();
        for _ in 0..(-exp) {
            if int.is_empty() {
                frac.insert(0, '0');
            } else {
                frac.insert(0, int.pop().unwrap());
            }
        }
        if int.is_empty() {
            int = "0".to_string();
        }
        Ok((int, frac))
    }
}

// --- operator impls ---

impl Add for Decimal {
    type Output = Decimal;
    fn add(self, rhs: Decimal) -> Decimal {
        Decimal {
            raw: self.raw.saturating_add(rhs.raw),
        }
    }
}
impl Sub for Decimal {
    type Output = Decimal;
    fn sub(self, rhs: Decimal) -> Decimal {
        Decimal {
            raw: self.raw.saturating_sub(rhs.raw),
        }
    }
}
impl Neg for Decimal {
    type Output = Decimal;
    fn neg(self) -> Decimal {
        Decimal {
            raw: self.raw.saturating_neg(),
        }
    }
}
impl Mul for Decimal {
    type Output = Decimal;
    fn mul(self, rhs: Decimal) -> Decimal {
        Decimal::mul(self, rhs)
    }
}
impl Div for Decimal {
    type Output = Decimal;
    fn div(self, rhs: Decimal) -> Decimal {
        Decimal::div(self, rhs)
    }
}
impl AddAssign for Decimal {
    fn add_assign(&mut self, rhs: Decimal) {
        *self = *self + rhs;
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}

// --- serde ---

/// Serialize through [`fmt::Display`], i.e. Python's `decimal_string()`
/// normalization: values with > 28 significant digits are rounded to 28
/// (ROUND_HALF_EVEN) before rendering, while [`Deserialize`] accepts the full
/// `NUMERIC(38,18)` range. This asymmetry is **intentional and locked in**:
/// the API/storage wire contract is the 28-significant-digit `decimal_string`
/// form, and `to_exact_string` is the lossless storage boundary. A serde
/// round-trip of a 29–38 significant-digit value therefore normalizes (loses
/// the digits beyond 28); callers that need exactness must use
/// [`Decimal::to_exact_string`] explicitly.
impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
        let s = String::deserialize(deserializer)?;
        Decimal::from_str_strict(&s).map_err(serde::de::Error::custom)
    }
}

/// `fmt::Display` reproduces Python's `decimal_string()`: `0` renders as `"0"`;
/// otherwise the value is normalized (trailing fractional zeros removed) and
/// rendered as a fixed-point (never scientific) string.
///
/// Python's `Decimal.normalize()` also rounds values with more than the
/// context's 28 significant digits (ROUND_HALF_EVEN), so `decimal_string` of
/// e.g. `99999999999999999999.999999999999999999` is `"100000000000000000000"`.
/// We replicate that: values with > 28 significant digits are rounded first.
impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.raw == 0 {
            return f.write_str("0");
        }
        let raw = self.raw_for_display();
        let neg = raw < 0;
        let abs = raw.unsigned_abs();
        let int = abs / SCALE_I128 as u128;
        let frac = abs % SCALE_I128 as u128;
        if frac == 0 {
            write!(f, "{}{}", if neg { "-" } else { "" }, int)
        } else {
            let frac_str = format!("{frac:018}");
            let trimmed = frac_str.trim_end_matches('0');
            write!(
                f,
                "{}{}.{}",
                if neg { "-" } else { "" },
                int,
                if trimmed.is_empty() { "0" } else { trimmed }
            )
        }
    }
}

impl Decimal {
    /// Render the exact fixed-point value (raw / 10^18) without Python's
    /// 28-significant-digit normalize rounding. Used for the storage boundary
    /// where exact round-trips through `NUMERIC(38,18)` matter.
    pub fn to_exact_string(&self) -> String {
        if self.raw == 0 {
            return "0".to_string();
        }
        let neg = self.raw < 0;
        let abs = self.raw.unsigned_abs();
        let int = abs / SCALE_I128 as u128;
        let frac = abs % SCALE_I128 as u128;
        if frac == 0 {
            format!("{}{}", if neg { "-" } else { "" }, int)
        } else {
            let frac_str = format!("{frac:018}");
            let trimmed = frac_str.trim_end_matches('0');
            format!(
                "{}{}.{}",
                if neg { "-" } else { "" },
                int,
                if trimmed.is_empty() { "0" } else { trimmed }
            )
        }
    }

    /// Raw value after applying Python's `decimal.normalize()` rounding: values
    /// with > 28 significant digits are rounded to 28 (ROUND_HALF_EVEN); the
    /// rest are unchanged. Fast path: most values have few digits.
    fn raw_for_display(&self) -> i128 {
        let (coeff, _exp) = self.as_tuple();
        if significant_digits(coeff.unsigned_abs()) <= 28 {
            self.raw
        } else {
            self.round_significant_half_even(28).raw
        }
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decimal({})", self)
    }
}

// --- error ---

/// Errors produced while parsing/constructing a `Decimal`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecimalError {
    #[error("must be a base-10 decimal string")]
    InvalidFormat,
    #[error("decimal value must be finite")]
    NotFinite,
    #[error("must fit NUMERIC(38,18)")]
    TooManySignificantDigits,
    #[error("must fit NUMERIC(38,18)")]
    TooManyFractionalDigits,
    #[error("decimal value out of range")]
    OutOfRange,
}

// --- semantic wrappers ---

macro_rules! semantic_decimal {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(Decimal);

        impl $name {
            pub const ZERO: $name = $name(Decimal::ZERO);
            pub const fn new(d: Decimal) -> $name {
                $name(d)
            }
            pub const fn from_raw(raw: i128) -> $name {
                $name(Decimal { raw })
            }
            pub const fn inner(&self) -> Decimal {
                self.0
            }
            pub const fn raw(&self) -> i128 {
                self.0.raw
            }
            pub const fn is_zero(&self) -> bool {
                self.0.is_zero()
            }
        }

        impl std::ops::Deref for $name {
            type Target = Decimal;
            fn deref(&self) -> &Decimal {
                &self.0
            }
        }

        impl From<Decimal> for $name {
            fn from(d: Decimal) -> $name {
                $name(d)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }
        impl serde::Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.serialize(serializer)
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<$name, D::Error> {
                Decimal::deserialize(deserializer).map($name)
            }
        }
    };
}

semantic_decimal!(Price, "Exact instrument price.");
semantic_decimal!(Size, "Exact contract quantity.");
semantic_decimal!(Usd, "Exact USDC-denominated monetary amount.");
semantic_decimal!(
    Pct,
    "Exact percentage as a fraction (range chosen by the caller)."
);

// --- FromStr ---

impl std::str::FromStr for Decimal {
    type Err = DecimalError;
    fn from_str(s: &str) -> Result<Decimal, DecimalError> {
        Decimal::from_str_strict(s)
    }
}

// --- unit tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn display_zero() {
        assert_eq!(Decimal::ZERO.to_string(), "0");
        assert_eq!(Decimal::from_raw(0).to_string(), "0");
    }

    #[test]
    fn display_integers() {
        assert_eq!(Decimal::from_i128(0).to_string(), "0");
        assert_eq!(Decimal::from_i128(1).to_string(), "1");
        assert_eq!(Decimal::from_i128(100).to_string(), "100");
        assert_eq!(Decimal::from_i128(-5).to_string(), "-5");
        assert_eq!(
            Decimal::from_i128(1_000_000_000_000_000_000).to_string(),
            "1000000000000000000"
        );
    }

    #[test]
    fn display_fractional_trailing_zeros_trimmed() {
        assert_eq!(
            Decimal::from_str_strict("1.2300").unwrap().to_string(),
            "1.23"
        );
        assert_eq!(
            Decimal::from_str_strict("0.100").unwrap().to_string(),
            "0.1"
        );
        assert_eq!(Decimal::from_str_strict("0.5").unwrap().to_string(), "0.5");
        assert_eq!(
            Decimal::from_str_strict("-0.5").unwrap().to_string(),
            "-0.5"
        );
        assert_eq!(
            Decimal::from_str_strict("120.00").unwrap().to_string(),
            "120"
        );
        assert_eq!(
            Decimal::from_str_strict("0.000000000000000001")
                .unwrap()
                .to_string(),
            "0.000000000000000001"
        );
    }

    #[test]
    fn strict_parse_rejects_bad_shapes() {
        for bad in [
            "", " ", ".5", "5.", "+1", "1e5", "1E-7", "00.5", "0x1", "1_000", "nan", "inf",
            "Infinity",
        ] {
            assert!(
                Decimal::from_str_strict(bad).is_err(),
                "should reject {bad:?}"
            );
        }
    }

    #[test]
    fn strict_parse_accepts() {
        assert_eq!(Decimal::from_str_strict("0").unwrap().to_string(), "0");
        assert_eq!(Decimal::from_str_strict("123").unwrap().to_string(), "123");
        assert_eq!(
            Decimal::from_str_strict("0.001").unwrap().to_string(),
            "0.001"
        );
        // 38 significant digits parse exactly; `decimal_string()` rounds them
        // to 28 (Python's normalize context), so Display shows the rounded value.
        let big = Decimal::from_str_strict("99999999999999999999.999999999999999999").unwrap();
        assert_eq!(big.to_string(), "100000000000000000000");
    }

    #[test]
    fn strict_parse_rejects_bounds() {
        // 39 significant digits.
        assert!(Decimal::from_str_strict("999999999999999999999999999999999999999").is_err());
        // 19 fractional digits.
        assert!(Decimal::from_str_strict("1.0000000000000000001").is_err());
    }

    #[test]
    fn lenient_parse_exponents() {
        assert_eq!(
            Decimal::from_str_lenient("1e-07").unwrap().to_string(),
            "0.0000001"
        );
        assert_eq!(
            Decimal::from_str_lenient("1.5E3").unwrap().to_string(),
            "1500"
        );
        assert_eq!(
            Decimal::from_str_lenient("1.5e-3").unwrap().to_string(),
            "0.0015"
        );
        assert_eq!(
            Decimal::from_str_lenient("123.456e2").unwrap().to_string(),
            "12345.6"
        );
        assert_eq!(
            Decimal::from_str_lenient("-2.5e1").unwrap().to_string(),
            "-25"
        );
        assert_eq!(
            Decimal::from_str_lenient("1000.5").unwrap().to_string(),
            "1000.5"
        );
    }

    #[test]
    fn from_f64_matches_python_str_coercion() {
        // Python: Decimal(str(0.1)) == Decimal("0.1"); str(0.1) == "0.1".
        assert_eq!(Decimal::from_f64(0.1).unwrap().to_string(), "0.1");
        // Python: str(0.30000000000000004) == "0.30000000000000004".
        assert_eq!(
            Decimal::from_f64(0.30000000000000004).unwrap().to_string(),
            "0.30000000000000004"
        );
        assert_eq!(Decimal::from_f64(1000.5).unwrap().to_string(), "1000.5");
        assert_eq!(Decimal::from_f64(-2.25).unwrap().to_string(), "-2.25");
        assert_eq!(Decimal::from_f64(f64::NAN), Err(DecimalError::NotFinite));
        assert_eq!(
            Decimal::from_f64(f64::INFINITY),
            Err(DecimalError::NotFinite)
        );
    }

    #[test]
    fn add_sub_neg() {
        let a = Decimal::from_str_strict("1.5").unwrap();
        let b = Decimal::from_str_strict("0.25").unwrap();
        assert_eq!((a + b).to_string(), "1.75");
        assert_eq!((a - b).to_string(), "1.25");
        assert_eq!((-a).to_string(), "-1.5");
    }

    #[test]
    fn mul_exact() {
        let a = Decimal::from_str_strict("1.5").unwrap();
        let b = Decimal::from_str_strict("2").unwrap();
        assert_eq!((a * b).to_string(), "3");
        let px = Decimal::from_str_strict("0.00012").unwrap();
        let sz = Decimal::from_str_strict("1000").unwrap();
        assert_eq!((px * sz).to_string(), "0.12");
    }

    #[test]
    fn mul_rounds_half_up() {
        // 0.5 × 0.5 = 0.25 exactly representable.
        let a = Decimal::from_str_strict("0.5").unwrap();
        assert_eq!((a * a).to_string(), "0.25");
        // 1/3 × 3 = 1 (0.333333333333333333 × 3 = 0.999999999999999999).
        let third = Decimal::from_str_strict("0.333333333333333333").unwrap();
        assert_eq!(
            (third * Decimal::from_i128(3)).to_string(),
            "0.999999999999999999"
        );
    }

    #[test]
    fn div_exact() {
        let a = Decimal::from_str_strict("1").unwrap();
        let b = Decimal::from_str_strict("4").unwrap();
        assert_eq!((a / b).to_string(), "0.25");
        assert_eq!(
            (Decimal::from_i128(10) / Decimal::from_i128(2)).to_string(),
            "5"
        );
        let notional = Decimal::from_str_strict("0.12").unwrap();
        let price = Decimal::from_str_strict("0.00012").unwrap();
        assert_eq!((notional / price).to_string(), "1000");
    }

    #[test]
    fn div_rounds_half_up() {
        // 1/3 rounds to 0.333333333333333333 (18 places, up).
        let one = Decimal::from_i128(1);
        let three = Decimal::from_i128(3);
        assert_eq!((one / three).to_string(), "0.333333333333333333");
    }

    #[test]
    fn quantize_truncates() {
        let v = Decimal::from_str_strict("1.23456789").unwrap();
        assert_eq!(v.quantize(4).to_string(), "1.2345");
        assert_eq!(v.quantize(2).to_string(), "1.23");
        assert_eq!(v.quantize(8).to_string(), "1.23456789");
    }

    #[test]
    fn floor_ceil_to_step() {
        let step = Decimal::from_str_strict("0.01").unwrap();
        let v = Decimal::from_str_strict("100.015").unwrap();
        assert_eq!(v.floor_to_step(step).to_string(), "100.01");
        assert_eq!(v.ceil_to_step(step).to_string(), "100.02");
        let neg = Decimal::from_str_strict("-100.015").unwrap();
        assert_eq!(neg.floor_to_step(step).to_string(), "-100.02");
        assert_eq!(neg.ceil_to_step(step).to_string(), "-100.01");
        // Exact step values pass through.
        let exact = Decimal::from_str_strict("100.01").unwrap();
        assert_eq!(exact.floor_to_step(step).to_string(), "100.01");
        assert_eq!(exact.ceil_to_step(step).to_string(), "100.01");
    }

    #[test]
    fn clamp_bounds() {
        let lo = Decimal::from_str_strict("1").unwrap();
        let hi = Decimal::from_str_strict("3").unwrap();
        assert_eq!(Decimal::from_str_strict("0.5").unwrap().clamp(lo, hi), lo);
        assert_eq!(
            Decimal::from_str_strict("2")
                .unwrap()
                .clamp(lo, hi)
                .to_string(),
            "2"
        );
        assert_eq!(Decimal::from_str_strict("5").unwrap().clamp(lo, hi), hi);
    }

    #[test]
    fn comparison_and_ord() {
        let a = Decimal::from_str_strict("1.5").unwrap();
        let b = Decimal::from_str_strict("1.50").unwrap();
        let c = Decimal::from_str_strict("2").unwrap();
        assert_eq!(a, b); // 1.5 == 1.50
        assert!(a < c);
        assert_eq!(a.min(c), a);
        assert_eq!(a.max(c), c);
    }

    #[test]
    fn as_tuple_normalizes() {
        // Note: unlike Python's `Decimal.as_tuple()` (which retains the source
        // digit string), the fixed-point representation only remembers the
        // normalized value — trailing zeros are stripped to the integer part.
        let v = Decimal::from_str_strict("120.00").unwrap();
        assert_eq!(v.as_tuple(), (120, 0)); // 120 × 10^0
        let w = Decimal::from_str_strict("0.0015").unwrap();
        assert_eq!(w.as_tuple(), (15, -4)); // 15 × 10^-4
        let x = Decimal::from_str_strict("1.5").unwrap();
        assert_eq!(x.as_tuple(), (15, -1));
        assert_eq!(Decimal::ZERO.as_tuple(), (0, -18));
    }

    #[test]
    fn round_python_prec28() {
        // 20 int + 18 frac = 38 significant digits, exactly representable.
        let long = Decimal::from_str_strict("99999999999999999999.999999999999999999").unwrap();
        // Rounding to 28 significant digits carries: 38 nines -> 1×10^20.
        assert_eq!(
            long.round_python_prec28().to_string(),
            "100000000000000000000"
        );
        // 19 significant digits is already within 28 -> unchanged.
        let within = Decimal::from_str_strict("1.234567890123456789").unwrap();
        assert_eq!(
            within.round_python_prec28().to_string(),
            "1.234567890123456789"
        );
        // 30 significant digits, dropped part "95" > half -> round up.
        let up = Decimal::from_str_strict("123456789012.345678901234567895").unwrap();
        assert_eq!(
            up.round_python_prec28().to_string(),
            "123456789012.3456789012345679"
        );
    }

    #[test]
    fn serde_roundtrip() {
        let v = Decimal::from_str_strict("123.4500").unwrap();
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "\"123.45\"");
        let back: Decimal = serde_json::from_str(&s).unwrap();
        assert_eq!(back, v);
        // Deserialize rejects a non-decimal-string.
        assert!(serde_json::from_str::<Decimal>("\"1e5\"").is_err());
    }

    #[test]
    fn semantic_wrappers() {
        let p: Price = Price::from_raw(Decimal::from_str_strict("100.5").unwrap().raw());
        assert_eq!(p.to_string(), "100.5");
        assert_eq!(*p, Decimal::from_str_strict("100.5").unwrap());
        let s: Size = Size::from_raw(Decimal::from_str_strict("1.25").unwrap().raw());
        assert_eq!(s.to_string(), "1.25");
    }

    #[test]
    fn max_range_value_roundtrips() {
        // The value parses exactly; Display applies Python's 28-sig-digit
        // normalize rounding and then fixed-point expansion.
        let big = "99999999999999999999.999999999999999999";
        let d = Decimal::from_str_strict(big).unwrap();
        assert_eq!(d.to_string(), "100000000000000000000");
        let neg = "-99999999999999999999.999999999999999999";
        assert_eq!(
            Decimal::from_str_strict(neg).unwrap().to_string(),
            "-100000000000000000000"
        );
    }

    // --- H-DM1: div at i128::MIN must not flip the sign ---

    #[test]
    fn div_min_by_one_is_min() {
        // Regression: `unsigned_abs() as i128` wrapped 2^127 to a negative
        // value and flipped the sign of the quotient.
        let q = Decimal::MIN.div(Decimal::from_i128(1));
        assert_eq!(q.raw(), i128::MIN);
        assert_eq!(q, Decimal::MIN);
    }

    #[test]
    fn div_min_by_two_truncates_toward_zero() {
        // MIN / 2 == -2^126 == i128::MIN / 2 (exact, no rounding involved).
        let q = Decimal::MIN.div(Decimal::from_i128(2));
        assert_eq!(q.raw(), i128::MIN / 2);
        assert!(q.is_negative());
    }

    #[test]
    fn div_min_by_neg_two_sign_correct() {
        // MIN / -2 must be positive (sign = (-) x (-)).
        let q = Decimal::MIN.div(Decimal::from_i128(-2));
        assert_eq!(q.raw(), -(i128::MIN / 2));
        assert!(q.is_positive());
    }

    #[test]
    fn div_by_one_is_identity_at_bounds() {
        assert_eq!(Decimal::MAX.div(Decimal::ONE).raw(), i128::MAX);
        assert_eq!(Decimal::MIN.div(Decimal::ONE).raw(), i128::MIN);
        assert_eq!(Decimal::from_raw(12345).div(Decimal::ONE).raw(), 12345);
    }

    // --- H-DM2: abs() must never return a negative value ---

    #[test]
    fn abs_min_saturates_to_max() {
        let a = Decimal::MIN.abs();
        assert!(a.raw() >= 0, "abs(MIN) must be non-negative");
        assert_eq!(a, Decimal::MAX, "abs(MIN) saturates to MAX");
        assert_eq!(Decimal::MAX.abs(), Decimal::MAX);
        assert_eq!(Decimal::ZERO.abs(), Decimal::ZERO);
        assert_eq!(Decimal::from_i128(-42).abs().to_string(), "42");
    }

    #[test]
    fn round_to_places_min_no_garbage() {
        // Must not panic (i128::abs on MIN): rounding MIN at 2 places keeps
        // the sign and a magnitude that is a multiple of the rounding step.
        let r = Decimal::MIN.round_to_places(2);
        assert!(r.raw() <= 0);
        assert_eq!(r.raw() % 10_000_000_000_000_000, 0); // multiple of 10^16
        // And the same magnitude rounding on the positive side.
        let p = Decimal::MAX.round_to_places(2);
        assert!(p.raw() > 0);
        assert_eq!(p.raw() % 10_000_000_000_000_000, 0);
    }

    // --- H-DM3: floor/ceil must not overflow at the extremes ---

    #[test]
    fn floor_ceil_extremes_saturate() {
        let fl = Decimal::MIN.floor();
        let ce = Decimal::MAX.ceil();
        // No panic, no wrap: the results are ordered against the extremes and
        // are integral (a wrapped result would not be).
        assert!(fl <= Decimal::MIN);
        assert!(Decimal::MAX <= ce);
        assert_eq!(fl, Decimal::MIN); // saturated floor
        assert_eq!(ce, Decimal::MAX); // saturated ceil
        // Mid-range still behaves exactly.
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
    }

    // --- M-DM4: lenient exponent cap (DoS guard) ---

    #[test]
    fn lenient_rejects_absurd_exponents() {
        // Huge exponents must fail fast instead of looping ~2^31 times.
        for bad in [
            "1e2147483647",
            "-1e2147483647",
            "1e-2147483648",
            "1e50",
            "1e-50",
        ] {
            assert!(
                Decimal::from_str_lenient(bad).is_err(),
                "should reject {bad:?}"
            );
        }
        // Exponents within the cap still behave; the significant-digit gate
        // rejects what cannot fit NUMERIC(38,18).
        assert!(Decimal::from_str_lenient("1e40").is_err()); // 41 digits
        assert_eq!(Decimal::from_str_lenient("1e-40").unwrap().to_string(), "0");
        assert_eq!(
            Decimal::from_str_lenient("1e20").unwrap().to_string(),
            "100000000000000000000"
        );
    }

    #[test]
    fn lenient_truncates_excess_fractional_digits() {
        // Documented lenient behavior: > 18 fractional digits truncate (the
        // strict parser is the API gate and rejects them).
        assert_eq!(
            Decimal::from_str_lenient("1.0000000000000000001").unwrap(),
            Decimal::from_i128(1)
        );
        assert_eq!(
            Decimal::from_str_lenient("0.1234567890123456789").unwrap(),
            Decimal::from_str_strict("0.123456789012345678").unwrap()
        );
    }

    #[test]
    fn lenient_accepts_plus_and_dot_shapes() {
        // M-DM5: the lenient entry deliberately accepts shapes the strict
        // parser rejects (+1, .5); documented contract, not a bug.
        assert_eq!(
            Decimal::from_str_lenient("+1").unwrap(),
            Decimal::from_i128(1)
        );
        assert_eq!(
            Decimal::from_str_lenient(".5").unwrap(),
            Decimal::from_str_strict("0.5").unwrap()
        );
    }

    // --- M-DM3: serde round-trip contract for 29..=38 significant digits ---

    #[test]
    fn serde_roundtrip_normalizes_28_sig_digits() {
        // The wire contract is the 28-significant-digit `decimal_string`
        // form: Serialize normalizes, Deserialize accepts 38. A round-trip of
        // a 38-sig-digit value therefore is not identity; the lossless
        // storage boundary is `to_exact_string`.
        let v = Decimal::from_str_strict("99999999999999999999.999999999999999999").unwrap();
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "\"100000000000000000000\"");
        let back: Decimal = serde_json::from_str(&s).unwrap();
        assert_eq!(back.to_string(), "100000000000000000000");
        assert_ne!(back, v, "28-sig normalize is lossy by contract");
        assert_eq!(
            v.to_exact_string(),
            "99999999999999999999.999999999999999999"
        );
        // 29 significant digits (within the 20-int + 18-frac envelope):
        // serde round-trip normalizes to 28, exact-string round-trips.
        let v29 = Decimal::from_str_strict("12345678901234567890.123456789").unwrap();
        assert_ne!(
            serde_json::from_str::<Decimal>(&serde_json::to_string(&v29).unwrap()).unwrap(),
            v29
        );
        assert_eq!(
            Decimal::from_str_strict(&v29.to_exact_string()).unwrap(),
            v29,
            "to_exact_string is the lossless round-trip boundary"
        );
    }

    proptest! {
        #[test]
        fn mul_by_one_is_identity(v in any::<i64>()) {
            let d = Decimal::from_i128(v as i128);
            assert_eq!((d * Decimal::ONE).to_string(), d.to_string());
        }

        #[test]
        fn add_commutes(a in any::<i64>(), b in any::<i64>()) {
            let da = Decimal::from_i128(a as i128);
            let db = Decimal::from_i128(b as i128);
            assert_eq!(da + db, db + da);
        }

        // --- H-DM1..3: boundary-safety properties over the full i128 range ---

        #[test]
        fn div_sign_matches_operands(a in any::<i128>(), b in any::<i128>()) {
            let da = Decimal::from_raw(a);
            let db = Decimal::from_raw(b);
            prop_assume!(!db.is_zero());
            let q = da.div(db);
            if a == 0 {
                prop_assert!(q.is_zero());
            } else {
                // Saturation only clamps magnitude; it never flips the sign.
                // (A quotient may underflow to zero when |a/b| < 1e-18.)
                let expected = (a.signum() * b.signum()) as i8;
                prop_assert!(
                    q.is_zero() || q.signum() == expected,
                    "div sign mismatch for raw a={a} b={b}: q.raw={}",
                    q.raw()
                );
            }
        }

        #[test]
        fn abs_is_never_negative(a in any::<i128>()) {
            let d = Decimal::from_raw(a);
            prop_assert!(d.abs().raw() >= 0, "abs({a}) must be non-negative");
        }

        #[test]
        fn floor_ceil_ordered_and_integral(a in any::<i128>()) {
            let d = Decimal::from_raw(a);
            let fl = d.floor();
            let ce = d.ceil();
            prop_assert!(fl <= d && d <= ce, "floor <= d <= ceil for raw {a}");
            // Integral in raw units (multiple of SCALE), except when the true
            // result saturates at the extremes.
            prop_assert!(
                fl.raw() % SCALE_I128 == 0 || fl == Decimal::MIN,
                "floor({a}) must be integral or saturated"
            );
            prop_assert!(
                ce.raw() % SCALE_I128 == 0 || ce == Decimal::MAX,
                "ceil({a}) must be integral or saturated"
            );
            prop_assert!(
                ce.raw().saturating_sub(fl.raw()) <= SCALE_I128,
                "ceil - floor must be at most one unit for raw {a}"
            );
        }

        #[test]
        fn div_roundtrip_recovers_dividend(a in any::<i64>(), b in any::<i64>()) {
            let da = Decimal::from_i128(a as i128);
            let db = Decimal::from_i128(b as i128);
            prop_assume!(!db.is_zero());
            // q = a/b at scale 18; q * b must land within half a unit of a.
            let q = da.div(db);
            let back = q * db;
            let err = (back - da).abs();
            prop_assert!(
                err <= db.abs(),
                "div/mul roundtrip error too large for {a}/{b}: {err} (db={})",
                db
            );
        }
    }
}
