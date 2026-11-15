//! Databend's types as SpelaionDB's logical types, and a result's rows built from the values the
//! HTTP API sends: every value is a string (or `null`), typed by its column.
//!
//! - `Nullable(T)` is `T`; `NULL` (the type of a bare `null`) is `Text`;
//! - `Boolean` is `Bool` (sent as `1` or `0`);
//! - `Int8` to `Int64` and `UInt8` to `UInt32` are `Int`; `UInt64` is `Decimal`, kept as the
//!   server's text, because it does not fit in a signed 64-bit integer;
//! - `Float32` and `Float64` are `Float` (`NaN`, `inf` and `-inf` included);
//! - `Decimal(p, s)` is `Decimal`, kept as the server's text;
//! - `String` is `Text`; `Binary` is `Bytes` (sent in hexadecimal);
//! - `Date` is `Date`; `Timestamp` is `Timestamp`, to the microsecond, as the session's time
//!   zone writes it (Databend keeps no zone with the value);
//! - `Variant` is `Json`; `Array(…)` is `Array`;
//! - everything else (`Tuple`, `Map`, `Interval`, `Bitmap`, `Geometry`, `Geography`, `Vector`…)
//!   is `Native`, its text as the server writes it.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use serde_json::Value as Json;
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// `Nullable(T)` → `T`, any number of times.
fn unwrap_nullable(native: &str) -> &str {
    let mut t = native.trim();
    while let Some(inner) = t
        .strip_prefix("Nullable(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        t = inner.trim();
    }
    t
}

/// The base name of a type: `Decimal(10, 2)` → `Decimal`.
fn base(native: &str) -> &str {
    let t = unwrap_nullable(native);
    match t.find('(') {
        Some(open) => t[..open].trim(),
        None => t,
    }
}

/// The logical type of a type, as the HTTP API names it.
pub fn logical(native: &str) -> LogicalType {
    match base(native) {
        "Boolean" => LogicalType::Bool,
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" => LogicalType::Int,
        "UInt64" | "Decimal" => LogicalType::Decimal,
        "Float32" | "Float64" => LogicalType::Float,
        "String" | "NULL" => LogicalType::Text,
        "Binary" => LogicalType::Bytes,
        "Date" => LogicalType::Date,
        "Timestamp" => LogicalType::Timestamp,
        "Variant" => LogicalType::Json,
        "Array" => LogicalType::Array,
        _ => LogicalType::Native,
    }
}

pub fn column(name: String, native_type: String) -> ColumnMeta {
    ColumnMeta {
        name,
        logical: logical(&native_type),
        native_type,
        native_id: None,
        origin: None,
    }
}

enum Builder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Bytes(BinaryBuilder),
    Text(StringBuilder),
}

/// The rows of one result, a batch at a time.
pub struct Batches {
    schema: Arc<ResultSchema>,
    builders: Vec<Builder>,
    rows: usize,
}

impl Batches {
    pub fn new(schema: Arc<ResultSchema>) -> Self {
        let builders = fresh(&schema);
        Self {
            schema,
            builders,
            rows: 0,
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// One row as the API sends it.
    pub fn push(&mut self, row: &[Json]) -> Result<()> {
        if row.len() != self.builders.len() {
            return Err(DriverError::Other(format!(
                "a row of {} values in a result of {} columns",
                row.len(),
                self.builders.len()
            )));
        }
        for ((builder, value), column) in
            self.builders.iter_mut().zip(row).zip(&self.schema.columns)
        {
            let bad = || DriverError::Decode {
                type_name: column.native_type.clone(),
                message: format!("{value} is not a value of this type"),
            };
            let text = match value {
                Json::Null => {
                    match builder {
                        Builder::Bool(b) => b.append_null(),
                        Builder::Int(b) => b.append_null(),
                        Builder::Float(b) => b.append_null(),
                        Builder::Date(b) => b.append_null(),
                        Builder::Timestamp(b) => b.append_null(),
                        Builder::Bytes(b) => b.append_null(),
                        Builder::Text(b) => b.append_null(),
                    }
                    continue;
                }
                Json::String(s) => s.as_str(),
                _ => return Err(bad()),
            };
            match builder {
                Builder::Bool(b) => b.append_value(match text {
                    "1" | "true" => true,
                    "0" | "false" => false,
                    _ => return Err(bad()),
                }),
                Builder::Int(b) => b.append_value(text.parse().map_err(|_| bad())?),
                Builder::Float(b) => b.append_value(float(text).ok_or_else(bad)?),
                Builder::Date(b) => b.append_value(days(text).ok_or_else(bad)?),
                Builder::Timestamp(b) => b.append_value(micros(text).ok_or_else(bad)?),
                Builder::Bytes(b) => b.append_value(unhex(text).ok_or_else(bad)?),
                Builder::Text(b) => b.append_value(text),
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows pushed since the last batch, as one batch; `None` when there are none.
    pub fn take(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let builders = std::mem::replace(&mut self.builders, fresh(&self.schema));
        self.rows = 0;
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|builder| -> ArrayRef {
                match builder {
                    Builder::Bool(mut b) => Arc::new(b.finish()),
                    Builder::Int(mut b) => Arc::new(b.finish()),
                    Builder::Float(mut b) => Arc::new(b.finish()),
                    Builder::Date(mut b) => Arc::new(b.finish()),
                    Builder::Timestamp(mut b) => Arc::new(b.finish()),
                    Builder::Bytes(mut b) => Arc::new(b.finish()),
                    Builder::Text(mut b) => Arc::new(b.finish()),
                }
            })
            .collect();
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map(Some)
            .map_err(|e| DriverError::Other(format!("building a batch: {e}")))
    }
}

fn fresh(schema: &ResultSchema) -> Vec<Builder> {
    schema
        .columns
        .iter()
        .map(|column| match column.logical {
            LogicalType::Bool => Builder::Bool(BooleanBuilder::new()),
            LogicalType::Int => Builder::Int(Int64Builder::new()),
            LogicalType::Float => Builder::Float(Float64Builder::new()),
            LogicalType::Date => Builder::Date(Date32Builder::new()),
            LogicalType::Timestamp => Builder::Timestamp(TimestampMicrosecondBuilder::new()),
            LogicalType::Bytes => Builder::Bytes(BinaryBuilder::new()),
            LogicalType::Decimal
            | LogicalType::Text
            | LogicalType::Time
            | LogicalType::TimestampTz
            | LogicalType::Uuid
            | LogicalType::Json
            | LogicalType::Interval
            | LogicalType::Array
            | LogicalType::Native => Builder::Text(StringBuilder::new()),
        })
        .collect()
}

/// A float as Databend writes it: a number, `NaN`, `inf` or `-inf`.
fn float(text: &str) -> Option<f64> {
    match text {
        "NaN" | "nan" => Some(f64::NAN),
        "inf" | "Infinity" => Some(f64::INFINITY),
        "-inf" | "-Infinity" => Some(f64::NEG_INFINITY),
        _ => text.parse().ok(),
    }
}

/// Hexadecimal text back to bytes (`6162` → `ab`); `None` for anything that is not.
fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

/// Microseconds since 1970-01-01 00:00 of `YYYY-MM-DD HH:MM:SS[.fraction]`; digits past the
/// sixth are dropped.
fn micros(text: &str) -> Option<i64> {
    let (date, time) = text.split_once(' ')?;
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, fraction),
        None => (time, ""),
    };
    let mut parts = clock.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if h > 23 || m > 59 || s > 60 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits: String = fraction
        .chars()
        .chain(std::iter::repeat('0'))
        .take(6)
        .collect();
    let fraction: i64 = digits.parse().ok()?;
    let seconds = i64::from(days(date)?) * 86_400 + h * 3_600 + m * 60 + s;
    seconds.checked_mul(1_000_000)?.checked_add(fraction)
}

/// Days since 1970-01-01 of `YYYY-MM-DD` (a year may have more digits or a sign).
fn days(text: &str) -> Option<i32> {
    let (sign, rest) = match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text),
    };
    let mut parts = rest.splitn(3, '-');
    let y: i64 = sign * parts.next()?.parse::<i64>().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let d: i64 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i32::try_from(era * 146_097 + doe - 719_468).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_map_to_logical_types() {
        for (native, logical_type) in [
            ("Boolean", LogicalType::Bool),
            ("Nullable(Boolean)", LogicalType::Bool),
            ("Int8", LogicalType::Int),
            ("UInt32", LogicalType::Int),
            ("Int64", LogicalType::Int),
            ("UInt64", LogicalType::Decimal),
            ("Float32", LogicalType::Float),
            ("Decimal(10, 2)", LogicalType::Decimal),
            ("Nullable(Decimal(38, 10))", LogicalType::Decimal),
            ("String", LogicalType::Text),
            ("NULL", LogicalType::Text),
            ("Binary", LogicalType::Bytes),
            ("Date", LogicalType::Date),
            ("Timestamp", LogicalType::Timestamp),
            ("Variant", LogicalType::Json),
            ("Array(UInt8)", LogicalType::Array),
            ("Nullable(Array(Nullable(String)))", LogicalType::Array),
            ("Tuple(UInt8, String)", LogicalType::Native),
            ("Map(String, UInt8)", LogicalType::Native),
            ("Interval", LogicalType::Native),
            ("Bitmap", LogicalType::Native),
        ] {
            assert_eq!(logical(native), logical_type, "{native}");
        }
    }

    #[test]
    fn dates_count_days_from_1970() {
        assert_eq!(days("1970-01-01"), Some(0));
        assert_eq!(days("2000-03-01"), Some(11_017));
        assert_eq!(days("1969-12-31"), Some(-1));
        assert_eq!(days("x"), None);
    }

    #[test]
    fn timestamps_count_microseconds_from_1970() {
        assert_eq!(micros("1970-01-01 00:00:00.000000"), Some(0));
        assert_eq!(micros("1970-01-01 00:00:01"), Some(1_000_000));
        assert_eq!(
            micros("2024-02-29 10:20:30.123456"),
            Some(1_709_202_030_123_456)
        );
        assert_eq!(micros("1969-12-31 23:59:59.5"), Some(-500_000));
        assert_eq!(
            micros("2024-02-29 10:20:30.1234567"),
            Some(1_709_202_030_123_456)
        );
        for bad in [
            "2024-02-29",
            "2024-02-29 25:00:00",
            "2024-02-29 10:20:30.x",
            "x y",
        ] {
            assert_eq!(micros(bad), None, "{bad}");
        }
    }

    #[test]
    fn hex_and_floats_read_as_databend_writes_them() {
        assert_eq!(unhex("6162").as_deref(), Some(&b"ab"[..]));
        assert_eq!(unhex("").as_deref(), Some(&b""[..]));
        assert_eq!(unhex("616"), None);
        assert_eq!(unhex("zz"), None);
        assert!(float("NaN").unwrap().is_nan());
        assert_eq!(float("inf"), Some(f64::INFINITY));
        assert_eq!(float("-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(float("1e300"), Some(1e300));
        assert_eq!(float("x"), None);
    }

    #[test]
    fn rows_build_from_text() {
        let schema = Arc::new(ResultSchema::new(vec![
            column("b".into(), "Boolean".into()),
            column("n".into(), "Int64".into()),
            column("f".into(), "Float64".into()),
            column("d".into(), "Date".into()),
            column("v".into(), "Binary".into()),
            column("a".into(), "Array(UInt8)".into()),
            column("u".into(), "UInt64".into()),
        ]));
        let mut batches = Batches::new(schema);
        let row: Vec<Json> = serde_json::from_str(
            r#"["1", "-7", "NaN", "2024-02-29", "6162", "[1,2]", "18446744073709551615"]"#,
        )
        .unwrap();
        batches.push(&row).unwrap();
        let nulls: Vec<Json> =
            serde_json::from_str("[null, null, null, null, null, null, null]").unwrap();
        batches.push(&nulls).unwrap();
        let batch = batches.take().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        for bad in [
            r#"["2", "1", "1", "2024-02-29", "61", "[]", "1"]"#,
            r#"["1", "x", "1", "2024-02-29", "61", "[]", "1"]"#,
            r#"["1", "1", "1", "2024-02-29", "6", "[]", "1"]"#,
            r#"[true, "1", "1", "2024-02-29", "61", "[]", "1"]"#,
        ] {
            let row: Vec<Json> = serde_json::from_str(bad).unwrap();
            assert!(batches.push(&row).is_err(), "{bad}");
        }
    }
}
