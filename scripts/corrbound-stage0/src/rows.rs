// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Result-row extraction and tolerant comparison.
//!
//! `ORDER BY score DESC LIMIT 10` can order ties in more than one way, and
//! average values are floating point, so the probe compares result sets with a
//! relative/absolute float tolerance and a deterministic tie order instead of
//! comparing raw strings.

use arrow::array::RecordBatch as ArrowRecordBatch;
use arrow::util::display::array_value_to_string;

/// One output row of the fixture query: `(host, zone, avg(value * weight))`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultRow {
    pub host: String,
    pub zone: String,
    pub score: f64,
    pub raw: Vec<String>,
}

pub fn extract_rows(batches: &[ArrowRecordBatch]) -> Result<Vec<ResultRow>, String> {
    let mut rows = Vec::new();
    for batch in batches {
        if batch.num_columns() != 3 {
            return Err(format!(
                "expected the fixture query to return 3 columns, got {}",
                batch.num_columns()
            ));
        }
        for index in 0..batch.num_rows() {
            let raw: Vec<String> = (0..batch.num_columns())
                .map(|column| {
                    array_value_to_string(batch.column(column), index)
                        .unwrap_or_else(|err| format!("<error:{err}>"))
                })
                .collect();
            let score = raw[2]
                .trim()
                .parse::<f64>()
                .map_err(|err| format!("score `{}` is not numeric: {err}", raw[2]))?;
            rows.push(ResultRow {
                host: raw[0].clone(),
                zone: raw[1].clone(),
                score,
                raw,
            });
        }
    }
    Ok(rows)
}

/// Sorts rows into a canonical order: score descending, then `(host, zone)`.
pub fn canonical(rows: &[ResultRow]) -> Vec<ResultRow> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.host.cmp(&b.host))
            .then_with(|| a.zone.cmp(&b.zone))
    });
    sorted
}

fn tolerance_for(a: f64, b: f64, abs_tol: f64, rel_tol: f64) -> f64 {
    abs_tol + rel_tol * a.abs().max(b.abs())
}

/// Groups scores that agree within tolerance into buckets.
///
/// `ORDER BY score DESC LIMIT n` can return rows whose scores differ only by
/// floating-point noise in either order, so rows are compared per tolerance
/// bucket rather than positionally.
fn score_buckets(rows: &[ResultRow], abs_tol: f64, rel_tol: f64) -> Vec<(f64, Vec<String>)> {
    let mut buckets: Vec<(f64, Vec<String>)> = Vec::new();
    for row in canonical(rows) {
        let key = format!("{}|{}", row.host, row.zone);
        match buckets.last_mut() {
            Some((seed, keys))
                if (row.score - *seed).abs()
                    <= tolerance_for(*seed, row.score, abs_tol, rel_tol) =>
            {
                keys.push(key);
            }
            _ => buckets.push((row.score, vec![key])),
        }
    }
    for (_, keys) in &mut buckets {
        keys.sort();
    }
    buckets
}

/// Compares two result sets with float tolerance and tolerance-bucketed ties.
pub fn compare_results(
    left: &[ResultRow],
    right: &[ResultRow],
    abs_tol: f64,
    rel_tol: f64,
) -> (bool, String) {
    if left.len() != right.len() {
        return (
            false,
            format!("row count differs: {} vs {}", left.len(), right.len()),
        );
    }
    let left_buckets = score_buckets(left, abs_tol, rel_tol);
    let right_buckets = score_buckets(right, abs_tol, rel_tol);
    if left_buckets.len() != right_buckets.len() {
        return (
            false,
            format!(
                "number of distinct score groups differs: {} vs {} ({:?} vs {:?})",
                left_buckets.len(),
                right_buckets.len(),
                left_buckets
                    .iter()
                    .map(|(score, keys)| format!("{score}:{keys:?}"))
                    .collect::<Vec<_>>(),
                right_buckets
                    .iter()
                    .map(|(score, keys)| format!("{score}:{keys:?}"))
                    .collect::<Vec<_>>(),
            ),
        );
    }
    for (index, ((left_score, left_keys), (right_score, right_keys))) in
        left_buckets.iter().zip(right_buckets.iter()).enumerate()
    {
        if left_keys != right_keys {
            return (
                false,
                format!(
                    "score group {index} covers different keys: {left_keys:?} vs {right_keys:?} \
                     (scores {left_score} vs {right_score})"
                ),
            );
        }
        let tolerance = tolerance_for(*left_score, *right_score, abs_tol, rel_tol);
        if (left_score - right_score).abs() > tolerance {
            return (
                false,
                format!(
                    "score group {index} score differs: {left_score} vs {right_score} \
                     (tolerance {tolerance})"
                ),
            );
        }
    }
    (
        true,
        format!(
            "{} rows in {} score group(s) identical within abs_tol={abs_tol}, rel_tol={rel_tol}",
            left.len(),
            left_buckets.len()
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(rows: &[(&str, &str, f64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("zone", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let hosts: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let zones: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        ));
        let scores: ArrayRef = Arc::new(Float64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, vec![hosts, zones, scores]).unwrap()
    }

    fn rows(rows: &[(&str, &str, f64)]) -> Vec<ResultRow> {
        extract_rows(&[batch(rows)]).unwrap()
    }

    #[test]
    fn extraction_and_tolerance() {
        let base = rows(&[("host00", "zone1", 1.0), ("host01", "zone2", 2.0)]);
        assert_eq!(base.len(), 2);
        // tiny float differences are accepted
        let other = rows(&[("host00", "zone1", 1.0 + 1e-12), ("host01", "zone2", 2.0)]);
        let (ok, detail) = compare_results(&base, &other, 1e-9, 1e-9);
        assert!(ok, "{detail}");
        // real differences are rejected
        let bad = rows(&[("host00", "zone1", 1.5), ("host01", "zone2", 2.0)]);
        let (ok, _) = compare_results(&base, &bad, 1e-9, 1e-9);
        assert!(!ok);
    }

    #[test]
    fn tie_order_does_not_matter() {
        let a = rows(&[("host00", "zone1", 5.0), ("host01", "zone1", 5.0)]);
        let b = rows(&[("host01", "zone1", 5.0), ("host00", "zone1", 5.0)]);
        let (ok, detail) = compare_results(&a, &b, 1e-9, 1e-9);
        assert!(ok, "{detail}");
    }

    /// A near-tie ordered differently (the shape a `LIMIT` sees when two scores
    /// differ only by float noise) must still compare equal.
    #[test]
    fn near_tie_ordering_does_not_matter() {
        let a = rows(&[
            ("host07", "zone2", 1302.888_888_888_889),
            ("host07", "zone1", 1302.888_888_888_888_7),
        ]);
        let b = rows(&[
            ("host07", "zone1", 1302.888_888_888_888_7),
            ("host07", "zone2", 1302.888_888_888_889),
        ]);
        let (ok, detail) = compare_results(&a, &b, 1e-9, 1e-12);
        assert!(ok, "{detail}");
    }

    /// Different score groups with different keys must not compare equal.
    #[test]
    fn different_keys_are_rejected() {
        let a = rows(&[("host07", "zone1", 10.0), ("host07", "zone1", 5.0)]);
        let b = rows(&[("host07", "zone2", 10.0), ("host07", "zone1", 5.0)]);
        let (ok, _) = compare_results(&a, &b, 1e-9, 1e-12);
        assert!(!ok);
    }
}
