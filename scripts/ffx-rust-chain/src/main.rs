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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Float64Array, Int64Array, ListArray, RecordBatch};
use arrow::buffer::OffsetBuffer;
use arrow::compute::filter;
use arrow::compute::kernels::aggregate::{max, min, sum, sum_checked};
use arrow::compute::kernels::numeric::add;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::{ArrowError, Result};

type Agg = BTreeMap<i64, (Option<i64>, i64, i64)>;
type RawGroup = (Vec<Option<i64>>, Vec<Option<i64>>);
type Row = (i64, Option<i64>, Option<i64>);
fn err(s: &str) -> ArrowError {
    ArrowError::ComputeError(s.into())
}
fn type_list() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
}
fn lists(b: &RecordBatch) -> Result<(Int64Array, ListArray, ListArray)> {
    if b.num_columns() != 3 || b.column(0).data_type() != &DataType::Int64 {
        return Err(err("expected k:Int64, A:List<Int64>, B:List<Int64>"));
    }
    let k = b
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| err("invalid key"))?
        .clone();
    let get = |n| -> Result<ListArray> {
        let a = b
            .column(n)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| err("expected List<Int64>"))?;
        if a.value_type() != DataType::Int64 {
            return Err(err("expected List<Int64>"));
        }
        Ok(a.clone())
    };
    let a = get(1)?;
    let c = get(2)?;
    let mut seen = BTreeSet::new();
    for i in 0..b.num_rows() {
        if k.is_null(i) || !seen.insert(k.value(i)) {
            return Err(err(
                "one complete, unique non-null key per input batch is required",
            ));
        }
        if a.is_null(i) || c.is_null(i) {
            return Err(err("parent lists must be non-null"));
        }
    }
    Ok((k, a, c))
}
fn child(a: &ListArray, i: usize) -> Result<Int64Array> {
    a.value(i)
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| err("list child must be Int64"))
}
fn projected(b: &RecordBatch) -> Result<RecordBatch> {
    let (k, a, c) = lists(b)?;
    let mut offsets = vec![0_i32];
    let mut parts = Vec::new();
    for i in 0..b.num_rows() {
        let v = child(&a, i)?;
        if c.value_length(i) == 0 {
            offsets.push(*offsets.last().unwrap());
            parts.push(Int64Array::from(Vec::<Option<i64>>::new()));
            continue;
        }
        let mask = arrow::compute::kernels::cmp::gt_eq(&v, &Int64Array::new_scalar(2))?;
        let f = filter(&v, &mask)?;
        let p = add(&f, &Int64Array::new_scalar(1))?;
        let p = p
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("projection result must be Int64"))?
            .clone();
        offsets.push(
            offsets
                .last()
                .copied()
                .unwrap()
                .checked_add(p.len() as i32)
                .ok_or_else(|| err("list offsets overflow"))?,
        );
        parts.push(p)
    }
    let refs = parts.iter().map(|x| x as &dyn Array).collect::<Vec<_>>();
    let vals = if refs.is_empty() {
        Int64Array::from(Vec::<Option<i64>>::new())
    } else {
        arrow::compute::concat(&refs)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("concat result must be Int64"))?
            .clone()
    };
    let pa = ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(vals),
        None,
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("A", type_list(), false),
            Field::new("B", c.data_type().clone(), false),
        ])),
        vec![Arc::new(k), Arc::new(pa), Arc::new(c)],
    )
}
fn aggregate(b: &RecordBatch) -> Result<Agg> {
    let (k, a, c) = lists(b)?;
    let mut out = BTreeMap::new();
    for i in 0..b.num_rows() {
        let x = child(&a, i)?;
        let y = child(&c, i)?;
        let nx = i64::try_from(x.len()).map_err(|_| err("COUNT overflow"))?;
        let ny = i64::try_from(y.len()).map_err(|_| err("COUNT overflow"))?;
        let n = nx.checked_mul(ny).ok_or_else(|| err("COUNT(*) overflow"))?;
        if n == 0 {
            continue;
        }
        let cx = i64::try_from(x.len() - x.null_count()).map_err(|_| err("COUNT overflow"))?;
        let cy = i64::try_from(y.len() - y.null_count()).map_err(|_| err("COUNT overflow"))?;
        let count_a = cx
            .checked_mul(ny)
            .ok_or_else(|| err("COUNT(A') overflow"))?;
        let sum = if cx == 0 || cy == 0 {
            None
        } else {
            let xmin = min(&x).ok_or_else(|| err("A min absent"))?;
            let xmax = max(&x).ok_or_else(|| err("A max absent"))?;
            let ymin = min(&y).ok_or_else(|| err("B min absent"))?;
            let ymax = max(&y).ok_or_else(|| err("B max absent"))?;
            let lo = xmin
                .checked_add(ymin)
                .ok_or_else(|| err("pair sum overflow"))?;
            let hi = xmax
                .checked_add(ymax)
                .ok_or_else(|| err("pair sum overflow"))?;
            let pairs = i128::from(lo)
                .abs()
                .max(i128::from(hi).abs())
                .checked_mul(i128::from(cx) * i128::from(cy))
                .ok_or_else(|| err("SUM bound overflow"))?;
            let safe = pairs <= i128::from(i64::MAX);
            let sx = sum_checked(&x).ok().flatten();
            let sy = sum_checked(&y).ok().flatten();
            let fast = if safe {
                sx.and_then(|sx| {
                    sy.and_then(|sy| {
                        sx.checked_mul(cy)
                            .and_then(|v| sy.checked_mul(cx).and_then(|w| v.checked_add(w)))
                    })
                })
            } else {
                None
            };
            Some(if let Some(v) = fast {
                v
            } else {
                let mut total = 0_i64;
                for xv in x.iter().flatten() {
                    for yv in y.iter().flatten() {
                        total = total
                            .checked_add(
                                xv.checked_add(yv).ok_or_else(|| err("pair sum overflow"))?,
                            )
                            .ok_or_else(|| err("SUM overflow"))?
                    }
                }
                total
            })
        };
        out.insert(k.value(i), (sum, n, count_a));
    }
    Ok(out)
}
fn flat(b: &RecordBatch, predicate: bool, transform: bool) -> Result<(Vec<Row>, Agg)> {
    let (k, a, c) = lists(b)?;
    let mut rows = Vec::new();
    let mut sums: BTreeMap<i64, (i64, i64, i64, bool)> = BTreeMap::new();
    for i in 0..b.num_rows() {
        for av in child(&a, i)?.iter() {
            for bv in child(&c, i)?.iter() {
                let projected = if transform {
                    av.filter(|v| *v >= 2)
                        .map(|v| v.checked_add(1).ok_or_else(|| err("projection overflow")))
                        .transpose()?
                } else {
                    av
                };
                if transform && projected.is_none() {
                    continue;
                }
                if predicate && !matches!((projected,bv),(Some(x),Some(y))if x<y) {
                    continue;
                }
                let key = k.value(i);
                rows.push((key, projected, bv));
                let entry = sums.entry(key).or_insert((0, 0, 0, false));
                entry.1 = entry
                    .1
                    .checked_add(1)
                    .ok_or_else(|| err("COUNT(*) overflow"))?;
                if projected.is_some() {
                    entry.2 = entry
                        .2
                        .checked_add(1)
                        .ok_or_else(|| err("COUNT(A') overflow"))?
                }
                if let (Some(x), Some(y)) = (projected, bv) {
                    entry.0 = entry
                        .0
                        .checked_add(x.checked_add(y).ok_or_else(|| err("pair sum overflow"))?)
                        .ok_or_else(|| err("SUM overflow"))?;
                    entry.3 = true
                }
            }
        }
    }
    rows.sort();
    let aggs = sums
        .into_iter()
        .map(|(k, (s, n, c, has_sum))| (k, (if has_sum { Some(s) } else { None }, n, c)))
        .collect();
    Ok((rows, aggs))
}
fn batch(
    keys: Vec<Option<i64>>,
    as_: Vec<Vec<Option<i64>>>,
    bs: Vec<Vec<Option<i64>>>,
) -> RecordBatch {
    fn la(rows: Vec<Vec<Option<i64>>>) -> ListArray {
        let mut off = vec![0_i32];
        let mut val = Vec::new();
        for r in rows {
            val.extend(r);
            off.push(val.len() as i32)
        }
        ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(off.into()),
            Arc::new(Int64Array::from(val)),
            None,
        )
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("A", type_list(), false),
            Field::new("B", type_list(), false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(la(as_)),
            Arc::new(la(bs)),
        ],
    )
    .unwrap()
}
fn raw_batch(keys: Vec<Option<i64>>, values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn raw_columns(batch: &RecordBatch) -> Result<(Int64Array, Int64Array)> {
    if batch.num_columns() != 2
        || batch.column(0).data_type() != &DataType::Int64
        || batch.column(1).data_type() != &DataType::Int64
    {
        return Err(err("expected key:Int64, value:Int64"));
    }
    Ok((
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("invalid raw key"))?
            .clone(),
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("invalid raw value"))?
            .clone(),
    ))
}

/// Raw SQL-shape approximation: equality INNER JOIN followed by GROUP BY key.
/// Payloads (including NULLs) and duplicates are retained in compact child lists; NULL keys do not join.
fn group_raw(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let (left_keys, left_values) = raw_columns(left)?;
    let (right_keys, right_values) = raw_columns(right)?;
    let mut groups: BTreeMap<i64, RawGroup> = BTreeMap::new();
    for i in 0..left.num_rows() {
        if !left_keys.is_null(i) {
            groups
                .entry(left_keys.value(i))
                .or_default()
                .0
                .push(left_values.is_valid(i).then(|| left_values.value(i)));
        }
    }
    for i in 0..right.num_rows() {
        if !right_keys.is_null(i) {
            groups
                .entry(right_keys.value(i))
                .or_default()
                .1
                .push(right_values.is_valid(i).then(|| right_values.value(i)));
        }
    }
    let mut keys = Vec::new();
    let mut as_ = Vec::new();
    let mut bs = Vec::new();
    for (key, (a, b)) in groups {
        if !a.is_empty() && !b.is_empty() {
            keys.push(Some(key));
            as_.push(a);
            bs.push(b);
        }
    }
    Ok(batch(keys, as_, bs))
}

/// Independent direct raw-pair oracle; it does not consume the grouped candidate.
fn flat_raw(left: &RecordBatch, right: &RecordBatch) -> Result<(Vec<Row>, Agg, usize, usize)> {
    let (lk, lv) = raw_columns(left)?;
    let (rk, rv) = raw_columns(right)?;
    let mut rows = Vec::new();
    let mut sums: BTreeMap<i64, (i64, i64, i64, bool)> = BTreeMap::new();
    let mut pairs = 0;
    let mut pushed = 0;
    for i in 0..left.num_rows() {
        if lk.is_null(i) {
            continue;
        }
        for j in 0..right.num_rows() {
            if rk.is_null(j) || lk.value(i) != rk.value(j) {
                continue;
            }
            pairs += 1;
            let a = lv.is_valid(i).then(|| lv.value(i));
            let b = rv.is_valid(j).then(|| rv.value(j));
            let projected = a
                .filter(|v| *v >= 2)
                .map(|v| v.checked_add(1).ok_or_else(|| err("projection overflow")))
                .transpose()?;
            if projected.is_none() {
                continue;
            }
            pushed += 1;
            let key = lk.value(i);
            rows.push((key, projected, b));
            let entry = sums.entry(key).or_insert((0, 0, 0, false));
            entry.1 = entry
                .1
                .checked_add(1)
                .ok_or_else(|| err("COUNT(*) overflow"))?;
            entry.2 = entry
                .2
                .checked_add(1)
                .ok_or_else(|| err("COUNT(A') overflow"))?;
            if let (Some(x), Some(y)) = (projected, b) {
                entry.0 = entry
                    .0
                    .checked_add(x.checked_add(y).ok_or_else(|| err("pair sum overflow"))?)
                    .ok_or_else(|| err("SUM overflow"))?;
                entry.3 = true;
            }
        }
    }
    rows.sort();
    let aggs = sums
        .into_iter()
        .map(|(k, (s, n, c, has_sum))| (k, (has_sum.then_some(s), n, c)))
        .collect();
    Ok((rows, aggs, pairs, pushed))
}

/// Fair flat baseline: hash-build the right side, push A>=2 before probing,
/// project A+1, and aggregate qualifying join pairs without materializing rows.
/// Counters report actual hash entries, probes, and matched pair visits.
fn stream_raw(left: &RecordBatch, right: &RecordBatch) -> Result<(Agg, usize, usize, usize)> {
    let (lk, lv) = raw_columns(left)?;
    let (rk, rv) = raw_columns(right)?;
    let mut index: HashMap<i64, Vec<Option<i64>>> = HashMap::new();
    let mut build_entries = 0;
    for j in 0..right.num_rows() {
        if rk.is_null(j) {
            continue;
        }
        index
            .entry(rk.value(j))
            .or_default()
            .push(rv.is_valid(j).then(|| rv.value(j)));
        build_entries += 1;
    }
    let mut out: Agg = BTreeMap::new();
    let mut lookups = 0;
    let mut pair_visits = 0;
    for i in 0..left.num_rows() {
        if lk.is_null(i) || lv.is_null(i) || lv.value(i) < 2 {
            continue;
        }
        lookups += 1;
        let Some(matches) = index.get(&lk.value(i)) else {
            continue;
        };
        let projected = lv
            .value(i)
            .checked_add(1)
            .ok_or_else(|| err("projection overflow"))?;
        for b in matches {
            pair_visits += 1;
            let entry = out.entry(lk.value(i)).or_insert((None, 0, 0));
            entry.1 = entry
                .1
                .checked_add(1)
                .ok_or_else(|| err("COUNT(*) overflow"))?;
            entry.2 = entry
                .2
                .checked_add(1)
                .ok_or_else(|| err("COUNT(A') overflow"))?;
            if let Some(b) = b {
                let value = projected
                    .checked_add(*b)
                    .ok_or_else(|| err("pair sum overflow"))?;
                entry.0 = Some(
                    entry
                        .0
                        .unwrap_or(0)
                        .checked_add(value)
                        .ok_or_else(|| err("SUM overflow"))?,
                );
            }
        }
    }
    Ok((out, build_entries, lookups, pair_visits))
}

fn raw_fixture(
    name: &str,
    keys: i64,
    left_width: i64,
    right_width: i64,
) -> (RecordBatch, RecordBatch) {
    let mut left_keys = Vec::new();
    let mut left_values = Vec::new();
    let mut right_keys = Vec::new();
    let mut right_values = Vec::new();
    for key in (0..keys).rev() {
        for n in 0..left_width {
            left_keys.push(Some(key));
            left_values.push(if name == "raw-low-fanout" {
                Some(3)
            } else if n % 7 == 0 {
                None
            } else {
                Some(n - 4)
            });
        }
        for n in 0..right_width {
            right_keys.push(Some(key));
            right_values.push(if name == "raw-low-fanout" {
                Some(5)
            } else if n % 6 == 0 {
                None
            } else {
                Some(n + 1)
            });
        }
    }
    left_keys.extend([Some(99), None]);
    left_values.extend([Some(5), Some(8)]);
    right_keys.extend([Some(100), None]);
    right_values.extend([Some(9), Some(9)]);
    (
        raw_batch(left_keys, left_values),
        raw_batch(right_keys, right_values),
    )
}

fn run_bench(fixtures: &[(&str, RecordBatch, RecordBatch)]) -> Result<()> {
    const ROUNDS: usize = 11;
    const QUERIES_PER_ROUND: usize = 64;
    for (name, left, right) in fixtures {
        let (oracle_rows, oracle, raw_pairs, pushed_pairs) = flat_raw(left, right)?;
        let (expected, _, _, _) = stream_raw(left, right)?;
        let grouped = group_raw(left, right)?;
        let projected_grouped = projected(&grouped)?;
        let candidate = aggregate(&projected_grouped)?;
        let (candidate_rows, candidate_flat_agg) = flat(&projected_grouped, false, false)?;
        if expected != oracle
            || candidate != oracle
            || candidate_flat_agg != oracle
            || candidate_rows != oracle_rows
        {
            return Err(err(
                "benchmark paths differ from independent raw flat oracle",
            ));
        }
        let groups = expected.len();
        let mut samples = [Vec::new(), Vec::new()];
        // Warm both paths on the same immutable raw Arrow batches.
        black_box(stream_raw(left, right)?.0);
        let grouped = group_raw(left, right)?;
        black_box(aggregate(&projected(&grouped)?)?);
        for round in 0..ROUNDS {
            let order = [round % 2, 1 - round % 2];
            for path in order {
                let start = Instant::now();
                for _ in 0..QUERIES_PER_ROUND {
                    let result = if path == 0 {
                        stream_raw(black_box(left), black_box(right))?.0
                    } else {
                        let grouped = group_raw(black_box(left), black_box(right))?;
                        aggregate(&projected(&grouped)?)?
                    };
                    black_box(result);
                }
                samples[path].push(start.elapsed() / QUERIES_PER_ROUND as u32);
            }
        }
        let median = |values: &mut [Duration]| {
            values.sort_unstable();
            values[values.len() / 2].as_nanos()
        };
        println!(
            "{name} --bench: left_rows={}, right_rows={}, groups={groups}, raw_pairs={raw_pairs}, pushed_pairs={pushed_pairs}, rounds={ROUNDS}, queries_per_round={QUERIES_PER_ROUND}, stream_median_ns_per_query={}, candidate_median_ns_per_query={}, construction_included=true",
            left.num_rows(),
            right.num_rows(),
            median(&mut samples[0]),
            median(&mut samples[1]),
        );
    }
    Ok(())
}

type Weighted = BTreeMap<(i64, Option<i64>), (Option<f64>, i64, i64, Option<f64>)>;
type WeightedAccum = (f64, i64, i64);
type FactFactor = (Option<f64>, i64, i64);
type TopWeighted = Vec<((i64, Option<i64>), Option<f64>)>;
type StreamIndex = HashMap<i64, Vec<(Option<i64>, Option<f64>)>>;

fn weighted_batch(
    hosts: Vec<Option<i64>>,
    labels: Vec<Option<i64>>,
    values: Vec<Option<f64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("area", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(hosts)),
            Arc::new(Int64Array::from(labels)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

fn weighted_columns(batch: &RecordBatch) -> Result<(Int64Array, Int64Array, Float64Array)> {
    if batch.num_columns() != 3
        || batch.column(0).data_type() != &DataType::Int64
        || batch.column(1).data_type() != &DataType::Int64
        || batch.column(2).data_type() != &DataType::Float64
    {
        return Err(err("expected host:Int64, area:Int64, value:Float64"));
    }
    Ok((
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("invalid host"))?
            .clone(),
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| err("invalid area"))?
            .clone(),
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| err("invalid value"))?
            .clone(),
    ))
}

fn factor_sum(values: Vec<Option<f64>>) -> Result<(Option<f64>, i64, i64)> {
    let array = Float64Array::from(values);
    let total = sum(&array);
    if total.is_some_and(|v| !v.is_finite()) {
        return Err(err("non-finite factor sum unsupported"));
    }
    Ok((
        total,
        (array.len() - array.null_count()) as i64,
        array.len() as i64,
    ))
}

fn weighted_finite(values: &Float64Array) -> Result<()> {
    if values.iter().flatten().any(|v| !v.is_finite()) {
        return Err(err("non-finite Float64 input unsupported"));
    }
    Ok(())
}

/// Factor candidate: each fact value and each dimension weight is aggregated once per host/area.
/// It never constructs or visits fact/dimension product pairs. NULL area is represented by Option,
/// not a sentinel, so every Int64 label remains representable.
fn weighted_candidate(facts: &RecordBatch, dims: &RecordBatch) -> Result<Weighted> {
    let (fh, _, fv) = weighted_columns(facts)?;
    let (dh, da, dw) = weighted_columns(dims)?;
    weighted_finite(&fv)?;
    weighted_finite(&dw)?;
    let mut fact_values: BTreeMap<i64, Vec<Option<f64>>> = BTreeMap::new();
    let mut dim_factors: BTreeMap<(i64, Option<i64>), Vec<Option<f64>>> = BTreeMap::new();
    for i in 0..facts.num_rows() {
        if !fh.is_null(i) {
            fact_values
                .entry(fh.value(i))
                .or_default()
                .push(fv.is_valid(i).then(|| fv.value(i)));
        }
    }
    for j in 0..dims.num_rows() {
        if !dh.is_null(j) {
            dim_factors
                .entry((dh.value(j), da.is_valid(j).then(|| da.value(j))))
                .or_default()
                .push(dw.is_valid(j).then(|| dw.value(j)));
        }
    }
    let mut fact_factors: BTreeMap<i64, FactFactor> = BTreeMap::new();
    for (host, values) in fact_values {
        fact_factors.insert(host, factor_sum(values)?);
    }
    let mut out = BTreeMap::new();
    for ((host, area), weights) in dim_factors {
        let Some((sum_a, count_a, len_a)) = fact_factors.get(&host) else {
            continue;
        };
        let (sum_w, count_w, len_w) = factor_sum(weights)?;
        let count = count_a
            .checked_mul(count_w)
            .ok_or_else(|| err("COUNT(product) overflow"))?;
        let all = len_a
            .checked_mul(len_w)
            .ok_or_else(|| err("COUNT(*) overflow"))?;
        // Keep matched groups even when all products are NULL; SUM/AVG stay NULL at count zero.
        let total = if count == 0 {
            None
        } else {
            let (Some(a), Some(w)) = (*sum_a, sum_w) else {
                return Err(err("factor count/SUM inconsistency"));
            };
            let numerator = a * w;
            if !numerator.is_finite() {
                return Err(err(
                    "factorized weighted SUM unsupported for overflowing finite factors",
                ));
            }
            Some(numerator)
        };
        let avg = total.map(|s| s / count as f64);
        out.insert((host, area), (total, all, count, avg));
    }
    Ok(out)
}

/// Fair streaming hash baseline; hashes dimension rows then visits matching pairs directly into groups.
/// No joined-row vector or raw pair list is materialized.
fn weighted_stream(facts: &RecordBatch, dims: &RecordBatch) -> Result<Weighted> {
    let (fh, _, fv) = weighted_columns(facts)?;
    let (dh, da, dw) = weighted_columns(dims)?;
    weighted_finite(&fv)?;
    weighted_finite(&dw)?;
    let mut index: StreamIndex = HashMap::new();
    for j in 0..dims.num_rows() {
        if !dh.is_null(j) {
            index.entry(dh.value(j)).or_default().push((
                da.is_valid(j).then(|| da.value(j)),
                dw.is_valid(j).then(|| dw.value(j)),
            ));
        }
    }
    let mut accum: BTreeMap<(i64, Option<i64>), WeightedAccum> = BTreeMap::new();
    for i in 0..facts.num_rows() {
        if fh.is_null(i) {
            continue;
        }
        if let Some(matches) = index.get(&fh.value(i)) {
            for (area, weight) in matches {
                let entry = accum.entry((fh.value(i), *area)).or_insert((0.0, 0, 0));
                entry.2 += 1;
                if let (Some(a), Some(w)) = (fv.is_valid(i).then(|| fv.value(i)), weight) {
                    let product = a * *w;
                    if !product.is_finite() {
                        return Err(err("weighted AVG requires finite products"));
                    }
                    entry.0 += product;
                    if !entry.0.is_finite() {
                        return Err(err("weighted SUM overflow"));
                    }
                    entry.1 += 1;
                }
            }
        }
    }
    Ok(accum
        .into_iter()
        .map(|(k, (s, n, all))| {
            (
                k,
                ((n > 0).then_some(s), all, n, (n > 0).then(|| s / n as f64)),
            )
        })
        .collect())
}

/// Independent nested-loop raw-pair oracle; used only for validation, never timing.
fn weighted_oracle(facts: &RecordBatch, dims: &RecordBatch) -> Result<Weighted> {
    let (fh, _, fv) = weighted_columns(facts)?;
    let (dh, da, dw) = weighted_columns(dims)?;
    weighted_finite(&fv)?;
    weighted_finite(&dw)?;
    let mut accum: BTreeMap<(i64, Option<i64>), WeightedAccum> = BTreeMap::new();
    for i in 0..facts.num_rows() {
        for j in 0..dims.num_rows() {
            if fh.is_null(i) || dh.is_null(j) || fh.value(i) != dh.value(j) {
                continue;
            }
            let entry = accum
                .entry((fh.value(i), da.is_valid(j).then(|| da.value(j))))
                .or_insert((0.0, 0, 0));
            entry.2 += 1;
            if let (Some(a), Some(w)) = (
                fv.is_valid(i).then(|| fv.value(i)),
                dw.is_valid(j).then(|| dw.value(j)),
            ) {
                let product = a * w;
                if !product.is_finite() {
                    return Err(err("weighted AVG requires finite products"));
                }
                entry.0 += product;
                if !entry.0.is_finite() {
                    return Err(err("weighted SUM overflow"));
                }
                entry.1 += 1;
            }
        }
    }
    Ok(accum
        .into_iter()
        .map(|(k, (s, n, all))| {
            (
                k,
                ((n > 0).then_some(s), all, n, (n > 0).then(|| s / n as f64)),
            )
        })
        .collect())
}

fn weighted_top10(groups: &Weighted) -> TopWeighted {
    let mut rows = groups
        .iter()
        .map(|(key, value)| (*key, value.3))
        .collect::<Vec<_>>();
    rows.sort_by(|(ka, va), (kb, vb)| match (va, vb) {
        (Some(a), Some(b)) => b
            .total_cmp(a)
            .then_with(|| ka.0.cmp(&kb.0))
            .then_with(|| ka.1.cmp(&kb.1)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => ka.0.cmp(&kb.0).then_with(|| ka.1.cmp(&kb.1)),
    });
    rows.truncate(10);
    rows
}

fn weighted_fixture(shape: &str) -> (RecordBatch, RecordBatch) {
    let (fact_count, dim_count) = match shape {
        "hot" => (128, 128),
        "low" => (16, 16),
        _ => (256, 64),
    };
    let facts = (0..fact_count)
        .map(|i| {
            let host = if shape == "hot" { 0 } else { i % 16 };
            (Some(host as i64), Some((1 + (i * 37) % 499) as f64))
        })
        .collect::<Vec<_>>();
    let dims = (0..dim_count)
        .map(|i| {
            let host = match shape {
                "hot" => 0,
                "low" => i,
                _ => i / 4,
            };
            let area = if shape == "hot" {
                i / 32
            } else if shape == "low" {
                0
            } else {
                i % 4
            };
            (
                Some(host as i64),
                Some(area as i64),
                Some((1 + (i * 13) % 97) as f64 / 10.0),
            )
        })
        .collect::<Vec<_>>();
    (
        weighted_batch(
            facts.iter().map(|x| x.0).collect(),
            vec![None; facts.len()],
            facts.iter().map(|x| x.1).collect(),
        ),
        weighted_batch(
            dims.iter().map(|x| x.0).collect(),
            dims.iter().map(|x| x.1).collect(),
            dims.iter().map(|x| x.2).collect(),
        ),
    )
}

fn weighted_filtered(
    facts: &RecordBatch,
    dims: &RecordBatch,
    start: usize,
    end: usize,
    rare_area: i64,
) -> Result<(RecordBatch, RecordBatch)> {
    let (fh, fa, fv) = weighted_columns(facts)?;
    let (dh, da, dw) = weighted_columns(dims)?;
    let fact_mask = arrow::array::BooleanArray::from(
        (0..facts.num_rows())
            .map(|i| i >= start && i < end)
            .collect::<Vec<_>>(),
    );
    let dim_mask = arrow::array::BooleanArray::from(
        (0..dims.num_rows())
            .map(|i| da.is_valid(i) && da.value(i) == rare_area)
            .collect::<Vec<_>>(),
    );
    let f = |a: &dyn Array, m: &arrow::array::BooleanArray| arrow::compute::filter(a, m);
    let facts = weighted_batch(
        f(&fh, &fact_mask)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        f(&fa, &fact_mask)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        f(&fv, &fact_mask)?
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect(),
    );
    let dims = weighted_batch(
        f(&dh, &dim_mask)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        f(&da, &dim_mask)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        f(&dw, &dim_mask)?
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect(),
    );
    Ok((facts, dims))
}

fn assert_weighted_close(actual: &Weighted, expected: &Weighted) -> Result<()> {
    if actual.len() != expected.len() {
        return Err(err("weighted groups differ"));
    }
    for (key, a) in actual {
        let Some(e) = expected.get(key) else {
            return Err(err("weighted group key differs"));
        };
        if a.1 != e.1
            || a.2 != e.2
            || a.0.is_some() != e.0.is_some()
            || a.3.is_some() != e.3.is_some()
        {
            return Err(err("weighted count/NULL status differs"));
        }
        for (x, y) in [(a.0, e.0), (a.3, e.3)] {
            if x.is_some_and(|v| !v.is_finite()) || y.is_some_and(|v| !v.is_finite()) {
                return Err(err("non-finite result in weighted comparison"));
            }
            if let (Some(x), Some(y)) = (x, y)
                && (x - y).abs() > 1e-12_f64.max(y.abs() * 1e-12)
            {
                return Err(err("weighted Float64 value outside abs/relative tolerance"));
            }
        }
    }
    Ok(())
}

fn assert_weighted_top10_close(actual: &Weighted, expected: &Weighted) -> Result<()> {
    let a = weighted_top10(actual);
    let e = weighted_top10(expected);
    if a.len() != e.len()
        || a.iter().zip(&e).any(|((ka, va), (ke, ve))| {
            ka != ke
                || match (va, ve) {
                    (Some(x), Some(y)) => {
                        !x.is_finite()
                            || !y.is_finite()
                            || (x - y).abs() > 1e-12_f64.max(y.abs() * 1e-12)
                    }
                    (None, None) => false,
                    _ => true,
                }
        })
    {
        return Err(err("weighted TOP10 differs"));
    }
    Ok(())
}

fn run_weighted_bench(fixtures: &[(&str, RecordBatch, RecordBatch)]) -> Result<()> {
    const ROUNDS: usize = 11;
    const QUERIES: usize = 64;
    for (name, facts, dims) in fixtures {
        let oracle = weighted_oracle(facts, dims)?;
        assert_weighted_close(&weighted_candidate(facts, dims)?, &oracle)?;
        assert_weighted_close(&weighted_stream(facts, dims)?, &oracle)?;
        assert_weighted_top10_close(&weighted_candidate(facts, dims)?, &oracle)?;
        let mut samples = [Vec::new(), Vec::new()];
        for round in 0..ROUNDS {
            for path in [round % 2, 1 - round % 2] {
                let start = Instant::now();
                for _ in 0..QUERIES {
                    let result = if path == 0 {
                        weighted_stream(black_box(facts), black_box(dims))?
                    } else {
                        weighted_candidate(black_box(facts), black_box(dims))?
                    };
                    black_box(result);
                }
                samples[path].push(start.elapsed() / QUERIES as u32);
            }
        }
        let median = |v: &mut [Duration]| {
            v.sort_unstable();
            v[v.len() / 2].as_nanos()
        };
        println!(
            "{name} weighted-AVG --bench: fact_rows={}, dim_rows={}, groups={}, rounds={ROUNDS}, queries_per_round={QUERIES}, stream_median_ns_per_query={}, factor_median_ns_per_query={}, construction_included=true, tolerance=1e-12_abs_rel",
            facts.num_rows(),
            dims.num_rows(),
            oracle.len(),
            median(&mut samples[0]),
            median(&mut samples[1])
        );
    }
    Ok(())
}

fn run() -> Result<()> {
    let s = batch(
        vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        vec![
            vec![Some(2), Some(2), None, Some(1)],
            vec![Some(3)],
            vec![Some(4)],
            vec![None],
            vec![Some(3)],
        ],
        vec![
            vec![Some(5), Some(5), None],
            vec![None, Some(8)],
            vec![],
            vec![Some(7)],
            vec![None, None],
        ],
    );
    let big = batch(
        vec![Some(1)],
        vec![(0..100).map(Some).collect()],
        vec![(0..100).map(Some).collect()],
    );
    for (name, src) in [("sample", s), ("scaled-100x100", big)] {
        let p = projected(&src)?;
        let candidate = aggregate(&p)?;
        let (raw_bag, oracle) = flat(&src, false, true)?;
        let (candidate_bag, candidate_flat_agg) = flat(&p, false, false)?;
        if (candidate_bag, candidate_flat_agg) != (raw_bag.clone(), oracle.clone()) {
            return Err(err(
                "projected full bag or aggregate differs from raw flat baseline",
            ));
        }
        if candidate != oracle {
            return Err(err("factor aggregate differs from raw flat baseline"));
        }
        let (raw_boundary, raw_boundary_agg) = flat(&src, true, true)?;
        let (boundary, candidate_boundary_agg) = flat(&p, true, false)?;
        if (boundary.clone(), candidate_boundary_agg) != (raw_boundary, raw_boundary_agg) {
            return Err(err(
                "conditional full bag or aggregate differs from raw flat baseline",
            ));
        }
        let raw_lists = lists(&src)?;
        let raw_product_rows: usize = (0..src.num_rows())
            .map(|i| raw_lists.1.value_length(i) as usize * raw_lists.2.value_length(i) as usize)
            .sum();
        println!(
            "{name}: raw_product_rows={}, pushed_flat_rows={}, groups={}, boundary_rows={}, source_array_memory_bytes={}, projected_array_memory_bytes={}",
            raw_product_rows,
            raw_bag.len(),
            candidate.len(),
            boundary.len(),
            src.get_array_memory_size(),
            p.get_array_memory_size()
        )
    }

    // Shape-derived synthetic data: host equality join, compact GROUP BY host,
    // then the existing INT64 A>=2 / A'=A+1 and SUM(A'+B), COUNT aggregates.
    for (name, keys, left_width, right_width) in [
        ("raw-host-join", 16, 24, 20),
        ("raw-one-hot", 1, 128, 128),
        ("raw-low-fanout", 16, 1, 1),
    ] {
        let (left, right) = raw_fixture(name, keys, left_width, right_width);
        let grouped = group_raw(&left, &right)?;
        let projected_grouped = projected(&grouped)?;
        let result = aggregate(&projected_grouped)?;
        let (candidate_rows, candidate_agg) = flat(&projected_grouped, false, false)?;
        let (oracle_rows, oracle_agg, raw_pairs, pushed_pairs) = flat_raw(&left, &right)?;
        let (stream_agg, build_entries, lookups, pair_visits) = stream_raw(&left, &right)?;
        if candidate_rows != oracle_rows
            || result != oracle_agg
            || candidate_agg != oracle_agg
            || stream_agg != oracle_agg
        {
            return Err(err(
                "raw grouped candidate differs from independent flat oracle",
            ));
        }
        let (_, a, b) = lists(&grouped)?;
        let child_entries = (0..grouped.num_rows())
            .map(|i| a.value_length(i) as usize + b.value_length(i) as usize)
            .sum::<usize>();
        println!(
            "{name} synthetic shape: raw_left_rows={}, raw_right_rows={}, matched_groups={}, matched_child_entries_including_null_payloads={}, raw_join_pairs={}, pushed_flat_pairs={}, aggregate_groups={}, stream_hash_entries={}, stream_left_lookups={}, stream_pair_visits={}, source_array_memory_bytes={}, compact_array_memory_bytes={}",
            left.num_rows(),
            right.num_rows(),
            grouped.num_rows(),
            child_entries,
            raw_pairs,
            pushed_pairs,
            oracle_agg.len(),
            build_entries,
            lookups,
            pair_visits,
            left.get_array_memory_size() + right.get_array_memory_size(),
            grouped.get_array_memory_size()
        );
    }
    for shape in ["balanced", "hot", "low"] {
        let (facts, dims) = weighted_fixture(shape);
        let oracle = weighted_oracle(&facts, &dims)?;
        let candidate = weighted_candidate(&facts, &dims)?;
        assert_weighted_close(&candidate, &oracle)?;
        assert_weighted_close(&weighted_stream(&facts, &dims)?, &oracle)?;
        for (start, end, area) in [(0, 16, 0), (0, 16, 99), (64, 128, 2), (256, 256, 0)] {
            let (filtered_facts, filtered_dims) =
                weighted_filtered(&facts, &dims, start, end, area)?;
            let filtered_oracle = weighted_oracle(&filtered_facts, &filtered_dims)?;
            assert_weighted_close(
                &weighted_candidate(&filtered_facts, &filtered_dims)?,
                &filtered_oracle,
            )?;
            assert_weighted_close(
                &weighted_stream(&filtered_facts, &filtered_dims)?,
                &filtered_oracle,
            )?;
        }
        let top10 = weighted_top10(&candidate);
        assert_weighted_top10_close(&candidate, &oracle)?;
        println!("weighted-{shape} top10={top10:?}");
        println!(
            "weighted-{shape}: fact_rows={}, dim_rows={}, groups={}, formula=AVG(product)=SUM(fact*weight)/COUNT(product), float_tolerance=1e-12_abs_rel",
            facts.num_rows(),
            dims.num_rows(),
            oracle.len()
        );
    }
    Ok(())
}
fn main() {
    let result = if std::env::args().nth(1).as_deref() == Some("--bench") {
        let fixtures = [
            ("raw-host-join", 16, 24, 20),
            ("raw-one-hot", 1, 128, 128),
            ("raw-low-fanout", 16, 1, 1),
        ]
        .map(|(name, keys, left_width, right_width)| {
            let (left, right) = raw_fixture(name, keys, left_width, right_width);
            (name, left, right)
        });
        run_bench(&fixtures).and_then(|_| {
            let balanced = weighted_fixture("balanced");
            let hot = weighted_fixture("hot");
            let low = weighted_fixture("low");
            let (wide_f, wide_d) = weighted_filtered(&balanced.0, &balanced.1, 0, 16, 1)?;
            let (narrow_f, narrow_d) = weighted_filtered(&balanced.0, &balanced.1, 64, 128, 0)?;
            for (f, d) in [(&wide_f, &wide_d), (&narrow_f, &narrow_d)] {
                let oracle = weighted_oracle(f, d)?;
                assert_weighted_close(&weighted_candidate(f, d)?, &oracle)?;
                assert_weighted_close(&weighted_stream(f, d)?, &oracle)?;
            }
            let weighted = [
                ("balanced", balanced.0, balanced.1),
                ("hot", hot.0, hot.1),
                ("low", low.0, low.1),
            ];
            run_weighted_bench(&weighted)
        })
    } else {
        run()
    };
    if let Err(e) = result {
        eprintln!("{e}");
        std::process::exit(1)
    }
}

#[cfg(test)]
mod tests {
    use arrow::buffer::NullBuffer;

    use super::*;

    fn compare_raw(left: &RecordBatch, right: &RecordBatch) {
        let grouped = group_raw(left, right).unwrap();
        let p = projected(&grouped).unwrap();
        let (candidate_rows, candidate_agg) = flat(&p, false, false).unwrap();
        let (oracle_rows, oracle_agg, _, _) = flat_raw(left, right).unwrap();
        assert_eq!(stream_raw(left, right).unwrap().0, oracle_agg);
        assert_eq!(candidate_rows, oracle_rows);
        assert_eq!(aggregate(&p).unwrap(), oracle_agg);
        assert_eq!(candidate_agg, oracle_agg);
        assert_eq!(
            flat(&p, true, false).unwrap().0,
            flat_raw(left, right)
                .unwrap()
                .0
                .into_iter()
                .filter(|(_, a, b)| matches!((a, b), (Some(x), Some(y)) if x < y))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn weighted_filters_and_ordering_are_stable() {
        let (facts, dims) = weighted_fixture("balanced");
        for (start, end, area) in [(0, 16, 0), (0, 16, 99), (64, 128, 2), (256, 256, 0)] {
            let (f, d) = weighted_filtered(&facts, &dims, start, end, area).unwrap();
            let oracle = weighted_oracle(&f, &d).unwrap();
            assert_weighted_close(&weighted_candidate(&f, &d).unwrap(), &oracle).unwrap();
            assert_weighted_close(&weighted_stream(&f, &d).unwrap(), &oracle).unwrap();
        }
        let mut groups = BTreeMap::new();
        for (key, score) in [(1, 1.0), (2, 1.0 + 0.75e-12), (3, 1.0 + 1.5e-12)] {
            groups.insert((key, Some(0)), (Some(score), 1, 1, Some(score)));
        }
        for key in 4..=10 {
            groups.insert((key, Some(0)), (Some(0.5), 1, 1, Some(0.5)));
        }
        groups.insert(
            (11, Some(0)),
            (Some(1.0 + 2.5e-12), 1, 1, Some(1.0 + 2.5e-12)),
        );
        let top = weighted_top10(&groups);
        assert_eq!(top[0].0, (11, Some(0)));
        assert_eq!(top[1].0, (3, Some(0)));
        assert_eq!(top[2].0, (2, Some(0)));
        assert_eq!(top[3].0, (1, Some(0)));
        assert!(top.iter().any(|row| row.0.0 == 4));
        assert!(!top.iter().any(|row| row.0.0 == 10)); // cutoff membership is deterministic
    }

    #[test]
    fn weighted_factorization_documents_float_cancellation_boundary() {
        let facts = weighted_batch(
            vec![Some(1), Some(1)],
            vec![None; 2],
            vec![Some(1e16), Some(-1e16)],
        );
        let dims = weighted_batch(
            vec![Some(1), Some(1)],
            vec![Some(0), Some(0)],
            vec![Some(1.0), Some(1e-16)],
        );
        let stream = weighted_stream(&facts, &dims).unwrap();
        let oracle = weighted_oracle(&facts, &dims).unwrap();
        let factor = weighted_candidate(&facts, &dims).unwrap();
        assert_eq!(stream, oracle);
        assert_eq!(oracle[&(1, Some(0))].0, Some(-1.0));
        assert_eq!(oracle[&(1, Some(0))].3, Some(-0.25));
        assert_eq!(factor[&(1, Some(0))].0, Some(0.0));
        // Algebraic factorization is not bitwise-safe under cancellation; this finite-input case is intentionally unsupported.
        assert!(assert_weighted_close(&factor, &oracle).is_err());
        let negatives = weighted_batch(
            vec![Some(1), Some(1)],
            vec![None; 2],
            vec![Some(-2.25), Some(0.5)],
        );
        let fractions = weighted_batch(
            vec![Some(1), Some(1)],
            vec![Some(0), Some(0)],
            vec![Some(0.4), Some(1.2)],
        );
        assert_weighted_close(
            &weighted_candidate(&negatives, &fractions).unwrap(),
            &weighted_oracle(&negatives, &fractions).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn weighted_avg_matches_raw_join_with_nulls_duplicates_and_null_groups() {
        let facts = weighted_batch(
            vec![Some(1), Some(1), Some(1), Some(99), None],
            vec![None; 5],
            vec![Some(2.0), Some(-1e-15), None, Some(9.0), Some(3.0)],
        );
        let dims = weighted_batch(
            vec![Some(1), Some(1), Some(1), Some(98), Some(1)],
            vec![Some(4), Some(4), None, Some(7), Some(8)],
            vec![Some(3.0), Some(3.0), None, None, Some(2.0)],
        );
        let expected = weighted_oracle(&facts, &dims).unwrap();
        let actual = weighted_candidate(&facts, &dims).unwrap();
        assert_weighted_close(&actual, &expected).unwrap();
        assert_weighted_close(&weighted_stream(&facts, &dims).unwrap(), &expected).unwrap();
        assert_weighted_top10_close(&actual, &expected).unwrap();
        assert_eq!(actual[&(1, Some(4))].1, 6); // duplicate pair multiplicity retained
        assert_eq!(actual[&(1, Some(4))].2, 4);
        assert_eq!(actual[&(1, None)].1, 3);
        assert_eq!(actual[&(1, None)].2, 0); // group remains with NULL AVG
        assert!(!actual.contains_key(&(99, Some(7)))); // unmatched NULL host never joins
        let nonfinite = weighted_batch(vec![Some(1)], vec![None], vec![Some(f64::INFINITY)]);
        let finite_dim = weighted_batch(vec![Some(1)], vec![Some(1)], vec![Some(1.0)]);
        assert!(weighted_candidate(&nonfinite, &finite_dim).is_err());
        assert!(weighted_stream(&nonfinite, &finite_dim).is_err());
        for bad in [f64::NAN, f64::NEG_INFINITY] {
            let invalid_fact = weighted_batch(vec![Some(1)], vec![None], vec![Some(1.0)]);
            let invalid_dim = weighted_batch(vec![Some(1)], vec![Some(0)], vec![Some(bad)]);
            assert!(weighted_candidate(&invalid_fact, &invalid_dim).is_err());
            assert!(weighted_stream(&invalid_fact, &invalid_dim).is_err());
            assert!(weighted_oracle(&invalid_fact, &invalid_dim).is_err());
        }
        let overflow_facts = weighted_batch(
            vec![Some(1), Some(1)],
            vec![None; 2],
            vec![Some(1e308), Some(1e308)],
        );
        let overflow_dims = weighted_batch(vec![Some(1)], vec![Some(0)], vec![Some(1.0)]);
        assert!(weighted_candidate(&overflow_facts, &overflow_dims).is_err());
        assert!(weighted_stream(&overflow_facts, &overflow_dims).is_err());
        assert!(weighted_oracle(&overflow_facts, &overflow_dims).is_err());
        let all_null_facts = weighted_batch(vec![Some(1), Some(1)], vec![None; 2], vec![None; 2]);
        let null_group_dims = weighted_batch(vec![Some(1)], vec![None], vec![Some(1.0)]);
        let null_groups = weighted_candidate(&all_null_facts, &null_group_dims).unwrap();
        assert_eq!(null_groups[&(1, None)].1, 2);
        assert_eq!(null_groups[&(1, None)].2, 0);
        assert_eq!(null_groups[&(1, None)].3, None);
        let empty = weighted_batch(vec![], vec![], vec![]);
        assert!(weighted_candidate(&empty, &empty).unwrap().is_empty());
        assert!(weighted_stream(&empty, &empty).unwrap().is_empty());

        let min_area = weighted_batch(
            vec![Some(1), Some(1)],
            vec![None, Some(1)],
            vec![Some(2.0), Some(3.0)],
        );
        let min_dims = weighted_batch(
            vec![Some(1), Some(1)],
            vec![Some(i64::MIN), None],
            vec![Some(1.0), Some(1.0)],
        );
        let distinct = weighted_candidate(&min_area, &min_dims).unwrap();
        assert!(distinct.contains_key(&(1, None)));
        assert!(distinct.contains_key(&(1, Some(i64::MIN))));
        assert_eq!(distinct.len(), 2);
    }

    #[test]
    fn raw_join_groups_bag_before_projection_and_aggregation() {
        let left = raw_batch(
            vec![Some(8), Some(2), Some(8), Some(8), Some(4), Some(99), None],
            vec![Some(3), Some(1), Some(3), None, Some(2), Some(9), Some(8)],
        );
        let right = raw_batch(
            vec![Some(8), Some(2), Some(8), Some(4), Some(100), None],
            vec![Some(4), Some(5), None, Some(7), Some(3), Some(3)],
        );
        compare_raw(&left, &right);
        let grouped = group_raw(&left, &right).unwrap();
        let (keys, a, b) = lists(&grouped).unwrap();
        assert_eq!(
            keys.values().iter().copied().collect::<Vec<_>>(),
            vec![2, 4, 8]
        );
        assert_eq!(a.value_length(2), 3);
        assert_eq!(b.value_length(2), 2);
        assert_eq!(a.value(2).null_count(), 1);
        assert_eq!(b.value(2).null_count(), 1);
        let empty_after_filter = raw_batch(vec![Some(1)], vec![Some(1)]);
        let matching = raw_batch(vec![Some(1)], vec![Some(5)]);
        compare_raw(&empty_after_filter, &matching);
        assert!(
            aggregate(&projected(&group_raw(&empty_after_filter, &matching).unwrap()).unwrap())
                .unwrap()
                .is_empty()
        );
        let absent = raw_batch(vec![Some(3)], vec![Some(3)]);
        compare_raw(&absent, &matching);
        assert!(group_raw(&absent, &matching).unwrap().num_rows() == 0);
        let empty = raw_batch(vec![], vec![]);
        let null_keys = raw_batch(vec![None, None], vec![Some(3), Some(4)]);
        compare_raw(&empty, &matching);
        compare_raw(&null_keys, &matching);
        let max_unmatched = raw_batch(vec![Some(i64::MAX)], vec![Some(i64::MAX)]);
        compare_raw(&max_unmatched, &matching);
        let max_matching = raw_batch(vec![Some(1)], vec![Some(i64::MAX)]);
        let max_right = raw_batch(vec![Some(1)], vec![Some(3)]);
        assert!(flat_raw(&max_matching, &max_right).is_err());
        assert!(stream_raw(&max_matching, &max_right).is_err());
        assert!(projected(&group_raw(&max_matching, &max_right).unwrap()).is_err());
        let sliced_left = raw_batch(
            vec![Some(90), Some(6), Some(91)],
            vec![Some(90), Some(4), Some(91)],
        )
        .slice(1, 1);
        let sliced_right = raw_batch(
            vec![Some(90), Some(6), Some(91)],
            vec![Some(90), Some(7), Some(91)],
        )
        .slice(1, 1);
        compare_raw(&sliced_left, &sliced_right);
        let wrong = batch(vec![Some(1)], vec![vec![]], vec![vec![]]);
        assert!(group_raw(&wrong, &matching).is_err());
    }

    fn compare(source: &RecordBatch) {
        let p = projected(source).unwrap();
        let (raw_bag, raw_agg) = flat(source, false, true).unwrap();
        assert_eq!(aggregate(&p).unwrap(), raw_agg);
        let (candidate_bag, _) = flat(&p, false, false).unwrap();
        assert_eq!(candidate_bag, raw_bag);
        let (raw_boundary, _) = flat(source, true, true).unwrap();
        let (candidate_boundary, _) = flat(&p, true, false).unwrap();
        assert_eq!(candidate_boundary, raw_boundary);
        assert_eq!(
            flat(&p, true, false).unwrap().1,
            flat(source, true, true).unwrap().1
        );
    }

    #[test]
    fn sample_empty_and_null_bags() {
        let sample = batch(
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
            vec![
                vec![Some(2), Some(2), None, Some(1)],
                vec![Some(3)],
                vec![Some(4)],
                vec![None],
                vec![Some(3)],
            ],
            vec![
                vec![Some(5), Some(5), None],
                vec![None, Some(8)],
                vec![],
                vec![Some(7)],
                vec![None, None],
            ],
        );
        compare(&sample);
        let p = projected(&sample).unwrap();
        let agg = aggregate(&p).unwrap();
        assert_eq!(agg.get(&1), Some(&(Some(32), 6, 6)));
        assert_eq!(agg.get(&2), Some(&(Some(12), 2, 2)));
        assert_eq!(agg.get(&5), Some(&(None, 2, 2)));
        compare(&batch(
            vec![Some(9), Some(10)],
            vec![vec![Some(3), None], vec![]],
            vec![vec![None, None], vec![Some(2)]],
        ));
        compare(&batch(vec![], vec![], vec![]));
        let empty_product = batch(vec![Some(1)], vec![vec![Some(i64::MAX)]], vec![vec![]]);
        let p = projected(&empty_product).unwrap();
        assert_eq!(
            flat(&empty_product, false, true).unwrap(),
            flat(&p, false, false).unwrap()
        );
        assert!(aggregate(&p).unwrap().is_empty());
        let null_b = batch(vec![Some(1)], vec![vec![Some(i64::MAX)]], vec![vec![None]]);
        assert!(projected(&null_b).is_err());
    }

    #[test]
    fn arithmetic_fallback_and_overflow_match_oracle() {
        let cases = [
            (vec![Some(2)], vec![Some(i64::MAX), Some(-i64::MAX)], false),
            (
                vec![Some(i64::MAX - 1), Some(i64::MAX - 1)],
                vec![None],
                true,
            ),
            (vec![Some(2)], vec![Some(i64::MIN), Some(-1)], true),
            (
                vec![Some(i64::MAX - 1), Some(i64::MAX - 1)],
                vec![Some(-(i64::MAX - 1))],
                true,
            ),
            (vec![Some(2), Some(2)], vec![Some(i64::MAX - 3)], false),
        ];
        for (av, bv, succeeds) in cases {
            let source = batch(vec![Some(1)], vec![av], vec![bv]);
            let raw = flat(&source, false, true);
            let candidate = aggregate(&projected(&source).unwrap());
            if succeeds {
                assert_eq!(candidate.unwrap(), raw.unwrap().1);
            } else {
                assert!(candidate.is_err());
                assert!(raw.is_err());
            }
        }
        let projection_overflow = batch(
            vec![Some(1)],
            vec![vec![Some(i64::MAX)]],
            vec![vec![Some(0)]],
        );
        assert!(projected(&projection_overflow).is_err());
        assert!(flat(&projection_overflow, false, true).is_err());
    }

    #[test]
    fn validation_and_sliced_parent_offsets() {
        let duplicate = batch(
            vec![Some(1), Some(1)],
            vec![vec![], vec![]],
            vec![vec![], vec![]],
        );
        assert!(
            lists(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("unique")
        );
        let null_key = batch(vec![None], vec![vec![]], vec![vec![]]);
        assert!(lists(&null_key).unwrap_err().to_string().contains("unique"));
        let base = batch(
            vec![Some(0), Some(1), Some(2)],
            vec![vec![Some(90)], vec![Some(2), None], vec![Some(91)]],
            vec![vec![Some(90)], vec![Some(5)], vec![Some(91)]],
        );
        let sliced = base.slice(1, 1);
        compare(&sliced);
        let renamed_nullable = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new(
                    "A",
                    DataType::List(Arc::new(Field::new("values", DataType::Int64, false))),
                    false,
                ),
                Field::new("B", type_list(), false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![Some(7)])),
                Arc::new(ListArray::new(
                    Arc::new(Field::new("values", DataType::Int64, false)),
                    OffsetBuffer::new(vec![0_i32, 2].into()),
                    Arc::new(Int64Array::from(vec![Some(2), Some(3)])),
                    None,
                )),
                Arc::new(
                    batch(vec![Some(1)], vec![vec![Some(4)]], vec![vec![Some(4)]])
                        .column(2)
                        .clone(),
                ),
            ],
        )
        .unwrap();
        compare(&renamed_nullable);
        let keys = Int64Array::from(vec![Some(1)]);
        let values = Int64Array::from(vec![Some(2)]);
        let offsets = OffsetBuffer::new(vec![0_i32, 1].into());
        let null_lists = ListArray::new(
            Arc::new(Field::new("anything", DataType::Int64, true)),
            offsets,
            Arc::new(values),
            Some(NullBuffer::new_null(1)),
        );
        let null_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("A", null_lists.data_type().clone(), true),
                Field::new("B", type_list(), false),
            ])),
            vec![
                Arc::new(keys),
                Arc::new(null_lists),
                Arc::new(
                    batch(vec![Some(1)], vec![vec![]], vec![vec![]])
                        .column(1)
                        .clone(),
                ),
            ],
        )
        .unwrap();
        assert!(
            lists(&null_batch)
                .unwrap_err()
                .to_string()
                .contains("parent lists")
        );
        let wrong = ListArray::new(
            Arc::new(Field::new("values", DataType::Int32, true)),
            OffsetBuffer::new(vec![0_i32, 1].into()),
            Arc::new(arrow::array::Int32Array::from(vec![1])),
            None,
        );
        let wrong_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("A", wrong.data_type().clone(), false),
                Field::new("B", type_list(), false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![Some(1)])),
                Arc::new(wrong),
                Arc::new(
                    batch(vec![Some(1)], vec![vec![]], vec![vec![]])
                        .column(1)
                        .clone(),
                ),
            ],
        )
        .unwrap();
        assert!(lists(&wrong_batch).is_err());
    }

    #[test]
    fn three_hundred_deterministic_small_fixtures() {
        let mut seed = 0x1234_5678_u64;
        for case in 0..300 {
            let mut next = || {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                seed
            };
            let n = (next() % 3 + 1) as usize;
            let mut aa = Vec::new();
            let mut bb = Vec::new();
            for _ in 0..n {
                let mut branch = || {
                    let len = (next() % 4) as usize;
                    (0..len)
                        .map(|_| {
                            let v = (next() % 11) as i64 - 5;
                            if next() % 5 == 0 { None } else { Some(v) }
                        })
                        .collect::<Vec<_>>()
                };
                aa.push(branch());
                bb.push(branch());
            }
            let source = batch(
                (0..n).map(|i| Some((case * 4 + i) as i64)).collect(),
                aa,
                bb,
            );
            compare(&source);
        }
    }
}
