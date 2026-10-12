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

//! Bounded `COUNT(*)` and certified-domain (or switch-gated approximate)
//! `AVG(value * weight)` / `SUM(value * weight)` join-aggregation rewrites.
//!
//! Every rewrite turns `... JOIN ... GROUP BY host, area` with a single
//! aggregate into a join of two partial aggregates computed on one side of the
//! join each.
//!
//! `COUNT(*)`: the pair count is replaced by the product of the two branch row
//! counts, which is only sound while the join cannot produce more pairs than
//! `COUNT(*)` can hold in its `Int64` result. That bound is rule state: it is
//! supplied when the rule is constructed and the rule declines to rewrite when
//! its bound does not prove the pair count.
//!
//! `AVG(value * weight)`: the average of the pairwise products is replaced by
//! `SUM(value) * SUM(weight) / (COUNT(value) * COUNT(weight))`; `SUM(value *
//! weight)` is replaced by the same numerator. Such a factorization is exact only
//! on a data domain where every intermediate value is exactly representable and
//! the cancellation of exact nonzero values cannot leave a different signed zero
//! than the pairwise sum does. Whether the data is in that domain is not visible
//! in a `LogicalPlan`, so the caller certifies it with [`exact_avg_domain`] over
//! the actual input batches and passes the certificate to the rule at
//! construction, exactly like the pair bound.
//!
//! # Exact and approximate payload rewrites
//!
//! The payload rewrites are opt-in rule state, selected at construction:
//!
//! - [`CountJoinRewrite::new`] is count-only: no payload aggregate is touched.
//! - [`CountJoinRewrite::with_avg`] rewrites `AVG(value * weight)` only when the
//!   caller passes the [`exact_avg_domain`] certificate. The rewrite is then
//!   justified by [`exact_avg_domain`] alone, every result is bit-identical to
//!   the pairwise baseline, and `SUM` keeps the plan it always had.
//! - [`CountJoinRewrite::with_approx_avg`] and
//!   [`CountJoinRewrite::with_avg_and_approx`] additionally enable the
//!   **approximate** rewrite, justified by the caller's [`approx_avg_domain`]
//!   anti-absurdity fuse instead of an exactness proof. Approximate mode covers
//!   `AVG(value * weight)` and `SUM(value * weight)`: the exact certificate's
//!   path is deliberately left as it was, so `SUM` is only ever rewritten by the
//!   approximate path.
//!
//! Approximate mode is an explicit, bounded approximation and **not** a drop-in
//! replacement for the baseline: it does **not** guarantee bitwise (or even
//! exact) equality with the pairwise `AVG`/`SUM`. It exists to factorize
//! well-conditioned general-float data — the case the exact certificate declines
//! for reasons of exactness — where the factorization error is of the same order
//! as the baseline's own rounding: negligible (roughly `1e-15` to `1e-10`
//! relative for modest cancellation) as long as no group cancels, no intermediate
//! overflows and no magnitude underflows into the subnormal range.
//! [`approx_avg_domain`] is the fuse that keeps such inputs away from the
//! rewrite; it is a heuristic guard, not a proof. Inputs it rejects — heavy
//! cancellation, overflow-prone magnitudes, subnormal magnitudes, non-finite
//! payloads — keep the baseline plan and therefore the baseline's results.
//!
//! One known, accepted difference: where the pairwise products cancel to a
//! signed zero, the naive factorization could produce `-0.0` where the baseline
//! produces `+0.0`. The rewrite normalizes a zero numerator to `+0.0` (a `CASE`
//! that costs nothing), so the mismatch is not observable in practice; had it
//! been left in, `-0.0` would have been acceptable approximation noise rather
//! than an absurd result.
//!
//! The rule is intentionally narrow: it matches only the exact join/group/fact
//! and dim shapes described by `matching_shape` and `matching_payload_shape`. It
//! is not registered in any default rule list, so engine construction is
//! unaffected; the regression tests in `query_engine::state` install an
//! instance explicitly.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow_schema::DataType;
use common_recordbatch::RecordBatch;
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::{Count, count_udaf};
use datafusion::functions_aggregate::sum::{Sum, sum_udaf};
use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{Column, DFSchema, NullEquality, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, BinaryExpr, Case, Cast};
use datafusion_expr::logical_plan::{Aggregate, Join, JoinConstraint, JoinType, Projection};
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, Operator};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// Which payload aggregate rewrite a rule applies, in the rule's precedence
/// order.
///
/// `Exact` is the bitwise rewrite justified by [`exact_avg_domain`]; it rewrites
/// `AVG(value * weight)` alone, exactly as it always has. `Approximate` is the
/// bounded approximation justified by [`approx_avg_domain`]; it rewrites
/// `AVG(value * weight)` and `SUM(value * weight)`, because a batch the exact
/// certificate declines is exactly the batch where the two shapes are handled
/// together. `Baseline` leaves the payload aggregate on the unrewritten plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AvgRewrite {
    /// No payload rewrite: the aggregate stays on the baseline plan.
    Baseline,
    /// Bitwise `AVG` rewrite, justified by the [`exact_avg_domain`]
    /// certificate.
    Exact,
    /// Bounded `AVG`/`SUM` approximation, justified by the [`approx_avg_domain`]
    /// fuse.
    Approximate,
}

/// Rewrites the supported `COUNT(*)` join-aggregate shape into a join of
/// partial counts multiplied in a projection, and — when it was constructed
/// with a payload certificate — the `AVG(value * weight)` / `SUM(value *
/// weight)` shape into a join of partial sums and counts.
#[derive(Debug)]
pub(crate) struct CountJoinRewrite {
    /// `left_rows * right_rows` when the bound proves that product is
    /// representable as the `Int64` value `COUNT(*)` produces, `None` when it
    /// does not. The bound belongs to the rule so it travels with the instance
    /// that applies the rewrite.
    pair_bound: Option<u64>,
    /// Whether the caller proved the exact-domain certificate of the payload
    /// inputs with [`exact_avg_domain`]. A certified rule rewrites `AVG` —
    /// bitwise, and only `AVG`, so a certified rule keeps its historical
    /// behaviour.
    exact_avg: bool,
    /// Whether the caller proved the approximate fuse of the payload inputs
    /// with [`approx_avg_domain`]. The flag is only ever set by a constructor
    /// that opts into approximate mode; it lets the approximate rewrite of `AVG`
    /// and `SUM` run where the exact certificate does not hold.
    approx_avg: bool,
}

impl CountJoinRewrite {
    /// Creates the rule from the row-count bounds of the two joined inputs.
    ///
    /// `fact_rows_bound * dim_rows_bound` is stored when the product neither
    /// overflows `u64` nor exceeds `i64::MAX`; otherwise the rule records that
    /// no sound bound is available and declines every `COUNT(*)` rewrite.
    /// Declining keeps the query on the unrewritten plan instead of failing it.
    /// No payload aggregate is rewritten by a count-only rule.
    pub(crate) fn new(fact_rows_bound: u64, dim_rows_bound: u64) -> Self {
        Self {
            pair_bound: pair_bound(fact_rows_bound, dim_rows_bound),
            exact_avg: false,
            approx_avg: false,
        }
    }

    /// Creates the rule from the row-count bounds and the exact-domain
    /// certificate of the two joined inputs.
    ///
    /// `exact_avg` must come from [`exact_avg_domain`] applied to the batches
    /// the two inputs will actually read; the rule trusts it and rewrites
    /// `AVG(value * weight)` bitwise. Without it the rule leaves every payload
    /// aggregate on the unrewritten plan while still rewriting `COUNT(*)`.
    /// Approximate mode stays off, and `SUM` keeps its baseline plan either way:
    /// only the approximate path covers it.
    pub(crate) fn with_avg(fact_rows_bound: u64, dim_rows_bound: u64, exact_avg: bool) -> Self {
        Self {
            pair_bound: pair_bound(fact_rows_bound, dim_rows_bound),
            exact_avg,
            approx_avg: false,
        }
    }

    /// Creates the rule with the **approximate** payload rewrite opted in.
    ///
    /// `approx_avg` must come from [`approx_avg_domain`] applied to the batches
    /// the two inputs will actually read; the rule trusts it and rewrites
    /// `AVG(value * weight)` and `SUM(value * weight)` without an exactness
    /// proof, under the fused domain described on [`approx_avg_domain`]. The
    /// exact certificate is not consulted: a caller that has one passes it to
    /// [`Self::with_avg_and_approx`] instead, which resolves the precedence in
    /// favour of the exact (and so `AVG`-only) rewrite.
    pub(crate) fn with_approx_avg(
        fact_rows_bound: u64,
        dim_rows_bound: u64,
        approx_avg: bool,
    ) -> Self {
        Self {
            pair_bound: pair_bound(fact_rows_bound, dim_rows_bound),
            exact_avg: false,
            approx_avg,
        }
    }

    /// Creates the rule with both the exact certificate and the approximate
    /// switch.
    ///
    /// The two flags come from [`exact_avg_domain`] and [`approx_avg_domain`]
    /// applied to the same batches. The rule keeps the exact rewrite's
    /// precedence: [`Self::avg_rewrite`] reports `Exact` whenever `exact_avg`
    /// holds, and the emitted plan — and therefore every result — is the exact
    /// rewrite's, bit for bit. The approximate flag only matters when the exact
    /// certificate does not hold; until then `SUM` stays on the baseline plan
    /// like it always did.
    pub(crate) fn with_avg_and_approx(
        fact_rows_bound: u64,
        dim_rows_bound: u64,
        exact_avg: bool,
        approx_avg: bool,
    ) -> Self {
        Self {
            pair_bound: pair_bound(fact_rows_bound, dim_rows_bound),
            exact_avg,
            approx_avg,
        }
    }

    /// The pair count the rule is allowed to assume, if its bound proves one.
    pub(crate) fn pair_bound(&self) -> Option<u64> {
        self.pair_bound
    }

    /// Whether `AVG(value * weight)` may be rewritten with the certificate the
    /// rule was constructed with.
    pub(crate) fn exact_avg(&self) -> bool {
        self.exact_avg
    }

    /// Whether the caller certified the approximate fuse of the payload inputs.
    pub(crate) fn approx_avg(&self) -> bool {
        self.approx_avg
    }

    /// The payload rewrite this rule applies, in the rule's precedence order:
    /// the exact certificate first, then the approximate switch, then nothing.
    pub(crate) fn avg_rewrite(&self) -> AvgRewrite {
        if self.exact_avg {
            AvgRewrite::Exact
        } else if self.approx_avg {
            AvgRewrite::Approximate
        } else {
            AvgRewrite::Baseline
        }
    }
}

/// The product of the two input row-count bounds when it is representable as
/// the `Int64` value `COUNT(*)` produces.
fn pair_bound(fact_rows_bound: u64, dim_rows_bound: u64) -> Option<u64> {
    fact_rows_bound
        .checked_mul(dim_rows_bound)
        .filter(|pairs| *pairs <= i64::MAX as u64)
}

/// Certifies that `AVG(value * weight)` over the cross product of `fact` and
/// `dim` is exactly reproduced by
/// `SUM(value) * SUM(weight) / (COUNT(value) * COUNT(weight))`, bit for bit.
///
/// The certificate is a pure function of the data the two inputs will read,
/// which the rule itself cannot see:
///
/// - every non-null `fact.value` is finite, integral (`fract() == 0`) and
///   within `+-1_000_000`;
/// - every non-null `dim.weight` is finite, a quarter step
///   (`(weight * 4).fract() == 0`) and within `+-4`;
/// - every `host` group of `fact` and every `(host, area)` group of `dim`
///   keeps at least one non-null payload, and
/// - `4 * max_n_f * 1_000_000 * max_n_w * 16 + 2^20 < 2^53`, where `max_n_f` is
///   the largest non-null payload count of a `fact` group and `max_n_w` the
///   same for a `dim` group, computed in `u128`.
///
/// Integral values of magnitude at most `1_000_000` and quarter steps of
/// magnitude at most `4` multiply into exact multiples of `1/4` below `2^52`,
/// so both the pairwise sum the baseline computes and the factored product are
/// exact (`f64`-representable without rounding) and agree. The explicit
/// `2^20` of headroom keeps the bound strictly below the `2^53` integer-exact
/// limit rather than merely at it.
pub(crate) fn exact_avg_domain(fact: &RecordBatch, dim: &RecordBatch) -> bool {
    /// Largest magnitude `fact.value` may take.
    const MAX_VALUE_MAGNITUDE: f64 = 1_000_000.0;
    /// Largest magnitude `dim.weight` may take.
    const MAX_WEIGHT_MAGNITUDE: f64 = 4.0;
    /// `dim.weight` granularity: quarter steps.
    const QUARTER_STEPS: f64 = 4.0;

    let (Some(fact_host), Some(fact_value)) =
        (string_column(fact, "host"), float_column(fact, "value"))
    else {
        return false;
    };
    let (Some(dim_host), Some(dim_area), Some(dim_weight)) = (
        string_column(dim, "host"),
        string_column(dim, "area"),
        float_column(dim, "weight"),
    ) else {
        return false;
    };

    // Payload counts per group, including the groups the rewritten branches
    // would aggregate: `fact` by `host`, `dim` by `(host, area)`. A null join
    // key cannot match (`NullEqualsNothing`), but its group still exists in the
    // rewritten branch, so it is counted and checked like any other.
    let mut fact_payloads: BTreeMap<Option<&str>, u64> = BTreeMap::new();
    for row in 0..fact.num_rows() {
        let group = (!fact_host.is_null(row)).then(|| fact_host.value(row));
        let payloads = fact_payloads.entry(group).or_insert(0);
        if fact_value.is_null(row) {
            continue;
        }
        let value = fact_value.value(row);
        if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_VALUE_MAGNITUDE {
            return false;
        }
        *payloads += 1;
    }

    let mut dim_payloads: BTreeMap<(Option<&str>, Option<&str>), u64> = BTreeMap::new();
    for row in 0..dim.num_rows() {
        let group = (
            (!dim_host.is_null(row)).then(|| dim_host.value(row)),
            (!dim_area.is_null(row)).then(|| dim_area.value(row)),
        );
        let payloads = dim_payloads.entry(group).or_insert(0);
        if dim_weight.is_null(row) {
            continue;
        }
        let weight = dim_weight.value(row);
        if !weight.is_finite()
            || (weight * QUARTER_STEPS).fract() != 0.0
            || weight.abs() > MAX_WEIGHT_MAGNITUDE
        {
            return false;
        }
        *payloads += 1;
    }

    // A group without a single non-null payload can only average to NULL, which
    // the certificate does not vouch for.
    if fact_payloads.values().any(|payloads| *payloads == 0)
        || dim_payloads.values().any(|payloads| *payloads == 0)
    {
        return false;
    }

    let max_fact_payloads = fact_payloads.values().copied().max().unwrap_or_default();
    let max_dim_payloads = dim_payloads.values().copied().max().unwrap_or_default();
    avg_domain_bound(max_fact_payloads, max_dim_payloads)
}

/// Whether the widest group pair count of the two inputs still bounds every
/// intermediate of the `AVG` factorization below the integer-exact limit.
///
/// `max_fact_payloads` is the largest payload count of a `fact` group and
/// `max_dim_payloads` the same for a `dim` group. The checked `u128` arithmetic
/// implements the conservative global bound
/// `4 * max_n_f * 1_000_000 * max_n_w * 16 + 2^20 < 2^53`: the leading `4` is
/// the weight magnitude, `1_000_000` the value magnitude, the trailing `16`
/// keeps a further margin over a pair's magnitude, and the `2^20` keeps the
/// result strictly below the limit instead of at it. An input with no rows has
/// no intermediate to bound and is accepted.
pub(crate) fn avg_domain_bound(max_fact_payloads: u64, max_dim_payloads: u64) -> bool {
    /// Largest magnitude `fact.value` may take.
    const MAX_VALUE_MAGNITUDE: u128 = 1_000_000;
    /// Largest magnitude `dim.weight` may take.
    const MAX_WEIGHT_MAGNITUDE: u128 = 4;
    /// Headroom over a pair's magnitude, keeping the bound conservative.
    const PRODUCT_HEADROOM: u128 = 16;
    /// Keeps the bound below the integer-exact limit instead of at it.
    const EXACT_HEADROOM: u128 = 1 << 20;
    /// Largest integer magnitude an `f64` represents exactly.
    const EXACT_LIMIT: u128 = 1 << 53;

    let bound = MAX_WEIGHT_MAGNITUDE
        .checked_mul(u128::from(max_fact_payloads))
        .and_then(|bound| bound.checked_mul(MAX_VALUE_MAGNITUDE))
        .and_then(|bound| bound.checked_mul(u128::from(max_dim_payloads)))
        .and_then(|bound| bound.checked_mul(PRODUCT_HEADROOM))
        .and_then(|bound| bound.checked_add(EXACT_HEADROOM));
    matches!(bound, Some(bound) if bound < EXACT_LIMIT)
}

/// The payload rewrite the caller should let the rule apply to `fact` and
/// `dim`, in the same precedence order the rule's own flags use: the exact
/// certificate first, then — only when the caller opted into approximate mode —
/// the approximate fuse.
///
/// The rule cannot see the batches, so this is the caller-side decision the
/// constructors [`CountJoinRewrite::with_avg`],
/// [`CountJoinRewrite::with_approx_avg`] and
/// [`CountJoinRewrite::with_avg_and_approx`] encode. It exists as a pure
/// function so the precedence is directly testable over real batches.
pub(crate) fn avg_rewrite_for(
    fact: &RecordBatch,
    dim: &RecordBatch,
    approx_enabled: bool,
) -> AvgRewrite {
    if exact_avg_domain(fact, dim) {
        AvgRewrite::Exact
    } else if approx_enabled && approx_avg_domain(fact, dim) {
        AvgRewrite::Approximate
    } else {
        AvgRewrite::Baseline
    }
}

/// The anti-absurdity fuse of the **approximate** `AVG(value * weight)` /
/// `SUM(value * weight)` rewrite.
///
/// Unlike [`exact_avg_domain`], this is deliberately **not** a certificate:
/// it does not prove the factorized result bit-identical, or even exactly
/// equal, to the pairwise baseline. It only rejects the inputs on which the
/// factorization could produce an *absurd* result, i.e. one whose relative
/// error is unbounded rather than merely of the baseline's own rounding order.
/// It is a heuristic guard — a proof of safety, not of exactness — and every
/// input it rejects keeps the baseline plan, and therefore the baseline's
/// results.
///
/// Factorizing `AVG`/`SUM` of the pairwise products is well behaved while all
/// three of the following hold, and the fuse enforces exactly that:
///
/// - the shapes and types of [`exact_avg_domain`]: `fact` carries a `Float64`
///   `value` and a `Utf8` `host`, `dim` a `Float64` `weight` and `Utf8`
///   `host`/`area` (so the rewrite's payload columns exist and are floats);
/// - **no cancellation**: no `(host)` group of `fact` and no `(host, area)`
///   group of `dim` has `|sum| < 1e-6 * sum|payload|` while `sum|payload| > 0`.
///   Such a group can have a true product sum near zero while the magnitudes
///   that built it are not, which makes the *relative* error of any
///   factorization unbounded and can flip the sign of the result. A group whose
///   payloads are all exactly zero (`sum|payload| == 0`) is fine: its result is
///   exactly zero, not a cancellation;
/// - **no overflow and no deep subnormal underflow**: every non-null payload is
///   finite, is either exactly zero or at least `2^-1000`, and the widest group
///   payload count scaled by the largest payload magnitude — on each side and
///   for their product, the scaled numerator — stays below `2^1000`, far under
///   the `f64` maximum of about `1.8e308`. See [`approx_avg_domain_bound`].
///
/// Any group with heavy cancellation rejects the whole batch. That is the
/// simplest sound policy: the fused rewrite stays a per-query switch rather
/// than a per-group decision, so one unsafe group falls the entire query back to
/// the baseline. Groups are keyed exactly like the rewritten branches aggregate
/// them, so a null join key forms its own group and is checked like any other.
///
/// The guard is checked with exact comparisons on payload magnitudes and with
/// log-domain comparisons on the scaled intermediates, so the check itself can
/// neither overflow nor round into a wrong answer.
pub(crate) fn approx_avg_domain(fact: &RecordBatch, dim: &RecordBatch) -> bool {
    let (Some(fact_host), Some(fact_value)) =
        (string_column(fact, "host"), float_column(fact, "value"))
    else {
        return false;
    };
    let (Some(dim_host), Some(dim_area), Some(dim_weight)) = (
        string_column(dim, "host"),
        string_column(dim, "area"),
        float_column(dim, "weight"),
    ) else {
        return false;
    };

    // Payload statistics per group, keyed the way the rewritten branches
    // aggregate: `fact` by `host`, `dim` by `(host, area)`.
    let mut fact_groups: BTreeMap<Option<&str>, PayloadStats> = BTreeMap::new();
    for row in 0..fact.num_rows() {
        let group = (!fact_host.is_null(row)).then(|| fact_host.value(row));
        let stats = fact_groups.entry(group).or_default();
        if fact_value.is_null(row) {
            continue;
        }
        let value = fact_value.value(row);
        if !payload_magnitude_usable(value) {
            return false;
        }
        stats.push(value);
    }

    let mut dim_groups: BTreeMap<(Option<&str>, Option<&str>), PayloadStats> = BTreeMap::new();
    for row in 0..dim.num_rows() {
        let group = (
            (!dim_host.is_null(row)).then(|| dim_host.value(row)),
            (!dim_area.is_null(row)).then(|| dim_area.value(row)),
        );
        let stats = dim_groups.entry(group).or_default();
        if dim_weight.is_null(row) {
            continue;
        }
        let weight = dim_weight.value(row);
        if !payload_magnitude_usable(weight) {
            return false;
        }
        stats.push(weight);
    }

    // One cancelling group is enough to make the whole query's factorization
    // unbounded in relative error, so the whole batch falls back.
    if fact_groups.values().any(PayloadStats::cancels)
        || dim_groups.values().any(PayloadStats::cancels)
    {
        return false;
    }

    let (max_fact_payloads, max_value_magnitude) = extreme_payloads(fact_groups.values());
    let (max_dim_payloads, max_weight_magnitude) = extreme_payloads(dim_groups.values());
    approx_avg_domain_bound(
        max_fact_payloads,
        max_value_magnitude,
        max_dim_payloads,
        max_weight_magnitude,
    )
}

/// The widest payload count and the largest payload magnitude over the groups
/// of one input, used to scale that branch's sums.
fn extreme_payloads<'a>(groups: impl Iterator<Item = &'a PayloadStats>) -> (u64, f64) {
    groups.fold((0, 0.0), |(payloads, magnitude), stats| {
        (
            payloads.max(stats.payloads),
            magnitude.max(stats.max_magnitude),
        )
    })
}

/// Smallest base-2 exponent a non-zero payload magnitude may carry: `2^-1000`,
/// about `9.33e-302`. Below it products and sums lose precision wholesale in
/// the subnormal range instead of merely rounding.
const MIN_PAYLOAD_LOG2: f64 = -1_000.0;

/// Largest base-2 exponent any scaled intermediate of the approximate
/// factorization may carry: `2^1000`, about `1.07e301` and more than `2^24`
/// below the `f64` maximum of about `2^1024`.
const MAX_INTERMEDIATE_LOG2: f64 = 1_000.0;

/// Whether the widest payload counts and the largest payload magnitudes keep
/// every intermediate of the approximate factorization inside the usable
/// exponent window.
///
/// `max_fact_payloads * max_value_magnitude` and `max_dim_payloads *
/// max_weight_magnitude` bound the two branch sums, and their product bounds the
/// numerator `SUM(value) * SUM(weight)`; all three must stay below `2^1000`,
/// which leaves more than `2^24` of headroom under the `f64` maximum of about
/// `2^1024`, so neither those products nor the summation of several of them can
/// overflow. Zero magnitudes bound nothing and are always accepted, and so is a
/// zero payload count.
///
/// The comparison is done in the base-2 log domain, so a bound that would
/// overflow an `f64` — a `u64` payload count times a `f64::MAX` magnitude — is
/// still decided exactly rather than saturating to a passing or failing
/// infinity. The threshold itself is a fixed headroom, not a tight limit: an
/// input just below it is as safe as one far below it, because `2^1000` is
/// thousands of powers of two away from any rounding or overflow boundary.
///
/// The helper also refuses a non-finite magnitude and a non-zero magnitude below
/// `2^-1000` (see `MIN_PAYLOAD_LOG2`), so it decides the whole exponent window of
/// a branch and can be tested on that window directly.
pub(crate) fn approx_avg_domain_bound(
    max_fact_payloads: u64,
    max_value_magnitude: f64,
    max_dim_payloads: u64,
    max_weight_magnitude: f64,
) -> bool {
    let (Some(fact), Some(dim)) = (
        scaled_log2(max_fact_payloads, max_value_magnitude),
        scaled_log2(max_dim_payloads, max_weight_magnitude),
    ) else {
        return false;
    };
    // A zero magnitude makes the log `-inf`, which passes every comparison
    // below and can never build an infinite intermediate.
    fact < MAX_INTERMEDIATE_LOG2
        && dim < MAX_INTERMEDIATE_LOG2
        && fact + dim < MAX_INTERMEDIATE_LOG2
}

/// `log2(count * magnitude)` of one branch's payloads, or `None` when the
/// magnitude is not usable at all — non-finite, or a non-zero magnitude the
/// subnormal floor rejects.
///
/// Computing the scaled magnitude as a sum of logarithms is what keeps the
/// bound itself from overflowing: the largest `f64` magnitude has a base-2
/// exponent of `1024` and a `u64` count adds at most `64`, far inside the
/// representable range of the sum.
fn scaled_log2(count: u64, magnitude: f64) -> Option<f64> {
    if !payload_magnitude_usable(magnitude) {
        return None;
    }
    Some((count as f64).log2() + magnitude.abs().log2())
}

/// Whether one payload magnitude can take part in the approximate
/// factorization: it must be a finite number and either exactly zero or at
/// least `2^-1000`, `MIN_PAYLOAD_LOG2`.
///
/// A non-finite payload has no factorization to reason about at all. A payload
/// below `2^-1000` sits in the deep subnormal range, where products and sums
/// lose precision wholesale instead of merely rounding, so it keeps the
/// baseline too.
fn payload_magnitude_usable(payload: f64) -> bool {
    payload.is_finite() && (payload == 0.0 || payload.abs().log2() >= MIN_PAYLOAD_LOG2)
}

/// Payload statistics of one `(host)` or `(host, area)` group: how many
/// non-null payloads it holds, their running `f64` sum, the running sum of
/// their magnitudes and the largest magnitude among them.
#[derive(Debug, Default)]
struct PayloadStats {
    payloads: u64,
    sum: f64,
    magnitude_sum: f64,
    max_magnitude: f64,
}

impl PayloadStats {
    /// Records one non-null payload in batch order.
    fn push(&mut self, payload: f64) {
        let magnitude = payload.abs();
        self.payloads += 1;
        self.sum += payload;
        self.magnitude_sum += magnitude;
        self.max_magnitude = self.max_magnitude.max(magnitude);
    }

    /// Whether this group's payloads cancel heavily: its sum is negligible
    /// against the magnitudes that built it, so a factorized result would carry
    /// an unbounded relative error. A group of exact zeros cannot cancel — its
    /// sum is exactly zero and so is every magnitude that built it.
    fn cancels(&self) -> bool {
        /// Heavy cancellation: the sum is below one millionth of the magnitude
        /// sum, e.g. `[1e16, 1, -1e16]`.
        const CANCELLATION_RATIO: f64 = 1e-6;

        self.magnitude_sum > 0.0 && self.sum.abs() < CANCELLATION_RATIO * self.magnitude_sum
    }
}

fn float_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Float64Array> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<Float64Array>()
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<StringArray>()
}

impl OptimizerRule for CountJoinRewrite {
    fn name(&self) -> &str {
        "bounded_count_join_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // An unsatisfiable bound means the pair count cannot be proven to fit
        // `COUNT(*)`'s result, so the `COUNT(*)` rewrite must not be applied.
        if self.pair_bound.is_some()
            && let Some((join, count, left_key, right_key)) = matching_shape(agg)
        {
            let Expr::Column(right_group) = &agg.group_expr[1] else {
                return Ok(Transformed::no(plan));
            };

            let left_count_name = config.alias_generator().next("partial_count");
            let right_count_name = config.alias_generator().next("partial_count");
            let left = Aggregate::try_new(
                Arc::clone(&join.left),
                vec![Expr::Column(left_key)],
                vec![Expr::AggregateFunction(count.clone()).alias(&left_count_name)],
            )?;
            let right = Aggregate::try_new(
                Arc::clone(&join.right),
                vec![Expr::Column(right_key), Expr::Column(right_group.clone())],
                vec![Expr::AggregateFunction(count.clone()).alias(&right_count_name)],
            )?;

            let left = Arc::new(LogicalPlan::Aggregate(left));
            let right = Arc::new(LogicalPlan::Aggregate(right));
            let join_keys = (column_at(left.schema(), 0), column_at(right.schema(), 0));
            let rewritten_join = Join::try_new(
                left,
                right,
                vec![(Expr::Column(join_keys.0), Expr::Column(join_keys.1))],
                None,
                JoinType::Inner,
                JoinConstraint::On,
                NullEquality::NullEqualsNothing,
                false,
            )?;
            let product = Expr::Column(Column::new_unqualified(left_count_name))
                * Expr::Column(Column::new_unqualified(right_count_name));
            let output = vec![
                Expr::Column(column_at(&rewritten_join.schema, 0)),
                Expr::Column(column_at(&rewritten_join.schema, 3)),
                product.alias(agg.schema.field(2).name()),
            ];
            let projection =
                Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
            return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
        }
        // The payload rewrites need no pair bound: they never materialize the
        // pair count, and both domain guards keep `COUNT(value) * COUNT(weight)`
        // far below `Int64::MAX`. They do need the caller's payload decision,
        // which a plan cannot show: `avg_rewrite` reports the mode the rule was
        // constructed with, and the exact path keeps its original, `AVG`-only
        // behaviour while approximate mode covers `AVG` and `SUM`.
        match self.avg_rewrite() {
            AvgRewrite::Baseline => {}
            AvgRewrite::Exact => {
                if let Some(matched) = matching_payload_shape(agg, PayloadFunction::Avg) {
                    return rewrite_payload(agg, &matched, PayloadFunction::Avg, config);
                }
            }
            AvgRewrite::Approximate => {
                for function in [PayloadFunction::Avg, PayloadFunction::Sum] {
                    if let Some(matched) = matching_payload_shape(agg, function) {
                        return rewrite_payload(agg, &matched, function, config);
                    }
                }
            }
        }
        Ok(Transformed::no(plan))
    }
}

/// Rewrites the certified `AVG(value * weight)` or `SUM(value * weight)`
/// aggregate into a join of two partial aggregates whose projection
/// reconstructs the payload function from them.
///
/// The projection computes, from the branch `nf`/`nv`/`sv` (fact) and
/// `nd`/`nw`/`sw` (dim) outputs:
///
/// ```text
/// valid = nv * nw
/// total = CASE WHEN sv * sw = 0.0 THEN 0.0 ELSE sv * sw END
/// AVG   = CASE WHEN valid = 0 THEN NULL ELSE total / CAST(valid AS Float64) END
/// SUM   = CASE WHEN valid = 0 THEN NULL ELSE total END
/// ```
///
/// Both reconstructions return `NULL` for a group without a single non-null
/// product, which is exactly when `valid` is zero: `nv` and `nw` are payload
/// counts, so their product is zero iff one side has no non-null payload and the
/// join therefore has no non-null product. `AVG` over an empty pair set is
/// `NULL` and so is `SUM`.
///
/// `total` normalizes a zero numerator to `+0.0` because the pairwise numerator
/// it must match can only cancel to `+0.0`: for facts `[1, -1]` with weight
/// `[-1]`, the baseline sums the products `-1` and `+1` into `+0.0`, while
/// `SUM(value)` is `+0.0`, `SUM(weight)` is `-1.0` and their naive product is
/// `-0.0`, which would divide into `-0.0` instead of the baseline's `+0.0`. On
/// the exact domain all sums are exact quarter-integers below `2^53` (see
/// [`exact_avg_domain`]), so no nonzero difference can be a rounding-order gap
/// and the normalized zero is the only place where the factorization could
/// disagree with the baseline. On the approximate domain ([`approx_avg_domain`])
/// the zero normalization is kept as well: a signed-zero mismatch would be
/// acceptable approximation noise, but the normalization is free and removes it.
fn rewrite_payload(
    agg: &Aggregate,
    matched: &PayloadMatch<'_>,
    function: PayloadFunction,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let PayloadMatch {
        join,
        left_key,
        right_key,
        right_group,
        fact_payload,
        dim_payload,
    } = matched;

    let fact_rows_name = config.alias_generator().next("partial_nf");
    let fact_payloads_name = config.alias_generator().next("partial_nv");
    let fact_sum_name = config.alias_generator().next("partial_sv");
    let dim_rows_name = config.alias_generator().next("partial_nd");
    let dim_payloads_name = config.alias_generator().next("partial_nw");
    let dim_sum_name = config.alias_generator().next("partial_sw");

    let left = Aggregate::try_new(
        Arc::clone(&join.left),
        vec![Expr::Column(left_key.clone())],
        vec![
            partial_aggregate(count_udaf(), count_star(), &fact_rows_name),
            partial_aggregate(
                count_udaf(),
                vec![Expr::Column(fact_payload.clone())],
                &fact_payloads_name,
            ),
            partial_aggregate(
                sum_udaf(),
                vec![Expr::Column(fact_payload.clone())],
                &fact_sum_name,
            ),
        ],
    )?;
    let right = Aggregate::try_new(
        Arc::clone(&join.right),
        vec![
            Expr::Column(right_key.clone()),
            Expr::Column(right_group.clone()),
        ],
        vec![
            partial_aggregate(count_udaf(), count_star(), &dim_rows_name),
            partial_aggregate(
                count_udaf(),
                vec![Expr::Column(dim_payload.clone())],
                &dim_payloads_name,
            ),
            partial_aggregate(
                sum_udaf(),
                vec![Expr::Column(dim_payload.clone())],
                &dim_sum_name,
            ),
        ],
    )?;

    let left = Arc::new(LogicalPlan::Aggregate(left));
    let right = Arc::new(LogicalPlan::Aggregate(right));
    let right_area = left.schema().fields().len() + 1;
    let join_keys = (column_at(left.schema(), 0), column_at(right.schema(), 0));
    let rewritten_join = Join::try_new(
        left,
        right,
        vec![(Expr::Column(join_keys.0), Expr::Column(join_keys.1))],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;

    let partial = |name: &str| Expr::Column(Column::new_unqualified(name));
    let valid = partial(&fact_payloads_name) * partial(&dim_payloads_name);
    let product = partial(&fact_sum_name) * partial(&dim_sum_name);
    // `total`: the signed-zero normalization described on this function.
    let total = Expr::Case(Case::new(
        None,
        vec![(
            Box::new(is_zero(product.clone(), float_literal(0.0))),
            Box::new(float_literal(0.0)),
        )],
        Some(Box::new(product)),
    ));
    let numerator = Expr::Case(Case::new(
        None,
        vec![(
            Box::new(is_zero(valid.clone(), int_literal(0))),
            Box::new(Expr::Literal(ScalarValue::Float64(None), None)),
        )],
        Some(Box::new(match function {
            PayloadFunction::Avg => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(total),
                Operator::Divide,
                Box::new(Expr::Cast(Cast::new(Box::new(valid), DataType::Float64))),
            )),
            PayloadFunction::Sum => total,
        })),
    ));
    let output = vec![
        Expr::Column(column_at(&rewritten_join.schema, 0)),
        Expr::Column(column_at(&rewritten_join.schema, right_area)),
        numerator.alias(agg.schema.field(2).name()),
    ];
    let projection = Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

fn partial_aggregate(udaf: Arc<AggregateUDF>, args: Vec<Expr>, alias: &str) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        args,
        false,
        None,
        vec![],
        None,
    ))
    .alias(alias)
}

fn count_star() -> Vec<Expr> {
    vec![Expr::Literal(COUNT_STAR_EXPANSION, None)]
}

fn int_literal(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(value)), None)
}

fn float_literal(value: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(Some(value)), None)
}

/// `<lhs> = <rhs>`, used to detect the zero and NULL cases of the `AVG`
/// reconstruction.
fn is_zero(lhs: Expr, rhs: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), Operator::Eq, Box::new(rhs)))
}

fn column_at(schema: &DFSchema, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

/// Matches the narrow supported shape and returns the join plus everything the
/// rewrite needs from it.
///
/// Required shape: an inner `ON` equi-join on the `host` columns, grouped by the
/// left `host` and the right `area`, aggregated by a single plain `COUNT(*)`
/// (i.e. `COUNT` over `COUNT_STAR_EXPANSION` with no modifier), with a `fact`
/// table or subquery alias on the left and a `dim` one on the right. The group
/// expressions may be separated from the join by a projection that repeats them.
fn matching_shape(agg: &Aggregate) -> Option<(&Join, &AggregateFunction, Column, Column)> {
    if agg.group_expr.len() != 2 || agg.aggr_expr.len() != 1 {
        return None;
    }
    let join = match agg.input.as_ref() {
        LogicalPlan::Join(join) => join,
        LogicalPlan::Projection(projection) if projection.expr == agg.group_expr => {
            let LogicalPlan::Join(join) = projection.input.as_ref() else {
                return None;
            };
            join
        }
        _ => return None,
    };
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.null_equality != NullEquality::NullEqualsNothing
    {
        return None;
    }
    let (left_key, right_key) = equality_keys(join)?;
    if left_key.name != "host" || right_key.name != "host" {
        return None;
    }
    let Expr::AggregateFunction(count) = &agg.aggr_expr[0] else {
        return None;
    };
    let params = &count.params;
    if count.func.inner().downcast_ref::<Count>().is_none()
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || params.args != [Expr::Literal(COUNT_STAR_EXPANSION, None)]
    {
        return None;
    }
    let (Expr::Column(left_group), Expr::Column(right_group)) =
        (&agg.group_expr[0], &agg.group_expr[1])
    else {
        return None;
    };
    if left_group != left_key
        || !right_group.name.eq_ignore_ascii_case("area")
        || right_group.relation != right_key.relation
        || !fixture_scan(&join.left, "fact")
        || !fixture_scan(&join.right, "dim")
    {
        return None;
    }
    Some((join, count, left_key.clone(), right_key.clone()))
}

/// The payload aggregate a rewrite reconstructs from the branch statistics.
///
/// Both functions share the same shape, the same two payload columns and the
/// same branch statistics; they differ only in what the projection computes from
/// the payload sum product: `AVG` divides it by the payload count product, `SUM`
/// is the product itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadFunction {
    /// `AVG(<fact payload> * <dim payload>)`.
    Avg,
    /// `SUM(<fact payload> * <dim payload>)`.
    Sum,
}

/// The pieces of the `AVG`/`SUM(<fact payload> * <dim payload>)` shape the
/// rewrite needs.
struct PayloadMatch<'a> {
    join: &'a Join,
    left_key: Column,
    right_key: Column,
    right_group: Column,
    fact_payload: Column,
    dim_payload: Column,
}

/// Matches the narrow supported `AVG(payload * payload)` or
/// `SUM(payload * payload)` shape and returns the join plus everything the
/// rewrite needs from it.
///
/// Required shape: the same inner `ON` equi-join on `host` with a `fact` table
/// or subquery alias on the left and a `dim` one on the right (so the payload
/// columns are the `fact.value` and `dim.weight` columns the domain guards
/// check), grouped by the left `host` and the right `area`, aggregated by a
/// single `AVG` — or, for [`PayloadFunction::Sum`], `SUM` — of the product of
/// those two columns, with no `DISTINCT`, `FILTER`, `ORDER BY`, null treatment
/// or extra argument. The product operands may be given in either order and may
/// reach the aggregate through a projection that repeats them.
fn matching_payload_shape(agg: &Aggregate, function: PayloadFunction) -> Option<PayloadMatch<'_>> {
    if agg.group_expr.len() != 2 || agg.aggr_expr.len() != 1 {
        return None;
    }
    let (join, projection) = match agg.input.as_ref() {
        LogicalPlan::Join(join) => (join, None),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Join(join) => (join, Some(projection)),
            _ => return None,
        },
        _ => return None,
    };
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.null_equality != NullEquality::NullEqualsNothing
    {
        return None;
    }
    let (left_key, right_key) = equality_keys(join)?;
    if left_key.name != "host" || right_key.name != "host" {
        return None;
    }
    let aggregate = match &agg.aggr_expr[0] {
        Expr::AggregateFunction(aggregate) => aggregate,
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::AggregateFunction(aggregate) => aggregate,
            _ => return None,
        },
        _ => return None,
    };
    let params = &aggregate.params;
    let inner = aggregate.func.inner();
    let matches_function = match function {
        PayloadFunction::Avg => inner.downcast_ref::<Avg>().is_some(),
        PayloadFunction::Sum => inner.downcast_ref::<Sum>().is_some(),
    };
    if !matches_function
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || params.args.len() != 1
    {
        return None;
    }
    let (Expr::Column(left_group), Expr::Column(right_group)) =
        (&agg.group_expr[0], &agg.group_expr[1])
    else {
        return None;
    };
    if left_group != left_key
        || !right_group.name.eq_ignore_ascii_case("area")
        || right_group.relation != right_key.relation
        || !fixture_scan(&join.left, "fact")
        || !fixture_scan(&join.right, "dim")
    {
        return None;
    }
    let (first, second) = product_operands(&params.args[0], projection)?;
    Some(PayloadMatch {
        join,
        left_key: left_key.clone(),
        right_key: right_key.clone(),
        right_group: right_group.clone(),
        fact_payload: payload_column(&join.left, "value", first)?,
        dim_payload: payload_column(&join.right, "weight", second)?,
    })
}

/// The two operands of a payload aggregate's argument, which must be a
/// multiplication of two columns of the joined inputs, in either order.
///
/// The projection that may sit between the aggregate and the join either passes
/// the product's operands through under their original names (the argument is
/// then the multiplication itself) or computes the product on its own, in which
/// case the argument is a column of the projection and the multiplication is
/// looked up there.
fn product_operands<'a>(
    argument: &'a Expr,
    projection: Option<&'a Projection>,
) -> Option<(&'a Expr, &'a Expr)> {
    let entry = match argument {
        Expr::BinaryExpr(binary) => binary,
        Expr::Column(column) => {
            let projection = projection?;
            let index = projection
                .schema
                .maybe_index_of_column(column)
                .or_else(|| {
                    projection
                        .schema
                        .index_of_column_by_name(None, &column.name)
                })?;
            let entry = projection.expr.get(index)?;
            match entry {
                Expr::BinaryExpr(binary) => binary,
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::BinaryExpr(binary) => binary,
                    _ => return None,
                },
                _ => return None,
            }
        }
        _ => return None,
    };
    if entry.op != Operator::Multiply {
        return None;
    }
    Some((entry.left.as_ref(), entry.right.as_ref()))
}

/// Resolves `operand` to the `name` column of the input it must come from, so a
/// payload from the wrong side of the join — or any expression other than a
/// plain column — cannot be rewritten.
fn payload_column(input: &LogicalPlan, name: &str, operand: &Expr) -> Option<Column> {
    let Expr::Column(column) = operand else {
        return None;
    };
    if !column.name.eq(name) || !input.schema().has_column(column) {
        return None;
    }
    Some(column.clone())
}

fn equality_keys(join: &Join) -> Option<(&Column, &Column)> {
    if join.on.len() != 1 || join.filter.is_some() {
        return None;
    }
    let (Expr::Column(left), Expr::Column(right)) = (&join.on[0].0, &join.on[0].1) else {
        return None;
    };
    Some((left, right))
}

fn fixture_scan(plan: &LogicalPlan, table: &str) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => scan.table_name.to_string().eq_ignore_ascii_case(table),
        LogicalPlan::SubqueryAlias(alias) => match alias.input.as_ref() {
            LogicalPlan::TableScan(scan) => scan.table_name.to_string().eq_ignore_ascii_case(table),
            _ => false,
        },
        _ => false,
    }
}
