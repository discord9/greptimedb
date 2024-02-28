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

//! Accumulators for aggregate functions that's is accumulatable. i.e. sum/count
//!
//! Accumulator will only be restore from row and being updated every time dataflow need process a new batch of rows.
//! So the overhead is acceptable.
//!
//! Currently support sum, count, any, all and min/max(with one caveat that min/max can't support delete with aggregate).

use std::fmt::Display;

use common_decimal::Decimal128;
use common_time::{Date, DateTime};
use datatypes::data_type::ConcreteDataType;
use datatypes::value::{OrderedF32, OrderedF64, OrderedFloat, Value};
use enum_dispatch::enum_dispatch;
use hydroflow::futures::stream::Concat;
use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::expr::error::{InternalSnafu, TryFromValueSnafu, TypeMismatchSnafu};
use crate::expr::{AggregateFunc, EvalError};
use crate::repr::Diff;

/// Accumulates values for the various types of accumulable aggregations.
#[enum_dispatch]
pub trait Accumulator: Sized {
    fn into_state(self) -> Vec<Value>;
    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError>;
    fn update_batch<I>(&mut self, aggr_fn: &AggregateFunc, value_diffs: I) -> Result<(), EvalError>
    where
        I: IntoIterator<Item = (Value, Diff)>,
    {
        for (v, d) in value_diffs {
            self.update(aggr_fn, v, d)?;
        }
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Bool {
    /// The number of `true` values observed.
    trues: Diff,
    /// The number of `false` values observed.
    falses: Diff,
}

impl TryFrom<Vec<Value>> for Bool {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 2,
            InternalSnafu {
                reason: "Bool Accumulator state should have 2 values",
            }
        );

        let mut iter = state.into_iter();

        Ok(Self {
            trues: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            falses: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }
}

impl Accumulator for Bool {
    fn into_state(self) -> Vec<Value> {
        vec![self.trues.into(), self.falses.into()]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            matches!(
                aggr_fn,
                AggregateFunc::Any
                    | AggregateFunc::All
                    | AggregateFunc::MaxBool
                    | AggregateFunc::MinBool
            ),
            InternalSnafu {
                reason: format!(
                    "Bool Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );
        match value {
            Value::Boolean(true) => self.trues += diff,
            Value::Boolean(false) => self.falses += diff,
            x => {
                return Err(TypeMismatchSnafu {
                    expected: ConcreteDataType::boolean_datatype(),
                    actual: x.data_type(),
                }
                .build())
            }
        };
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match aggr_fn {
            AggregateFunc::Any => Ok(Value::from(self.trues > 0)),
            AggregateFunc::All => Ok(Value::from(self.falses == 0)),
            AggregateFunc::MaxBool => Ok(Value::from(self.trues > 0)),
            AggregateFunc::MinBool => Ok(Value::from(self.falses == 0)),
            _ => Err(InternalSnafu {
                reason: format!(
                    "Bool Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build()),
        }
    }
}

/// Accumulates simple numeric values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimpleNumber {
    /// The accumulation of all non-NULL values observed.
    accum: i128,
    /// The number of non-NULL values observed.
    non_nulls: Diff,
}

impl TryFrom<Vec<Value>> for SimpleNumber {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 2,
            InternalSnafu {
                reason: "Number Accumulator state should have 2 values",
            }
        );
        let mut iter = state.into_iter();

        Ok(Self {
            accum: Decimal128::try_from(iter.next().unwrap())
                .map_err(err_try_from_val)?
                .val(),
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }
}

impl Accumulator for SimpleNumber {
    fn into_state(self) -> Vec<Value> {
        vec![
            Value::Decimal128(Decimal128::new(self.accum, 38, 0)),
            self.non_nulls.into(),
        ]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            matches!(
                aggr_fn,
                AggregateFunc::SumInt16
                    | AggregateFunc::SumInt32
                    | AggregateFunc::SumInt64
                    | AggregateFunc::SumUInt16
                    | AggregateFunc::SumUInt32
                    | AggregateFunc::SumUInt64
            ),
            InternalSnafu {
                reason: format!(
                    "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );

        let v = match value {
            Value::Int16(x) => i128::from(x),
            Value::Int32(x) => i128::from(x),
            Value::Int64(x) => i128::from(x),
            Value::UInt16(x) => i128::from(x),
            Value::UInt32(x) => i128::from(x),
            Value::UInt64(x) => i128::from(x),
            v => {
                let expected_datatype = match aggr_fn {
                    AggregateFunc::SumInt16 => ConcreteDataType::int16_datatype(),
                    AggregateFunc::SumInt32 => ConcreteDataType::int32_datatype(),
                    AggregateFunc::SumInt64 => ConcreteDataType::int64_datatype(),
                    AggregateFunc::SumUInt16 => ConcreteDataType::uint16_datatype(),
                    AggregateFunc::SumUInt32 => ConcreteDataType::uint32_datatype(),
                    AggregateFunc::SumUInt64 => ConcreteDataType::uint64_datatype(),
                    _ => unreachable!(),
                };
                return Err(TypeMismatchSnafu {
                    expected: expected_datatype,
                    actual: v.data_type(),
                }
                .build())?;
            }
        };

        self.accum += v * i128::from(diff);

        self.non_nulls += diff;
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match aggr_fn {
            AggregateFunc::SumInt16 | AggregateFunc::SumInt32 | AggregateFunc::SumInt64 => {
                Ok(Value::from(self.accum as i64))
            }
            AggregateFunc::SumUInt16 | AggregateFunc::SumUInt32 | AggregateFunc::SumUInt64 => {
                Ok(Value::from(self.accum as u64))
            }
            _ => Err(InternalSnafu {
                reason: format!(
                    "SimpleNumber Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build()),
        }
    }
}
/// Accumulates float values.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]

pub struct Float {
    /// Accumulates non-special float values, i.e. not NaN, +inf, -inf.
    /// accum will be set to zero if `non_nulls` is zero.
    accum: OrderedF64,
    /// Counts +inf
    pos_infs: Diff,
    /// Counts -inf
    neg_infs: Diff,
    /// Counts NaNs
    nans: Diff,
    /// Counts non-NULL values
    non_nulls: Diff,
}

impl TryFrom<Vec<Value>> for Float {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 5,
            InternalSnafu {
                reason: "Float Accumulator state should have 5 values",
            }
        );

        let mut iter = state.into_iter();

        let mut ret = Self {
            accum: OrderedF64::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            pos_infs: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            neg_infs: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            nans: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        };

        // This prevent counter-intuitive behavior of summing over no values
        if ret.non_nulls == 0 {
            ret.accum = OrderedFloat::from(0.0);
        }

        Ok(ret)
    }
}

impl Accumulator for Float {
    fn into_state(self) -> Vec<Value> {
        vec![
            self.accum.into(),
            self.pos_infs.into(),
            self.neg_infs.into(),
            self.nans.into(),
            self.non_nulls.into(),
        ]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            matches!(
                aggr_fn,
                AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64
            ),
            InternalSnafu {
                reason: format!(
                    "Float Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );
        let x = match value {
            Value::Float32(x) => OrderedF64::from(*x as f64),
            Value::Float64(x) => OrderedF64::from(x),
            v => {
                let expected_datatype = match aggr_fn {
                    AggregateFunc::SumFloat32 => ConcreteDataType::float32_datatype(),
                    AggregateFunc::SumFloat64 => ConcreteDataType::float64_datatype(),
                    _ => unreachable!(),
                };
                return Err(TypeMismatchSnafu {
                    expected: expected_datatype,
                    actual: v.data_type(),
                }
                .build())?;
            }
        };

        if x.is_nan() {
            self.nans += diff;
        } else if x.is_infinite() {
            if x.is_sign_positive() {
                self.pos_infs += diff;
            } else {
                self.neg_infs += diff;
            }
        } else {
            self.accum += *(x * OrderedF64::from(diff as f64));
        }

        self.non_nulls += diff;
        Ok(())
    }

    fn eval(&self, aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        match aggr_fn {
            AggregateFunc::SumFloat32 => Ok(Value::Float32(OrderedF32::from(self.accum.0 as f32))),
            AggregateFunc::SumFloat64 => Ok(Value::Float64(self.accum)),
            _ => Err(InternalSnafu {
                reason: format!(
                    "Float Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
            .build()),
        }
    }
}

/// Accumulates a single `Ord`ed `Value`, useful for min/max aggregations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrdValue {
    val: Option<Value>,
    non_nulls: Diff,
}

impl TryFrom<Vec<Value>> for OrdValue {
    type Error = EvalError;

    fn try_from(state: Vec<Value>) -> Result<Self, Self::Error> {
        ensure!(
            state.len() == 2,
            InternalSnafu {
                reason: "OrdValue Accumulator state should have 2 values",
            }
        );

        let mut iter = state.into_iter();

        Ok(Self {
            val: Some(iter.next().unwrap()),
            non_nulls: Diff::try_from(iter.next().unwrap()).map_err(err_try_from_val)?,
        })
    }
}

impl Accumulator for OrdValue {
    fn into_state(self) -> Vec<Value> {
        vec![self.val.unwrap_or(Value::Null), self.non_nulls.into()]
    }

    fn update(
        &mut self,
        aggr_fn: &AggregateFunc,
        value: Value,
        diff: Diff,
    ) -> Result<(), EvalError> {
        ensure!(
            aggr_fn.is_max() || aggr_fn.is_min() || matches!(aggr_fn, AggregateFunc::Count),
            InternalSnafu {
                reason: format!(
                    "OrdValue Accumulator does not support this aggregation function: {:?}",
                    aggr_fn
                ),
            }
        );
        if let Some(v) = &self.val {
            if v.data_type() != value.data_type() {
                return Err(TypeMismatchSnafu {
                    expected: v.data_type(),
                    actual: value.data_type(),
                }
                .build());
            }
        }
        if diff <= 0 && (aggr_fn.is_max() || aggr_fn.is_min()) {
            return Err(InternalSnafu {
                reason: "OrdValue Accumulator does not support non-monotonic input for min/max aggregation".to_string(),
            }.build());
        }
        if aggr_fn.is_max() {
            self.val = self
                .val
                .clone()
                .map(|v| v.max(value.clone()))
                .or_else(|| Some(value));
        } else if aggr_fn.is_min() {
            self.val = self
                .val
                .clone()
                .map(|v| v.min(value.clone()))
                .or_else(|| Some(value));
        }
        self.non_nulls += diff;
        Ok(())
    }

    fn eval(&self, _aggr_fn: &AggregateFunc) -> Result<Value, EvalError> {
        Ok(self.val.clone().unwrap_or(Value::Null))
    }
}

/// Accumulates values for the various types of accumulable aggregations.
///
/// We assume that there are not more than 2^32 elements for the aggregation.
/// Thus we can perform a summation over i32 in an i64 accumulator
/// and not worry about exceeding its bounds.
///
/// The float accumulator performs accumulation with tolerance for floating point error.
///
/// TODO(discord9): check for overflowing
#[enum_dispatch(Accumulator)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Accum {
    /// Accumulates boolean values.
    Bool(Bool),
    /// Accumulates simple numeric values.
    SimpleNumber(SimpleNumber),
    /// Accumulates float values.
    Float(Float),
    /// Accumulate Values that impl `Ord`
    OrdValue(OrdValue),
}

impl Accum {
    pub fn new_accum(aggr_fn: &AggregateFunc) -> Result<Self, EvalError> {
        Ok(match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Self::from(Bool {
                trues: 0,
                falses: 0,
            }),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64 => Self::from(SimpleNumber {
                accum: 0,
                non_nulls: 0,
            }),
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => Self::from(Float {
                accum: OrderedF64::from(0.0),
                pos_infs: 0,
                neg_infs: 0,
                nans: 0,
                non_nulls: 0,
            }),
            f => {
                if f.is_max() || f.is_min() || matches!(f, AggregateFunc::Count) {
                    Self::from(OrdValue {
                        val: None,
                        non_nulls: 0,
                    })
                } else {
                    return Err(InternalSnafu {
                        reason: format!(
                            "Accumulator does not support this aggregation function: {:?}",
                            f
                        ),
                    }
                    .build());
                }
            }
        })
    }
    pub fn try_into_accum(aggr_fn: &AggregateFunc, state: Vec<Value>) -> Result<Self, EvalError> {
        match aggr_fn {
            AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::MaxBool
            | AggregateFunc::MinBool => Ok(Self::from(Bool::try_from(state)?)),
            AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64 => Ok(Self::from(SimpleNumber::try_from(state)?)),
            AggregateFunc::SumFloat32 | AggregateFunc::SumFloat64 => {
                Ok(Self::from(Float::try_from(state)?))
            }
            f => {
                if f.is_max() || f.is_min() || matches!(f, AggregateFunc::Count) {
                    Ok(Self::from(OrdValue::try_from(state)?))
                } else {
                    Err(InternalSnafu {
                        reason: format!(
                            "Accumulator does not support this aggregation function: {:?}",
                            f
                        ),
                    }
                    .build())
                }
            }
        }
    }
}

fn err_try_from_val<T: Display>(reason: T) -> EvalError {
    TryFromValueSnafu {
        msg: reason.to_string(),
    }
    .build()
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_accum() {
        let testcases = vec![
            (
                AggregateFunc::SumInt32,
                vec![(Value::Int32(1), 1)],
                (
                    Value::Int64(1),
                    vec![Value::Decimal128(Decimal128::new(1, 38, 0)), 1i64.into()],
                ),
            ),
            (
                AggregateFunc::SumFloat32,
                vec![(Value::Float32(OrderedF32::from(1.0)), 1)],
                (
                    Value::Float32(OrderedF32::from(1.0)),
                    vec![
                        Value::Float64(OrderedF64::from(1.0)),
                        0i64.into(),
                        0i64.into(),
                        0i64.into(),
                        1i64.into(),
                    ],
                ),
            ),
            (
                AggregateFunc::MaxInt32,
                vec![(Value::Int32(1), 1), (Value::Int32(2), 1)],
                (Value::Int32(2), vec![Value::Int32(2), 2i64.into()]),
            ),
            (
                AggregateFunc::MinInt32,
                vec![(Value::Int32(2), 1), (Value::Int32(1), 1)],
                (Value::Int32(1), vec![Value::Int32(1), 2i64.into()]),
            ),
            (
                AggregateFunc::MaxFloat32,
                vec![
                    (Value::Float32(OrderedF32::from(1.0)), 1),
                    (Value::Float32(OrderedF32::from(2.0)), 1),
                ],
                (
                    Value::Float32(OrderedF32::from(2.0)),
                    vec![Value::Float32(OrderedF32::from(2.0)), 2i64.into()],
                ),
            ),
            (
                AggregateFunc::MaxDateTime,
                vec![
                    (Value::DateTime(DateTime::from(0)), 1),
                    (Value::DateTime(DateTime::from(1)), 1),
                ],
                (
                    Value::DateTime(DateTime::from(1)),
                    vec![Value::DateTime(DateTime::from(1)), 2i64.into()],
                ),
            ),
            (
                AggregateFunc::Count,
                vec![(Value::Int32(1), 1), (Value::Int32(2), 1)],
                (Value::Null, vec![Value::Null, 2i64.into()]),
            ),
            (
                AggregateFunc::Any,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                ],
                (
                    Value::Boolean(true),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
            (
                AggregateFunc::All,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                ],
                (
                    Value::Boolean(false),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
            (
                AggregateFunc::MaxBool,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                ],
                (
                    Value::Boolean(true),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
            (
                AggregateFunc::MinBool,
                vec![
                    (Value::Boolean(false), 1),
                    (Value::Boolean(false), 1),
                    (Value::Boolean(true), 1),
                ],
                (
                    Value::Boolean(false),
                    vec![Value::from(1i64), Value::from(2i64)],
                ),
            ),
        ];

        for (aggr_fn, input, (eval_res, state)) in testcases {
            let mut acc = Accum::new_accum(&aggr_fn).unwrap();
            acc.update_batch(&aggr_fn, input).unwrap();
            let row = acc.into_state();
            let acc = Accum::try_into_accum(&aggr_fn, row).unwrap();

            assert_eq!(acc.eval(&aggr_fn).unwrap(), eval_res);
            assert_eq!(acc.into_state(), state);
        }
    }
}
