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

//! Shared native histogram field contract.
//!
//! Prom remote-write v2 stores these names as children of one Struct field, while
//! metric-engine uses the same contract to recognize native histogram tables.
//!
//! [`PublicHistogramBatch`] retains such a struct array as it was supplied: it validates the
//! structural schema and indexes rows, but keeps every child value, buffer, and null exactly as
//! given, so integer payloads above `2^53` and float payload bits survive unchanged.

use std::sync::{Arc, LazyLock};

use crate::arrow::array::{Array, Float64Array, StructArray};
use crate::arrow::datatypes::DataType as ArrowDataType;
use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{InvalidSeriesSnafu, Result};
use crate::types::{StructField, StructType};

pub const NATIVE_HISTOGRAM_FIELD: &str = "greptime_native_histogram";
pub const SCHEMA_FIELD: &str = "schema";
pub const ZERO_THRESHOLD_FIELD: &str = "zero_threshold";
pub const SUM_FIELD: &str = "sum";
pub const RESET_HINT_FIELD: &str = "reset_hint";
pub const START_TIMESTAMP_FIELD: &str = "start_timestamp";
pub const CUSTOM_VALUES_FIELD: &str = "custom_values";
pub const POSITIVE_SPAN_OFFSETS_FIELD: &str = "positive_span_offsets";
pub const POSITIVE_SPAN_LENGTHS_FIELD: &str = "positive_span_lengths";
pub const NEGATIVE_SPAN_OFFSETS_FIELD: &str = "negative_span_offsets";
pub const NEGATIVE_SPAN_LENGTHS_FIELD: &str = "negative_span_lengths";
pub const COUNT_I64_FIELD: &str = "count_i64";
pub const ZERO_COUNT_I64_FIELD: &str = "zero_count_i64";
pub const POSITIVE_BUCKETS_I64_FIELD: &str = "positive_buckets_i64";
pub const NEGATIVE_BUCKETS_I64_FIELD: &str = "negative_buckets_i64";
pub const COUNT_F64_FIELD: &str = "count_f64";
pub const ZERO_COUNT_F64_FIELD: &str = "zero_count_f64";
pub const POSITIVE_BUCKETS_F64_FIELD: &str = "positive_buckets_f64";
pub const NEGATIVE_BUCKETS_F64_FIELD: &str = "negative_buckets_f64";

// Keep int and float payloads in separate columns. The populated family is the
// type signal, so we don't need to persist an extra histogram-type tag.
pub const NATIVE_HISTOGRAM_FIELD_NAMES: &[&str] = &[
    SCHEMA_FIELD,
    ZERO_THRESHOLD_FIELD,
    SUM_FIELD,
    RESET_HINT_FIELD,
    START_TIMESTAMP_FIELD,
    CUSTOM_VALUES_FIELD,
    POSITIVE_SPAN_OFFSETS_FIELD,
    POSITIVE_SPAN_LENGTHS_FIELD,
    NEGATIVE_SPAN_OFFSETS_FIELD,
    NEGATIVE_SPAN_LENGTHS_FIELD,
    COUNT_I64_FIELD,
    ZERO_COUNT_I64_FIELD,
    POSITIVE_BUCKETS_I64_FIELD,
    NEGATIVE_BUCKETS_I64_FIELD,
    COUNT_F64_FIELD,
    ZERO_COUNT_F64_FIELD,
    POSITIVE_BUCKETS_F64_FIELD,
    NEGATIVE_BUCKETS_F64_FIELD,
];

static NATIVE_HISTOGRAM_VALUE_TYPE: LazyLock<ConcreteDataType> = LazyLock::new(|| {
    let fields = NATIVE_HISTOGRAM_FIELD_NAMES
        .iter()
        .filter_map(|name| {
            let data_type = native_histogram_field_type(name)?;
            Some(StructField::new((*name).to_string(), data_type, true))
        })
        .collect();
    ConcreteDataType::struct_datatype(StructType::new(Arc::new(fields)))
});

/// Returns the exact Greptime type for a persisted native histogram field.
pub fn native_histogram_field_type(name: &str) -> Option<ConcreteDataType> {
    match name {
        SCHEMA_FIELD | RESET_HINT_FIELD => Some(ConcreteDataType::int32_datatype()),
        ZERO_THRESHOLD_FIELD | SUM_FIELD | COUNT_F64_FIELD | ZERO_COUNT_F64_FIELD => {
            Some(ConcreteDataType::float64_datatype())
        }
        START_TIMESTAMP_FIELD => Some(ConcreteDataType::timestamp_millisecond_datatype()),
        CUSTOM_VALUES_FIELD | POSITIVE_BUCKETS_F64_FIELD | NEGATIVE_BUCKETS_F64_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::float64_datatype())),
        ),
        POSITIVE_SPAN_OFFSETS_FIELD | NEGATIVE_SPAN_OFFSETS_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype())),
        ),
        POSITIVE_SPAN_LENGTHS_FIELD | NEGATIVE_SPAN_LENGTHS_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype())),
        ),
        COUNT_I64_FIELD | ZERO_COUNT_I64_FIELD => Some(ConcreteDataType::int64_datatype()),
        POSITIVE_BUCKETS_I64_FIELD | NEGATIVE_BUCKETS_I64_FIELD => Some(
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int64_datatype())),
        ),
        _ => None,
    }
}

/// Returns the exact Greptime type for a complete native histogram value.
pub fn native_histogram_value_type() -> &'static ConcreteDataType {
    &NATIVE_HISTOGRAM_VALUE_TYPE
}

/// Returns the Arrow type for a complete native histogram value.
pub fn native_histogram_arrow_type() -> ArrowDataType {
    native_histogram_value_type().as_arrow_type()
}

/// Position of [`SUM_FIELD`] in [`NATIVE_HISTOGRAM_FIELD_NAMES`].
///
/// [`PublicHistogramBatch::try_from_arrow`] rejects any payload whose member order differs, so the
/// sum column is always at this position in a validated batch.
const SUM_COLUMN: usize = 2;

/// An immutable public (remote-write v2) native histogram table.
///
/// The batch owns exactly the supplied struct array: all 18 members, their buffers, and their
/// hidden values are retained as they are, without semantic validation of spans, buckets, or
/// algorithms, and without rebuilding individual members.
#[derive(Clone, Debug)]
pub struct PublicHistogramBatch {
    array: StructArray,
}
impl PublicHistogramBatch {
    /// Validate the exact native histogram structural schema and retain the payload.
    ///
    /// The check covers member names, their order, their types, and their item-level metadata and
    /// nullability. Values themselves are not inspected: a payload is accepted as it stands.
    pub fn try_from_arrow(array: StructArray) -> Result<Self> {
        let ArrowDataType::Struct(fields) = native_histogram_arrow_type() else {
            return invalid("native histogram value type is not a struct");
        };
        if array.fields() != &fields {
            return invalid("public histogram fields, order, types, or nullability mismatch");
        }
        Ok(Self { array })
    }
    /// The retained struct array, shared with every view and slice.
    pub fn as_arrow(&self) -> &StructArray {
        &self.array
    }
    /// Number of histogram rows.
    pub fn len(&self) -> usize {
        self.array.len()
    }
    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }
    /// Returns a borrowed row, or `None` only when `row` is out of bounds.
    ///
    /// A null parent row still yields a view; the view reports it through
    /// [`PublicHistogramView::is_null`].
    pub fn view(&self, row: usize) -> Option<PublicHistogramView<'_>> {
        if row >= self.len() {
            return None;
        }
        Some(PublicHistogramView { batch: self, row })
    }
}

/// A row borrowed from a [`PublicHistogramBatch`].
#[derive(Clone, Copy, Debug)]
pub struct PublicHistogramView<'a> {
    batch: &'a PublicHistogramBatch,
    row: usize,
}
impl<'a> PublicHistogramView<'a> {
    /// The table this row belongs to.
    pub fn batch(&self) -> &'a PublicHistogramBatch {
        self.batch
    }
    /// Row position within [`Self::batch`].
    pub fn row(&self) -> usize {
        self.row
    }
    /// Whether the parent row is null.
    pub fn is_null(&self) -> bool {
        !self.batch.array.is_valid(self.row)
    }
    /// The row's `sum`, or `None` when the parent row is null or the scalar itself is null.
    ///
    /// A present value is returned bit for bit; no arithmetic or canonicalization is applied. A
    /// null parent row therefore hides its children instead of exposing them.
    pub fn sum(&self) -> Option<f64> {
        if self.is_null() {
            return None;
        }
        let sum = self
            .batch
            .array
            .column(SUM_COLUMN)
            .as_any()
            .downcast_ref::<Float64Array>()?;
        if sum.is_valid(self.row) {
            Some(sum.value(self.row))
        } else {
            None
        }
    }
}

fn invalid<T>(reason: impl Into<String>) -> Result<T> {
    Err(InvalidSeriesSnafu {
        reason: reason.into(),
    }
    .build())
}

#[cfg(test)]
mod tests;
