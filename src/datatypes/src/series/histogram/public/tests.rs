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

use std::sync::Arc;

use super::*;
use crate::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};

/// Expected contract: field name, Greptime type, and nullability, in order.
fn expected_fields() -> Vec<(&'static str, ConcreteDataType, bool)> {
    let list = |item| ConcreteDataType::list_datatype(Arc::new(item));
    vec![
        ("schema", ConcreteDataType::int32_datatype(), true),
        ("zero_threshold", ConcreteDataType::float64_datatype(), true),
        ("sum", ConcreteDataType::float64_datatype(), true),
        ("reset_hint", ConcreteDataType::int32_datatype(), true),
        (
            "start_timestamp",
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        ),
        (
            "custom_values",
            list(ConcreteDataType::float64_datatype()),
            true,
        ),
        (
            "positive_span_offsets",
            list(ConcreteDataType::int32_datatype()),
            true,
        ),
        (
            "positive_span_lengths",
            list(ConcreteDataType::int32_datatype()),
            true,
        ),
        (
            "negative_span_offsets",
            list(ConcreteDataType::int32_datatype()),
            true,
        ),
        (
            "negative_span_lengths",
            list(ConcreteDataType::int32_datatype()),
            true,
        ),
        ("count_i64", ConcreteDataType::int64_datatype(), true),
        ("zero_count_i64", ConcreteDataType::int64_datatype(), true),
        (
            "positive_buckets_i64",
            list(ConcreteDataType::int64_datatype()),
            true,
        ),
        (
            "negative_buckets_i64",
            list(ConcreteDataType::int64_datatype()),
            true,
        ),
        ("count_f64", ConcreteDataType::float64_datatype(), true),
        ("zero_count_f64", ConcreteDataType::float64_datatype(), true),
        (
            "positive_buckets_f64",
            list(ConcreteDataType::float64_datatype()),
            true,
        ),
        (
            "negative_buckets_f64",
            list(ConcreteDataType::float64_datatype()),
            true,
        ),
    ]
}

#[test]
fn native_histogram_field_names_are_frozen_in_order() {
    assert_eq!(NATIVE_HISTOGRAM_FIELD, "greptime_native_histogram");
    assert_eq!(
        NATIVE_HISTOGRAM_FIELD_NAMES,
        &[
            "schema",
            "zero_threshold",
            "sum",
            "reset_hint",
            "start_timestamp",
            "custom_values",
            "positive_span_offsets",
            "positive_span_lengths",
            "negative_span_offsets",
            "negative_span_lengths",
            "count_i64",
            "zero_count_i64",
            "positive_buckets_i64",
            "negative_buckets_i64",
            "count_f64",
            "zero_count_f64",
            "positive_buckets_f64",
            "negative_buckets_f64",
        ]
    );
}

#[test]
fn native_histogram_value_type_fields_are_frozen() {
    let fields = expected_fields()
        .into_iter()
        .map(|(name, data_type, nullable)| StructField::new(name, data_type, nullable))
        .collect();
    // `StructType` equality covers field names, their order, their types, and
    // their nullability.
    assert_eq!(
        native_histogram_value_type(),
        &ConcreteDataType::struct_datatype(StructType::new(Arc::new(fields)))
    );
}

#[test]
fn native_histogram_arrow_type_is_frozen() {
    let list = |item| ArrowDataType::List(Arc::new(Field::new("item", item, true)));
    let expected = ArrowDataType::Struct(
        vec![
            Field::new("schema", ArrowDataType::Int32, true),
            Field::new("zero_threshold", ArrowDataType::Float64, true),
            Field::new("sum", ArrowDataType::Float64, true),
            Field::new("reset_hint", ArrowDataType::Int32, true),
            Field::new(
                "start_timestamp",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true,
            ),
            Field::new("custom_values", list(ArrowDataType::Float64), true),
            Field::new("positive_span_offsets", list(ArrowDataType::Int32), true),
            Field::new("positive_span_lengths", list(ArrowDataType::Int32), true),
            Field::new("negative_span_offsets", list(ArrowDataType::Int32), true),
            Field::new("negative_span_lengths", list(ArrowDataType::Int32), true),
            Field::new("count_i64", ArrowDataType::Int64, true),
            Field::new("zero_count_i64", ArrowDataType::Int64, true),
            Field::new("positive_buckets_i64", list(ArrowDataType::Int64), true),
            Field::new("negative_buckets_i64", list(ArrowDataType::Int64), true),
            Field::new("count_f64", ArrowDataType::Float64, true),
            Field::new("zero_count_f64", ArrowDataType::Float64, true),
            Field::new("positive_buckets_f64", list(ArrowDataType::Float64), true),
            Field::new("negative_buckets_f64", list(ArrowDataType::Float64), true),
        ]
        .into(),
    );
    assert_eq!(native_histogram_arrow_type(), expected);
}

#[test]
fn unknown_native_histogram_field_has_no_type() {
    assert_eq!(native_histogram_field_type("not_a_field"), None);
    assert_eq!(native_histogram_field_type(""), None);
    // Field names are matched exactly, byte for byte.
    assert_eq!(native_histogram_field_type("COUNT_F64"), None);
    assert_eq!(native_histogram_field_type("count_F64"), None);
    assert_eq!(native_histogram_field_type("count_f64 "), None);
}
