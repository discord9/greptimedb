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

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, PrimitiveArray, StructArray,
    TimestampMillisecondArray, UInt32Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};

use super::*;
use crate::arrow::datatypes::{
    ArrowPrimitiveType, DataType as ArrowDataType, Field, Fields, Float64Type, Int32Type,
    Int64Type, TimeUnit as ArrowTimeUnit, TimestampMillisecondType,
};
use crate::series::frame::{RawLabelMode, pack_raw_series_chunk};
use crate::series::histogram::{
    BucketSideView, CounterResetHint, HistogramBatch, HistogramBatchBuilder, HistogramView,
};
use crate::series::raw::{RawSeriesChunk, RawTimestamps};
use crate::series::samples::{
    DenseSamplesRef, FieldSpec, PublicHistogramSamples, SampleRef, Samples, SeriesFields,
    SeriesSchema,
};

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

// ---------------------------------------------------------------------------
// Public (remote-write v2) histogram batches and samples
//
// `public_fixture` is the single Arrow source for these tests: a six-row struct array over the
// exact `native_histogram_arrow_type()` fields, wrapped by most tests as `backing.slice(1, 4)`.
// Expected values come from the literal `PublicRows` description of all 18 members and are
// compared value by value, because Arrow array equality pins neither float payload bits nor the
// child values that a null parent row hides.
// ---------------------------------------------------------------------------

/// Parent (struct row) validity of the fixture: `true` marks a present, non-null histogram.
const PARENT_VALIDITY: [bool; 6] = [false, true, true, false, true, true];

/// Backing rows visible in the fixture payload `backing.slice(1, 4)`.
const VISIBLE_ROWS: &[usize] = &[1, 2, 3, 4];

/// Odd integer above `2^53`: routing these exact `i64` payloads through `f64` would lose them.
const BIG: i64 = (1 << 53) + 1;

/// Quiet NaN with a non-canonical payload.
const PAYLOAD_NAN: f64 = f64::from_bits(0x7ff8_0000_0000_0042);

/// Signaling NaN; ordinary NaN handling must not rewrite it.
const STALE_NAN: f64 = f64::from_bits(0x7ff0_0000_0000_0001);

/// Row description of the single public-histogram fixture, in backing row order.
///
/// Row 2 is a present row whose `sum` scalar is null and row 3 is a null parent row whose
/// children keep their values; both are visible. Rows 0 and 5 stay hidden. Every list column
/// covers a null list, an empty list, a null item, and non-null items, and rows 1 and 3 populate
/// both the `i64` and the `f64` bucket families.
struct PublicRows {
    schema: Vec<Option<i32>>,
    zero_threshold: Vec<Option<f64>>,
    sum: Vec<Option<f64>>,
    reset_hint: Vec<Option<i32>>,
    start_timestamp: Vec<Option<i64>>,
    custom_values: Vec<Option<Vec<Option<f64>>>>,
    positive_span_offsets: Vec<Option<Vec<Option<i32>>>>,
    positive_span_lengths: Vec<Option<Vec<Option<i32>>>>,
    negative_span_offsets: Vec<Option<Vec<Option<i32>>>>,
    negative_span_lengths: Vec<Option<Vec<Option<i32>>>>,
    count_i64: Vec<Option<i64>>,
    zero_count_i64: Vec<Option<i64>>,
    positive_buckets_i64: Vec<Option<Vec<Option<i64>>>>,
    negative_buckets_i64: Vec<Option<Vec<Option<i64>>>>,
    count_f64: Vec<Option<f64>>,
    zero_count_f64: Vec<Option<f64>>,
    positive_buckets_f64: Vec<Option<Vec<Option<f64>>>>,
    negative_buckets_f64: Vec<Option<Vec<Option<f64>>>>,
}

fn public_rows() -> PublicRows {
    PublicRows {
        schema: vec![Some(7), Some(1), None, Some(303), Some(4), Some(505)],
        zero_threshold: vec![
            Some(f64::INFINITY),
            Some(-0.0),
            None,
            Some(0.25),
            Some(0.0),
            Some(-1.0),
        ],
        sum: vec![
            Some(PAYLOAD_NAN),
            Some(f64::NAN),
            None,
            Some(-0.0),
            Some(f64::INFINITY),
            Some(STALE_NAN),
        ],
        reset_hint: vec![Some(3), Some(0), None, Some(2), Some(-7), Some(1)],
        start_timestamp: vec![
            Some(1),
            Some(1_700_000_000_001),
            None,
            Some(999),
            Some(0),
            Some(2),
        ],
        custom_values: vec![
            Some(vec![Some(0.5), None]),
            Some(vec![Some(1.5), Some(-2.5)]),
            None,
            Some(vec![Some(-1.0), None]),
            Some(vec![]),
            Some(vec![Some(5.5)]),
        ],
        positive_span_offsets: vec![
            Some(vec![Some(0)]),
            Some(vec![Some(0), Some(2)]),
            None,
            Some(vec![None]),
            Some(vec![]),
            Some(vec![Some(0)]),
        ],
        positive_span_lengths: vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2), Some(3)]),
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![Some(1)]),
        ],
        negative_span_offsets: vec![
            Some(vec![]),
            Some(vec![Some(0)]),
            Some(vec![None]),
            None,
            Some(vec![]),
            Some(vec![Some(0)]),
        ],
        negative_span_lengths: vec![
            Some(vec![]),
            Some(vec![Some(2)]),
            None,
            Some(vec![None]),
            Some(vec![]),
            Some(vec![Some(1)]),
        ],
        count_i64: vec![
            Some(BIG + 18),
            Some(BIG + 20),
            None,
            Some(BIG + 22),
            Some(0),
            Some(5),
        ],
        zero_count_i64: vec![Some(BIG + 24), Some(BIG + 26), None, None, Some(0), Some(1)],
        positive_buckets_i64: vec![
            None,
            Some(vec![
                Some(BIG + 2),
                Some(BIG + 4),
                Some(BIG + 6),
                Some(BIG + 8),
                Some(BIG + 10),
            ]),
            None,
            Some(vec![Some(BIG + 12), None]),
            Some(vec![]),
            Some(vec![Some(BIG + 30)]),
        ],
        negative_buckets_i64: vec![
            Some(vec![]),
            Some(vec![Some(BIG + 14), Some(BIG + 16)]),
            None,
            Some(vec![Some(BIG + 28), None]),
            Some(vec![]),
            Some(vec![Some(BIG + 32)]),
        ],
        count_f64: vec![
            Some(PAYLOAD_NAN),
            Some(f64::NAN),
            None,
            Some(f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(0.0),
        ],
        zero_count_f64: vec![
            Some(STALE_NAN),
            Some(-0.0),
            None,
            None,
            Some(0.0),
            Some(0.5),
        ],
        positive_buckets_f64: vec![
            Some(vec![Some(f64::NAN)]),
            Some(vec![
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
                Some(STALE_NAN),
            ]),
            Some(vec![Some(-0.0), None]),
            None,
            Some(vec![]),
            Some(vec![Some(5.5)]),
        ],
        negative_buckets_f64: vec![
            Some(vec![None]),
            Some(vec![Some(3.5), Some(-3.5)]),
            None,
            Some(vec![Some(PAYLOAD_NAN), None]),
            Some(vec![]),
            Some(vec![Some(6.5)]),
        ],
    }
}

/// The single Arrow fixture: the six-row backing array and the non-zero-offset payload
/// `backing.slice(1, 4)` that most tests wrap.
struct PublicFixture {
    rows: PublicRows,
    backing: StructArray,
    array: StructArray,
}

fn public_fixture() -> PublicFixture {
    let rows = public_rows();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(rows.schema.clone())),
        Arc::new(Float64Array::from(rows.zero_threshold.clone())),
        Arc::new(Float64Array::from(rows.sum.clone())),
        Arc::new(Int32Array::from(rows.reset_hint.clone())),
        Arc::new(TimestampMillisecondArray::from(
            rows.start_timestamp.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
            rows.custom_values.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            rows.positive_span_offsets.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            rows.positive_span_lengths.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            rows.negative_span_offsets.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            rows.negative_span_lengths.clone(),
        )),
        Arc::new(Int64Array::from(rows.count_i64.clone())),
        Arc::new(Int64Array::from(rows.zero_count_i64.clone())),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            rows.positive_buckets_i64.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            rows.negative_buckets_i64.clone(),
        )),
        Arc::new(Float64Array::from(rows.count_f64.clone())),
        Arc::new(Float64Array::from(rows.zero_count_f64.clone())),
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
            rows.positive_buckets_f64.clone(),
        )),
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
            rows.negative_buckets_f64.clone(),
        )),
    ];
    let nulls = NullBuffer::from(PARENT_VALIDITY.to_vec());
    let backing =
        StructArray::try_new(native_fields(), columns, Some(nulls)).expect("fixture struct array");
    let array = backing.slice(1, 4);
    PublicFixture {
        rows,
        backing,
        array,
    }
}

/// The exact 18 public fields, taken from the production type rather than retyped here.
fn native_fields() -> Fields {
    native_field_vec().into()
}

/// The same fields as owned values, so a negative case can modify one member at a time.
fn native_field_vec() -> Vec<Field> {
    match native_histogram_arrow_type() {
        ArrowDataType::Struct(fields) => {
            fields.iter().map(|field| field.as_ref().clone()).collect()
        }
        other => panic!("the native histogram Arrow type is not a struct: {other}"),
    }
}

/// Item field of the list member at `index`.
fn list_item(fields: &Fields, index: usize) -> Arc<Field> {
    match fields[index].data_type() {
        ArrowDataType::List(item) => Arc::clone(item),
        other => panic!("expected a list member at {index}, got {other}"),
    }
}

/// Minimal empty list column with the given item field, used only by the rejected item-metadata
/// cases.
fn empty_list<T: ArrowPrimitiveType>(item_field: Arc<Field>) -> ListArray {
    let values = PrimitiveArray::<T>::new(ScalarBuffer::from(Vec::<T::Native>::new()), None);
    ListArray::try_new(
        item_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; PARENT_VALIDITY.len() + 1])),
        Arc::new(values),
        None,
    )
    .expect("empty list column")
}

/// Downcasts a column to its concrete Arrow type.
fn downcast<T: Array + 'static>(array: &dyn Array) -> &T {
    let data_type = array.data_type();
    array
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("unexpected Arrow column type {data_type}"))
}

/// Asserts one scalar row: its validity, and its value when one is described.
///
/// Validity is compared on its own because the value a null slot hides is not part of the logical
/// contract.
fn assert_scalar<T: ArrowPrimitiveType>(
    column: &dyn Array,
    position: usize,
    expected: Option<T::Native>,
) {
    let column = downcast::<PrimitiveArray<T>>(column);
    assert_eq!(
        column.is_valid(position),
        expected.is_some(),
        "row {position} validity"
    );
    if let Some(expected) = expected {
        assert_eq!(column.value(position), expected, "row {position} value");
    }
}

/// Asserts one float row by payload bits, so NaN payloads and signed zeros are pinned.
fn assert_float(column: &dyn Array, position: usize, expected: Option<f64>) {
    let column = downcast::<Float64Array>(column);
    assert_eq!(
        column.is_valid(position),
        expected.is_some(),
        "row {position} validity"
    );
    if let Some(expected) = expected {
        assert_eq!(
            column.value(position).to_bits(),
            expected.to_bits(),
            "row {position} bits"
        );
    }
}

/// Asserts one list row: parent validity, item count, and every item's validity and value.
///
/// `same` decides item equality, so float lists compare payload bits while integer lists compare
/// values.
fn assert_list<T: ArrowPrimitiveType>(
    column: &dyn Array,
    position: usize,
    expected: Option<&Vec<Option<T::Native>>>,
    same: fn(T::Native, T::Native) -> bool,
) {
    let column = downcast::<ListArray>(column);
    assert_eq!(
        column.is_valid(position),
        expected.is_some(),
        "row {position} validity"
    );
    let Some(expected) = expected else {
        return;
    };
    let items = column.value(position);
    let items = downcast::<PrimitiveArray<T>>(items.as_ref());
    assert_eq!(items.len(), expected.len(), "row {position} item count");
    for (index, expected) in expected.iter().enumerate() {
        assert_eq!(
            items.is_valid(index),
            expected.is_some(),
            "item {index} validity"
        );
        if let Some(expected) = expected {
            assert!(same(items.value(index), *expected), "item {index} value");
        }
    }
}

/// Asserts one integer list member: validity, item count, and each item's validity and value.
fn assert_int_list<T: ArrowPrimitiveType>(
    column: &dyn Array,
    position: usize,
    expected: Option<&Vec<Option<T::Native>>>,
) {
    assert_list::<T>(column, position, expected, |left, right| left == right);
}

/// Asserts one float list member by payload bits, so NaN payloads and signed zeros are pinned.
fn assert_float_list(column: &dyn Array, position: usize, expected: Option<&Vec<Option<f64>>>) {
    assert_list::<Float64Type>(column, position, expected, |left, right| {
        left.to_bits() == right.to_bits()
    });
}

/// Asserts all 18 members of one batch row against the described backing row `row`.
///
/// A present member is checked even when its parent row is null, because a null parent hides
/// children without discarding them.
fn assert_members(array: &StructArray, position: usize, row: usize, rows: &PublicRows) {
    assert_eq!(array.num_columns(), 18, "row {position} member count");
    let column = |index: usize| array.column(index).as_ref();
    assert_scalar::<Int32Type>(column(0), position, rows.schema[row]);
    assert_float(column(1), position, rows.zero_threshold[row]);
    assert_float(column(2), position, rows.sum[row]);
    assert_scalar::<Int32Type>(column(3), position, rows.reset_hint[row]);
    assert_scalar::<TimestampMillisecondType>(column(4), position, rows.start_timestamp[row]);
    assert_float_list(column(5), position, rows.custom_values[row].as_ref());
    assert_int_list::<Int32Type>(
        column(6),
        position,
        rows.positive_span_offsets[row].as_ref(),
    );
    assert_int_list::<Int32Type>(
        column(7),
        position,
        rows.positive_span_lengths[row].as_ref(),
    );
    assert_int_list::<Int32Type>(
        column(8),
        position,
        rows.negative_span_offsets[row].as_ref(),
    );
    assert_int_list::<Int32Type>(
        column(9),
        position,
        rows.negative_span_lengths[row].as_ref(),
    );
    assert_scalar::<Int64Type>(column(10), position, rows.count_i64[row]);
    assert_scalar::<Int64Type>(column(11), position, rows.zero_count_i64[row]);
    assert_int_list::<Int64Type>(
        column(12),
        position,
        rows.positive_buckets_i64[row].as_ref(),
    );
    assert_int_list::<Int64Type>(
        column(13),
        position,
        rows.negative_buckets_i64[row].as_ref(),
    );
    assert_float(column(14), position, rows.count_f64[row]);
    assert_float(column(15), position, rows.zero_count_f64[row]);
    assert_float_list(
        column(16),
        position,
        rows.positive_buckets_f64[row].as_ref(),
    );
    assert_float_list(
        column(17),
        position,
        rows.negative_buckets_f64[row].as_ref(),
    );
}

/// Asserts the batch reports exactly the described `window` of backing rows, all 18 members
/// included, with each parent row's own validity.
fn assert_public_columns(array: &StructArray, rows: &PublicRows, window: &[usize]) {
    assert_eq!(array.fields(), &native_fields());
    assert_eq!(array.len(), window.len());
    for (position, row) in window.iter().enumerate() {
        assert_eq!(
            array.is_valid(position),
            PARENT_VALIDITY[*row],
            "parent row {position}"
        );
        assert_members(array, position, *row, rows);
    }
}

/// Wraps an Arrow payload the way every test below does.
fn public_batch(array: &StructArray) -> Arc<PublicHistogramBatch> {
    Arc::new(PublicHistogramBatch::try_from_arrow(array.clone()).expect("public batch payload"))
}

/// Asserts the batch kept the wrapped payload itself, buffers and offsets included, instead of
/// copying or rebuilding any member.
fn assert_wraps_payload(actual: &StructArray, payload: &StructArray) {
    assert_eq!(actual.fields(), payload.fields());
    assert!(
        actual.to_data().ptr_eq(&payload.to_data()),
        "the batch must retain the wrapped payload"
    );
}

/// The `sum` a view must report: nothing for a null parent row, else the described scalar, which
/// may itself be null.
fn expected_view_sum(rows: &PublicRows, row: usize) -> Option<f64> {
    PARENT_VALIDITY[row].then(|| rows.sum[row]).flatten()
}

/// Asserts a sample references a present row with exactly this `sum` payload.
fn assert_sum_sample(sample: SampleRef<'_>, row: usize, sum: f64) {
    let SampleRef::PublicHistogram(view) = sample else {
        panic!("expected a public histogram sample");
    };
    assert_eq!(view.row(), row);
    assert!(!view.is_null());
    assert_eq!(view.sum().map(f64::to_bits), Some(sum.to_bits()));
}

/// Asserts a sample references a present row whose `sum` scalar is null.
fn assert_null_sum_sample(sample: SampleRef<'_>, row: usize) {
    let SampleRef::PublicHistogram(view) = sample else {
        panic!("expected a public histogram sample");
    };
    assert_eq!(view.row(), row);
    assert!(!view.is_null());
    assert!(view.sum().is_none());
}

/// Asserts a sample references a null parent row: the view is present and reports no `sum`.
fn assert_null_parent_sample(sample: SampleRef<'_>, row: usize) {
    let SampleRef::PublicHistogram(view) = sample else {
        panic!("expected a public histogram sample");
    };
    assert_eq!(view.row(), row);
    assert!(view.is_null());
    assert!(view.sum().is_none());
}

/// Compares a `UInt32Array` against expected entries, nulls included.
fn assert_uint32(actual: &UInt32Array, expected: &[Option<u32>]) {
    assert_eq!(actual.iter().collect::<Vec<_>>(), expected);
}

/// Compares a float lane against expected values by validity, and by payload bits where present.
fn assert_float_array(actual: &Float64Array, expected: &[Option<f64>]) {
    assert_eq!(actual.len(), expected.len());
    for (position, expected) in expected.iter().enumerate() {
        assert_eq!(
            actual.is_valid(position),
            expected.is_some(),
            "row {position} validity"
        );
        if let Some(expected) = expected {
            assert_eq!(
                actual.value(position).to_bits(),
                expected.to_bits(),
                "row {position} bits"
            );
        }
    }
}

/// The view must stay `Copy`, `Clone`, and `Debug` for callers to hold it cheaply.
fn assert_view_contract<T: Copy + Clone + std::fmt::Debug>() {}

/// One canonical (Prometheus native, 14-field) row, for the incompatible-owner case.
fn canonical_histogram_batch(sum: f64) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    builder
        .push(HistogramView {
            schema: 1,
            zero_threshold: 0.0,
            sum,
            count: 2.0,
            zero_count: 0.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[1],
                buckets: &[3.5],
            },
            negative: BucketSideView {
                span_offsets: &[],
                span_lengths: &[],
                buckets: &[],
            },
        })
        .unwrap();
    builder.finish().unwrap()
}

#[test]
fn public_batch_retains_every_member_and_reports_rows() {
    let fixture = public_fixture();
    // The fixture is deliberately non-canonical: both bucket families are populated on one row,
    // spans disagree with their bucket counts, `custom_values` is unused, scalars and list items
    // are null, and several `f64` payloads are exceptional. Validation is structural, so the whole
    // six-row array is accepted exactly as it stands.
    let whole = public_batch(&fixture.backing);
    assert_eq!(whole.len(), PARENT_VALIDITY.len());
    assert_public_columns(whole.as_arrow(), &fixture.rows, &[0, 1, 2, 3, 4, 5]);
    // a present `i64` payload and a present `f64` payload coexist on one row, so the populated
    // family is a type signal rather than a filter
    assert!(downcast::<Int64Array>(whole.as_arrow().column(10).as_ref()).is_valid(1));
    assert!(downcast::<Float64Array>(whole.as_arrow().column(14).as_ref()).is_valid(1));

    // the wrapped payload starts at a non-zero offset, so rows are reported relative to it while
    // the hidden backing rows stay in the retained children
    let batch = public_batch(&fixture.array);
    assert_eq!(batch.len(), 4);
    assert!(!batch.is_empty());
    assert_public_columns(batch.as_arrow(), &fixture.rows, VISIBLE_ROWS);

    // a null parent row yields a present view; `sum` is absent for a null parent row and for a
    // present row whose own scalar is null
    for (position, row) in VISIBLE_ROWS.iter().enumerate() {
        let view = batch.view(position).expect("visible rows have a view");
        assert_eq!(view.row(), position);
        assert_eq!(view.is_null(), !PARENT_VALIDITY[*row], "row {position}");
        assert_eq!(
            view.sum().map(f64::to_bits),
            expected_view_sum(&fixture.rows, *row).map(f64::to_bits),
            "row {position}"
        );
    }
    assert_eq!(
        batch.view(0).unwrap().sum().map(f64::to_bits),
        Some(f64::NAN.to_bits())
    );
    assert!(!batch.view(1).unwrap().is_null());
    assert!(batch.view(1).unwrap().sum().is_none());
    assert!(batch.view(2).unwrap().is_null());
    assert!(batch.view(2).unwrap().sum().is_none());
    assert_eq!(
        batch.view(3).unwrap().sum().map(f64::to_bits),
        Some(f64::INFINITY.to_bits())
    );
    // out-of-range rows are the only `None` views
    assert!(batch.view(batch.len()).is_none());
    assert!(batch.view(usize::MAX).is_none());

    assert_view_contract::<PublicHistogramView<'_>>();

    let empty = PublicHistogramBatch::try_from_arrow(fixture.backing.slice(0, 0)).unwrap();
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());
    assert!(empty.view(0).is_none());
}

#[test]
fn public_batch_rejects_structural_schema_differences() {
    let fixture = public_fixture();
    let fields = native_field_vec();
    let columns = fixture.backing.columns().to_vec();
    let rejected = |fields: Vec<Field>, columns: Vec<ArrayRef>| {
        let array = StructArray::try_new(
            Fields::from(fields),
            columns,
            Some(NullBuffer::from(PARENT_VALIDITY.to_vec())),
        )
        .expect("structurally consistent struct array");
        assert!(
            PublicHistogramBatch::try_from_arrow(array).is_err(),
            "a schema that differs from the exact 18 members must be rejected"
        );
    };
    assert!(PublicHistogramBatch::try_from_arrow(fixture.backing.clone()).is_ok());

    // a renamed member
    let mut renamed = fields.clone();
    renamed[2] = Field::new("sum_total", ArrowDataType::Float64, true);
    rejected(renamed, columns.clone());

    // reordered members
    let mut reordered = fields.clone();
    reordered.swap(2, 3);
    let mut reordered_columns = columns.clone();
    reordered_columns.swap(2, 3);
    rejected(reordered, reordered_columns);

    // a retyped scalar member
    let mut retyped = fields.clone();
    retyped[2] = Field::new("sum", ArrowDataType::Int32, true);
    let mut retyped_columns = columns.clone();
    retyped_columns[2] = Arc::new(Int32Array::from(vec![1_i32; PARENT_VALIDITY.len()]));
    rejected(retyped, retyped_columns);

    // a non-nullable scalar member
    let mut non_nullable = fields.clone();
    non_nullable[2] = Field::new("sum", ArrowDataType::Float64, false);
    let mut non_nullable_columns = columns.clone();
    non_nullable_columns[2] = Arc::new(Float64Array::from(vec![1.0_f64; PARENT_VALIDITY.len()]));
    rejected(non_nullable, non_nullable_columns);

    // a retyped list item
    let mut item_type = fields.clone();
    item_type[5] = Field::new(
        "custom_values",
        ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true))),
        true,
    );
    let item_field = list_item(&Fields::from(item_type.clone()), 5);
    let mut item_type_columns = columns.clone();
    item_type_columns[5] = Arc::new(empty_list::<Int32Type>(item_field));
    rejected(item_type, item_type_columns);

    // a non-nullable list item
    let mut item_nullable = fields.clone();
    item_nullable[5] = Field::new(
        "custom_values",
        ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float64, false))),
        true,
    );
    let item_field = list_item(&Fields::from(item_nullable.clone()), 5);
    let mut item_nullable_columns = columns.clone();
    item_nullable_columns[5] = Arc::new(empty_list::<Float64Type>(item_field));
    rejected(item_nullable, item_nullable_columns);

    // a renamed list item
    let mut item_name = fields.clone();
    item_name[5] = Field::new(
        "custom_values",
        ArrowDataType::List(Arc::new(Field::new(
            "element",
            ArrowDataType::Float64,
            true,
        ))),
        true,
    );
    let item_field = list_item(&Fields::from(item_name.clone()), 5);
    let mut item_name_columns = columns.clone();
    item_name_columns[5] = Arc::new(empty_list::<Float64Type>(item_field));
    rejected(item_name, item_name_columns);
}

#[test]
fn public_batch_slices_share_every_original_buffer_and_keep_hidden_children() {
    let fixture = public_fixture();
    let batch = public_batch(&fixture.array);
    assert_public_columns(batch.as_arrow(), &fixture.rows, VISIBLE_ROWS);
    // the non-zero struct slice stays the same payload, so hidden rows survive
    assert_wraps_payload(batch.as_arrow(), &fixture.array);
    // list children are the whole backing children, hidden items included
    let custom = downcast::<ListArray>(batch.as_arrow().column(5).as_ref());
    let backing_custom = downcast::<ListArray>(fixture.backing.column(5).as_ref());
    assert!(
        custom
            .values()
            .to_data()
            .ptr_eq(&backing_custom.values().to_data())
    );
    assert_eq!(
        custom.values().len(),
        fixture
            .rows
            .custom_values
            .iter()
            .flatten()
            .map(Vec::len)
            .sum::<usize>()
    );

    // a slice in the middle of the backing array reports rows relative to itself
    let tail_input = fixture.backing.slice(3, 2);
    let tail = public_batch(&tail_input);
    assert_eq!(tail.len(), 2);
    assert_public_columns(tail.as_arrow(), &fixture.rows, &[3, 4]);
    assert!(tail.view(0).unwrap().is_null());
    assert!(!tail.view(1).unwrap().is_null());
    assert_wraps_payload(tail.as_arrow(), &tail_input);

    // a list item child that is itself a non-zero slice stays shared
    let visible = downcast::<ListArray>(fixture.array.column(5).as_ref());
    let start = visible.value_offsets()[0];
    let end = visible.value_offsets()[visible.len()];
    let child = visible
        .values()
        .slice(start as usize, (end - start) as usize);
    let offsets = visible
        .value_offsets()
        .iter()
        .map(|offset| offset - start)
        .collect::<Vec<_>>();
    let rebuilt = ListArray::try_new(
        list_item(&native_fields(), 5),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        child,
        visible.nulls().cloned(),
    )
    .unwrap();
    let mut columns = fixture.array.columns().to_vec();
    columns[5] = Arc::new(rebuilt);
    let sliced_child = StructArray::try_new(
        native_fields(),
        columns,
        Some(NullBuffer::from(PARENT_VALIDITY[1..5].to_vec())),
    )
    .unwrap();
    let batch = public_batch(&sliced_child);
    assert_public_columns(batch.as_arrow(), &fixture.rows, VISIBLE_ROWS);
    assert_wraps_payload(batch.as_arrow(), &sliced_child);
}

#[test]
fn public_batch_and_samples_retain_the_wrapped_payload() {
    let PublicFixture {
        rows,
        backing,
        array,
    } = public_fixture();
    let batch = public_batch(&array);
    assert_wraps_payload(batch.as_arrow(), &array);
    let custom = downcast::<ListArray>(batch.as_arrow().column(5).as_ref());
    let backing_custom = downcast::<ListArray>(backing.column(5).as_ref());
    assert!(
        custom
            .values()
            .to_data()
            .ptr_eq(&backing_custom.values().to_data())
    );
    let weak = Arc::downgrade(&batch);

    // the source arrays can go away: the batch keeps the whole payload, hidden rows included
    drop(backing);
    drop(array);
    assert_public_columns(batch.as_arrow(), &rows, VISIBLE_ROWS);

    let samples = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![Some(0), Some(3)]),
            Arc::clone(&batch),
            None,
        )
        .unwrap(),
    );
    drop(batch);
    assert!(
        weak.upgrade().is_some(),
        "samples keep the referenced table alive"
    );
    {
        let DenseSamplesRef::PublicHistogram(carrier) = samples.dense().unwrap() else {
            panic!("public samples stay public when dense");
        };
        assert_public_columns(carrier.batch().as_arrow(), &rows, VISIBLE_ROWS);
        assert_sum_sample(samples.sample(0), 0, f64::NAN);
        assert_sum_sample(samples.sample(1), 3, f64::INFINITY);
    }
    drop(samples);
    assert!(
        weak.upgrade().is_none(),
        "the table is released with its last reference"
    );
}

#[test]
fn public_samples_validate_bounds_lengths_and_exclusivity() {
    let fixture = public_fixture();
    let batch = public_batch(&fixture.array);
    let indices = UInt32Array::from(vec![Some(1), None, Some(1), Some(3)]);
    let carrier =
        PublicHistogramSamples::try_new(indices.clone(), Arc::clone(&batch), None).unwrap();
    assert_eq!(carrier.len(), 4);
    assert!(!carrier.is_empty());
    assert_uint32(carrier.indices(), &[Some(1), None, Some(1), Some(3)]);
    assert!(carrier.floats().is_none());
    assert!(Arc::ptr_eq(carrier.batch(), &batch));
    // repeated references are allowed and both report the same row
    let first = carrier.view_at(0).unwrap();
    let repeated = carrier.view_at(2).unwrap();
    assert_eq!(first.row(), 1);
    assert_eq!(repeated.row(), 1);
    assert!(std::ptr::eq(first.batch(), repeated.batch()));
    // a null index and an out-of-range position have no view
    assert!(carrier.view_at(1).is_none());
    assert!(carrier.view_at(4).is_none());
    assert!(carrier.view_at(usize::MAX).is_none());
    // the referenced row is present and its own `sum` scalar is null
    assert!(!carrier.view_at(0).unwrap().is_null());
    assert!(carrier.view_at(0).unwrap().sum().is_none());

    // index bounds are checked against the referenced owner, not the backing array it slices
    assert!(
        PublicHistogramSamples::try_new(UInt32Array::from(vec![Some(4)]), Arc::clone(&batch), None)
            .is_err()
    );
    let short = public_batch(&fixture.backing.slice(4, 2));
    assert!(
        PublicHistogramSamples::try_new(UInt32Array::from(vec![Some(2)]), short, None).is_err()
    );

    // a non-zero slice of a wider index buffer is read within its own view: the trailing
    // sentinels stay invisible and the referenced rows are relative to the slice
    let wide = UInt32Array::from(vec![Some(99), Some(0), Some(3), Some(99)]);
    let sliced =
        PublicHistogramSamples::try_new(wide.slice(1, 2), Arc::clone(&batch), None).unwrap();
    assert_eq!(sliced.len(), 2);
    let sliced_samples = Samples::from_public_histogram(sliced);
    assert_sum_sample(sliced_samples.sample(0), 0, f64::NAN);
    assert_sum_sample(sliced_samples.sample(1), 3, f64::INFINITY);
    assert!(matches!(sliced_samples.sample(2), SampleRef::Missing));

    // a float lane must match the index length
    assert!(
        PublicHistogramSamples::try_new(
            indices.clone(),
            Arc::clone(&batch),
            Some(Float64Array::from(vec![None, None, None])),
        )
        .is_err()
    );

    // float values and histogram indices stay mutually exclusive at every valid position, even
    // when the referenced row is a null parent row
    let null_parent = UInt32Array::from(vec![None, Some(2), Some(3), None]);
    assert!(batch.view(2).unwrap().is_null());
    assert!(
        PublicHistogramSamples::try_new(
            null_parent.clone(),
            Arc::clone(&batch),
            Some(Float64Array::from(vec![None, Some(1.0), None, None])),
        )
        .is_err()
    );
    let fits = PublicHistogramSamples::try_new(
        null_parent,
        Arc::clone(&batch),
        Some(Float64Array::from(vec![None, None, None, Some(-0.0)])),
    )
    .unwrap();
    let fits_samples = Samples::from_public_histogram(fits);
    // a valid index that references a null parent row still yields a present, null view
    assert_null_parent_sample(fits_samples.sample(1), 2);
    assert!(matches!(
        fits_samples.sample(3),
        SampleRef::Float(value) if value.to_bits() == (-0.0_f64).to_bits()
    ));

    let empty = PublicHistogramSamples::try_new(UInt32Array::new_null(0), Arc::clone(&batch), None)
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert!(empty.view_at(0).is_none());
    let empty_mixed = PublicHistogramSamples::try_new(
        UInt32Array::new_null(0),
        batch,
        Some(Float64Array::new_null(0)),
    )
    .unwrap();
    assert!(empty_mixed.floats().unwrap().is_empty());
}

#[test]
fn public_samples_iterate_slice_and_keep_their_buffers() {
    let fixture = public_fixture();
    let batch = public_batch(&fixture.array);
    let samples = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![Some(3), None, Some(1), Some(3)]),
            Arc::clone(&batch),
            None,
        )
        .unwrap(),
    );
    assert_eq!(samples.len(), 4);
    assert!(!samples.is_empty());
    let DenseSamplesRef::PublicHistogram(dense) = samples.dense().unwrap() else {
        panic!("public samples stay public when dense");
    };
    assert!(Arc::ptr_eq(dense.batch(), &batch));
    assert!(dense.floats().is_none());
    assert_uint32(dense.indices(), &[Some(3), None, Some(1), Some(3)]);
    let index_values = dense.indices().values().as_ptr();

    // every position reports its own variant, row, and `sum` bits
    assert_sum_sample(samples.sample(0), 3, f64::INFINITY);
    assert!(matches!(samples.sample(1), SampleRef::Missing));
    assert_null_sum_sample(samples.sample(2), 1);
    assert_sum_sample(samples.sample(3), 3, f64::INFINITY);
    assert!(matches!(samples.sample(4), SampleRef::Missing));

    // iteration reports the same variant, row, and `sum` bits at every position
    let mut iter = samples.iter();
    assert_eq!(iter.len(), 4);
    assert_sum_sample(iter.next().unwrap(), 3, f64::INFINITY);
    assert!(matches!(iter.next().unwrap(), SampleRef::Missing));
    assert_null_sum_sample(iter.next().unwrap(), 1);
    assert_sum_sample(iter.next().unwrap(), 3, f64::INFINITY);
    assert!(iter.next().is_none());

    // a slice keeps the index buffer and the table instead of rebuilding them
    let sliced = samples.slice(1, 2).unwrap();
    let mut sliced_iter = sliced.iter();
    assert_eq!(sliced_iter.len(), 2);
    assert!(matches!(sliced_iter.next().unwrap(), SampleRef::Missing));
    assert_null_sum_sample(sliced_iter.next().unwrap(), 1);
    assert!(sliced_iter.next().is_none());
    assert!(matches!(sliced.sample(0), SampleRef::Missing));
    assert_null_sum_sample(sliced.sample(1), 1);
    let DenseSamplesRef::PublicHistogram(dense) = sliced.dense().unwrap() else {
        panic!("a public slice stays public");
    };
    assert!(Arc::ptr_eq(dense.batch(), &batch));
    assert_eq!(
        dense.indices().values().as_ptr(),
        index_values.wrapping_add(1)
    );
    assert!(samples.slice(3, 2).is_err());

    // a mixed carrier keeps its float lane and its exclusion precedence; exceptional payload bits
    // survive the round trip
    let mixed = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![None, Some(0), Some(2), None]),
            Arc::clone(&batch),
            Some(Float64Array::from(vec![
                Some(-0.0),
                None,
                None,
                Some(PAYLOAD_NAN),
            ])),
        )
        .unwrap(),
    );
    assert!(matches!(
        mixed.sample(0),
        SampleRef::Float(value) if value.to_bits() == (-0.0_f64).to_bits()
    ));
    assert_sum_sample(mixed.sample(1), 0, f64::NAN);
    assert_null_parent_sample(mixed.sample(2), 2);
    assert!(matches!(
        mixed.sample(3),
        SampleRef::Float(value) if value.to_bits() == PAYLOAD_NAN.to_bits()
    ));
    let DenseSamplesRef::PublicHistogram(mixed_dense) = mixed.dense().unwrap() else {
        panic!("mixed public samples stay public when dense");
    };
    let lane = mixed_dense.floats().expect("the mixed lane survives");
    assert_float_array(lane, &[Some(-0.0), None, None, Some(PAYLOAD_NAN)]);

    // slicing keeps the float lane buffers and the exceptional bits they hold
    let lane_values = lane.values().as_ptr();
    let mixed_slice = mixed.slice(2, 2).unwrap();
    let DenseSamplesRef::PublicHistogram(inner) = mixed_slice.dense().unwrap() else {
        panic!("a mixed public slice stays public");
    };
    let lane = inner.floats().expect("the float lane survives slicing");
    assert_float_array(lane, &[None, Some(PAYLOAD_NAN)]);
    assert_eq!(lane.values().as_ptr(), lane_values.wrapping_add(2));
    assert_null_parent_sample(mixed_slice.sample(0), 2);
    assert!(matches!(
        mixed_slice.sample(1),
        SampleRef::Float(value) if value.to_bits() == PAYLOAD_NAN.to_bits()
    ));
}

#[test]
fn into_dense_reuses_shared_public_owners_and_matching_views() {
    let fixture = public_fixture();
    let batch = public_batch(&fixture.array);
    let pure = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![Some(0), None, Some(1)]),
            Arc::clone(&batch),
            None,
        )
        .unwrap(),
    );
    let mixed = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![None, Some(3)]),
            Arc::clone(&batch),
            Some(Float64Array::from(vec![Some(7.5), None])),
        )
        .unwrap(),
    );
    let dense = Samples::try_from_chunks(vec![pure, mixed])
        .unwrap()
        .into_dense()
        .unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = dense.dense().unwrap() else {
        panic!("shared owners densify to the public carrier");
    };
    assert!(Arc::ptr_eq(carrier.batch(), &batch));
    assert_uint32(carrier.indices(), &[Some(0), None, Some(1), None, Some(3)]);
    assert_float_array(
        carrier.floats().expect("mixed segments form a float lane"),
        &[None, None, None, Some(7.5), None],
    );
    assert_sum_sample(dense.sample(0), 0, f64::NAN);
    assert!(matches!(dense.sample(1), SampleRef::Missing));
    assert_null_sum_sample(dense.sample(2), 1);
    assert!(matches!(
        dense.sample(3),
        SampleRef::Float(value) if value.to_bits() == 7.5_f64.to_bits()
    ));
    assert_sum_sample(dense.sample(4), 3, f64::INFINITY);
    assert_public_columns(carrier.batch().as_arrow(), &fixture.rows, VISIBLE_ROWS);

    // distinct wrappers of one Arrow view share their table instead of concatenating it
    let first = public_batch(&fixture.array);
    let second = public_batch(&fixture.array);
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(
        first
            .as_arrow()
            .to_data()
            .ptr_eq(&second.as_arrow().to_data())
    );
    let dense = Samples::try_from_chunks(vec![
        Samples::from_public_histogram(
            PublicHistogramSamples::try_new(
                UInt32Array::from(vec![Some(0)]),
                Arc::clone(&first),
                None,
            )
            .unwrap(),
        ),
        Samples::from_public_histogram(
            PublicHistogramSamples::try_new(
                UInt32Array::from(vec![Some(3)]),
                Arc::clone(&second),
                None,
            )
            .unwrap(),
        ),
    ])
    .unwrap()
    .into_dense()
    .unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = dense.dense().unwrap() else {
        panic!("matching views densify to the public carrier");
    };
    assert!(
        Arc::ptr_eq(carrier.batch(), &first) || Arc::ptr_eq(carrier.batch(), &second),
        "one of the matching owners is reused"
    );
    assert_eq!(carrier.batch().len(), fixture.array.len());
    assert_wraps_payload(carrier.batch().as_arrow(), &fixture.array);
    assert_uint32(carrier.indices(), &[Some(0), Some(3)]);
    assert_sum_sample(dense.sample(0), 0, f64::NAN);
    assert_sum_sample(dense.sample(1), 3, f64::INFINITY);
}

#[test]
fn into_dense_concatenates_distinct_owners_and_remaps_indices() {
    let first = public_fixture();
    let second = public_fixture();
    let first_batch = public_batch(&first.backing.slice(1, 2));
    let second_batch = public_batch(&second.backing.slice(3, 2));
    assert!(
        !first_batch
            .as_arrow()
            .to_data()
            .ptr_eq(&second_batch.as_arrow().to_data())
    );
    let dense = Samples::try_from_chunks(vec![
        Samples::from_public_histogram(
            PublicHistogramSamples::try_new(
                UInt32Array::from(vec![Some(0), Some(1)]),
                Arc::clone(&first_batch),
                None,
            )
            .unwrap(),
        ),
        Samples::from_public_histogram(
            PublicHistogramSamples::try_new(
                UInt32Array::from(vec![Some(1), Some(0)]),
                Arc::clone(&second_batch),
                None,
            )
            .unwrap(),
        ),
    ])
    .unwrap()
    .into_dense()
    .unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = dense.dense().unwrap() else {
        panic!("distinct public owners densify to the public carrier");
    };
    assert_eq!(carrier.batch().len(), 4);
    assert!(carrier.floats().is_none());
    // the second table is appended whole, so its indices move by the first table length
    assert_uint32(carrier.indices(), &[Some(0), Some(1), Some(3), Some(2)]);
    assert_public_columns(carrier.batch().as_arrow(), &first.rows, &[1, 2, 3, 4]);
    assert_sum_sample(dense.sample(0), 0, f64::NAN);
    assert_null_sum_sample(dense.sample(1), 1);
    assert_sum_sample(dense.sample(2), 3, f64::INFINITY);
    assert_null_parent_sample(dense.sample(3), 2);
    assert!(
        !carrier
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&first_batch.as_arrow().to_data())
    );
    assert!(
        !carrier
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&second_batch.as_arrow().to_data())
    );
}

#[test]
fn into_dense_does_not_merge_distinct_slices_of_one_table() {
    let fixture = public_fixture();
    let first = public_batch(&fixture.backing.slice(0, 3));
    let second = public_batch(&fixture.backing.slice(3, 3));
    assert!(
        !first
            .as_arrow()
            .to_data()
            .ptr_eq(&second.as_arrow().to_data())
    );
    let dense = Samples::try_from_chunks(vec![
        Samples::from_public_histogram(
            PublicHistogramSamples::try_new(
                UInt32Array::from(vec![Some(1), Some(0)]),
                Arc::clone(&first),
                None,
            )
            .unwrap(),
        ),
        Samples::from_public_histogram(
            PublicHistogramSamples::try_new(
                UInt32Array::from(vec![Some(1), Some(0)]),
                Arc::clone(&second),
                None,
            )
            .unwrap(),
        ),
    ])
    .unwrap()
    .into_dense()
    .unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = dense.dense().unwrap() else {
        panic!("distinct slices densify to the public carrier");
    };
    assert_eq!(carrier.batch().len(), fixture.backing.len());
    assert_uint32(carrier.indices(), &[Some(1), Some(0), Some(4), Some(3)]);
    assert_public_columns(
        carrier.batch().as_arrow(),
        &fixture.rows,
        &[0, 1, 2, 3, 4, 5],
    );
    assert_sum_sample(dense.sample(0), 1, f64::NAN);
    assert_null_parent_sample(dense.sample(1), 0);
    assert_sum_sample(dense.sample(2), 4, f64::INFINITY);
    assert_null_parent_sample(dense.sample(3), 3);
    assert!(
        !carrier
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&first.as_arrow().to_data())
    );
    assert!(
        !carrier
            .batch()
            .as_arrow()
            .to_data()
            .ptr_eq(&second.as_arrow().to_data())
    );
}

#[test]
fn into_dense_forms_a_float_lane_for_public_and_float_segments() {
    let fixture = public_fixture();
    let batch = public_batch(&fixture.backing.slice(1, 2));
    let pure = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![Some(0), None]),
            Arc::clone(&batch),
            None,
        )
        .unwrap(),
    );
    let dense = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(-0.0), None])),
        pure.clone(),
    ])
    .unwrap()
    .into_dense()
    .unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = dense.dense().unwrap() else {
        panic!("public segments densify to the public carrier");
    };
    assert_eq!(carrier.len(), 4);
    assert!(Arc::ptr_eq(carrier.batch(), &batch));
    assert_uint32(carrier.indices(), &[None, None, Some(0), None]);
    let lane = carrier.floats().expect("float segments form a float lane");
    assert_float_array(lane, &[Some(-0.0), None, None, None]);
    assert!(matches!(
        dense.sample(0),
        SampleRef::Float(value) if value.to_bits() == (-0.0_f64).to_bits()
    ));
    assert!(matches!(dense.sample(1), SampleRef::Missing));
    assert_sum_sample(dense.sample(2), 0, f64::NAN);
    assert!(matches!(dense.sample(3), SampleRef::Missing));

    // a public carrier that already owns a float lane merges lanes and remaps indices
    let mixed = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![None, Some(1)]),
            public_batch(&fixture.backing.slice(3, 2)),
            Some(Float64Array::from(vec![Some(PAYLOAD_NAN), None])),
        )
        .unwrap(),
    );
    let dense = Samples::try_from_chunks(vec![pure, mixed])
        .unwrap()
        .into_dense()
        .unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = dense.dense().unwrap() else {
        panic!("mixed public segments densify to the public carrier");
    };
    assert_eq!(carrier.batch().len(), 4);
    assert_uint32(carrier.indices(), &[Some(0), None, None, Some(3)]);
    let lane = carrier.floats().expect("the mixed lane is carried over");
    assert_float_array(lane, &[None, None, Some(PAYLOAD_NAN), None]);
    assert_public_columns(carrier.batch().as_arrow(), &fixture.rows, VISIBLE_ROWS);
    assert_sum_sample(dense.sample(0), 0, f64::NAN);
    assert!(matches!(dense.sample(1), SampleRef::Missing));
    assert!(matches!(
        dense.sample(2),
        SampleRef::Float(value) if value.to_bits() == PAYLOAD_NAN.to_bits()
    ));
    assert_sum_sample(dense.sample(3), 3, f64::INFINITY);
}

#[test]
fn canonical_and_public_chunks_iterate_but_cannot_densify() {
    let fixture = public_fixture();
    let table = public_batch(&fixture.backing.slice(1, 1));
    let canonical = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0)]),
        canonical_histogram_batch(4.25),
    )
    .unwrap();
    let public = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(UInt32Array::from(vec![Some(0)]), Arc::clone(&table), None)
            .unwrap(),
    );
    let chunks = Samples::try_from_chunks(vec![canonical, public]).unwrap();
    assert_eq!(chunks.len(), 2);
    assert!(matches!(
        chunks.segments()[0].dense(),
        Some(DenseSamplesRef::Histogram(_))
    ));
    assert!(matches!(
        chunks.segments()[1].dense(),
        Some(DenseSamplesRef::PublicHistogram(_))
    ));
    // both representations stay readable and iterable as they are
    assert!(matches!(
        chunks.sample(0),
        SampleRef::Histogram(view) if view.sum.to_bits() == 4.25_f64.to_bits()
    ));
    assert_sum_sample(chunks.sample(1), 0, f64::NAN);
    let mut iter = chunks.iter();
    assert_eq!(iter.len(), 2);
    let SampleRef::Histogram(view) = iter.next().unwrap() else {
        panic!("expected the canonical histogram variant");
    };
    assert_eq!(view.sum.to_bits(), 4.25_f64.to_bits());
    assert_eq!(view.positive.buckets.len(), 1);
    assert_eq!(view.positive.buckets[0].to_bits(), 3.5_f64.to_bits());
    assert_sum_sample(iter.next().unwrap(), 0, f64::NAN);
    assert!(iter.next().is_none());
    drop(iter);
    let sliced = chunks.slice(1, 1).unwrap();
    let DenseSamplesRef::PublicHistogram(carrier) = sliced.dense().unwrap() else {
        panic!("a sliced public segment stays public");
    };
    assert!(Arc::ptr_eq(carrier.batch(), &table));
    assert_sum_sample(sliced.sample(0), 0, f64::NAN);

    // densification cannot represent both table layouts at once, so it fails instead of
    // concatenating incompatible tables
    assert!(chunks.into_dense().is_err());
}

#[test]
fn v1_frame_packing_rejects_public_histogram_samples() {
    let fixture = public_fixture();
    let batch = public_batch(&fixture.array);
    let schema = SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from("hist"),
        }]
        .into(),
        eval_grid: None,
    };
    let time = RawTimestamps::try_new(vec![1_i64, 2].into()).unwrap();
    let pure = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![Some(0), None]),
            Arc::clone(&batch),
            None,
        )
        .unwrap(),
    );
    let mixed = Samples::from_public_histogram(
        PublicHistogramSamples::try_new(
            UInt32Array::from(vec![Some(0), None]),
            Arc::clone(&batch),
            Some(Float64Array::from(vec![None, Some(2.5)])),
        )
        .unwrap(),
    );

    for (label, samples) in [("pure", pure), ("mixed", mixed)] {
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from(vec![1_u8]),
            None,
            time.clone(),
            SeriesFields::one(samples),
            false,
        )
        .unwrap();
        let Err(error) = pack_raw_series_chunk(&schema, RawLabelMode::Key, &chunk) else {
            panic!("{label} public histogram samples must not pack into a v1 frame");
        };
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            message.contains("public") || message.contains("v2"),
            "{label}: the rejection should name the public/V2 payload, got: {message}"
        );
        assert!(
            message.contains("hist"),
            "{label}: the rejection should name the histogram payload, got: {message}"
        );
    }
}
