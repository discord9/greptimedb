# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compact factor-preserving layout/correctness demonstration, not a benchmark."""
import argparse
import json
import struct
import sys
import unittest

import pyarrow as pa
import pyarrow.compute as pc

THRESHOLD = 2
MASK = (1 << 63) - 1


def list_batch(keys, a_rows, b_rows):
    def lists(rows):
        offsets = [0]
        for row in rows:
            offsets.append(offsets[-1] + len(row))
        child = pa.array([x for row in rows for x in row], type=pa.int64())
        return pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()), child)
    return pa.record_batch([pa.array(keys, type=pa.int64()), lists(a_rows), lists(b_rows)], names=["k", "A", "B"])


def encode_children(array):
    vals = array.to_pylist()
    count = len(vals)
    if count > MASK:
        raise ValueError("child count exceeds format limit")
    has_null = any(v is None for v in vals)
    bitmap = bytearray((count + 7) // 8) if has_null else None
    data = bytearray()
    for i, value in enumerate(vals):
        data.extend(struct.pack("<q", value or 0))
        if value is not None and bitmap is not None:
            bitmap[i // 8] |= 1 << (i % 8)
    return struct.pack("<Q", count | ((1 << 63) if has_null else 0)) + (bytes(bitmap) if bitmap else b"") + data


def decode_children(blob):
    if len(blob) < 8:
        raise ValueError("truncated child-count header")
    header, = struct.unpack_from("<Q", blob)
    has_null, count = bool(header >> 63), header & MASK
    bitmap_len = (count + 7) // 8 if has_null else 0
    if len(blob) != 8 + bitmap_len + count * 8:
        raise ValueError("invalid child payload length")
    if has_null and count % 8 and blob[8 + bitmap_len - 1] & ~((1 << (count % 8)) - 1):
        raise ValueError("validity bitmap has bits beyond child count")
    owner = pa.py_buffer(blob)
    validity = owner.slice(8, bitmap_len) if has_null else None
    data = owner.slice(8 + bitmap_len, count * 8)
    # PyArrow accepts unaligned buffers on this path; portability is not claimed.
    return pa.Array.from_buffers(pa.int64(), count, [validity, data])


def binary_batch(batch):
    def encode(column):
        return pa.array([encode_children(children(batch, column, i)) for i in range(batch.num_rows)], type=pa.binary())
    return pa.record_batch([batch.column(0), encode(1), encode(2)], names=["k", "A", "B"])


def binary_child(binary, row):
    row += binary.offset
    offset_buffer, data_buffer = binary.buffers()[1:3]
    start, end = struct.unpack_from("<ii", offset_buffer, row * 4)
    return decode_children(data_buffer.slice(start, end - start))


def children(batch, column, row):
    array = batch.column(column)
    if array[row].is_valid is False:
        raise ValueError("parent-null list/binary values are unsupported")
    if pa.types.is_list(array.type):
        start, end = array.offsets[row].as_py(), array.offsets[row + 1].as_py()
        return array.values.slice(start, end - start)
    return binary_child(array, row)


def validate_keys(batch):
    if batch.num_columns != 3 or batch.column(0).type != pa.int64():
        raise ValueError("expected k:Int64, A:List<Int64>/Binary, B:List<Int64>/Binary")
    for index in (1, 2):
        data_type = batch.column(index).type
        if not (pa.types.is_binary(data_type) or (pa.types.is_list(data_type) and data_type.value_type == pa.int64())):
            raise ValueError("expected List<Int64> or Binary child branches")
    keys = batch.column(0).to_pylist()
    for row in range(batch.num_rows):
        children(batch, 1, row)
        children(batch, 2, row)
    if any(k is None for k in keys) or len(set(keys)) != len(keys):
        raise ValueError("one complete, unique non-null key per input batch is required")
    return keys


def filter_project(batch, binary=False):
    keys = validate_keys(batch)
    a_rows = []
    for row in range(batch.num_rows):
        a = children(batch, 1, row)
        filtered = pc.filter(a, pc.fill_null(pc.greater_equal(a, THRESHOLD), False))
        a_rows.append(pc.add_checked(filtered, 1))
    source_b = batch.column(2)
    if binary:
        projected = pa.record_batch([batch.column(0),
                                     pa.array([encode_children(a) for a in a_rows], type=pa.binary()), source_b],
                                    names=["k", "A", "B"])
    else:
        offsets = [0]
        for a in a_rows:
            offsets.append(offsets[-1] + len(a))
        projected_a = pa.ListArray.from_arrays(pa.array(offsets, type=pa.int32()),
                                               pa.concat_arrays(a_rows) if a_rows else pa.array([], type=pa.int64()))
        projected = pa.record_batch([batch.column(0), projected_a, source_b], names=["k", "A", "B"])
    return projected


def aggregate(batch):
    keys = validate_keys(batch)
    result = {}
    for row, key in enumerate(keys):
        a, b = children(batch, 1, row), children(batch, 2, row)
        valid_a, valid_b = len(a) - a.null_count, len(b) - b.null_count
        product = len(a) * len(b)
        if product == 0:
            continue
        limit = (1 << 63) - 1
        for values in (a, b):
            bounds = pc.min_max(values).as_py()
            if bounds["min"] is not None and max(abs(bounds["min"]), abs(bounds["max"])) * len(values) > limit:
                raise OverflowError("conservative Int64 child SUM bound exceeded")
        sum_a = pc.sum(a).as_py() or 0
        sum_b = pc.sum(b).as_py() or 0
        total = sum_a * valid_b + sum_b * valid_a if valid_a and valid_b else None
        if total is not None and not -(1 << 63) <= total <= limit:
            raise OverflowError("SUM(A+B) exceeds Int64")
        result[key] = (total, product, valid_a * len(b))
    return result


def flatten(batch, predicate=True):
    keys = validate_keys(batch)
    output = []
    for row, key in enumerate(keys):
        a, b = children(batch, 1, row).to_pylist(), children(batch, 2, row).to_pylist()
        for x in a:
            for y in b:
                if not predicate or (x is not None and y is not None and x < y):
                    output.append((key, x, y))
    return sorted(output, key=repr)


def reference(batch):
    """Materialize raw inner-product bag before downstream filter/project."""
    keys = validate_keys(batch)
    flat = []
    for row, key in enumerate(keys):
        for a in children(batch, 1, row).to_pylist():
            for b in children(batch, 2, row).to_pylist():
                flat.append((key, a, b))
    projected = [(k, a + 1, b) for k, a, b in flat if a is not None and a >= THRESHOLD]
    grouped = {}
    for key in keys:
        rows = [(a, b) for k, a, b in projected if k == key]
        if rows:
            nonnull_sum = [a + b for a, b in rows if a is not None and b is not None]
            grouped[key] = (sum(nonnull_sum) if nonnull_sum else None, len(rows), sum(a is not None for a, _ in rows))
    return sorted(projected, key=repr), sorted([r for r in projected if r[1] is not None and r[2] is not None and r[1] < r[2]], key=repr), grouped


def fixture(scaled=False):
    if scaled:
        return list_batch([1], [list(range(100))], [list(range(100))])
    return list_batch([1, 2, 3, 4, 5], [[2, 2, None, 1], [3], [4], [None], [3]],
                      [[5, 5, None], [None, 8], [], [7], [None, None]])


def metrics(batch, projected):
    return {"top_level_rows": batch.num_rows,
            "factor_product_rows": sum(len(children(projected, 1, i)) * len(children(projected, 2, i))
                                        for i in range(projected.num_rows)),
            "input_nbytes": batch.nbytes,
            "input_buffer_bytes": sum(buf.size for c in batch.columns for buf in c.buffers() if buf),
            "projected_nbytes": projected.nbytes}


def unique_buffer_bytes(batch):
    seen, total = set(), 0
    for column in batch.columns:
        for buffer in column.buffers():
            if buffer is not None and buffer.address not in seen:
                seen.add(buffer.address)
                total += buffer.size
    return total


def flat_arrays(rows):
    return pa.record_batch([pa.array([r[0] for r in rows], type=pa.int64()),
                            pa.array([r[1] for r in rows], type=pa.int64()),
                            pa.array([r[2] for r in rows], type=pa.int64())], names=["k", "A", "B"])


def run_case(batch):
    expected_flat, expected_predicate, expected_aggregate = reference(batch)
    raw_product_rows = sum(len(children(batch, 1, i)) * len(children(batch, 2, i))
                           for i in range(batch.num_rows))
    pushed_flat = flat_arrays(expected_flat)
    result = {"Flat pushed-down reference": {"top_level_rows": pushed_flat.num_rows,
                                             "array_nbytes": pushed_flat.nbytes,
                                             "unique_buffer_bytes": unique_buffer_bytes(pushed_flat),
                                             "raw_inner_product_rows": raw_product_rows,
                                             "pushed_down_rows": len(expected_flat)}}
    for layout, source in (("List", batch), ("Binary", binary_batch(batch))):
        projected = filter_project(source, binary=layout == "Binary")
        flat, pred, agg = flatten(projected, False), flatten(projected, True), aggregate(projected)
        if (flat, pred, agg) != (expected_flat, expected_predicate, expected_aggregate):
            raise AssertionError(f"{layout} differs from full flat bag reference")
        result[layout] = {"flat_bag_exact": True, "pair_predicate_exact": True,
                          "aggregate_exact": True, "flat_product_rows": len(expected_flat),
                          "flat_product_array_nbytes": pushed_flat.nbytes,
                          "flat_product_unique_buffer_bytes": unique_buffer_bytes(pushed_flat),
                          **metrics(source, projected)}
    return result, expected_flat, expected_predicate, expected_aggregate


class ExperimentTests(unittest.TestCase):
    def test_full_bags_and_aggregate(self):
        result, flat, pred, grouped = run_case(fixture())
        self.assertEqual(len(flat), 10)
        self.assertEqual(pred, [(1, 3, 5)] * 4 + [(2, 4, 8)])
        self.assertEqual(grouped, {1: (32, 6, 6), 2: (12, 2, 2), 5: (None, 2, 2)})
        self.assertEqual(result["List"]["top_level_rows"], 5)
        self.assertEqual(result["List"]["factor_product_rows"], 10)

    def test_all_null_b_and_raw_null_counts(self):
        batch = list_batch([9], [[3, None]], [[None, None]])
        self.assertEqual(aggregate(batch), {9: (None, 4, 2)})
        self.assertEqual(aggregate(filter_project(batch)), {9: (None, 2, 2)})

    def test_binary_malformed_and_view(self):
        for blob in (b"", struct.pack("<Q", 2) + b"x", struct.pack("<Q", (1 << 63) | 1) + b"\x81" + b"x" * 8):
            with self.assertRaises(ValueError):
                decode_children(blob)
        batch = list_batch([0, 1], [[1], [10, None, 20]], [[0], [1]])
        binary = binary_batch(batch).column(1).slice(1, 1)
        child = binary_child(binary, 0)
        self.assertEqual(child.to_pylist(), [10, None, 20])
        self.assertEqual(child.buffers()[1].address, binary.buffers()[2].address + struct.unpack_from("<i", binary.buffers()[1], 4)[0] + 9)
        full = list_batch([0, 1], [[1], [10, None, 20]], [[0], [1]])
        encoded = binary_batch(full)
        sliced = encoded.slice(1, 1)
        self.assertEqual(flatten(sliced, False), [(1, 10, 1), (1, 20, 1), (1, None, 1)])

    def test_sliced_record_batches(self):
        full_batch = list_batch([0, 1], [[1], [3]], [[2], [5]])
        batch = full_batch.slice(1, 1)
        for source in (batch, binary_batch(full_batch).slice(1, 1)):
            projected = filter_project(source, binary=pa.types.is_binary(source.column(1).type))
            self.assertEqual(flatten(projected, False), [(1, 4, 5)])

    def test_parent_null_values_rejected(self):
        values = pa.array([3], type=pa.int64())
        offsets = pa.array([0, 1], type=pa.int32())
        null_list = pa.ListArray.from_arrays(offsets, values, mask=pa.array([True]))
        batch = pa.record_batch([pa.array([1]), null_list, list_batch([1], [[4]], [[5]]).column(2)], names=["k", "A", "B"])
        with self.assertRaises(ValueError):
            flatten(batch, False)
        valid = list_batch([1], [[3]], [[4]])
        binary = binary_batch(valid)
        null_binary = pa.array([None], type=pa.binary())
        malformed = pa.record_batch([valid.column(0), null_binary, binary.column(2)], names=["k", "A", "B"])
        with self.assertRaises(ValueError):
            flatten(malformed, False)

    def test_overflow_rejected(self):
        max_value = (1 << 63) - 1
        batch = list_batch([1], [[max_value]], [[1]])
        with self.assertRaises((OverflowError, pa.ArrowInvalid)):
            filter_project(batch)
        with self.assertRaises(OverflowError):
            aggregate(list_batch([1], [[max_value]], [[1]]))

    def test_reject_fragmented_keys(self):
        with self.assertRaises(ValueError):
            aggregate(list_batch([1, 1], [[2], [3]], [[4], [5]]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    if args.test:
        result = unittest.main(argv=[sys.argv[0]], exit=False)
        if not result.result.wasSuccessful():
            raise SystemExit(1)
        return
    small, _, _, _ = run_case(fixture())
    large, _, _, _ = run_case(fixture(True))
    print(json.dumps({"runtime": {"python": sys.version.split()[0], "pyarrow": pa.__version__,
                                   "greptimedb_arrow_runtime": "not used; repository Rust Arrow 59.2.0 is separate"},
                      "scope": "correctness/layout only; not GreptimeDB/FFX or a speed benchmark",
                      "shape": "one batch, unique complete non-null Int64 keys, nullable Int64 child arrays",
                      "unsupported": "cross-batch aggregation/merge, null or repeated keys, arbitrary schema, parent-null lists",
                      "exactness": {"small": True, "100x100": True},
                      "numeric_scope": "bounded Int64; checked projection and conservative SUM bounds; overflow raises",
                      "work_notes": {"aggregate_pair_enumeration": False,
                                     "filter_project_uses_arrow_kernels": True,
                                     "baseline_materializes_full_raw_product": True,
                                     "late_flatten_pair_work_required": True,
                                     "planner_pushdown_flat_reference_can_filter_project_branches_before_join": True,
                                     "flat_matrix": {"raw_inner_product_rows": 10000,
                                                     "post_pushdown_product_rows": 9800}},
                      "ordering": "bag equality only; flattened output is sorted for stable comparisons",
                      "statistics": {"small": small, "non_null_100x100": large},
                      "layout_notes": {"factor_product_rows_are_not_batch_rows": True,
                                       "B_array_preserved_without_reconstruction": True,
                                       "Binary_A_reencoded_after_projection": True,
                                       "Binary_child_values_typed_view_zero_copy": True,
                                       "array_bytes_are_not_RSS": True,
                                       "unaligned_buffers_limited_to_this_pyarrow_path": True}}, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
