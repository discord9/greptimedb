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

//! Local executor benchmarks, not scanner, end-to-end or wall-clock performance claims.
//! Setup clones prepared inputs and constructs execution state outside timing; all cases time
//! pushing, draining and finishing, and return execution and output for teardown outside timing.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datatypes::arrow::array::Float64Array;
use datatypes::series::samples::{
    FieldSpec, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use promql::evaluated::EvaluatedChunk;
use promql::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use promql::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundScalar, BoundWindow, Grouping,
    NativeExpr, NativeNode, NativeTree, NativeType,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use promql::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

struct Fixture {
    tree: NativeTree,
    chunks: Vec<EvaluatedChunk>,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Window,
    Sum,
    Labels { dirty: bool },
}

fn grid(len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1_000, len).unwrap()).unwrap())
}

fn schema(grid: Arc<EvalGrid>, count: usize) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: (0..count)
            .map(|i| FieldSpec {
                name: format!("v{i}").into(),
            })
            .collect(),
        eval_grid: Some(grid),
    })
}

fn source(schema: &Arc<SeriesSchema>) -> NativeNode {
    let output = NativeType::Samples(Arc::clone(schema));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

fn broadcast(input: NativeNode, grid: Arc<EvalGrid>) -> NativeNode {
    let output = input.output.clone();
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(input),
            right: Box::new(NativeNode {
                expr: NativeExpr::Scalar(BoundScalar { value: 2.0 }),
                output: NativeType::Scalar(grid),
            }),
            matching: BinaryMatching {
                op: BinaryOp::Mul,
                return_bool: false,
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output,
    }
}

fn labels(id: usize, tags: usize, dirty: bool) -> SeriesLabels {
    let mut labels: Vec<_> = (0..tags)
        .map(|tag| Label {
            name: format!("tag{tag}").into(),
            value: format!("{id}-{tag}").into(),
        })
        .collect();
    if dirty {
        labels.extend([
            Label {
                name: "__type__".into(),
                value: "gauge".into(),
            },
            Label {
                name: "__unit__".into(),
                value: "seconds".into(),
            },
            Label {
                name: "empty".into(),
                value: "".into(),
            },
        ]);
    }
    SeriesLabels::try_new(Some("requests".into()), labels).unwrap()
}

fn chunk(
    schema: &SeriesSchema,
    metric: SeriesLabels,
    time: TimeAxis,
    fields: Vec<Vec<Option<f64>>>,
    is_last: bool,
) -> EvaluatedChunk {
    let fields = fields
        .into_iter()
        .map(|v| Samples::from_float(Float64Array::from(v)))
        .collect();
    EvaluatedChunk::try_new(
        Series::try_new(
            schema,
            metric,
            time,
            SeriesFields::try_from_vec(fields).unwrap(),
        )
        .unwrap(),
        is_last,
    )
    .unwrap()
}

fn window_fixture() -> Fixture {
    let grid = grid(128);
    let schema = schema(Arc::clone(&grid), 1);
    let window = NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(source(&schema)),
            op: BoundWindow {
                function: RangeFunction::SumOverTime,
                window: RangeWindow {
                    range_ms: 1,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: Some(Arc::clone(&grid)),
                output_grid: Arc::clone(&grid),
            },
        },
        output: NativeType::Samples(Arc::clone(&schema)),
    };
    let mut chunks = Vec::new();
    for id in 0..16 {
        for part in 0..4 {
            let start = part * 32;
            let values = (start..start + 32)
                .map(|p| (p % 11 != 0).then_some((id * 128 + p) as f64))
                .collect();
            chunks.push(chunk(
                &schema,
                labels(id, 1, false),
                TimeAxis::Grid(GridRange::try_new(Arc::clone(&grid), start, 32).unwrap()),
                vec![values],
                part == 3,
            ));
        }
    }
    Fixture {
        tree: NativeTree::try_new(broadcast(window, grid)).unwrap(),
        chunks,
        kind: Kind::Window,
    }
}

fn sum_fixture() -> Fixture {
    let grid = grid(256);
    let schema = schema(Arc::clone(&grid), 2);
    let input = source(&schema);
    let root = NativeNode {
        output: input.output.clone(),
        expr: NativeExpr::Aggregate {
            input: Box::new(input),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                stage: AggregateStage::Complete,
                group: Grouping::By(Arc::new([])),
            },
        },
    };
    let mut chunks = Vec::new();
    for id in 0..2 {
        // The middle chunk crosses 63/64 and also has two spans separated by an empty block.
        for (spans, is_last) in [
            (vec![GridSpan::new(32, 31)], false),
            (vec![GridSpan::new(63, 33), GridSpan::new(192, 33)], false),
            (vec![GridSpan::new(225, 31)], true),
        ] {
            let positions: Vec<_> = spans
                .iter()
                .flat_map(|span| span.start()..span.start() + span.len())
                .collect();
            let fields = (0..2)
                .map(|field| {
                    positions
                        .iter()
                        .map(|&p| {
                            (p != 70 && p % (field + 3) != id)
                                .then_some((id * 10 + field + p) as f64)
                        })
                        .collect()
                })
                .collect();
            let time = TimeAxis::from_grid_spans(Arc::clone(&grid), spans).unwrap();
            chunks.push(chunk(&schema, labels(id, 1, false), time, fields, is_last));
        }
    }
    Fixture {
        tree: NativeTree::try_new(root).unwrap(),
        chunks,
        kind: Kind::Sum,
    }
}

fn label_fixture(dirty: bool) -> Fixture {
    let grid = grid(8);
    let schema = schema(Arc::clone(&grid), 1);
    let mut chunks = Vec::new();
    for id in 0..16 {
        let len = id % 8 + 1;
        let values = (0..len).map(|p| Some((id + p) as f64)).collect();
        chunks.push(chunk(
            &schema,
            labels(id, 8, dirty),
            TimeAxis::Grid(GridRange::try_new(Arc::clone(&grid), 0, len).unwrap()),
            vec![values],
            true,
        ));
    }
    Fixture {
        tree: NativeTree::try_new(broadcast(source(&schema), grid)).unwrap(),
        chunks,
        kind: Kind::Labels { dirty },
    }
}

fn prepare(fixture: &Fixture) -> (NativeExecutor, Vec<EvaluatedChunk>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 20));
    let state = NativeExecutionState::try_new(&fixture.tree, pool, None).unwrap();
    (NativeExecutor::new(state), fixture.chunks.clone())
}

fn execute(
    (mut executor, mut chunks): (NativeExecutor, Vec<EvaluatedChunk>),
) -> (
    NativeExecutor,
    Vec<EvaluatedChunk>,
    Vec<EvaluatedChunk>,
    Vec<EvaluatedChunk>,
) {
    let mut output = Vec::new();
    for chunk in chunks.drain(..) {
        executor.push_evaluated(InputId::new(0), chunk).unwrap();
        while let Some(chunk) = executor.next_chunk().unwrap() {
            output.push(chunk);
        }
    }
    let finished = executor.finish().unwrap();
    (executor, chunks, output, finished)
}

// Independent arithmetic oracle and finality/axis checks, run once before timing each fixture.
fn verify(fixture: &Fixture) {
    let (executor, _inputs, streamed, finished) = execute(prepare(fixture));
    assert_eq!(executor.state().reserved_bytes(), 0);
    let (series_count, len, fields) = match fixture.kind {
        Kind::Window => (16, 128, 1),
        Kind::Sum => (1, 256, 2),
        Kind::Labels { .. } => (16, 8, 1),
    };
    let mut values = vec![vec![vec![None; len]; fields]; series_count];
    let mut closes = vec![0; series_count];
    for chunk in streamed.iter().chain(&finished) {
        assert_eq!(chunk.series.metric.name(), None);
        let id = match fixture.kind {
            Kind::Sum => {
                assert!(chunk.series.metric.tags().is_empty());
                0
            }
            _ => chunk
                .series
                .metric
                .get("tag0")
                .unwrap()
                .split('-')
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
        };
        if let Kind::Labels { dirty } = fixture.kind {
            assert_eq!(chunk.series.metric.tags().len(), 8);
            assert_eq!(chunk.series.metric.get("__type__"), None);
            assert_eq!(chunk.series.metric.get("__unit__"), None);
            assert_eq!(chunk.series.metric.get("empty"), None);
            assert_eq!(
                chunk.series.metric.tags().as_ptr()
                    == fixture.chunks[id].series.metric.tags().as_ptr(),
                !dirty
            );
        }
        closes[id] += usize::from(chunk.is_last);
        for (field, field_values) in values[id].iter_mut().enumerate() {
            let samples: Vec<_> = chunk
                .series
                .fields
                .get(field)
                .unwrap()
                .iter()
                .map(|sample| match sample {
                    SampleRef::Float(v) => Some(v),
                    SampleRef::Missing => None,
                    _ => panic!("unexpected histogram"),
                })
                .collect();
            for (p, row) in chunk.series.time.positions() {
                assert_eq!(chunk.series.time.timestamp(row), Some(p as i64 * 1_000));
                if let Some(value) = samples[row] {
                    assert!(field_values[p].replace(value).is_none());
                }
            }
        }
    }
    assert_eq!(closes, vec![1; series_count]);
    for (id, series) in values.iter().enumerate() {
        for (field, data) in series.iter().enumerate() {
            for (p, actual) in data.iter().enumerate() {
                let expected = match fixture.kind {
                    Kind::Window => (p % 11 != 0).then_some((id * 128 + p) as f64 * 2.0),
                    Kind::Labels { .. } => (p < id % 8 + 1).then_some((id + p) as f64 * 2.0),
                    Kind::Sum => {
                        let samples: Vec<_> = (0..2)
                            .filter(|&i| {
                                ((32..96).contains(&p) || (192..256).contains(&p))
                                    && p != 70
                                    && p % (field + 3) != i
                            })
                            .map(|i| (i * 10 + field + p) as f64)
                            .collect();
                        (!samples.is_empty()).then(|| samples.iter().sum())
                    }
                };
                assert_eq!(
                    *actual, expected,
                    "series {id}, field {field}, position {p}"
                );
            }
        }
    }
}

fn native_execute(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_execute");
    for (name, fixture) in [
        ("window_literal_16x128", window_fixture()),
        ("sum_direct_fold_2fields_sparse128", sum_fixture()),
        ("clean_label_sharing_16x1-8", label_fixture(false)),
        ("dirty_label_filter_16x1-8", label_fixture(true)),
    ] {
        verify(&fixture);
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare(&fixture),
                |input| black_box(execute(input)),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, native_execute);
