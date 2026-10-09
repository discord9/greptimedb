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

use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

const BLOCK_SIZE: usize = 64;
const DICTIONARY_SIZE: usize = 64;
const ROUNDS: usize = 11;

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    literal: &'static str,
}

const CASES: [Case; 8] = [
    Case {
        name: "sparse",
        literal: "RARE_MARKER_7Q",
    },
    Case {
        name: "dense",
        literal: "INFO",
    },
    Case {
        name: "absent",
        literal: "NOT_IN_CORPUS",
    },
    Case {
        name: "unknown_gram",
        literal: "ZZZ_UNTRAINED",
    },
    Case {
        name: "empty",
        literal: "",
    },
    Case {
        name: "short",
        literal: "x",
    },
    Case {
        name: "unicode",
        literal: "雪の",
    },
    Case {
        name: "newline",
        literal: "line\nend",
    },
];

const TRAINING_CASES: [Case; 7] = [
    CASES[0], CASES[1], CASES[2], CASES[4], CASES[5], CASES[6], CASES[7],
];

type Rows = Vec<Option<String>>;

struct Index {
    grams: Vec<[u8; 2]>,
    block_masks: Vec<u64>,
}

fn grams_in(text: &str) -> BTreeSet<[u8; 2]> {
    text.as_bytes().windows(2).map(|w| [w[0], w[1]]).collect()
}

fn select_grams(cases: &[Case]) -> Vec<[u8; 2]> {
    let mut counts = BTreeMap::<[u8; 2], usize>::new();
    for case in cases {
        for gram in grams_in(case.literal) {
            *counts.entry(gram).or_default() += 1;
        }
    }
    let mut scored: Vec<_> = counts.into_iter().collect();
    scored.sort_by(|(a, ac), (b, bc)| bc.cmp(ac).then_with(|| a.cmp(b)));
    scored.truncate(DICTIONARY_SIZE);
    scored.into_iter().map(|(gram, _)| gram).collect()
}

fn mask(text: &str, grams: &[[u8; 2]]) -> u64 {
    let mut result = 0;
    for pair in text.as_bytes().windows(2) {
        if let Some(bit) = grams.iter().position(|g| *g == [pair[0], pair[1]]) {
            result |= 1u64 << bit;
        }
    }
    result
}

fn build(rows: &Rows, cases: &[Case]) -> Index {
    let grams = select_grams(cases);
    let block_masks = rows
        .chunks(BLOCK_SIZE)
        .map(|block| {
            block
                .iter()
                .flatten()
                .fold(0, |acc, row| acc | mask(row, &grams))
        })
        .collect();
    Index { grams, block_masks }
}

fn required_mask(index: &Index, literal: &str) -> Option<u64> {
    let query_grams = grams_in(literal);
    if literal.len() < 2 || query_grams.is_empty() {
        return None;
    }
    let known = query_grams
        .iter()
        .filter(|g| index.grams.contains(g))
        .count();
    // A missing required gram may not be interpreted as proof of absence.
    // Unknown query grams conservatively disable pruning.
    if known != query_grams.len() {
        return None;
    }
    Some(mask(literal, &index.grams))
}

fn candidates(rows: &Rows, index: &Index, required: Option<u64>) -> Vec<usize> {
    let mut ids = Vec::new();
    for (block, &block_mask) in index.block_masks.iter().enumerate() {
        if required.is_none_or(|r| block_mask & r == r) {
            let start = block * BLOCK_SIZE;
            ids.extend(start..(start + BLOCK_SIZE).min(rows.len()));
        }
    }
    ids
}

fn scan(rows: &Rows, literal: &str) -> Vec<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(id, row)| row.as_ref().filter(|s| s.contains(literal)).map(|_| id))
        .collect()
}

fn filtered(rows: &Rows, index: &Index, literal: &str) -> Vec<usize> {
    let required = required_mask(index, literal);
    candidates(rows, index, required)
        .into_iter()
        .filter(|&id| rows[id].as_ref().is_some_and(|s| s.contains(literal)))
        .collect()
}

fn corpus() -> Rows {
    (0..16_384)
        .map(|id| {
            if id % 997 == 0 {
                return None;
            }
            let body = match id % 8 {
                0 => format!(
                    "{id:05} INFO routine heartbeat shard={} payload={}",
                    id % 31,
                    "a".repeat(900)
                ),
                1 => format!("{id:05} WARN queue depth payload={}", "b".repeat(900)),
                2 => format!(
                    "{id:05} ERROR connection timed out payload={}",
                    "c".repeat(900)
                ),
                3 => format!(
                    "{id:05} DEBUG request completed payload={}",
                    "d".repeat(900)
                ),
                4 if id % 4096 == 4 => {
                    format!("{id:05} RARE_MARKER_7Q payload={}", "e".repeat(900))
                }
                5 => format!(
                    "{id:05} user 雪の観測 line\nend payload={}",
                    "f".repeat(900)
                ),
                6 => format!("{id:05} x payload={}", "g".repeat(900)),
                _ => format!("{id:05} routine event payload={}", "h".repeat(900)),
            };
            Some(body)
        })
        .collect()
}

fn edge_fixture() {
    let rows = vec![
        Some("".into()),
        None,
        Some("a".into()),
        Some("aa".into()),
        Some("雪\nend".into()),
        Some("雪\nend".into()),
    ];
    let cases = [
        Case {
            name: "edge",
            literal: "雪",
        },
        Case {
            name: "other",
            literal: "aa",
        },
    ];
    let index = build(&rows, &cases);
    for literal in ["", "a", "aa", "雪", "\n", "absent"] {
        assert_eq!(
            scan(&rows, literal),
            filtered(&rows, &index, literal),
            "edge {literal:?}"
        );
    }
    assert!(
        required_mask(&index, "absent").is_none(),
        "unknown gram must fall back"
    );
    assert!(
        required_mask(&index, "a").is_none(),
        "one-byte literal must fall back"
    );
    assert_eq!(scan(&rows, ""), vec![0, 2, 3, 4, 5]);
    assert_eq!(scan(&rows, "雪"), vec![4, 5]);
}

fn median(values: &mut [Duration]) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn measure_once<F: FnOnce()>(f: F) -> Duration {
    let start = Instant::now();
    f();
    start.elapsed()
}

fn measure_queries<F: FnMut()>(mut f: F) -> Duration {
    const QUERIES_PER_ROUND: u32 = 20;
    let start = Instant::now();
    for _ in 0..QUERIES_PER_ROUND {
        f();
    }
    start.elapsed() / QUERIES_PER_ROUND
}

fn correctness_check(rows: &Rows) -> (Index, Vec<(usize, usize, bool)>) {
    edge_fixture();
    let index = build(rows, &TRAINING_CASES);
    let mut results = Vec::new();
    for case in &CASES {
        let truth = scan(rows, case.literal);
        let output = filtered(rows, &index, case.literal);
        assert_eq!(truth, output, "complete ordered IDs differ: {}", case.name);
        let required = required_mask(&index, case.literal);
        let cand = candidates(rows, &index, required);
        if case.name == "unknown_gram" {
            assert!(required.is_none(), "unknown grams must use fallback");
            assert_eq!(cand.len(), rows.len(), "fallback must pass every row");
        }
        results.push((truth.len(), cand.len(), required.is_none()));
    }
    assert!(required_mask(&index, "ZZZ_UNTRAINED").is_none());
    assert_eq!(candidates(rows, &index, None).len(), rows.len());
    assert!(required_mask(&index, "x").is_none());
    assert!(required_mask(&index, "").is_none());
    (index, results)
}

fn main() {
    let rows = corpus();
    let (index, case_results) = correctness_check(&rows);
    let ids: usize = rows.iter().filter(|r| r.is_some()).count();
    let byte_lengths: Vec<usize> = rows.iter().flatten().map(String::len).collect();
    let total_bytes: usize = byte_lengths.iter().sum();
    let min_bytes = byte_lengths.iter().min().copied().unwrap_or(0);
    let max_bytes = byte_lengths.iter().max().copied().unwrap_or(0);
    println!(
        "corpus rows={} nonnull={} null={} block_size={} blocks={} total_utf8_bytes={} min_row_bytes={} max_row_bytes={} avg_row_bytes={:.1}",
        rows.len(),
        ids,
        rows.len() - ids,
        BLOCK_SIZE,
        index.block_masks.len(),
        total_bytes,
        min_bytes,
        max_bytes,
        total_bytes as f64 / ids as f64
    );
    for (case, &(full_ids, candidate_rows, fallback)) in CASES.iter().zip(&case_results) {
        let required = required_mask(&index, case.literal);
        let cand = candidates(&rows, &index, required);
        assert_eq!(cand.len(), candidate_rows);
        let residual = cand.iter().filter(|&&id| rows[id].is_some()).count();
        println!(
            "case={} literal={:?} full_ids={} filtered_ids={} candidate_blocks={} candidate_rows={} residual_nonnull={} fallback={}",
            case.name,
            case.literal,
            full_ids,
            full_ids,
            cand.len().div_ceil(BLOCK_SIZE),
            cand.len(),
            residual,
            fallback
        );
    }
    if !std::env::args().any(|arg| arg == "--bench") {
        return;
    }

    let mut builds = Vec::with_capacity(ROUNDS);
    black_box(build(&rows, &TRAINING_CASES));
    for _ in 0..ROUNDS {
        builds.push(measure_once(|| {
            black_box(build(black_box(&rows), black_box(&TRAINING_CASES)));
        }));
    }
    let build_time = median(&mut builds);
    println!("construction_median_ns={}", build_time.as_nanos());
    for case in &CASES {
        let mut scan_times = Vec::with_capacity(ROUNDS);
        let mut filtered_times = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS {
            if round % 2 == 0 {
                scan_times.push(measure_queries(|| {
                    black_box(scan(black_box(&rows), black_box(case.literal)));
                }));
                filtered_times.push(measure_queries(|| {
                    black_box(filtered(
                        black_box(&rows),
                        black_box(&index),
                        black_box(case.literal),
                    ));
                }));
            } else {
                filtered_times.push(measure_queries(|| {
                    black_box(filtered(
                        black_box(&rows),
                        black_box(&index),
                        black_box(case.literal),
                    ));
                }));
                scan_times.push(measure_queries(|| {
                    black_box(scan(black_box(&rows), black_box(case.literal)));
                }));
            }
        }
        let scan_ns = median(&mut scan_times).as_nanos();
        let filter_ns = median(&mut filtered_times).as_nanos();
        let build_ns = build_time.as_nanos();
        println!(
            "timing case={} scan_ns_per_query={} signature_plus_exact_ns_per_query={} construction_ns={} calculated_baseline_total_ns_1={} calculated_indexed_total_ns_1={} calculated_baseline_total_ns_10={} calculated_indexed_total_ns_10={} calculated_baseline_total_ns_100={} calculated_indexed_total_ns_100={} calculated_indexed_amortized_ns_per_query_1={} calculated_indexed_amortized_ns_per_query_10={} calculated_indexed_amortized_ns_per_query_100={}",
            case.name,
            scan_ns,
            filter_ns,
            build_ns,
            scan_ns,
            build_ns + filter_ns,
            scan_ns * 10,
            build_ns + filter_ns * 10,
            scan_ns * 100,
            build_ns + filter_ns * 100,
            build_ns + filter_ns,
            build_ns / 10 + filter_ns,
            build_ns / 100 + filter_ns
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_fixture_matches_exact_scan() {
        edge_fixture();
    }

    #[test]
    fn full_corpus_queries_match_and_unknown_gram_falls_back() {
        let rows = corpus();
        let (index, results) = correctness_check(&rows);
        assert_eq!(results.len(), CASES.len());
        let unknown = &CASES[3];
        assert!(required_mask(&index, unknown.literal).is_none());
        assert_eq!(candidates(&rows, &index, None).len(), rows.len());
        assert_eq!(scan(&rows, unknown.literal).len(), 0);
        assert!(required_mask(&index, "").is_none());
        assert!(required_mask(&index, "x").is_none());
    }
}
