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

//! Incremental trace tail sampling PoC.
//!
//! Each round finds the traces affected by new writes (a sequence delta) or by
//! time movement (the waiting and lookback boundaries crossing), then reruns the
//! original sampling SQL for those traces only and upserts the result into the
//! PoC sink. The full-query sink is maintained alongside it, so the two can be
//! compared directly.

use std::collections::{BTreeSet, HashMap};

use api::v1::QueryRequest;
use api::v1::query_request::Query;
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use client::{Client, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, Database, OutputData};
use datatypes::arrow::array::{Array, StringArray};
use futures_util::StreamExt;
use servers::server::Server;
use tests_integration::test_util::{StorageType, setup_grpc_server};

const POC_SINK: &str = "sampled_spans_poc";
const BASELINE_SINK: &str = "sampled_spans_baseline";

/// The span columns shared by the source table and both sinks.
const SPAN_COLUMNS: &str = "`timestamp`, timestamp_end, duration_nano, trace_id, span_id, \
     parent_span_id, service_name, span_name, span_kind, span_status_code, \
     span_status_message, `span_attributes.poc.case`";

/// Per-round progress. `after_seqs` is the last consumed source sequence per
/// region; `previous_eval_time` is the evaluation time of the last successful
/// round, used by the time-movement query.
#[derive(Default)]
struct PocState {
    after_seqs: HashMap<u64, u64>,
    previous_eval_time: Option<DateTime<Utc>>,
}

impl PocState {
    fn is_initialized(&self) -> bool {
        !self.after_seqs.is_empty()
    }
}

/// What one round did, mirroring the columns the cost comparison records.
#[derive(Debug, Default)]
struct RoundOutcome {
    new_trace_ids: BTreeSet<String>,
    time_trace_ids: BTreeSet<String>,
    affected_trace_ids: BTreeSet<String>,
    high: HashMap<u64, u64>,
    submitted_rows: usize,
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_timestamp(value: DateTime<Utc>) -> String {
    format!("'{}'", value.format("%Y-%m-%d %H:%M:%S%.3f"))
}

fn after_seqs_json(seqs: &HashMap<u64, u64>) -> String {
    let entries = seqs
        .iter()
        .map(|(region, seq)| format!("\"{region}\":{seq}"))
        .collect::<Vec<_>>();
    format!("{{{}}}", entries.join(","))
}

/// Runs a SELECT and returns the first column of every row as a string.
async fn fetch_string_column(db: &Database, sql: &str, extensions: &[(&str, &str)]) -> Vec<String> {
    let result = db
        .flight_request()
        .with_flow_extensions(extensions)
        .query_with_terminal_metrics(QueryRequest {
            query: Some(Query::Sql(sql.to_string())),
        })
        .await
        .unwrap_or_else(|err| panic!("query failed: {err:?}\n{sql}"));
    let OutputData::Stream(mut stream) = result.output.data else {
        panic!("expected a stream for: {sql}");
    };

    let mut values = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("the projected column must be a utf8 column");
        for index in 0..column.len() {
            if column.is_valid(index) {
                values.push(column.value(index).to_string());
            }
        }
    }
    values
}

/// Runs a `SELECT COUNT(*)` and returns the count.
///
/// Counts come back as an integer column, so this does not go through
/// [`fetch_string_column`].
async fn count_rows(db: &Database, sql: &str) -> usize {
    let result = db
        .flight_request()
        .query_with_terminal_metrics(QueryRequest {
            query: Some(Query::Sql(sql.to_string())),
        })
        .await
        .unwrap_or_else(|err| panic!("count query failed: {err:?}\n{sql}"));
    let OutputData::Stream(mut stream) = result.output.data else {
        panic!("expected a stream for: {sql}");
    };

    let mut counts = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let column = batch.column(0);
        for index in 0..column.len() {
            assert!(!column.is_null(index), "count must not be null: {sql}");
            counts.push(count_cell(column.as_any(), index));
        }
    }
    assert_eq!(
        counts.len(),
        1,
        "expected exactly one count row from: {sql}"
    );
    counts[0]
}

/// Extracts one count value regardless of the integer width the engine chose.
fn count_cell(column: &dyn std::any::Any, index: usize) -> usize {
    use datatypes::arrow::array::{Int64Array, UInt64Array};
    if let Some(array) = column.downcast_ref::<UInt64Array>() {
        return array.value(index) as usize;
    }
    if let Some(array) = column.downcast_ref::<Int64Array>() {
        return usize::try_from(array.value(index)).expect("a count must not be negative");
    }
    panic!("unexpected count column type");
}

/// Returns how many rows differ between two sinks in either direction.
///
/// Comparing whole rows on both sides keeps every column and the full primary
/// key in the comparison, so a stale `sample_reason` or a missing span shows up
/// the same way.
async fn sink_difference(db: &Database, left: &str, right: &str) -> usize {
    let left_only = count_rows(
        db,
        &format!("SELECT COUNT(*) FROM (SELECT {SPAN_COLUMNS}, sample_reason FROM {left} EXCEPT SELECT {SPAN_COLUMNS}, sample_reason FROM {right})"),
    )
    .await;
    let right_only = count_rows(
        db,
        &format!("SELECT COUNT(*) FROM (SELECT {SPAN_COLUMNS}, sample_reason FROM {right} EXCEPT SELECT {SPAN_COLUMNS}, sample_reason FROM {left})"),
    )
    .await;
    left_only + right_only
}

/// The sampling rule from the plan appendix, restricted to `trace_ids` when the
/// incremental round needs it. `now()` is bound to the round's evaluation time
/// through the `flow.scheduled_time_millis` extension.
fn sampling_select(trace_ids: Option<&BTreeSet<String>>) -> String {
    let trace_filter = trace_ids
        .map(|ids| {
            let list = ids
                .iter()
                .map(|id| sql_string(id))
                .collect::<Vec<_>>()
                .join(",");
            format!(" AND trace_id IN ({list})")
        })
        .unwrap_or_default();

    format!(
        r#"SELECT s.`timestamp`, s.timestamp_end, s.duration_nano, s.trace_id, s.span_id,
       s.parent_span_id, s.service_name, s.span_name, s.span_kind,
       s.span_status_code, s.span_status_message, s.`span_attributes.poc.case`,
       t.sample_reason
FROM (SELECT * FROM raw_spans
      WHERE `timestamp` >= now() - INTERVAL '10' MINUTE{trace_filter}) s
JOIN (SELECT trace_id,
             CASE WHEN max(CASE WHEN span_status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) = 1
                  THEN 'error' ELSE 'slow' END AS sample_reason
      FROM (SELECT * FROM raw_spans
            WHERE `timestamp` >= now() - INTERVAL '10' MINUTE{trace_filter})
      GROUP BY trace_id
      HAVING min(`timestamp`) <= now() - INTERVAL '30' SECOND
         AND (max(CASE WHEN span_status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) = 1
              OR max(timestamp_end) > min(`timestamp`) + INTERVAL '1' SECOND)) t
  ON s.trace_id = t.trace_id"#
    )
}

/// Runs the full sampling query into the baseline sink.
async fn run_baseline(db: &Database, t: DateTime<Utc>) -> usize {
    let sql = format!("INSERT INTO {BASELINE_SINK} {}", sampling_select(None));
    api_affected_rows(db, &sql, t).await
}

/// Executes a statement and returns the affected row count.
async fn api_affected_rows(db: &Database, sql: &str, t: DateTime<Utc>) -> usize {
    let scheduled = t.timestamp_millis().to_string();
    let result = db
        .flight_request()
        .with_flow_extensions(&[("flow.scheduled_time_millis", &scheduled)])
        .query_with_terminal_metrics(QueryRequest {
            query: Some(Query::Sql(sql.to_string())),
        })
        .await
        .unwrap_or_else(|err| panic!("statement failed: {err:?}\n{sql}"));
    match result.output.data {
        OutputData::AffectedRows(affected) => affected,
        other => panic!("expected affected rows from a write, got {other:?}\n{sql}"),
    }
}

/// Q1: finds the traces touched by rows written since the last checkpoint, and
/// the watermark this round may advance to.
///
/// The request carries the per-region lower bound `C` and no upper bound, so the
/// scan pins the upper bound itself and reports it as the terminal watermark.
/// That reported value is the same one used for row-level filtering, which is
/// what makes advancing `C` to it safe.
async fn query_new_traces(
    db: &Database,
    after: &HashMap<u64, u64>,
    t: DateTime<Utc>,
) -> (BTreeSet<String>, HashMap<u64, u64>) {
    let sql = format!(
        "SELECT DISTINCT trace_id FROM raw_spans WHERE `timestamp` >= {}",
        sql_timestamp(t - ChronoDuration::minutes(10))
    );
    let after_json = after_seqs_json(after);
    let scheduled = t.timestamp_millis().to_string();
    let extensions = [
        ("flow.return_region_seq", "true"),
        ("flow.incremental_mode", "sequence_range"),
        ("flow.incremental_after_seqs", after_json.as_str()),
        ("flow.scheduled_time_millis", scheduled.as_str()),
    ];

    let result = db
        .flight_request()
        .with_flow_extensions(&extensions)
        .query_with_terminal_metrics(QueryRequest {
            query: Some(Query::Sql(sql.clone())),
        })
        .await
        .unwrap_or_else(|err| panic!("q1 failed: {err:?}\n{sql}"));
    let metrics = result.metrics.clone();
    let OutputData::Stream(mut stream) = result.output.data else {
        panic!("q1 must return a stream");
    };

    let mut traces = BTreeSet::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap_or_else(|err| panic!("q1 stream failed: {err:?}"));
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trace_id must be utf8");
        for index in 0..column.len() {
            if column.is_valid(index) {
                traces.insert(column.value(index).to_string());
            }
        }
    }

    let high = metrics
        .region_watermark_map()
        .expect("q1 must report a terminal watermark for every scanned region");
    assert!(
        !high.is_empty(),
        "q1 must report at least one source region watermark"
    );
    (traces, high)
}

/// Q2: finds the traces whose decision may have changed because time moved.
///
/// The first interval releases traces whose earliest span just crossed the 30
/// second wait; the second catches traces with a span that just left the 10
/// minute lookback, whose `sample_reason` therefore has to be recomputed. Both
/// read the exact range `(0, H]`, because the rows of interest were written long
/// before `C` and a lower bound of `C` would miss them.
async fn query_time_traces(
    db: &Database,
    previous: DateTime<Utc>,
    t: DateTime<Utc>,
    high: &HashMap<u64, u64>,
) -> BTreeSet<String> {
    let sql = format!(
        "SELECT trace_id FROM raw_spans \
           WHERE `timestamp` >  {prev_30} AND `timestamp` <= {t_30} \
         UNION \
         SELECT trace_id FROM raw_spans \
           WHERE `timestamp` >= {prev_10m} AND `timestamp` <  {t_10m}",
        prev_30 = sql_timestamp(previous - ChronoDuration::seconds(30)),
        t_30 = sql_timestamp(t - ChronoDuration::seconds(30)),
        prev_10m = sql_timestamp(previous - ChronoDuration::minutes(10)),
        t_10m = sql_timestamp(t - ChronoDuration::minutes(10)),
    );

    let zero_lower = high
        .keys()
        .map(|region| (*region, 0u64))
        .collect::<HashMap<_, _>>();
    let after_json = after_seqs_json(&zero_lower);
    let high_json = after_seqs_json(high);
    let scheduled = t.timestamp_millis().to_string();
    let extensions = [
        ("flow.return_region_seq", "true"),
        ("flow.incremental_mode", "sequence_range"),
        ("flow.incremental_after_seqs", after_json.as_str()),
        ("flow.scheduled_time_millis", scheduled.as_str()),
    ];

    let snapshot = high.clone();
    let result = db
        .flight_request()
        .with_flow_extensions(&extensions)
        .with_snapshot_seqs(&snapshot)
        .query_with_terminal_metrics(QueryRequest {
            query: Some(Query::Sql(sql.clone())),
        })
        .await
        .unwrap_or_else(|err| panic!("q2 failed: {err:?} high={high_json}\n{sql}"));
    let OutputData::Stream(mut stream) = result.output.data else {
        panic!("q2 must return a stream");
    };

    let mut traces = BTreeSet::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap_or_else(|err| panic!("q2 stream failed: {err:?}"));
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trace_id must be utf8");
        for index in 0..column.len() {
            if column.is_valid(index) {
                traces.insert(column.value(index).to_string());
            }
        }
    }
    traces
}

/// Q3: reruns the original sampling SQL for the affected traces only and upserts
/// the result into the PoC sink.
///
/// This reads the exact range `(0, H]` rather than `(C, H]`: every span of an
/// affected trace that is still inside the lookback window has to be recomputed,
/// including spans consumed in earlier rounds.
async fn write_poc_sink(
    db: &Database,
    affected: &BTreeSet<String>,
    t: DateTime<Utc>,
    high: &HashMap<u64, u64>,
) -> usize {
    if affected.is_empty() {
        return 0;
    }

    let zero_lower = high
        .keys()
        .map(|region| (*region, 0u64))
        .collect::<HashMap<_, _>>();
    let after_json = after_seqs_json(&zero_lower);
    let scheduled = t.timestamp_millis().to_string();

    let mut submitted = 0;
    for chunk in affected.iter().collect::<Vec<_>>().chunks(1000) {
        let ids = chunk
            .iter()
            .map(|id| (*id).clone())
            .collect::<BTreeSet<_>>();
        let sql = format!("INSERT INTO {POC_SINK} {}", sampling_select(Some(&ids)));
        let extensions = [
            ("flow.return_region_seq", "true"),
            ("flow.incremental_mode", "sequence_range"),
            ("flow.incremental_after_seqs", after_json.as_str()),
            ("flow.scheduled_time_millis", scheduled.as_str()),
        ];
        let snapshot = high.clone();
        let result = db
            .flight_request()
            .with_flow_extensions(&extensions)
            .with_snapshot_seqs(&snapshot)
            .query_with_terminal_metrics(QueryRequest {
                query: Some(Query::Sql(sql.clone())),
            })
            .await
            .unwrap_or_else(|err| panic!("q3 failed: {err:?}\n{sql}"));
        match result.output.data {
            OutputData::AffectedRows(affected_rows) => submitted += affected_rows,
            other => panic!("q3 must write rows, got {other:?}"),
        }
    }
    submitted
}

/// Reads the source region watermark for this round.
///
/// The scan is issued without a lower or upper bound, so the engine pins the
/// upper bound itself at scan open and reports that same value as the terminal
/// watermark. That is the value a round may safely advance to.
async fn discover_watermark(db: &Database, t: DateTime<Utc>) -> HashMap<u64, u64> {
    let sql = format!(
        "SELECT DISTINCT trace_id FROM raw_spans WHERE `timestamp` >= {}",
        sql_timestamp(t - ChronoDuration::minutes(10))
    );
    let scheduled = t.timestamp_millis().to_string();
    let result = db
        .flight_request()
        .with_flow_extensions(&[
            ("flow.return_region_seq", "true"),
            ("flow.scheduled_time_millis", scheduled.as_str()),
        ])
        .query_with_terminal_metrics(QueryRequest {
            query: Some(Query::Sql(sql.clone())),
        })
        .await
        .unwrap_or_else(|err| panic!("watermark discovery failed: {err:?}\n{sql}"));
    let metrics = result.metrics.clone();

    // Drain the stream: terminal metrics only become ready once it is consumed.
    let OutputData::Stream(mut stream) = result.output.data else {
        panic!("watermark discovery must return a stream");
    };
    while let Some(batch) = stream.next().await {
        batch.unwrap_or_else(|err| panic!("watermark discovery stream failed: {err:?}"));
    }

    let high = metrics
        .region_watermark_map()
        .expect("watermark discovery must report terminal metrics");
    assert!(
        !high.is_empty(),
        "watermark discovery must learn at least one source region"
    );
    high
}

/// Runs one incremental round.
///
/// The first round bootstraps: it runs the full sampling query so the sink and
/// the progress are both established, then records `C` and `Tprev`. Later rounds
/// only recompute the affected traces. Progress advances only after every step
/// succeeded, so a failed round is simply retried with the previous state.
async fn run_round(db: &Database, state: &mut PocState, t: DateTime<Utc>) -> RoundOutcome {
    let mut outcome = RoundOutcome::default();

    if !state.is_initialized() {
        // Learn the source region and its watermark with a plain read. The
        // write below cannot be used for this: an `INSERT ... SELECT` returns
        // affected rows rather than the scanned batches, so it carries no
        // terminal region watermark on this path.
        outcome.high = discover_watermark(db, t).await;

        let sql = format!("INSERT INTO {POC_SINK} {}", sampling_select(None));
        outcome.submitted_rows = api_affected_rows(db, &sql, t).await;

        state.after_seqs = outcome.high.clone();
        state.previous_eval_time = Some(t);
        return outcome;
    }

    let (new_traces, high) = query_new_traces(db, &state.after_seqs, t).await;
    outcome.new_trace_ids = new_traces;
    outcome.high = high;

    // Always run the time query, even when no row was written: a wait expiring
    // or a span leaving the lookback does not depend on new input.
    let previous = state
        .previous_eval_time
        .expect("an initialized round must have a previous evaluation time");
    let time_traces = query_time_traces(db, previous, t, &outcome.high).await;
    outcome.time_trace_ids = time_traces;

    let affected = outcome
        .new_trace_ids
        .union(&outcome.time_trace_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    outcome.affected_trace_ids = affected;

    outcome.submitted_rows =
        write_poc_sink(db, &outcome.affected_trace_ids, t, &outcome.high).await;

    state.after_seqs = outcome.high.clone();
    state.previous_eval_time = Some(t);
    outcome
}

async fn create_tables(db: &Database) {
    db.sql(
        r#"CREATE TABLE raw_spans (
            `timestamp` TIMESTAMP(9) NOT NULL,
            timestamp_end TIMESTAMP(9) NULL,
            duration_nano BIGINT UNSIGNED NULL,
            trace_id STRING NULL SKIPPING INDEX,
            span_id STRING NULL,
            parent_span_id STRING NULL,
            service_name STRING NULL,
            span_name STRING NULL,
            span_kind STRING NULL,
            span_status_code STRING NULL,
            span_status_message STRING NULL,
            `span_attributes.poc.case` STRING NULL,
            TIME INDEX (`timestamp`),
            PRIMARY KEY (trace_id, span_id)
        ) WITH ('append_mode' = 'true', 'preserve_row_sequence' = 'true', 'ttl' = '1h')"#,
    )
    .await
    .unwrap();

    for table in [POC_SINK, BASELINE_SINK] {
        db.sql(&format!(
            r#"CREATE TABLE {table} (
                `timestamp` TIMESTAMP(9) NOT NULL,
                timestamp_end TIMESTAMP(9) NULL,
                duration_nano BIGINT UNSIGNED NULL,
                trace_id STRING NULL SKIPPING INDEX,
                span_id STRING NULL,
                parent_span_id STRING NULL,
                service_name STRING NULL,
                span_name STRING NULL,
                span_kind STRING NULL,
                span_status_code STRING NULL,
                span_status_message STRING NULL,
                `span_attributes.poc.case` STRING NULL,
                sample_reason STRING NULL,
                TIME INDEX (`timestamp`),
                PRIMARY KEY (trace_id, span_id)
            ) WITH ('ttl' = '30d')"#
        ))
        .await
        .unwrap();
    }
}

/// Inserts spans given as `(trace_id, span_id, start offset, end offset, status)`.
async fn insert_spans(db: &Database, t: DateTime<Utc>, spans: &[(&str, &str, i64, i64, &str)]) {
    if spans.is_empty() {
        return;
    }
    let values = spans
        .iter()
        .map(|(trace, span, start, end, status)| {
            format!(
                "({}, {}, {}, {}, {}, 'svc', {}, 'SPAN_KIND_SERVER', {}, 'poc')",
                sql_timestamp(t + ChronoDuration::milliseconds(*start)),
                sql_timestamp(t + ChronoDuration::milliseconds(*end)),
                (*end - *start) * 1_000_000,
                sql_string(trace),
                sql_string(span),
                sql_string(span),
                sql_string(status),
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT INTO raw_spans (`timestamp`, timestamp_end, duration_nano, trace_id, span_id, \
         service_name, span_name, span_kind, span_status_code, `span_attributes.poc.case`) \
         VALUES {values}"
    );
    api_affected_rows(db, &sql, t).await;
}

/// The PoC decision must match the full-query decision after every round.
async fn assert_sinks_agree(db: &Database, label: &str) {
    let difference = sink_difference(db, POC_SINK, BASELINE_SINK).await;
    assert_eq!(
        difference, 0,
        "after {label}: the PoC sink must match the baseline sink exactly"
    );
}

fn poc_eval_time() -> DateTime<Utc> {
    // Placed inside the current minute and behind the wall clock, so the one
    // hour source TTL is harmless and the offsets below stay in the past.
    let now = Utc::now();
    now.with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .expect("truncating the current time to a minute must succeed")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_incremental_tail_sampling_rounds() {
    common_telemetry::init_default_ut_logging();
    let (_db, grpc_server) =
        setup_grpc_server(StorageType::File, "test_incremental_tail_sampling_rounds").await;
    let addr = grpc_server.bind_addr().unwrap().to_string();
    let db = Database::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        Client::with_urls(vec![addr]),
    );

    create_tables(&db).await;

    let t0 = poc_eval_time();
    let tick = |round: i64| t0 + ChronoDuration::seconds(round * 5);
    let mut state = PocState::default();

    // Round 0 bootstraps on an empty table: nothing to sample, but the region
    // and the first watermark must still be learned.
    run_baseline(&db, tick(0)).await;
    let bootstrap = run_round(&db, &mut state, tick(0)).await;
    assert!(
        state.is_initialized(),
        "the bootstrap round must discover the source region"
    );
    assert_eq!(
        bootstrap.submitted_rows, 0,
        "an empty source must sample nothing"
    );
    assert_sinks_agree(&db, "bootstrap").await;

    // A plain slow trace and a plain fast trace.
    insert_spans(
        &db,
        t0,
        &[
            ("plain_slow", "p1", -10_000, -10_000, "STATUS_CODE_OK"),
            ("plain_slow", "p2", -8_500, -8_500, "STATUS_CODE_OK"),
            ("plain_fast", "f1", -9_000, -9_000, "STATUS_CODE_OK"),
        ],
    )
    .await;
    run_baseline(&db, tick(1)).await;
    let round1 = run_round(&db, &mut state, tick(1)).await;
    assert_eq!(
        round1.new_trace_ids,
        BTreeSet::from(["plain_fast".to_string(), "plain_slow".to_string()]),
        "q1 must see exactly the traces written since the checkpoint"
    );
    // `plain_slow` starts 10s before the evaluation time, so the 30s wait has
    // not elapsed yet; nothing is sampled.
    assert!(
        round1.submitted_rows == 0,
        "a trace younger than the wait must not be sampled yet, got {} rows",
        round1.submitted_rows
    );
    assert_sinks_agree(&db, "round 1").await;

    // Late ERROR span for an already written normal trace. Its event time is
    // older than the previous evaluation time, so only the sequence delta can
    // find it.
    insert_spans(
        &db,
        t0,
        &[("plain_fast", "f2", -9_500, -9_500, "STATUS_CODE_ERROR")],
    )
    .await;
    run_baseline(&db, tick(2)).await;
    let round2 = run_round(&db, &mut state, tick(2)).await;
    assert!(
        round2.new_trace_ids.contains("plain_fast"),
        "a late span with an old event time must still be discovered: {:?}",
        round2.new_trace_ids
    );
    assert_sinks_agree(&db, "round 2").await;

    // Advance time past the 30s wait with no new writes at all. Both traces must
    // be released by the time query alone.
    let wait_rounds = [3, 4, 5, 6];
    for round in wait_rounds {
        run_baseline(&db, tick(round)).await;
        let outcome = run_round(&db, &mut state, tick(round)).await;
        assert!(
            outcome.new_trace_ids.is_empty(),
            "no rows were written, so the sequence delta must be empty: {:?}",
            outcome.new_trace_ids
        );
        assert_sinks_agree(&db, &format!("wait round {round}")).await;
    }

    let sampled = count_rows(&db, &format!("SELECT COUNT(*) FROM {POC_SINK}")).await;
    assert!(
        sampled > 0,
        "the wait expiring must release traces without any new write"
    );

    // Append a normal span to the already sampled slow trace, and an ERROR span
    // to it as well, so the reason has to be recomputed.
    insert_spans(
        &db,
        t0,
        &[
            ("plain_slow", "p3", -12_000, -12_000, "STATUS_CODE_OK"),
            ("plain_slow", "p4", -11_000, -11_000, "STATUS_CODE_ERROR"),
        ],
    )
    .await;
    run_baseline(&db, tick(7)).await;
    let round7 = run_round(&db, &mut state, tick(7)).await;
    assert!(
        round7.affected_trace_ids.contains("plain_slow"),
        "appending to a sampled trace must re-select it: {:?}",
        round7.affected_trace_ids
    );
    assert_sinks_agree(&db, "round 7").await;

    let reason = fetch_string_column(
        &db,
        &format!("SELECT DISTINCT sample_reason FROM {POC_SINK} WHERE trace_id = 'plain_slow'"),
        &[],
    )
    .await;
    assert_eq!(
        reason,
        vec!["error".to_string()],
        "the newly appended ERROR span must update the reason"
    );

    // Push time far enough that every span leaves the lookback window. The sinks
    // must keep agreeing and the long-term rows must stay in place.
    let before_expiry = count_rows(&db, &format!("SELECT COUNT(*) FROM {POC_SINK}")).await;
    for round in [200, 201] {
        run_baseline(&db, tick(round)).await;
        run_round(&db, &mut state, tick(round)).await;
        assert_sinks_agree(&db, &format!("expiry round {round}")).await;
    }
    let after_expiry = count_rows(&db, &format!("SELECT COUNT(*) FROM {POC_SINK}")).await;
    assert_eq!(
        before_expiry, after_expiry,
        "an expired trace must not remove long-term rows"
    );

    let _ = grpc_server.shutdown().await;
}
