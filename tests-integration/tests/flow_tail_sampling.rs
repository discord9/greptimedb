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
use std::sync::Arc;

use api::v1::QueryRequest;
use api::v1::query_request::Query;
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use client::{Client, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, Database, OutputData};
use datatypes::arrow::array::{Array, ArrayRef, StringArray};
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
    /// Wall time of the sequence-delta query, in milliseconds.
    q1_ms: u128,
    /// Wall time of the time-movement query, in milliseconds.
    q2_ms: u128,
    /// Wall time of the restricted recomputation and its writes, in milliseconds.
    q3_ms: u128,
    /// Wall time of the whole round, in milliseconds.
    total_ms: u128,
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
    let round_started = std::time::Instant::now();
    let mut outcome = RoundOutcome::default();

    if !state.is_initialized() {
        // Learn the source region and its watermark with a plain read. The
        // sampling query itself cannot provide it: it scans `raw_spans` twice
        // and a JOIN branch that reads no rows still counts as participating
        // without proving a watermark, which vetoes the other branch's proof.
        // A single-scan query proves its watermark even on an empty table.
        outcome.high = discover_watermark(db, t).await;

        let sql = format!("INSERT INTO {POC_SINK} {}", sampling_select(None));
        outcome.submitted_rows = api_affected_rows(db, &sql, t).await;

        state.after_seqs = outcome.high.clone();
        state.previous_eval_time = Some(t);
        outcome.total_ms = round_started.elapsed().as_millis();
        return outcome;
    }

    let q1_started = std::time::Instant::now();
    let (new_traces, high) = query_new_traces(db, &state.after_seqs, t).await;
    outcome.q1_ms = q1_started.elapsed().as_millis();
    outcome.new_trace_ids = new_traces;
    outcome.high = high;

    // Always run the time query, even when no row was written: a wait expiring
    // or a span leaving the lookback does not depend on new input.
    let previous = state
        .previous_eval_time
        .expect("an initialized round must have a previous evaluation time");
    let q2_started = std::time::Instant::now();
    let time_traces = query_time_traces(db, previous, t, &outcome.high).await;
    outcome.q2_ms = q2_started.elapsed().as_millis();
    outcome.time_trace_ids = time_traces;

    let affected = outcome
        .new_trace_ids
        .union(&outcome.time_trace_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    outcome.affected_trace_ids = affected;

    let q3_started = std::time::Instant::now();
    outcome.submitted_rows =
        write_poc_sink(db, &outcome.affected_trace_ids, t, &outcome.high).await;
    outcome.q3_ms = q3_started.elapsed().as_millis();
    outcome.total_ms = round_started.elapsed().as_millis();

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

/// Inserts many spans, splitting the statement so a large dataset does not build
/// one oversized request.
async fn insert_spans_chunked(db: &Database, t: DateTime<Utc>, spans: &[BenchSpan]) -> usize {
    // A large chunk keeps the insert count low: each statement rebuilds memtable
    // state, and with 60k spans a 500-row chunk means 120 expensive round trips.
    const CHUNK: usize = 5_000;
    let mut written = 0;
    for chunk in spans.chunks(CHUNK) {
        let values = chunk
            .iter()
            .map(|span| {
                format!(
                    "({}, {}, {}, {}, {}, 'svc', {}, 'SPAN_KIND_SERVER', {}, 'poc')",
                    sql_timestamp(t + ChronoDuration::milliseconds(span.start_ms)),
                    sql_timestamp(t + ChronoDuration::milliseconds(span.end_ms)),
                    (span.end_ms - span.start_ms) * 1_000_000,
                    sql_string(&span.trace),
                    sql_string(&span.span),
                    sql_string(&span.span),
                    sql_string(span.status),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO raw_spans (`timestamp`, timestamp_end, duration_nano, trace_id, span_id, \
             service_name, span_name, span_kind, span_status_code, `span_attributes.poc.case`) \
             VALUES {values}"
        );
        written += api_affected_rows(db, &sql, t).await;
    }
    written
}

/// The PoC decision must match the full-query decision after every round.
async fn assert_sinks_agree(db: &Database, label: &str) {
    let difference = sink_difference(db, POC_SINK, BASELINE_SINK).await;
    if difference == 0 {
        return;
    }
    // Report the offending rows, not just the count: a bare number does not say
    // which side is wrong or which column drifted.
    let columns = format!("{SPAN_COLUMNS}, sample_reason");
    let mut report = format!("after {label}: sinks differ by {difference} rows");
    for (side, sql) in [
        (
            "only in poc",
            format!(
                "SELECT {columns} FROM {POC_SINK} EXCEPT SELECT {columns} FROM {BASELINE_SINK}"
            ),
        ),
        (
            "only in baseline",
            format!(
                "SELECT {columns} FROM {BASELINE_SINK} EXCEPT SELECT {columns} FROM {POC_SINK}"
            ),
        ),
    ] {
        let rows = fetch_row_set(db, &sql, &[]).await;
        report.push_str(&format!("\n{side} ({} rows):", rows.len()));
        for row in rows.iter().take(8) {
            report.push_str(&format!("\n  {row:?}"));
        }
    }
    panic!("{report}");
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

/// Fetches every row as a vector of rendered cells.
///
/// Cells are rendered positionally so whole rows can be compared as sets. The
/// fallback renders the sliced array, which is deterministic for a given value,
/// so equality comparisons stay meaningful for types without an explicit arm.
async fn fetch_row_set(
    db: &Database,
    sql: &str,
    extensions: &[(&str, &str)],
) -> BTreeSet<Vec<String>> {
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

    let mut rows = BTreeSet::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for column_index in 0..batch.num_columns() {
                let column = batch.column(column_index);
                cells.push(if column.is_null(row) {
                    "NULL".to_string()
                } else {
                    render_cell(column, row)
                });
            }
            rows.insert(cells);
        }
    }
    rows
}

fn render_cell(column: &ArrayRef, row: usize) -> String {
    use datatypes::arrow::array::{
        Int64Array, LargeStringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, UInt64Array,
    };
    let any = column.as_any();
    if let Some(array) = any.downcast_ref::<StringArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = any.downcast_ref::<LargeStringArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = any.downcast_ref::<UInt64Array>() {
        return array.value(row).to_string();
    }
    if let Some(array) = any.downcast_ref::<Int64Array>() {
        return array.value(row).to_string();
    }
    if let Some(array) = any.downcast_ref::<TimestampMillisecondArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = any.downcast_ref::<TimestampMicrosecondArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = any.downcast_ref::<TimestampNanosecondArray>() {
        return array.value(row).to_string();
    }
    format!("{:?}", column.slice(row, 1))
}

/// Bootstraps an empty source and returns the starting time and state.
async fn bootstrap(db: &Database, name: &str) -> (DateTime<Utc>, PocState) {
    let _ = name;
    create_tables(db).await;
    let t0 = poc_eval_time();
    let mut state = PocState::default();
    run_baseline(db, t0).await;
    run_round(db, &mut state, t0).await;
    assert!(state.is_initialized(), "bootstrap must discover the source");
    (t0, state)
}

async fn test_db(name: &str) -> (Database, Arc<servers::grpc::GrpcServer>) {
    common_telemetry::init_default_ut_logging();
    let (_db, grpc_server) = setup_grpc_server(StorageType::File, name).await;
    let addr = grpc_server.bind_addr().unwrap().to_string();
    let db = Database::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        Client::with_urls(vec![addr]),
    );
    (db, grpc_server)
}

/// A trace keeps its ERROR reason while the ERROR span is inside the lookback
/// window, and must drift to `slow` once that span leaves it while a normal span
/// stays.
///
/// This is the one case that the second interval of the time query exists for: the
/// span that leaves the window was written long before the checkpoint, so only an
/// explicit lookback comparison can find the trace. Without it the PoC sink would
/// keep reporting `error` forever while the full query reports `slow`.
#[tokio::test(flavor = "multi_thread")]
async fn test_reason_drift_when_error_leaves_window() {
    let (db, grpc_server) = test_db("test_reason_drift_when_error_leaves_window").await;
    let (t0, mut state) = bootstrap(&db, "drift").await;

    // The ERROR span leaves the lookback at round 3. With `tick(n) = t0 + 5n` the
    // lookback lower bound is `t0 + 5n - 600s`, so the interval that just left the
    // window is `[t0 + 5n - 600s, t0 + 5(n-1) - 600s)`. A span at `t0 - 587s` falls
    // in that interval only when it is round 3: `[t0 - 590s, t0 - 585s)`.
    insert_spans(
        &db,
        t0,
        &[
            ("drift", "d_err", -587_000, -587_000, "STATUS_CODE_ERROR"),
            ("drift", "d_ok", -60_000, -58_500, "STATUS_CODE_OK"),
        ],
    )
    .await;

    run_baseline(&db, t0 + ChronoDuration::seconds(5)).await;
    run_round(&db, &mut state, t0 + ChronoDuration::seconds(5)).await;
    assert_sinks_agree(&db, "round 1").await;

    // Round 2 is quiet on purpose: the lookback lower bound is still `t0 - 590s`
    // and the ERROR span sits at `t0 - 587s`, so nothing has moved for this trace
    // yet. Recomputing it here would be wasted work.
    run_baseline(&db, t0 + ChronoDuration::seconds(10)).await;
    let round2 = run_round(&db, &mut state, t0 + ChronoDuration::seconds(10)).await;
    assert!(
        round2.affected_trace_ids.is_empty(),
        "nothing changed for this trace at round 2, so it must not be recomputed: {:?}",
        round2.affected_trace_ids
    );
    assert_sinks_agree(&db, "round 2").await;
    let reason_at_round2 = fetch_string_column(
        &db,
        &format!("SELECT DISTINCT sample_reason FROM {POC_SINK} WHERE trace_id = 'drift'"),
        &[],
    )
    .await;
    assert_eq!(
        reason_at_round2,
        vec!["error".to_string()],
        "the ERROR span is still inside the lookback, so the reason must stay `error`"
    );

    // Round 3 is the drift: no writes at all, only time moves.
    run_baseline(&db, t0 + ChronoDuration::seconds(15)).await;
    let round3 = run_round(&db, &mut state, t0 + ChronoDuration::seconds(15)).await;
    assert!(
        round3.new_trace_ids.is_empty(),
        "no rows were written, so the sequence delta must be empty: {:?}",
        round3.new_trace_ids
    );
    assert!(
        round3.time_trace_ids.contains("drift"),
        "the trace whose ERROR span left the window must be re-selected by the \
         time query: {:?}",
        round3.time_trace_ids
    );
    assert_sinks_agree(&db, "round 3").await;

    // The span that is still inside the window must now report `slow`, while the
    // expired ERROR row stays in the long-term table with its old reason.
    let normal_reason = fetch_string_column(
        &db,
        &format!(
            "SELECT sample_reason FROM {POC_SINK} WHERE trace_id = 'drift' AND span_id = 'd_ok'"
        ),
        &[],
    )
    .await;
    assert_eq!(
        normal_reason,
        vec!["slow".to_string()],
        "after the ERROR span leaves the window the remaining span must drift to `slow`"
    );
    let expired_reason = fetch_string_column(
        &db,
        &format!(
            "SELECT sample_reason FROM {POC_SINK} WHERE trace_id = 'drift' AND span_id = 'd_err'"
        ),
        &[],
    )
    .await;
    assert_eq!(
        expired_reason,
        vec!["error".to_string()],
        "an expired span must keep the reason it was written with"
    );

    let _ = grpc_server.shutdown().await;
}

/// A round whose new rows all fall outside the lookback still advances progress.
///
/// Such rows can never be discovered by the sequence query, so the checkpoint has
/// to move past them. If it did not, every round would rescan the same sequence
/// range forever, which looks like normal operation but keeps redoing work.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_result_still_advances_progress() {
    let (db, grpc_server) = test_db("test_empty_result_still_advances_progress").await;
    let (t0, mut state) = bootstrap(&db, "empty-advance").await;

    let before = state.after_seqs.clone();

    // Twenty minutes back: far outside the ten minute lookback.
    insert_spans(
        &db,
        t0,
        &[("stale", "s1", -1_200_000, -1_200_000, "STATUS_CODE_ERROR")],
    )
    .await;

    run_baseline(&db, t0 + ChronoDuration::seconds(5)).await;
    let round = run_round(&db, &mut state, t0 + ChronoDuration::seconds(5)).await;

    assert!(
        round.new_trace_ids.is_empty(),
        "a row outside the lookback must not be reported as an affected trace"
    );
    assert!(
        before
            .iter()
            .all(|(region, seq)| state.after_seqs[region] > *seq),
        "progress must advance even when the query returns no trace: before={before:?} after={:?}",
        state.after_seqs
    );

    // The next round must not rediscover anything: the checkpoint is past those
    // rows, and they are unreachable by the sequence query by construction.
    run_baseline(&db, t0 + ChronoDuration::seconds(10)).await;
    let next = run_round(&db, &mut state, t0 + ChronoDuration::seconds(10)).await;
    assert!(
        next.new_trace_ids.is_empty(),
        "rows the checkpoint already passed must not be rediscovered: {:?}",
        next.new_trace_ids
    );
    assert_sinks_agree(&db, "after advancement").await;

    let _ = grpc_server.shutdown().await;
}

/// Historical spans stay readable through the exact range after a flush.
///
/// The decision for a trace needs spans written in earlier rounds, which by then
/// live in SSTs. Both the time query and the recomputation read `(0, H]` rather
/// than `(C, H]` for exactly this reason, so this checks that a flush does not
/// hide the rows that decide the trace.
#[tokio::test(flavor = "multi_thread")]
async fn test_historical_spans_survive_flush() {
    let (db, grpc_server) = test_db("test_historical_spans_survive_flush").await;
    let (t0, mut state) = bootstrap(&db, "flush").await;

    // A slow trace whose deciding spans are written, flushed, and only sampled
    // once the wait elapses.
    insert_spans(
        &db,
        t0,
        &[
            // Ages are `5n + 20` seconds at round n: 25s at round 1 (inside the
            // wait) and exactly 30s at round 2 (released, since the rule is `<=`).
            ("flushed", "f1", -20_000, -20_000, "STATUS_CODE_OK"),
            ("flushed", "f2", -19_000, -18_500, "STATUS_CODE_OK"),
        ],
    )
    .await;
    // `admin flush_table` reports a result set rather than affected rows.
    db.sql("admin flush_table('raw_spans')").await.unwrap();

    run_baseline(&db, t0 + ChronoDuration::seconds(5)).await;
    let early = run_round(&db, &mut state, t0 + ChronoDuration::seconds(5)).await;
    assert_eq!(
        early.new_trace_ids,
        BTreeSet::from(["flushed".to_string()]),
        "the sequence query must find the flushed trace"
    );
    assert_eq!(
        early.submitted_rows, 0,
        "the trace is younger than the wait, so nothing may be sampled yet"
    );
    assert_sinks_agree(&db, "before the wait elapses").await;

    // Advance past the wait with no writes. Both deciding spans are now in an
    // SST, and the round must still see them.
    for round in [2, 3] {
        run_baseline(&db, t0 + ChronoDuration::seconds(round * 5)).await;
        let outcome = run_round(&db, &mut state, t0 + ChronoDuration::seconds(round * 5)).await;
        assert!(
            outcome.new_trace_ids.is_empty(),
            "no writes, so the sequence delta must stay empty"
        );
        assert_sinks_agree(&db, &format!("round {round}")).await;
    }

    let sampled = fetch_string_column(
        &db,
        &format!("SELECT sample_reason FROM {POC_SINK} WHERE trace_id = 'flushed'"),
        &[],
    )
    .await;
    assert_eq!(
        sampled.iter().filter(|reason| *reason == "slow").count(),
        2,
        "both flushed spans must be sampled once the wait elapses, got {sampled:?}"
    );

    let _ = grpc_server.shutdown().await;
}

/// The trace-restricted query must return exactly the full query's rows for those
/// traces, and nothing more.
///
/// This is the invariant behind recomputing only affected traces: restricting the
/// sampling SQL by trace id may not change any selected row, drop a span of a
/// selected trace, or pull in a trace that was not affected. Comparing the two
/// forms directly also catches a restriction applied to only one of the two
/// source scans.
#[tokio::test(flavor = "multi_thread")]
async fn test_local_result_equals_full_scoped_to_affected() {
    let (db, grpc_server) = test_db("test_local_result_equals_full_scoped_to_affected").await;
    let (t0, _state) = bootstrap(&db, "scope").await;

    insert_spans(
        &db,
        t0,
        &[
            ("kept_error", "k1", -200_000, -200_000, "STATUS_CODE_OK"),
            ("kept_error", "k2", -199_000, -199_000, "STATUS_CODE_ERROR"),
            ("kept_slow", "s1", -300_000, -300_000, "STATUS_CODE_OK"),
            ("kept_slow", "s2", -299_000, -298_500, "STATUS_CODE_OK"),
            ("dropped", "x1", -120_000, -120_000, "STATUS_CODE_OK"),
            ("young", "y1", -10_000, -10_000, "STATUS_CODE_ERROR"),
        ],
    )
    .await;

    let t = t0 + ChronoDuration::seconds(60);
    let scheduled = t.timestamp_millis().to_string();
    let extensions = [("flow.scheduled_time_millis", scheduled.as_str())];

    let full = fetch_row_set(&db, &sampling_select(None), &extensions).await;
    let affected = BTreeSet::from(["kept_error".to_string(), "kept_slow".to_string()]);
    let local = fetch_row_set(&db, &sampling_select(Some(&affected)), &extensions).await;

    let expected = full
        .iter()
        .filter(|row| affected.contains(&row[3]))
        .cloned()
        .collect::<BTreeSet<_>>();

    assert!(
        !expected.is_empty(),
        "the fixture must select at least one trace, otherwise this proves nothing"
    );
    assert_eq!(
        local, expected,
        "the trace-restricted query must equal the full query's rows for those traces"
    );

    // The restricted result must not contain a trace that was not requested.
    for row in &local {
        assert!(
            affected.contains(&row[3]),
            "the restricted query returned an unrequested trace: {row:?}"
        );
    }

    let _ = grpc_server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Cost comparison (plan section 8)
// ---------------------------------------------------------------------------

/// One generated span, with offsets relative to the measurement start time.
struct BenchSpan {
    trace: String,
    span: String,
    start_ms: i64,
    end_ms: i64,
    status: &'static str,
}

/// Deterministic pseudo-random source, so a rerun produces the same dataset
/// without depending on an external random number generator.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
}

/// Builds the background dataset: `trace_count` traces with three spans each,
/// one percent of them carrying an ERROR span and a disjoint one percent slow.
///
/// Every span sits between eight and two minutes before the measurement start,
/// so they are all past the thirty second wait and none of them crosses the ten
/// minute lookback boundary during a minute of ticks. That is what makes the
/// static scenario produce an empty delta.
fn gen_bench_dataset(trace_count: usize) -> Vec<BenchSpan> {
    const EARLIEST_MS: i64 = -480_000;
    const LATEST_MS: i64 = -120_000;
    let span_ms = LATEST_MS - EARLIEST_MS;
    let mut rng = Lcg(0x5EED_1234_ABCD_0001);
    let mut spans = Vec::with_capacity(trace_count * 3);

    for trace_index in 0..trace_count {
        // Spread the bases so they are not all aligned to the same millisecond.
        let jitter = (rng.next() % 1000) as i64;
        let base = EARLIEST_MS + (trace_index as i64 * span_ms / trace_count as i64) + jitter;
        let trace = format!("bench_trace_{trace_index:06}");
        let is_error = trace_index % 100 == 0;
        let is_slow = trace_index % 100 == 50;

        for span_index in 0..3i64 {
            let start = base + span_index * 100;
            let mut end = start;
            let mut status = "STATUS_CODE_OK";
            if is_error && span_index == 1 {
                status = "STATUS_CODE_ERROR";
            }
            if is_slow && span_index == 2 {
                // 1.5s extent, above the one second threshold.
                end = start + 1_500;
            }
            spans.push(BenchSpan {
                trace: trace.clone(),
                span: format!("s{span_index}"),
                start_ms: start,
                end_ms: end,
                status,
            });
        }
    }
    spans
}

/// Spans appended by one scenario tick: the traces chosen for this tick append a
/// single normal span, and every fifth of those appends an ERROR span so the
/// reason of an already sampled trace has to be recomputed.
fn gen_tick_appends(scenario: &str, trace_count: usize, tick: usize) -> Vec<BenchSpan> {
    let selector: usize = match scenario {
        // One percent of the traces change per tick.
        "sparse" => 100,
        // Half of the traces change per tick: the degraded case.
        "active" => 2,
        _ => return Vec::new(),
    };
    let mut spans = Vec::new();
    for trace_index in 0..trace_count {
        if (trace_index + tick) % selector != 0 {
            continue;
        }
        let trace = format!("bench_trace_{trace_index:06}");
        // Sixty seconds old, so the appended span is immediately eligible and the
        // recomputation has a real chance to change the decision.
        let start = tick as i64 * 5_000 - 60_000;
        let status = if (trace_index + tick) % (selector * 5) == 0 {
            "STATUS_CODE_ERROR"
        } else {
            "STATUS_CODE_OK"
        };
        spans.push(BenchSpan {
            trace: trace.clone(),
            span: format!("t{tick}"),
            start_ms: start,
            end_ms: start,
            status,
        });
    }
    spans
}

async fn reset_tables(db: &Database) {
    for table in ["raw_spans", POC_SINK, BASELINE_SINK] {
        let _ = db.sql(&format!("DROP TABLE IF EXISTS {table}")).await;
    }
    create_tables(db).await;
}

async fn sink_rows(db: &Database, table: &str) -> BTreeSet<Vec<String>> {
    fetch_row_set(
        db,
        &format!("SELECT {SPAN_COLUMNS}, sample_reason FROM {table}"),
        &[],
    )
    .await
}

/// Compares the two strategies on the same generated input and writes one CSV row
/// per measured round.
///
/// The per-round timings come from the round helper itself, so the extra work of
/// keeping the full-query sink in sync for the correctness check is not counted
/// against the incremental path.
#[tokio::test(flavor = "multi_thread")]
async fn test_cost_comparison() {
    let trace_count: usize = std::env::var("POC_TRACE_COUNT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);
    let csv_path = std::env::var("POC_RESULTS_CSV")
        .unwrap_or_else(|_| "/tmp/flow-tail-sampling-results.csv".to_string());

    let (db, grpc_server) = test_db("test_cost_comparison").await;
    let dataset = gen_bench_dataset(trace_count);
    println!("dataset: {trace_count} traces, {} spans", dataset.len());

    let mut csv = vec![
        "mode,scenario,tick,eval_time,new_trace_ids,time_trace_ids,\
affected_trace_ids,q1_ms,q2_ms,q3_ms,total_ms,sink_submitted_rows,sink_visible_rows"
            .to_string(),
    ];

    let t0 = poc_eval_time();
    let tick_time = |tick: usize| t0 + ChronoDuration::seconds(tick as i64 * 5);

    let only: Vec<String> = std::env::var("POC_SCENARIOS")
        .unwrap_or_else(|_| "static,sparse,active".to_string())
        .split(',')
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();

    for scenario in ["static", "sparse", "active"]
        .into_iter()
        .filter(|name| only.iter().any(|wanted| wanted == name))
    {
        let ticks: usize = match std::env::var("POC_TICKS").ok().and_then(|v| v.parse().ok()) {
            Some(explicit) => explicit,
            None if scenario == "static" => 12,
            None => 6,
        };

        // Incremental mode: measure the rounds, and keep the baseline sink in
        // sync so the final contents can be compared.
        reset_tables(&db).await;
        let inserted = insert_spans_chunked(&db, t0, &dataset).await;
        assert_eq!(inserted, dataset.len());
        // Move the dataset into SSTs. The `trace_id` skipping index only prunes
        // row groups in SSTs, so without this flush the trace-restricted query
        // would have to scan every row of the memtable.
        db.sql("admin flush_table('raw_spans')").await.unwrap();
        // Capture one plan for the full query and one for a trace-restricted
        // query, so the report can state where the trace predicate lands
        // instead of guessing. GreptimeDB exposes no scan-row counter, so the
        // plan is the only available evidence.
        if scenario == "static" {
            let probe: BTreeSet<String> = dataset
                .iter()
                .take(200)
                .map(|span| span.trace.clone())
                .collect();
            for (label, sql) in [
                ("full", sampling_select(None)),
                ("restricted", sampling_select(Some(&probe))),
            ] {
                let plan = format!("EXPLAIN {sql}");
                let lines = fetch_string_column(&db, &plan, &[]).await;
                println!("--- EXPLAIN {label} ---");
                for line in lines {
                    println!("{line}");
                }
            }
        }
        let mut state = PocState::default();
        for tick in 0..=ticks {
            let now = tick_time(tick);
            let appends = gen_tick_appends(scenario, trace_count, tick);
            if !appends.is_empty() {
                insert_spans_chunked(&db, t0, &appends).await;
            }
            run_baseline(&db, now).await;
            let outcome = run_round(&db, &mut state, now).await;

            if scenario == "static" && tick > 0 {
                assert!(
                    outcome.affected_trace_ids.is_empty(),
                    "static scenario must produce an empty delta at tick {tick}: {:?}",
                    outcome.affected_trace_ids
                );
                assert_eq!(
                    outcome.submitted_rows, 0,
                    "static scenario must submit nothing at tick {tick}"
                );
            }

            let visible = count_rows(&db, &format!("SELECT COUNT(*) FROM {POC_SINK}")).await;
            // Thirteen fields, in header order; the full-query-only columns stay
            // empty here and the per-query timings stay empty there.
            csv.push(
                [
                    "poc".to_string(),
                    scenario.to_string(),
                    tick.to_string(),
                    now.timestamp_millis().to_string(),
                    outcome.new_trace_ids.len().to_string(),
                    outcome.time_trace_ids.len().to_string(),
                    outcome.affected_trace_ids.len().to_string(),
                    outcome.q1_ms.to_string(),
                    outcome.q2_ms.to_string(),
                    outcome.q3_ms.to_string(),
                    outcome.total_ms.to_string(),
                    outcome.submitted_rows.to_string(),
                    visible.to_string(),
                ]
                .join(","),
            );
        }
        let poc_rows = sink_rows(&db, POC_SINK).await;
        let reference = sink_rows(&db, BASELINE_SINK).await;
        assert_eq!(
            poc_rows, reference,
            "scenario {scenario}: the incremental sink must match the full-query sink"
        );

        // Full-query mode on the same input, so the comparison uses one data set.
        reset_tables(&db).await;
        insert_spans_chunked(&db, t0, &dataset).await;
        db.sql("admin flush_table('raw_spans')").await.unwrap();
        for tick in 0..=ticks {
            let now = tick_time(tick);
            let appends = gen_tick_appends(scenario, trace_count, tick);
            if !appends.is_empty() {
                insert_spans_chunked(&db, t0, &appends).await;
            }
            let started = std::time::Instant::now();
            let submitted = run_baseline(&db, now).await;
            let elapsed = started.elapsed().as_millis();
            let visible = count_rows(&db, &format!("SELECT COUNT(*) FROM {BASELINE_SINK}")).await;
            csv.push(
                [
                    "baseline".to_string(),
                    scenario.to_string(),
                    tick.to_string(),
                    now.timestamp_millis().to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    elapsed.to_string(),
                    submitted.to_string(),
                    visible.to_string(),
                ]
                .join(","),
            );
        }
    }

    std::fs::write(&csv_path, csv.join("\n") + "\n").expect("writing the results csv");
    println!("results written to {csv_path} ({} rows)", csv.len() - 1);

    let _ = grpc_server.shutdown().await;
}
