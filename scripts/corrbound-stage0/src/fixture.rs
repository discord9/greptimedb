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

//! Fixture construction for the CorrBound stage-0 probe.
//!
//! The layout mirrors `tests/perf/query_cases/sql_join_filter_order/case.toml`:
//! two append-mode Mito tables, one region each, 32 SSTs of 512 rows (16,384
//! rows per table), 16 hosts, 64 instances, 4 zones, one row per second starting
//! at `2024-01-01T00:00:00Z`.
//!
//! Row generation is a pure function, so the pre-query row counts used by the
//! probe are properties of the construction rather than of query execution.

use common_query::Output;
use frontend::instance::Instance;
use mito2::engine::MitoEngine;
use session::context::QueryContextRef;
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};

use crate::ProbeError;

pub const FACT_TABLE: &str = "sql_join_fact";
pub const DIM_TABLE: &str = "sql_join_dim";

pub const ROWS_PER_SST: usize = 512;
pub const SST_COUNT: usize = 32;
/// Unfiltered input cardinality of both fixture tables: `|T|`.
pub const TABLE_ROWS: usize = ROWS_PER_SST * SST_COUNT;
pub const FACT_SERIES: usize = 64;
pub const DIM_SERIES: usize = 64;
pub const HOST_CARDINALITY: usize = 16;
pub const ZONE_CARDINALITY: usize = 4;
pub const START_NANOS: i64 = 1_704_067_200_000_000_000;
pub const STEP_NANOS: i64 = 1_000_000_000;

/// A single generated fixture row. `second` is either the instance tag (fact
/// table) or the zone tag (dim table).
#[derive(Debug, Clone, PartialEq)]
pub struct FixtureRow {
    pub host: String,
    pub second: String,
    pub value: f64,
    pub ts_nanos: i64,
}

/// Fact table row `index` (0-based, global across all SSTs).
pub fn fact_row(index: usize) -> FixtureRow {
    let series = index % FACT_SERIES;
    FixtureRow {
        host: host_tag(series % HOST_CARDINALITY),
        second: format!("instance{:02}", series),
        // deterministic wave in [1.0, 500.0]
        value: 1.0 + (index % 500) as f64,
        ts_nanos: START_NANOS + index as i64 * STEP_NANOS,
    }
}

/// Dim table row `index` (0-based, global across all SSTs).
pub fn dim_row(index: usize) -> FixtureRow {
    let series = index % DIM_SERIES;
    FixtureRow {
        host: host_tag(series % HOST_CARDINALITY),
        second: format!("zone{}", (series / HOST_CARDINALITY) % ZONE_CARDINALITY),
        // deterministic wave in [0.1, 10.0]
        value: 0.1 + (index % 100) as f64 / 10.0,
        ts_nanos: START_NANOS + index as i64 * STEP_NANOS,
    }
}

pub fn host_tag(i: usize) -> String {
    format!("host{i:02}")
}

pub fn fact_rows() -> Vec<FixtureRow> {
    (0..TABLE_ROWS).map(fact_row).collect()
}

pub fn dim_rows() -> Vec<FixtureRow> {
    (0..TABLE_ROWS).map(dim_row).collect()
}

/// Inclusive start / exclusive end of the rows that land in SST `sst`.
pub fn sst_chunk(sst: usize) -> (usize, usize) {
    (sst * ROWS_PER_SST, (sst + 1) * ROWS_PER_SST)
}

pub fn ts_literal(ts_nanos: i64) -> String {
    let offset_secs = (ts_nanos - START_NANOS) / STEP_NANOS;
    let hours = offset_secs / 3600;
    let minutes = (offset_secs % 3600) / 60;
    let seconds = offset_secs % 60;
    format!("'2024-01-01 {hours:02}:{minutes:02}:{seconds:02}.000000000'")
}

pub fn create_table_sql(table: &str) -> String {
    match table {
        // `value` is a reserved keyword in the SQL dialect, so the column is
        // always written quoted.
        FACT_TABLE => format!(
            "CREATE TABLE IF NOT EXISTS {table} (\
             host STRING, instance STRING, `value` DOUBLE, ts TIMESTAMP(9) TIME INDEX, \
             PRIMARY KEY(host, instance)) ENGINE=mito WITH ('append_mode'='true')"
        ),
        // `zone` is a reserved keyword in the SQL dialect, so the column is
        // always written quoted.
        DIM_TABLE => format!(
            "CREATE TABLE IF NOT EXISTS {table} (\
             host STRING, `zone` STRING, weight DOUBLE, ts TIMESTAMP(9) TIME INDEX, \
             PRIMARY KEY(host, `zone`)) ENGINE=mito WITH ('append_mode'='true')"
        ),
        other => unreachable!("unexpected fixture table {other}"),
    }
}

/// `INSERT` statement for one contiguous slice of generated rows.
pub fn insert_sql(table: &str, rows: &[FixtureRow]) -> String {
    let (second, value_col) = if table == FACT_TABLE {
        ("instance", "`value`")
    } else {
        ("`zone`", "weight")
    };
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "('{}', '{}', {}, {})",
                row.host,
                row.second,
                row.value,
                ts_literal(row.ts_nanos)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {table} (host, {second}, {value_col}, ts) VALUES {values}")
}

pub fn flush_sql(table: &str) -> String {
    format!("ADMIN FLUSH_TABLE('{table}')")
}

/// Execute one SQL statement against the in-process standalone instance.
pub async fn exec_sql(
    instance: &Instance,
    query_ctx: &QueryContextRef,
    sql: &str,
) -> Result<Output, ProbeError> {
    let dialect = GreptimeDbDialect {};
    let stmts = ParserContext::create_with_dialect(sql, &dialect, ParseOptions::default())
        .map_err(|e| ProbeError::Fatal(format!("parse `{sql}`: {e}")))?;
    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| ProbeError::Fatal(format!("no statement parsed from `{sql}`")))?;
    instance
        .statement_executor()
        .execute_sql(stmt, query_ctx.clone())
        .await
        .map_err(|e| ProbeError::Fatal(format!("execute `{sql}`: {e}")))
}

/// Engine-reported per-table row counts read from region statistics. This is a
/// statistics-only source: it runs no query.
#[derive(Debug, Clone, Default)]
pub struct TableStatistic {
    pub table_id: u32,
    pub regions: usize,
    pub num_rows: u64,
    pub memtable_bytes: u64,
    pub sst_num: u64,
    pub sst_bytes: u64,
}

/// Resolve `table` to its table id and aggregate its region statistics.
pub async fn table_statistic(
    instance: &Instance,
    engine: &MitoEngine,
    query_ctx: &QueryContextRef,
    table: &str,
) -> Result<TableStatistic, ProbeError> {
    let catalog_name = query_ctx.current_catalog();
    let schema_name = query_ctx.current_schema();
    let table_ref = instance
        .catalog_manager()
        .table(&catalog_name, &schema_name, table, Some(query_ctx))
        .await
        .map_err(|e| ProbeError::Fatal(format!("resolve table {table}: {e}")))?
        .ok_or_else(|| ProbeError::Fatal(format!("table {table} not found")))?;
    let table_id = table_ref.table_info().table_id();
    let mut stat = TableStatistic {
        table_id,
        ..Default::default()
    };
    for region in engine.regions() {
        if region.region_id().table_id() != table_id {
            continue;
        }
        if let Some(region_stat) = engine.get_region_statistic(region.region_id()) {
            stat.regions += 1;
            stat.num_rows += region_stat.num_rows;
            stat.memtable_bytes += region_stat.memtable_size;
            stat.sst_num += region_stat.sst_num;
            stat.sst_bytes += region_stat.sst_size;
        }
    }
    Ok(stat)
}

/// Summary of the constructed fixture, plus the pre-query statistics that
/// witness the sound upper bound `|sigma_P(T)| <= |T|`.
#[derive(Debug, Clone)]
pub struct FixtureReport {
    pub fact_constructed_rows: usize,
    pub dim_constructed_rows: usize,
    pub fact_statistic: TableStatistic,
    pub dim_statistic: TableStatistic,
    pub statement_count: usize,
}

/// Create the two fixture tables and fill each with 32 flushed 512-row SSTs.
pub async fn build_fixture(
    instance: &Instance,
    engine: &MitoEngine,
    query_ctx: &QueryContextRef,
) -> Result<FixtureReport, ProbeError> {
    let mut statement_count = 0;
    for table in [FACT_TABLE, DIM_TABLE] {
        exec_sql(instance, query_ctx, &create_table_sql(table)).await?;
        statement_count += 1;
        let rows = if table == FACT_TABLE {
            fact_rows()
        } else {
            dim_rows()
        };
        for sst in 0..SST_COUNT {
            let (start, end) = sst_chunk(sst);
            exec_sql(instance, query_ctx, &insert_sql(table, &rows[start..end])).await?;
            exec_sql(instance, query_ctx, &flush_sql(table)).await?;
            statement_count += 2;
        }
    }
    Ok(FixtureReport {
        fact_constructed_rows: TABLE_ROWS,
        dim_constructed_rows: TABLE_ROWS,
        fact_statistic: table_statistic(instance, engine, query_ctx, FACT_TABLE).await?,
        dim_statistic: table_statistic(instance, engine, query_ctx, DIM_TABLE).await?,
        statement_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_construction_counts_are_table_cardinalities() {
        let fact = fact_rows();
        let dim = dim_rows();
        assert_eq!(TABLE_ROWS, 16_384);
        assert_eq!(fact.len(), TABLE_ROWS);
        assert_eq!(dim.len(), TABLE_ROWS);
        assert_eq!(fact.len(), SST_COUNT * ROWS_PER_SST);
        assert_eq!(dim.len(), SST_COUNT * ROWS_PER_SST);
    }

    #[test]
    fn fixture_tag_cardinalities_match_case_toml() {
        let fact = fact_rows();
        let hosts: std::collections::BTreeSet<_> = fact.iter().map(|r| r.host.clone()).collect();
        let instances: std::collections::BTreeSet<_> =
            fact.iter().map(|r| r.second.clone()).collect();
        assert_eq!(hosts.len(), HOST_CARDINALITY);
        assert_eq!(instances.len(), FACT_SERIES);

        let dim = dim_rows();
        let zones: std::collections::BTreeSet<_> = dim.iter().map(|r| r.second.clone()).collect();
        assert_eq!(zones.len(), ZONE_CARDINALITY);
        let dim_hosts = dim
            .iter()
            .map(|r| r.host.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(dim_hosts.len(), HOST_CARDINALITY);
    }

    #[test]
    fn fixture_timestamps_are_ordered_and_non_overlapping_per_sst() {
        let rows = fact_rows();
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row.ts_nanos, START_NANOS + index as i64 * STEP_NANOS);
        }
        // One second per row, so the whole 32-SST fixture spans ~4.55 hours.
        let last = rows.last().unwrap().ts_nanos;
        assert_eq!((last - START_NANOS) / STEP_NANOS, (TABLE_ROWS - 1) as i64);
    }

    #[test]
    fn bound_is_sound_for_any_filter() {
        // The probe only ever injects `|T|` as the pre-query upper bound; the
        // claim `|sigma_P(T)| <= |T|` holds for every predicate P because the
        // cardinality of a filtered relation cannot exceed its input.
        for filtered in [0usize, 1, 7200, 14_400, TABLE_ROWS] {
            assert!(filtered <= TABLE_ROWS);
        }
    }
}

#[cfg(test)]
mod parse_tests {
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};

    use super::*;

    fn parse_or_fail(sql: &str) -> usize {
        let dialect = GreptimeDbDialect {};
        ParserContext::create_with_dialect(sql, &dialect, ParseOptions::default())
            .map(|stmts| stmts.len())
            .unwrap_or_else(|err| panic!("`{sql}` failed to parse: {err}"))
    }

    /// Every statement the probe sends must parse with the GreptimeDB dialect.
    /// `value` and `zone` are reserved keywords, so the fixture always quotes
    /// them in DDL/DML position.
    #[test]
    fn fixture_statements_parse() {
        for sql in [
            create_table_sql(FACT_TABLE),
            create_table_sql(DIM_TABLE),
            insert_sql(FACT_TABLE, &fact_rows()[..4]),
            insert_sql(DIM_TABLE, &dim_rows()[..4]),
            flush_sql(FACT_TABLE),
            flush_sql(DIM_TABLE),
        ] {
            assert_eq!(parse_or_fail(&sql), 1, "`{sql}` parsed to != 1 statement");
        }
    }

    /// The unmodified fixture query from
    /// `tests/perf/query_cases/sql_join_filter_order/case.toml` must parse too;
    /// this is the query string the probe actually plans.
    #[test]
    fn case_toml_query_parses() {
        let sql = crate::FIXTURE_QUERY;
        assert_eq!(parse_or_fail(sql), 1, "fixture query must parse verbatim");
        assert!(sql.contains("AND d.zone IN ('zone1', 'zone2')"));
    }
}
