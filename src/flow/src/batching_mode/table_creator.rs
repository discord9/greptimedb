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

use api::v1::CreateTableExpr;
use common_recordbatch::map_dictionary_to_values_data_type;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use operator::expr_helper::column_schemas_to_defs;
use snafu::ResultExt;
use table::TableRef;

use crate::Error;
use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
use crate::batching_mode::utils::FindGroupByFinalName;
use crate::error::{ConvertColumnSchemaSnafu, DatafusionSnafu};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryType {
    /// query is a tql query
    Tql,
    /// query is a sql query
    Sql,
}

// auto created table have a auto added column `update_at`, and optional have a `AUTO_CREATED_PLACEHOLDER_TS_COL` column for time index placeholder if no timestamp column is specified
// TODO(discord9): for now no default value is set for auto added column for compatibility reason with streaming mode, but this might change in favor of simpler code?
pub(super) fn create_table_with_expr(
    plan: &LogicalPlan,
    sink_table_name: &[String; 3],
    query_type: &QueryType,
) -> Result<CreateTableExpr, Error> {
    let table_def = match query_type {
        &QueryType::Sql => {
            if let Some(def) = build_pk_from_aggr(plan)? {
                def
            } else {
                build_by_sql_schema(plan)?
            }
        }
        QueryType::Tql => {
            // first try build from aggr, then from tql schema because tql query might not have aggr node
            if let Some(table_def) = build_pk_from_aggr(plan)? {
                table_def
            } else {
                build_by_tql_schema(plan)?
            }
        }
    };
    let first_time_stamp = table_def.ts_col;
    let primary_keys = table_def.pks;

    let mut column_schemas = Vec::new();
    for field in plan.schema().fields() {
        let name = field.name();
        let ty = map_dictionary_to_values_data_type(&ConcreteDataType::from_arrow_type(
            field.data_type(),
        ));
        let col_schema = if first_time_stamp == Some(name.clone()) {
            ColumnSchema::new(name, ty, false).with_time_index(true)
        } else {
            ColumnSchema::new(name, ty, true)
        };

        match query_type {
            QueryType::Sql => {
                column_schemas.push(col_schema);
            }
            QueryType::Tql => {
                // if is val column, need to rename as val DOUBLE NULL
                // if is tag column, need to cast type as STRING NULL
                let is_tag_column = primary_keys.contains(name);
                let is_val_column = !is_tag_column && first_time_stamp.as_ref() != Some(name);
                if is_val_column {
                    let col_schema =
                        ColumnSchema::new(name, ConcreteDataType::float64_datatype(), true);
                    column_schemas.push(col_schema);
                } else if is_tag_column {
                    let col_schema =
                        ColumnSchema::new(name, ConcreteDataType::string_datatype(), true);
                    column_schemas.push(col_schema);
                } else {
                    // time index column
                    column_schemas.push(col_schema);
                }
            }
        }
    }

    if query_type == &QueryType::Sql {
        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );
        column_schemas.push(update_at_schema);
    }

    let time_index = if let Some(time_index) = first_time_stamp {
        time_index
    } else {
        column_schemas.push(
            ColumnSchema::new(
                AUTO_CREATED_PLACEHOLDER_TS_COL,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        AUTO_CREATED_PLACEHOLDER_TS_COL.to_string()
    };

    let column_defs =
        column_schemas_to_defs(column_schemas, &primary_keys).context(ConvertColumnSchemaSnafu)?;
    Ok(CreateTableExpr {
        catalog_name: sink_table_name[0].clone(),
        schema_name: sink_table_name[1].clone(),
        table_name: sink_table_name[2].clone(),
        desc: "Auto created table by flow engine".to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: None,
        engine: "mito".to_string(),
    })
}

/// simply build by schema, return first timestamp column and no primary key
fn build_by_sql_schema(plan: &LogicalPlan) -> Result<TableDef, Error> {
    let first_time_stamp = plan.schema().fields().iter().find_map(|f| {
        if ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp() {
            Some(f.name().clone())
        } else {
            None
        }
    });
    Ok(TableDef {
        ts_col: first_time_stamp,
        pks: vec![],
    })
}

/// Builds a `CreateTableExpr` for a Phase-1 backfill staging table by cloning
/// the active sink/state table's schema: dimension (primary-key) columns kept
/// nullable, the window column kept as the time index, the BINARY state column
/// and any `update_at`/reserved epoch columns preserved as present.
///
/// The staging table is an ordinary mito table (never a SQL TEMPORARY TABLE)
/// living under `greptime_private`, so it survives restarts and can be dropped
/// explicitly by the backfill finalize path.
#[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
pub(super) fn create_staging_table_expr(
    sink_table: &TableRef,
    staging_table_name: &[String; 3],
) -> Result<CreateTableExpr, Error> {
    let meta = &sink_table.table_info().meta;
    let primary_key_indices = &meta.primary_key_indices;
    // Clone the full sink schema; force every primary-key (dimension) column
    // nullable so staging rows can carry NULL dimensions exactly like the
    // active sink's checkpoint/sentinel convention.
    let column_schemas = meta
        .schema
        .column_schemas()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            if primary_key_indices.contains(&idx) {
                col.clone().with_nullable_set()
            } else {
                col.clone()
            }
        })
        .collect::<Vec<_>>();
    let primary_keys = primary_key_indices
        .iter()
        .map(|&idx| meta.schema.column_name_by_index(idx).to_string())
        .collect::<Vec<_>>();
    let time_index = meta
        .schema
        .timestamp_column()
        .map(|col| col.name.clone())
        .or_else(|| {
            meta.schema
                .column_schemas()
                .iter()
                .find(|col| col.data_type.is_timestamp())
                .map(|col| col.name.clone())
        })
        .unwrap_or_default();
    let column_defs =
        column_schemas_to_defs(column_schemas, &primary_keys).context(ConvertColumnSchemaSnafu)?;
    Ok(CreateTableExpr {
        catalog_name: staging_table_name[0].clone(),
        schema_name: staging_table_name[1].clone(),
        table_name: staging_table_name[2].clone(),
        desc: "Auto created staging table by flow backfill".to_string(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: None,
        engine: "mito".to_string(),
    })
}

/// Return first timestamp column found in output schema and all string columns
fn build_by_tql_schema(plan: &LogicalPlan) -> Result<TableDef, Error> {
    let first_time_stamp = plan.schema().fields().iter().find_map(|f| {
        if ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp() {
            Some(f.name().clone())
        } else {
            None
        }
    });
    let string_columns = plan
        .schema()
        .fields()
        .iter()
        .filter_map(|f| {
            if map_dictionary_to_values_data_type(&ConcreteDataType::from_arrow_type(f.data_type()))
                .is_string()
            {
                Some(f.name().clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(TableDef {
        ts_col: first_time_stamp,
        pks: string_columns,
    })
}

struct TableDef {
    ts_col: Option<String>,
    pks: Vec<String>,
}

/// Return first timestamp column which is in group by clause and other columns which are also in group by clause
///
/// # Returns
///
/// * `Option<String>` - first timestamp column which is in group by clause
/// * `Vec<String>` - other columns which are also in group by clause
///
/// if no aggregation found, return None
fn build_pk_from_aggr(plan: &LogicalPlan) -> Result<Option<TableDef>, Error> {
    let fields = plan.schema().fields();
    let mut pk_names = FindGroupByFinalName::default();

    plan.visit(&mut pk_names)
        .with_context(|_| DatafusionSnafu {
            context: format!("Can't find aggr expr in plan {plan:?}"),
        })?;

    // if no group by clause, return empty with first timestamp column found in output schema
    let Some(pk_final_names) = pk_names.get_group_expr_names() else {
        return Ok(None);
    };
    if pk_final_names.is_empty() {
        let first_ts_col = fields
            .iter()
            .find(|f| ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp())
            .map(|f| f.name().clone());
        return Ok(Some(TableDef {
            ts_col: first_ts_col,
            pks: vec![],
        }));
    }

    let all_pk_cols: Vec<_> = fields
        .iter()
        .filter(|f| pk_final_names.contains(f.name()))
        .map(|f| f.name().clone())
        .collect();
    // Auto-created tables use the first timestamp column in the group-by keys
    // as the time index. It is possible that timestamp columns appear only as
    // aggregate outputs (for example `max(ts)`) and are not group-by keys; in
    // that case `first_time_stamp` stays `None` and the caller falls back to a
    // placeholder time index column.
    let first_time_stamp = fields
        .iter()
        .find(|f| {
            all_pk_cols.contains(&f.name().clone())
                && ConcreteDataType::from_arrow_type(f.data_type()).is_timestamp()
        })
        .map(|f| f.name().clone());

    let all_pk_cols: Vec<_> = all_pk_cols
        .into_iter()
        .filter(|col| first_time_stamp.as_ref() != Some(col))
        .collect();

    Ok(Some(TableDef {
        ts_col: first_time_stamp,
        pks: all_pk_cols,
    }))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use api::v1::column_def::try_as_column_schema;
    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit,
    };
    use datafusion_common::DFSchema;
    use datafusion_expr::logical_plan::EmptyRelation;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::*;
    use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
    use crate::batching_mode::utils::sql_to_df_plan;
    use crate::test_utils::create_test_query_engine;

    #[test]
    fn test_tql_dictionary_string_is_label() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new_dictionary("host", ArrowDataType::UInt32, ArrowDataType::Utf8, true),
            Field::new("value", ArrowDataType::Float64, true),
            Field::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(arrow_schema).unwrap()),
        });

        let expr = create_table_with_expr(
            &plan,
            &[
                "greptime".to_string(),
                "public".to_string(),
                "sink".to_string(),
            ],
            &QueryType::Tql,
        )
        .unwrap();
        let columns = expr
            .column_defs
            .iter()
            .map(|column| try_as_column_schema(column).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(vec!["host".to_string()], expr.primary_keys);
        assert_eq!("ts", expr.time_index);
        assert_eq!(ConcreteDataType::string_datatype(), columns[0].data_type);
        assert_eq!(ConcreteDataType::float64_datatype(), columns[1].data_type);
        assert!(columns[2].is_time_index());
    }

    #[tokio::test]
    async fn test_gen_create_table_sql() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        struct TestCase {
            sql: String,
            sink_table_name: String,
            column_schemas: Vec<ColumnSchema>,
            primary_keys: Vec<String>,
            time_index: String,
        }

        let update_at_schema = ColumnSchema::new(
            AUTO_CREATED_UPDATE_AT_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        );

        let ts_placeholder_schema = ColumnSchema::new(
            AUTO_CREATED_PLACEHOLDER_TS_COL,
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true);

        let testcases = vec![
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: "ts".to_string(),
            },
            TestCase {
                sql: "SELECT number, max(ts) FROM numbers_with_ts GROUP BY number".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "max(numbers_with_ts.ts)",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        true,
                    ),
                    update_at_schema.clone(),
                    ts_placeholder_schema.clone(),
                ],
                primary_keys: vec!["number".to_string()],
                time_index: AUTO_CREATED_PLACEHOLDER_TS_COL.to_string(),
            },
            TestCase {
                sql: "SELECT max(number), ts FROM numbers_with_ts GROUP BY ts".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new(
                        "max(numbers_with_ts.number)",
                        ConcreteDataType::uint32_datatype(),
                        true,
                    ),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec![],
                time_index: "ts".to_string(),
            },
            TestCase {
                sql: "SELECT number, ts FROM numbers_with_ts GROUP BY ts, number".to_string(),
                sink_table_name: "new_table".to_string(),
                column_schemas: vec![
                    ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    update_at_schema.clone(),
                ],
                primary_keys: vec!["number".to_string()],
                time_index: "ts".to_string(),
            },
        ];

        for tc in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), &tc.sql, true)
                .await
                .unwrap();
            let expr = create_table_with_expr(
                &plan,
                &[
                    "greptime".to_string(),
                    "public".to_string(),
                    tc.sink_table_name.clone(),
                ],
                &QueryType::Sql,
            )
            .unwrap();
            // TODO(discord9): assert expr
            let column_schemas = expr
                .column_defs
                .iter()
                .map(|c| try_as_column_schema(c).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(tc.column_schemas, column_schemas, "{:?}", tc.sql);
            assert_eq!(tc.primary_keys, expr.primary_keys, "{:?}", tc.sql);
            assert_eq!(tc.time_index, expr.time_index, "{:?}", tc.sql);
        }
    }

    #[tokio::test]
    async fn test_create_staging_table_expr_clones_sink_schema_with_nullable_pks() {
        use catalog::RegisterTableRequest;
        use catalog::memory::MemoryCatalogManager;
        use common_catalog::consts::{
            DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
        };
        use datatypes::schema::Schema;

        let query_engine = create_test_query_engine();
        let catalog_manager = query_engine.engine_state().catalog_manager().clone();
        let memory_catalog = catalog_manager
            .as_any()
            .downcast_ref::<MemoryCatalogManager>()
            .unwrap();
        memory_catalog
            .register_catalog_sync(DEFAULT_CATALOG_NAME)
            .unwrap();
        memory_catalog
            .register_schema_sync(catalog::RegisterSchemaRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            })
            .unwrap();

        // Sink schema: dimension `number` (non-nullable PK), time index `ts`,
        // a BINARY state column and the reserved internal epoch column.
        let sink_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("number", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("state", ConcreteDataType::binary_datatype(), true),
            ColumnSchema::new(
                crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
                ConcreteDataType::uint64_datatype(),
                true,
            ),
        ]));
        let sink_name = "sink_state";
        let table_info = table::metadata::TableInfoBuilder::default()
            .table_id(2048)
            .table_version(0)
            .name(sink_name)
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .desc(None)
            .table_type(table::metadata::TableType::Base)
            .meta(
                table::metadata::TableMetaBuilder::empty()
                    .schema(sink_schema.clone())
                    .primary_key_indices(vec![0])
                    .value_indices(vec![])
                    .engine("mito".to_string())
                    .next_column_id(0)
                    .options(Default::default())
                    .created_on(Default::default())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let sink_table = table::test_util::EmptyTable::from_table_info(&table_info);
        memory_catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: sink_name.to_string(),
                table_id: 2048,
                table: sink_table.clone(),
            })
            .unwrap();

        let staging_name = [
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            "__flow_backfill_1_42".to_string(),
        ];
        let expr = create_staging_table_expr(&sink_table, &staging_name).unwrap();
        assert_eq!(expr.catalog_name, DEFAULT_CATALOG_NAME);
        assert_eq!(expr.schema_name, DEFAULT_PRIVATE_SCHEMA_NAME);
        assert_eq!(expr.table_name, "__flow_backfill_1_42");
        assert_eq!(expr.engine, "mito");
        assert!(expr.create_if_not_exists);
        assert_eq!(expr.primary_keys, vec!["number".to_string()]);
        assert_eq!(expr.time_index, "ts");

        let staging_columns = expr
            .column_defs
            .iter()
            .map(|c| try_as_column_schema(c).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(staging_columns.len(), 4);
        // PK dimension forced nullable in the staging clone.
        assert!(staging_columns[0].is_nullable());
        assert_eq!(staging_columns[0].name, "number");
        // Window column keeps the time index.
        assert!(staging_columns[1].is_time_index());
        assert_eq!(staging_columns[1].name, "ts");
        // BINARY state column and the reserved epoch column preserved.
        assert!(
            staging_columns
                .iter()
                .any(|c| c.name == "state" && c.data_type == ConcreteDataType::binary_datatype())
        );
        assert!(staging_columns.iter().any(|c| {
            c.name == crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME
                && c.data_type == ConcreteDataType::uint64_datatype()
        }));
    }
}
