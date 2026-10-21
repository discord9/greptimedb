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

use arrow::datatypes::Schema;
use common_error::ext::BoxedError;
use common_function::function::FunctionContext;
use common_function::function_registry::FUNCTION_REGISTRY;
use common_query::error::RegisterUdfSnafu;
use common_query::logical_plan::SubstraitPlanDecoder;
use datafusion::catalog::CatalogProviderList;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion_common::DFSchema;
use datafusion_expr::UserDefinedLogicalNode;
use datafusion_expr::logical_plan::{
    EmptyRelation, LogicalTableSource, TableScanBuilder, UNNAMED_TABLE,
};
use greptime_proto::substrait_extension::MergeScan as PbMergeScan;
use promql::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, DoubleExponentialSmoothing,
    IDelta, Increase, LastOverTime, MaxOverTime, MinOverTime, MixedRange,
    NativeHistogramAbsentOverTime, NativeHistogramAdd, NativeHistogramAggAvg,
    NativeHistogramAggSum, NativeHistogramAvg, NativeHistogramAvgOverTime, NativeHistogramChanges,
    NativeHistogramCount, NativeHistogramCountOverTime, NativeHistogramDelta,
    NativeHistogramDivScalar, NativeHistogramDrop, NativeHistogramEq, NativeHistogramFraction,
    NativeHistogramIDelta, NativeHistogramIRate, NativeHistogramIncrease,
    NativeHistogramLastOverTime, NativeHistogramMulScalar, NativeHistogramNeg,
    NativeHistogramNotEq, NativeHistogramPresentOverTime, NativeHistogramQuantile,
    NativeHistogramRate, NativeHistogramResets, NativeHistogramScalarMul, NativeHistogramStddev,
    NativeHistogramStdvar, NativeHistogramSub, NativeHistogramSum, NativeHistogramSumOverTime,
    NativeHistogramToString, PredictLinear, PresentOverTime, PromqlFloatToString, QuantileOverTime,
    Rate, Resets, Round, StddevOverTime, StdvarOverTime, SumOverTime, quantile_udaf,
};
use prost::Message;
use session::context::QueryContextRef;
use snafu::ResultExt;
use substrait::extension_serializer::ExtensionSerializer;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

use crate::dist_plan::MergeScanLogicalPlan;
use crate::scan_sequence_binding::{RegionSeqs, ScanSequenceBinding};

/// Substrait extension payload of one [`ScanSequenceBinding`].
///
/// The payload holds the node's own state only: the wrapped plan travels as the
/// input of the `ExtensionSingleRel` that carries this node, and the Substrait
/// consumer re-attaches the decoded child with `with_exprs_and_inputs`.
///
/// The message lives here, next to the only code that reads and writes it,
/// because it is exchanged nowhere else: unlike the region-server scan request
/// it does not belong to the wire protocol of another crate.
#[derive(Clone, PartialEq, prost::Message)]
struct PbScanSequenceBinding {
    /// Id of the scan position, see [`ScanSequenceBinding::scan_id`].
    #[prost(uint32, tag = "1")]
    scan_id: u32,
    /// Per-region sequence lower bounds, ordered by region id.
    #[prost(message, repeated, tag = "2")]
    after_seqs: Vec<PbRegionSeq>,
}

/// Per-region sequence lower bound of one [`ScanSequenceBinding`].
#[derive(Clone, PartialEq, prost::Message)]
struct PbRegionSeq {
    /// Region the bound belongs to.
    #[prost(uint64, tag = "1")]
    region_id: u64,
    /// Sequence the scan of that region must read after.
    #[prost(uint64, tag = "2")]
    seq: u64,
}

/// Extended [`substrait::extension_serializer::ExtensionSerializer`] but supports [`MergeScanLogicalPlan`] serialization.
#[derive(Debug)]
pub struct DefaultSerializer;

impl SerializerRegistry for DefaultSerializer {
    fn serialize_logical_plan(&self, node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        if node.name() == MergeScanLogicalPlan::name() {
            let merge_scan = node
                .as_any()
                .downcast_ref::<MergeScanLogicalPlan>()
                .expect("Failed to downcast to MergeScanLogicalPlan");

            let input = merge_scan.input();
            let is_placeholder = merge_scan.is_placeholder();
            let input = DFLogicalSubstraitConvertor
                .encode(input, DefaultSerializer)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .to_vec();

            Ok(PbMergeScan {
                is_placeholder,
                input,
            }
            .encode_to_vec())
        } else if node.name() == ScanSequenceBinding::name() {
            Self::serialize_scan_sequence_binding(node)
        } else {
            ExtensionSerializer.serialize_logical_plan(node)
        }
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if name == MergeScanLogicalPlan::name() {
            // TODO(dennis): missing `session_state` to decode the logical plan in `MergeScanLogicalPlan`,
            // so we only save the unoptimized logical plan for view currently.
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan node: {name}"
            )))
        } else if name == ScanSequenceBinding::name() {
            Self::deserialize_scan_sequence_binding(bytes)
        } else {
            ExtensionSerializer.deserialize_logical_plan(name, bytes)
        }
    }
}

impl DefaultSerializer {
    /// Encodes the node-local payload of a [`ScanSequenceBinding`].
    ///
    /// The wrapped plan is left out on purpose: the Substrait producer encodes
    /// it as the input of the `ExtensionSingleRel` that carries this node, so
    /// this payload is exactly the part the node itself owns.
    fn serialize_scan_sequence_binding(node: &dyn UserDefinedLogicalNode) -> Result<Vec<u8>> {
        let binding = node
            .as_any()
            .downcast_ref::<ScanSequenceBinding>()
            .expect("Failed to downcast to ScanSequenceBinding");

        Ok(PbScanSequenceBinding {
            scan_id: binding.scan_id(),
            after_seqs: binding
                .after_seqs()
                .iter()
                .map(|(&region_id, &seq)| PbRegionSeq { region_id, seq })
                .collect(),
        }
        .encode_to_vec())
    }

    /// Rebuilds a [`ScanSequenceBinding`] from its node-local payload.
    ///
    /// The node comes back on a placeholder input and without its output schema:
    /// the Substrait consumer of the surrounding `ExtensionSingleRel` re-attaches
    /// the decoded child with `with_exprs_and_inputs`, which also restores the
    /// schema, because a binding's schema is its child's schema. This is the
    /// mechanism every single-input [`UserDefinedLogicalNode`] relies on; it does
    /// not work for [`MergeScanLogicalPlan`], whose `inputs()` returns nothing.
    fn deserialize_scan_sequence_binding(bytes: &[u8]) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        let pb = PbScanSequenceBinding::decode(bytes).map_err(|e| {
            DataFusionError::Substrait(format!(
                "Failed to decode {}: {e}",
                ScanSequenceBinding::name()
            ))
        })?;
        let after_seqs = pb
            .after_seqs
            .into_iter()
            .map(|PbRegionSeq { region_id, seq }| (region_id, seq))
            .collect::<RegionSeqs>();

        // A binding only wraps a `TableScan`, so the placeholder node is built
        // over an empty placeholder scan and then handed the placeholder input
        // the consumer replaces with the decoded child.
        let placeholder_scan = TableScanBuilder::new(
            UNNAMED_TABLE,
            Arc::new(LogicalTableSource::new(Arc::new(Schema::empty()))),
        )
        .build()?;
        let placeholder_input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        ScanSequenceBinding::new(pb.scan_id, after_seqs, placeholder_scan)
            .with_exprs_and_inputs(vec![], vec![placeholder_input])
    }
}

/// The datafusion `[LogicalPlan]` decoder.
pub struct DefaultPlanDecoder {
    session_state: SessionState,
    query_ctx: QueryContextRef,
}

impl DefaultPlanDecoder {
    pub fn new(
        session_state: SessionState,
        query_ctx: &QueryContextRef,
    ) -> crate::error::Result<Self> {
        Ok(Self {
            session_state,
            query_ctx: query_ctx.clone(),
        })
    }
}

#[async_trait::async_trait]
impl SubstraitPlanDecoder for DefaultPlanDecoder {
    async fn decode(
        &self,
        message: bytes::Bytes,
        catalog_list: Arc<dyn CatalogProviderList>,
        optimize: bool,
    ) -> common_query::error::Result<LogicalPlan> {
        // The session_state already has the `DefaultSerialzier` as `SerializerRegistry`.
        let mut session_state = SessionStateBuilder::new_from_existing(self.session_state.clone())
            .with_catalog_list(catalog_list)
            .build();
        // Substrait decoder will look up the UDFs in SessionState, so we need to register them
        // Note: the query context must be passed to set the timezone
        // We MUST register the UDFs after we build the session state, otherwise the UDFs will be lost
        // if they have the same name as the default UDFs or their alias.
        // e.g. The default UDF `to_char()` has an alias `date_format()`, if we register a UDF with the name `date_format()`
        // before we build the session state, the UDF will be lost.
        for func in FUNCTION_REGISTRY.scalar_functions() {
            let udf = func.provide(FunctionContext {
                query_ctx: self.query_ctx.clone(),
                state: Default::default(),
            });
            session_state
                .register_udf(Arc::new(udf))
                .context(RegisterUdfSnafu { name: func.name() })?;
        }

        for func in FUNCTION_REGISTRY.aggregate_functions() {
            let name = func.name().to_string();
            session_state
                .register_udaf(Arc::new(func))
                .context(RegisterUdfSnafu { name })?;
        }

        let _ = session_state.register_udaf(quantile_udaf());

        let _ = session_state.register_udf(Arc::new(IDelta::<false>::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(IDelta::<true>::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Rate::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Increase::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Delta::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Resets::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Changes::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Deriv::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(Round::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(AvgOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(MinOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(MaxOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(SumOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(CountOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(LastOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(AbsentOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(PresentOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(StddevOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(StdvarOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(QuantileOverTime::scalar_udf()));
        let _ = session_state.register_udf(Arc::new(PredictLinear::scalar_udf()));
        let double_exponential_smoothing_udf =
            DoubleExponentialSmoothing::scalar_udf().with_aliases(["prom_holt_winters"]);
        let _ = session_state.register_udf(Arc::new(double_exponential_smoothing_udf));

        for udf in [
            NativeHistogramAbsentOverTime::scalar_udf(),
            NativeHistogramAdd::scalar_udf(),
            NativeHistogramAvg::scalar_udf(),
            NativeHistogramAvgOverTime::scalar_udf(),
            NativeHistogramChanges::scalar_udf(),
            NativeHistogramCount::scalar_udf(),
            NativeHistogramCountOverTime::scalar_udf(),
            NativeHistogramDelta::scalar_udf(),
            NativeHistogramDivScalar::scalar_udf(),
            NativeHistogramDrop::bool_false_udf(String::new(), None),
            NativeHistogramDrop::bool_true_udf(String::new(), None),
            NativeHistogramDrop::float_null_udf(String::new(), None),
            NativeHistogramEq::scalar_udf(),
            NativeHistogramFraction::scalar_udf(),
            NativeHistogramIDelta::scalar_udf(),
            NativeHistogramIRate::scalar_udf(),
            NativeHistogramIncrease::scalar_udf(),
            NativeHistogramLastOverTime::scalar_udf(),
            MixedRange::float_udf(None),
            MixedRange::histogram_udf(None),
            NativeHistogramMulScalar::scalar_udf(),
            NativeHistogramNeg::scalar_udf(),
            NativeHistogramNotEq::scalar_udf(),
            NativeHistogramPresentOverTime::scalar_udf(),
            NativeHistogramQuantile::scalar_udf(),
            NativeHistogramRate::scalar_udf(),
            NativeHistogramResets::scalar_udf(),
            NativeHistogramScalarMul::scalar_udf(),
            NativeHistogramStddev::scalar_udf(),
            NativeHistogramStdvar::scalar_udf(),
            NativeHistogramSub::scalar_udf(),
            NativeHistogramSum::scalar_udf(),
            NativeHistogramSumOverTime::scalar_udf(),
            NativeHistogramToString::scalar_udf(),
            PromqlFloatToString::scalar_udf(),
        ] {
            let _ = session_state.register_udf(Arc::new(udf));
        }
        for udaf in [
            NativeHistogramAggAvg::aggregate_udf(),
            NativeHistogramAggSum::aggregate_udf(),
        ] {
            let _ = session_state.register_udaf(Arc::new(udaf));
        }

        let logical_plan = DFLogicalSubstraitConvertor
            .decode(message, session_state)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::DecodePlanSnafu)?;

        if optimize {
            self.session_state
                .optimize(&logical_plan)
                .map_err(Into::into)
        } else {
            Ok(logical_plan)
        }
    }
}

#[cfg(test)]
mod tests {
    use common_query::native_histogram::native_histogram_value_type;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::Extension;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Expr, LogicalPlanBuilder, LogicalTableSource, col, lit};
    use datatypes::arrow::datatypes::{
        DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit,
    };
    use datatypes::data_type::DataType;
    use promql::extension_plan::RangeManipulate;
    use session::context::QueryContext;

    use super::*;
    use crate::QueryEngineFactory;
    use crate::dummy_catalog::DummyCatalogList;
    use crate::optimizer::test_util::mock_table_provider;
    use crate::options::QueryOptions;

    fn mock_plan(schema: SchemaRef) -> LogicalPlan {
        let table_source = LogicalTableSource::new(schema);
        let projection = None;
        let builder =
            LogicalPlanBuilder::scan("devices", Arc::new(table_source), projection).unwrap();

        builder
            .filter(col("k0").eq(lit("hello")))
            .unwrap()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_serializer_decode_plan() {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let factory = QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );

        let engine = factory.query_engine();

        let table_provider = Arc::new(mock_table_provider(1.into()));
        let plan = mock_plan(table_provider.schema().clone());

        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();

        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();
        let catalog_list = Arc::new(DummyCatalogList::with_table_provider(table_provider));

        let decode_plan = plan_decoder
            .decode(bytes, catalog_list, false)
            .await
            .unwrap();

        assert_eq!(
            "Filter: devices.k0 = Utf8(\"hello\")
  TableScan: devices",
            decode_plan.to_string(),
        );
    }

    #[tokio::test]
    async fn test_serializer_decode_native_histogram_udf() {
        let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
        let factory = QueryEngineFactory::new(
            catalog_list,
            None,
            None,
            None,
            None,
            false,
            QueryOptions::default(),
        );
        let engine = factory.query_engine();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "histogram",
            native_histogram_value_type().as_arrow_type(),
            true,
        )]));
        let plan = LogicalPlanBuilder::scan(
            "devices",
            Arc::new(LogicalTableSource::new(schema.clone())),
            None,
        )
        .unwrap()
        .aggregate(
            Vec::<Expr>::new(),
            vec![
                Arc::new(NativeHistogramAggSum::aggregate_udf())
                    .call(vec![col("histogram")])
                    .alias("sum"),
                Arc::new(NativeHistogramAggAvg::aggregate_udf())
                    .call(vec![col("histogram")])
                    .alias("avg"),
            ],
        )
        .unwrap()
        .project(vec![
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramCount::scalar_udf()),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::bool_false_udf(
                    "ignored annotation".to_string(),
                    None,
                )),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::bool_true_udf(
                    "ignored annotation".to_string(),
                    None,
                )),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::float_null_udf(
                    "ignored annotation".to_string(),
                    None,
                )),
                args: vec![col("sum")],
            }),
            Expr::ScalarFunction(ScalarFunction {
                func: Arc::new(PromqlFloatToString::scalar_udf()),
                args: vec![lit(2.0)],
            }),
        ])
        .unwrap()
        .build()
        .unwrap();
        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();
        let table_provider = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let plan_decoder = engine
            .engine_context(QueryContext::arc())
            .new_plan_decoder()
            .unwrap();

        let decoded = plan_decoder
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(table_provider)),
                false,
            )
            .await
            .unwrap();

        let decoded = decoded.to_string();
        assert!(decoded.contains("prom_native_histogram_count"));
        assert!(decoded.contains("prom_native_histogram_drop_bool"));
        assert!(decoded.contains("prom_native_histogram_keep_bool"));
        assert!(decoded.contains("prom_native_histogram_drop_float"));
        assert!(decoded.contains(NativeHistogramAggSum::name()));
        assert!(decoded.contains(NativeHistogramAggAvg::name()));
        assert!(decoded.contains(PromqlFloatToString::name()));

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("float", ArrowDataType::Float64, true),
            Field::new(
                "histogram",
                native_histogram_value_type().as_arrow_type(),
                true,
            ),
        ]));
        let input = LogicalPlanBuilder::scan(
            "devices",
            Arc::new(LogicalTableSource::new(schema.clone())),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(
                RangeManipulate::new(
                    0,
                    1000,
                    1000,
                    0,
                    1000,
                    "timestamp".to_string(),
                    vec!["float".to_string(), "histogram".to_string()],
                    input,
                )
                .unwrap(),
            ),
        });
        let plan = LogicalPlanBuilder::from(input)
            .project(vec![
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(MixedRange::float_udf(None)),
                    args: vec![
                        lit("last_over_time"),
                        col("timestamp_range"),
                        col("float"),
                        col("histogram"),
                    ],
                })
                .alias("mixed_float"),
                Expr::ScalarFunction(ScalarFunction {
                    func: Arc::new(MixedRange::histogram_udf(None)),
                    args: vec![
                        lit("last_over_time"),
                        col("timestamp_range"),
                        col("float"),
                        col("histogram"),
                    ],
                })
                .alias("mixed_histogram"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let bytes = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();
        let table_provider = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let decoded = plan_decoder
            .decode(
                bytes,
                Arc::new(DummyCatalogList::with_table_provider(table_provider)),
                false,
            )
            .await
            .unwrap()
            .to_string();
        assert!(decoded.contains("prom_mixed_range_float"));
        assert!(decoded.contains("prom_mixed_range_histogram"));
    }
}
