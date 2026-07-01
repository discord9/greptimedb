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

#![allow(clippy::print_stderr, clippy::print_stdout)]

//! Offline readable SST fixture generator for GC lab testing.
//!
//! Uses real [`mito2::sst::parquet::writer::ParquetWriter`] to write
//! readable SST parquet files to a local filesystem object-store,
//! generates matching manifest checkpoint metadata with [`FileMeta`],
//! and optionally validates readback via Mito parquet reader.
//!
//! Supports two modes:
//! - **Synthetic mode** (no seed): generates fresh region metadata matching the
//!   fixed schema (tag_0, tag_1, field_0, ts).
//! - **Seed mode** (`--seed-checkpoint` or `--seed-delta-dir`): loads existing
//!   manifest metadata and validates it is compatible with this tool's batch
//!   generator. Useful as foundation for MinIO/datanode checkpoint swap tests.
//!
//! Intended as a lab-only tool; does not contact Kubernetes or MinIO.

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use api::v1::{OpType, SemanticType};
use async_trait::async_trait;
use clap::Parser;
use datatypes::arrow::array::{
    ArrayRef, BinaryDictionaryBuilder, RecordBatch, StringDictionaryBuilder,
    TimestampMillisecondArray, UInt8Array, UInt64Array,
};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions};
use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
use mito2::access_layer::{FilePathProvider, Metrics, WriteType};
use mito2::config::IndexConfig;
use mito2::manifest::action::{
    RegionCheckpoint, RegionManifest, RegionManifestBuilder, RegionMetaAction,
    RegionMetaActionList, RemovedFilesRecord,
};
use mito2::read::FlatSource;
use mito2::sst::file::{FileHandle, FileMeta, RegionFileId};
use mito2::sst::file_purger::{FilePurgerRef, NoopFilePurger};
use mito2::sst::index::{Indexer, IndexerBuilder};
use mito2::sst::parquet::reader::ParquetReaderBuilder;
use mito2::sst::parquet::writer::ParquetWriter;
use mito2::sst::parquet::{SstInfo, WriteOptions};
use mito2::sst::{
    DEFAULT_WRITE_BUFFER_SIZE, FlatSchemaOptions, FormatType, to_flat_sst_arrow_schema,
};
use object_store::ObjectStore;
use object_store::services::Fs as FsBuilder;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::path_utils::region_name;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

/// CLI for generating local FS readable SST fixture for GC lab testing.
///
/// Supports two modes:
/// - **Synthetic** (default): generates fresh region metadata with fixed schema.
/// - **Seed**: provide `--seed-checkpoint` or `--seed-delta-dir` to load
///   existing region metadata (must be schema-compatible).
#[derive(Parser, Debug)]
#[command(name = "gc_readable_sst_fixture")]
#[command(
    about = "Generate real readable SST parquet files with manifest checkpoint for GC lab testing"
)]
#[command(
    long_about = "Generate real readable SST parquet files with matching manifest \
checkpoint metadata. Supports synthetic metadata (default) or seed manifest \
metadata via --seed-checkpoint or --seed-delta-dir."
)]
struct Args {
    /// Output directory (creates object-store/, manifest/, files.jsonl, summary.json).
    #[arg(long, value_name = "DIR")]
    out_dir: PathBuf,

    /// Region ID (required).
    #[arg(long, value_name = "ID")]
    region_id: u64,

    /// Table directory relative to object store root, before the region subdir.
    ///
    /// Example: for region `1024_0000000000`, pass
    /// `data/greptime/public/1024/`; the region subdir is appended automatically.
    #[arg(long, value_name = "DIR")]
    table_dir: String,

    /// Number of SST files to generate.
    #[arg(long, default_value = "1")]
    sst_count: usize,

    /// Number of rows per SST file.
    #[arg(long, default_value = "10")]
    rows_per_sst: usize,

    /// Row group size for parquet writer.
    #[arg(long, default_value = "50")]
    row_group_size: usize,

    /// Manifest/checkpoint version.
    #[arg(long, default_value = "1000000")]
    checkpoint_version: u64,

    /// Path to a seed checkpoint file (uncompressed `.checkpoint`).
    ///
    /// The seed metadata must be schema-compatible with this tool's batch
    /// generator (dense encoding, tag_0/tag_1/field_0/ts columns).
    /// Exactly zero or one of --seed-checkpoint / --seed-delta-dir may be given.
    #[arg(long, value_name = "PATH")]
    seed_checkpoint: Option<PathBuf>,

    /// Directory containing delta JSON seed files (e.g. `00000000000000000000.json`).
    ///
    /// Files are read in filename-sorted order and replayed to reconstruct the
    /// seed manifest.  `.json.gz` files are not supported.
    /// Exactly zero or one of --seed-checkpoint / --seed-delta-dir may be given.
    #[arg(long, value_name = "DIR")]
    seed_delta_dir: Option<PathBuf>,

    /// Skip readback validation (by default, readback is performed).
    #[arg(long)]
    skip_readback: bool,

    /// Dry-run: print plan but don't write any files.
    #[arg(long)]
    dry_run: bool,

    /// Safety flag when sst-count exceeds 1000.
    #[arg(long)]
    allow_large: bool,
}

// ---------------------------------------------------------------------------
// Inlined helpers (avoid enabling mito2 "test" feature)
// ---------------------------------------------------------------------------

/// A no-op index builder that produces an empty [`Indexer`].
struct NoopIndexBuilder;

#[async_trait]
impl IndexerBuilder for NoopIndexBuilder {
    async fn build(&self, _file_id: FileId, _index_version: u64) -> Indexer {
        Indexer::default()
    }
}

/// A fixed path provider that uses the writer-provided file id to build paths.
#[derive(Clone)]
struct FixedPathProvider {
    table_dir: String,
}

impl FilePathProvider for FixedPathProvider {
    fn build_index_file_path(&self, file_id: RegionFileId) -> String {
        mito2::sst::location::index_file_path_legacy(&self.table_dir, file_id, PathType::Bare)
    }

    fn build_index_file_path_with_version(
        &self,
        index_id: mito2::sst::file::RegionIndexId,
    ) -> String {
        mito2::sst::location::index_file_path(&self.table_dir, index_id, PathType::Bare)
    }

    fn build_sst_file_path(&self, file_id: RegionFileId) -> String {
        mito2::sst::location::sst_file_path(&self.table_dir, file_id, PathType::Bare)
    }
}

// ---------------------------------------------------------------------------
// Region metadata & record batch helpers
// ---------------------------------------------------------------------------

/// Build a dense region metadata: tag_0(String) Tag col 0, tag_1(String) Tag col 1,
/// field_0(UInt64) Field col 2, ts(TimestampMillisecond) Timestamp col 3.
fn build_region_metadata(region_id: RegionId) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag_0".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )
            .with_inverted_index(true),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "tag_1".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            )
            .with_skipping_options(SkippingIndexOptions {
                granularity: 1,
                ..Default::default()
            })
            .unwrap(),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "field_0".to_string(),
                ConcreteDataType::uint64_datatype(),
                true,
            ),
            semantic_type: SemanticType::Field,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 3,
        });
    builder.primary_key(vec![0, 1]);
    builder.primary_key_encoding(PrimaryKeyEncoding::Dense);
    builder.build().unwrap()
}

/// Validate that `metadata` is schema-compatible with this tool's fixed batch generator.
///
/// Returns `Ok(())` if compatible, or an error message string.
fn validate_seed_metadata_compatible(
    metadata: &RegionMetadata,
    region_id: RegionId,
) -> Result<(), String> {
    // region_id must match
    if metadata.region_id != region_id {
        return Err(format!(
            "seed metadata region_id {} does not match CLI --region-id {}",
            metadata.region_id.as_u64(),
            region_id.as_u64()
        ));
    }

    // must be dense primary key encoding
    if metadata.primary_key_encoding != PrimaryKeyEncoding::Dense {
        return Err(format!(
            "seed metadata uses {:?} encoding, but this tool only supports Dense",
            metadata.primary_key_encoding
        ));
    }

    // primary key must be [0, 1]
    if metadata.primary_key != vec![0u32, 1u32] {
        return Err(format!(
            "seed metadata primary_key is {:?}, but this tool requires [0, 1]",
            metadata.primary_key
        ));
    }

    // must have exactly 4 user columns with expected types and semantic types
    if metadata.column_metadatas.len() < 4 {
        return Err(format!(
            "seed metadata has {} columns, but this tool requires at least 4 (tag_0, tag_1, field_0, ts)",
            metadata.column_metadatas.len()
        ));
    }

    // Check column 0: tag_0, String, Tag
    let col0 = &metadata.column_metadatas[0];
    if col0.column_id != 0
        || col0.column_schema.name != "tag_0"
        || col0.semantic_type != SemanticType::Tag
        || col0.column_schema.data_type != ConcreteDataType::string_datatype()
    {
        return Err(format!(
            "seed metadata column 0 mismatch: expected tag_0/String/Tag (id=0), got {}/{:?}/{:?} (id={})",
            col0.column_schema.name,
            col0.column_schema.data_type,
            col0.semantic_type,
            col0.column_id
        ));
    }

    // Check column 1: tag_1, String, Tag
    let col1 = &metadata.column_metadatas[1];
    if col1.column_id != 1
        || col1.column_schema.name != "tag_1"
        || col1.semantic_type != SemanticType::Tag
        || col1.column_schema.data_type != ConcreteDataType::string_datatype()
    {
        return Err(format!(
            "seed metadata column 1 mismatch: expected tag_1/String/Tag (id=1), got {}/{:?}/{:?} (id={})",
            col1.column_schema.name,
            col1.column_schema.data_type,
            col1.semantic_type,
            col1.column_id
        ));
    }

    // Check column 2: field_0, UInt64, Field
    let col2 = &metadata.column_metadatas[2];
    if col2.column_id != 2
        || col2.column_schema.name != "field_0"
        || col2.semantic_type != SemanticType::Field
        || col2.column_schema.data_type != ConcreteDataType::uint64_datatype()
    {
        return Err(format!(
            "seed metadata column 2 mismatch: expected field_0/UInt64/Field (id=2), got {}/{:?}/{:?} (id={})",
            col2.column_schema.name,
            col2.column_schema.data_type,
            col2.semantic_type,
            col2.column_id
        ));
    }

    // Check column 3: ts, TimestampMillisecond, Timestamp
    let col3 = &metadata.column_metadatas[3];
    if col3.column_id != 3
        || col3.column_schema.name != "ts"
        || col3.semantic_type != SemanticType::Timestamp
        || col3.column_schema.data_type != ConcreteDataType::timestamp_millisecond_datatype()
    {
        return Err(format!(
            "seed metadata column 3 mismatch: expected ts/TimestampMillisecond/Timestamp (id=3), got {}/{:?}/{:?} (id={})",
            col3.column_schema.name,
            col3.column_schema.data_type,
            col3.semantic_type,
            col3.column_id
        ));
    }

    Ok(())
}

/// Encode a dense primary key from tag values.
fn encode_dense_primary_key(tags: &[&str]) -> Vec<u8> {
    let fields = (0..tags.len())
        .map(|idx| {
            (
                idx as u32,
                SortField::new(ConcreteDataType::string_datatype()),
            )
        })
        .collect();
    let converter = DensePrimaryKeyCodec::with_fields(fields);
    converter
        .encode(
            tags.iter()
                .map(|tag| datatypes::value::ValueRef::String(tag)),
        )
        .unwrap()
}

/// Generate a flat-format RecordBatch for the given tags and time range.
///
/// Columns order: tag_0 (dict string), tag_1 (dict string), field_0 (UInt64),
/// ts (TimestampMillisecond), primary_key (binary dict), sequence (UInt64), op_type (UInt8).
fn generate_record_batch(
    metadata: &RegionMetadataRef,
    tags: &[String],
    start_ts: usize,
    end_ts: usize,
    sequence: u64,
) -> RecordBatch {
    assert!(end_ts > start_ts);
    let flat_schema = to_flat_sst_arrow_schema(metadata, &FlatSchemaOptions::default());
    let num_rows = end_ts - start_ts;

    let mut columns: Vec<ArrayRef> = Vec::new();

    // tag_0, tag_1 as dict string
    let mut tag_0_builder = StringDictionaryBuilder::<UInt32Type>::new();
    let mut tag_1_builder = StringDictionaryBuilder::<UInt32Type>::new();
    for _ in 0..num_rows {
        tag_0_builder.append_value(&tags[0]);
        tag_1_builder.append_value(&tags[1]);
    }
    columns.push(Arc::new(tag_0_builder.finish()));
    columns.push(Arc::new(tag_1_builder.finish()));

    // field_0: UInt64 (row index as value)
    let field_values: Vec<u64> = (start_ts..end_ts).map(|v| v as u64).collect();
    columns.push(Arc::new(UInt64Array::from(field_values)));

    // ts: TimestampMillisecond
    let timestamps: Vec<i64> = (start_ts..end_ts).map(|v| v as i64).collect();
    columns.push(Arc::new(TimestampMillisecondArray::from(timestamps)));

    // encoded primary key (dense, binary dictionary)
    let tags_ref: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
    let pk = encode_dense_primary_key(&tags_ref);
    let mut pk_builder = BinaryDictionaryBuilder::<UInt32Type>::new();
    for _ in 0..num_rows {
        pk_builder.append(&pk).unwrap();
    }
    columns.push(Arc::new(pk_builder.finish()));

    // sequence: UInt64
    columns.push(Arc::new(UInt64Array::from_value(sequence, num_rows)));

    // op_type: Put (1)
    columns.push(Arc::new(UInt8Array::from_value(
        OpType::Put as u8,
        num_rows,
    )));

    RecordBatch::try_new(flat_schema, columns).unwrap()
}

/// Build a [`FileMeta`] from [`SstInfo`] returned by the writer.
fn file_meta_from_sst_info(info: &SstInfo, region_id: RegionId, sequence: u64) -> FileMeta {
    FileMeta {
        region_id,
        file_id: info.file_id,
        time_range: info.time_range,
        level: 0,
        file_size: info.file_size,
        max_row_group_uncompressed_size: info.max_row_group_uncompressed_size,
        available_indexes: Default::default(),
        indexes: Default::default(),
        index_file_size: 0,
        index_version: 0,
        num_rows: info.num_rows as u64,
        num_row_groups: info.num_row_groups,
        sequence: NonZeroU64::new(sequence),
        partition_expr: None,
        num_series: info.num_series,
    }
}

// ---------------------------------------------------------------------------
// Seed manifest loading
// ---------------------------------------------------------------------------

/// Seed source descriptor for summary output.
#[allow(dead_code)]
enum SeedSource {
    Checkpoint(PathBuf),
    DeltaDir(PathBuf),
}

/// Result of loading a seed manifest, carrying metadata about the load.
struct SeedResult {
    manifest: RegionManifest,
    last_version: u64,
    existing_file_count: usize,
    source: SeedSource,
}

/// Validate that at most one seed source is specified.
fn validate_seed_args(checkpoint: &Option<PathBuf>, delta_dir: &Option<PathBuf>) {
    if checkpoint.is_some() && delta_dir.is_some() {
        eprintln!("ERROR: cannot specify both --seed-checkpoint and --seed-delta-dir");
        std::process::exit(1);
    }
}

/// Load seed from one of the seed sources. Returns `None` if neither is provided.
fn load_seed(
    checkpoint: &Option<PathBuf>,
    delta_dir: &Option<PathBuf>,
    region_id: u64,
) -> Option<SeedResult> {
    if let Some(dir) = delta_dir {
        Some(replay_delta_dir(dir, region_id))
    } else {
        checkpoint
            .as_ref()
            .map(|ck| load_seed_checkpoint(ck, region_id))
    }
}

/// Load seed manifest from a checkpoint file.
fn load_seed_checkpoint(seed_path: &PathBuf, region_id: u64) -> SeedResult {
    let seed_bytes = fs::read(seed_path).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot read seed checkpoint {:?}: {e}", seed_path);
        std::process::exit(1);
    });

    let seed_checkpoint = RegionCheckpoint::decode(&seed_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to decode seed checkpoint: {e}");
        std::process::exit(1);
    });

    let manifest = seed_checkpoint.checkpoint.as_ref().unwrap_or_else(|| {
        eprintln!("ERROR: seed checkpoint has no inner manifest (checkpoint field is None)");
        std::process::exit(1);
    });

    let expected = RegionId::from(region_id);
    let actual = manifest.metadata.region_id;
    if actual != expected {
        eprintln!(
            "ERROR: region_id mismatch: expected {} (CLI), got {} (seed manifest metadata)",
            expected.as_u64(),
            actual.as_u64(),
        );
        eprintln!("  full IDs: expected={expected:?}, actual={actual:?}");
        std::process::exit(1);
    }

    println!(
        "  seed checkpoint last_version={} compacted_actions={} existing_files={}",
        seed_checkpoint.last_version,
        seed_checkpoint.compacted_actions,
        manifest.files.len()
    );

    SeedResult {
        existing_file_count: manifest.files.len(),
        last_version: seed_checkpoint.last_version,
        manifest: manifest.clone(),
        source: SeedSource::Checkpoint(seed_path.clone()),
    }
}

/// Load seed manifest by replaying delta JSON files from a directory.
fn replay_delta_dir(dir: &PathBuf, region_id: u64) -> SeedResult {
    // Collect and sort JSON files (reject .json.gz)
    let mut delta_paths: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot read delta dir {:?}: {e}", dir);
        std::process::exit(1);
    }) {
        let entry = entry.unwrap_or_else(|e| {
            eprintln!("ERROR: directory entry error in {:?}: {e}", dir);
            std::process::exit(1);
        });
        let path = entry.path();
        let fname = path.file_name().unwrap_or_default().to_string_lossy();

        if path.is_file() && fname.ends_with(".json") && !fname.ends_with(".json.gz") {
            delta_paths.push(path);
        } else if fname.ends_with(".json.gz") {
            eprintln!(
                "ERROR: gzip-compressed delta found ({}) — .json.gz not supported. \
                 Only uncompressed .json delta files are accepted.",
                fname,
            );
            std::process::exit(1);
        }
        // skip other files silently (e.g. _last_checkpoint, .checkpoint files)
    }

    if delta_paths.is_empty() {
        eprintln!("ERROR: no .json delta files found in {:?}", dir);
        std::process::exit(1);
    }

    // Sort by filename (which is the version number, zero-padded)
    delta_paths.sort();

    // Print the sorted file list
    println!("  delta files ({} total):", delta_paths.len());
    for p in &delta_paths {
        println!(
            "    {}",
            p.file_name().unwrap_or_default().to_string_lossy()
        );
    }

    // Replay deltas into a fresh RegionManifestBuilder
    let mut builder = RegionManifestBuilder::default();
    let mut last_version: u64 = 0;

    for path in &delta_paths {
        // Extract version from filename (e.g. "00000000000000000001.json" -> 1)
        let fname = path.file_name().unwrap_or_default().to_string_lossy();
        let version_str = fname.trim_end_matches(".json");
        let delta_version: u64 = version_str.parse().unwrap_or_else(|_| {
            eprintln!(
                "ERROR: cannot parse version from delta filename '{}'",
                fname
            );
            std::process::exit(1);
        });

        let raw = fs::read(path).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot read delta file {:?}: {e}", path);
            std::process::exit(1);
        });

        let action_list = RegionMetaActionList::decode(&raw).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot decode delta {:?}: {e}", path);
            std::process::exit(1);
        });

        let action_count = action_list.actions.len();
        for action in action_list.actions {
            match action {
                RegionMetaAction::Change(change) => {
                    builder.apply_change(delta_version, change);
                }
                RegionMetaAction::PartitionExprChange(change) => {
                    builder.apply_partition_expr_change(delta_version, change);
                }
                RegionMetaAction::Edit(edit) => {
                    builder.apply_edit(delta_version, edit);
                }
                RegionMetaAction::Truncate(truncate) => {
                    builder.apply_truncate(delta_version, truncate);
                }
                RegionMetaAction::Remove(_) => {
                    eprintln!(
                        "ERROR: delta {:?} contains a Remove action. \
                         Seed regions that have been removed cannot be used for this tool.",
                        path
                    );
                    std::process::exit(1);
                }
            }
        }

        last_version = last_version.max(delta_version);
        println!("  replayed delta {}: {} action(s)", fname, action_count);
    }

    if !builder.contains_metadata() {
        eprintln!(
            "ERROR: after replaying {} delta(s), metadata is still not set. \
             A Change action with metadata is required.",
            delta_paths.len()
        );
        std::process::exit(1);
    }

    let manifest = builder.try_build().unwrap_or_else(|e| {
        eprintln!("ERROR: RegionManifestBuilder::try_build failed: {e}");
        std::process::exit(1);
    });

    let expected = RegionId::from(region_id);
    let actual = manifest.metadata.region_id;
    if actual != expected {
        eprintln!(
            "ERROR: region_id mismatch: expected {} (CLI), got {} (replayed manifest metadata)",
            expected.as_u64(),
            actual.as_u64(),
        );
        eprintln!("  full IDs: expected={expected:?}, actual={actual:?}");
        std::process::exit(1);
    }

    println!("  seed_last_version (max delta): {}", last_version);

    let existing_files = manifest.files.len();
    SeedResult {
        existing_file_count: existing_files,
        last_version,
        manifest,
        source: SeedSource::DeltaDir(dir.clone()),
    }
}

// ---------------------------------------------------------------------------
// Readback validation
// ---------------------------------------------------------------------------

/// Validate readback by opening each SST file with ParquetReaderBuilder
/// and checking that the number of rows matches.
async fn validate_readback(
    table_dir: &str,
    files: &HashMap<FileId, FileMeta>,
    object_store: ObjectStore,
) -> Result<(), String> {
    let file_purger: FilePurgerRef = Arc::new(NoopFilePurger);
    let mut total_rows_read = 0usize;

    for (file_id, meta) in files {
        let file_handle = FileHandle::new(meta.clone(), file_purger.clone());
        let builder = ParquetReaderBuilder::new(
            table_dir.to_string(),
            PathType::Bare,
            file_handle,
            object_store.clone(),
        );

        let reader = builder
            .build()
            .await
            .map_err(|e| format!("readback build failed for {}: {e}", file_id))?;

        let mut reader =
            reader.ok_or_else(|| format!("readback: no reader produced for {file_id}"))?;

        let mut rows_in_file = 0usize;
        while let Some(_batch) = reader
            .next_record_batch()
            .await
            .map_err(|e| format!("readback read failed for {}: {e}", file_id))?
        {
            rows_in_file += _batch.num_rows();
        }
        total_rows_read += rows_in_file;

        if rows_in_file as u64 != meta.num_rows {
            return Err(format!(
                "readback row count mismatch for {file_id}: expected {}, got {rows_in_file}",
                meta.num_rows
            ));
        }
    }

    let expected_total: u64 = files.values().map(|m| m.num_rows).sum();
    if total_rows_read as u64 != expected_total {
        return Err(format!(
            "readback total row count mismatch: expected {expected_total}, got {total_rows_read}"
        ));
    }

    println!(
        "  Readback validated: {total_rows_read} rows across {} files",
        files.len()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // -- Validation --
    if args.sst_count == 0 {
        eprintln!("ERROR: sst-count must be greater than 0");
        std::process::exit(1);
    }
    if args.rows_per_sst == 0 {
        eprintln!("ERROR: rows-per-sst must be greater than 0");
        std::process::exit(1);
    }
    if args.row_group_size == 0 {
        eprintln!("ERROR: row-group-size must be greater than 0");
        std::process::exit(1);
    }
    if args.sst_count > 1000 && !args.allow_large {
        eprintln!(
            "ERROR: sst-count={} exceeds 1000. Specify --allow-large to acknowledge.",
            args.sst_count
        );
        std::process::exit(1);
    }

    // Validate seed args: at most one seed source
    validate_seed_args(&args.seed_checkpoint, &args.seed_delta_dir);

    let region_id = RegionId::from(args.region_id);
    let expected_region_name = region_name(region_id.table_id(), region_id.region_sequence());
    let table_dir_trimmed = args.table_dir.trim_end_matches('/');
    if table_dir_trimmed.is_empty() {
        eprintln!("ERROR: table-dir must not be empty");
        std::process::exit(1);
    }
    if Path::new(table_dir_trimmed).is_absolute() {
        eprintln!("ERROR: --table-dir must be relative to the object-store root, not absolute");
        std::process::exit(1);
    }
    if table_dir_trimmed.rsplit('/').next() == Some(expected_region_name.as_str()) {
        eprintln!(
            "ERROR: --table-dir must be the table-level directory, not the region directory. \
             Pass the parent of `{}` instead.",
            expected_region_name
        );
        std::process::exit(1);
    }
    let region_dir =
        mito2::sst::location::region_dir_from_table_dir(&args.table_dir, region_id, PathType::Bare);

    // -- Load seed manifest (if provided) --
    let seed = load_seed(&args.seed_checkpoint, &args.seed_delta_dir, args.region_id);
    let is_seed_mode = seed.is_some();

    // -- Build (or load from seed) region metadata --
    let metadata: RegionMetadataRef = if let Some(ref seed_result) = seed {
        // Validate seed metadata compatibility
        if let Err(e) = validate_seed_metadata_compatible(&seed_result.manifest.metadata, region_id)
        {
            eprintln!("ERROR: seed metadata is not schema-compatible with this tool:");
            eprintln!("  {e}");
            std::process::exit(1);
        }
        // Guard version
        if args.checkpoint_version < seed_result.last_version {
            eprintln!(
                "ERROR: --checkpoint-version {} is less than seed last_version {}. \
                 The generated checkpoint would be overwritten by existing delta replay.",
                args.checkpoint_version, seed_result.last_version,
            );
            std::process::exit(1);
        }
        println!("  seed metadata validated: schema-compatible ✓");
        seed_result.manifest.metadata.clone()
    } else {
        Arc::new(build_region_metadata(region_id))
    };

    let metadata_source = if is_seed_mode { "seed" } else { "synthetic" };
    let target_sst_format = seed
        .as_ref()
        .map(|s| s.manifest.sst_format)
        .unwrap_or(FormatType::PrimaryKey);
    let seed_source_label: Option<&str> = seed.as_ref().map(|s| match &s.source {
        SeedSource::Checkpoint(p) => p.to_str().unwrap_or("checkpoint"),
        SeedSource::DeltaDir(p) => p.to_str().unwrap_or("delta-dir"),
    });

    // -- Print plan --
    println!("=== gc_readable_sst_fixture ===");
    println!("  out_dir:       {}", args.out_dir.display());
    println!(
        "  region_id:     {} (0x{:016x})",
        region_id.as_u64(),
        region_id.as_u64()
    );
    println!("  table_dir:     {}", args.table_dir);
    println!("  region_dir:    {}", region_dir);
    println!("  sst_count:     {}", args.sst_count);
    println!("  rows_per_sst:  {}", args.rows_per_sst);
    println!("  row_group_size: {}", args.row_group_size);
    println!("  version:       {}", args.checkpoint_version);
    println!("  metadata:      {}", metadata_source);
    println!("  sst_format:    {:?}", target_sst_format);
    if let Some(label) = seed_source_label {
        println!("  seed_source:   {}", label);
    }
    if let Some(ref seed_result) = seed {
        println!(
            "  seed_last_version: {} ({} existing files)",
            seed_result.last_version, seed_result.existing_file_count
        );
        println!(
            "  seed_files_replaced: {}",
            seed_result.existing_file_count > 0
        );
    }
    println!("  skip_readback: {}", args.skip_readback);
    println!("  dry_run:       {}", args.dry_run);

    if args.dry_run {
        println!("DRY-RUN complete — no files written.");
        return;
    }

    // -- Prepare output directories --
    let obj_store_dir = args.out_dir.join("object-store");
    let manifest_dir = args.out_dir.join("manifest");
    fs::create_dir_all(&obj_store_dir).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot create {:?}: {e}", obj_store_dir);
        std::process::exit(1);
    });
    fs::create_dir_all(&manifest_dir).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot create {:?}: {e}", manifest_dir);
        std::process::exit(1);
    });

    // -- Create local FS object store --
    let ostorage = {
        let builder = FsBuilder::default().root(&obj_store_dir.to_string_lossy());
        ObjectStore::new(builder)
            .unwrap_or_else(|e| {
                eprintln!("ERROR: cannot create object store: {e}");
                std::process::exit(1);
            })
            .finish()
    };

    // -- Write SST files --
    let mut files: HashMap<FileId, FileMeta> = HashMap::with_capacity(args.sst_count);

    for i in 0..args.sst_count {
        // Deterministic tags: tag_0 = tag{0..9}, tag_1 = series{i}
        let tag_0_val = format!("tag{}", i % 10);
        let tag_1_val = format!("series{}", i);
        let tags = vec![tag_0_val, tag_1_val];

        let start_ts = i * args.rows_per_sst;
        let end_ts = start_ts + args.rows_per_sst;
        let sequence = 1000 + i as u64;

        let batch = generate_record_batch(&metadata, &tags, start_ts, end_ts, sequence);
        let source = FlatSource::Iter(Box::new(vec![batch].into_iter().map(Ok)));

        let path_provider = FixedPathProvider {
            table_dir: args.table_dir.clone(),
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut writer = ParquetWriter::new_with_object_store(
            ostorage.clone(),
            metadata.clone(),
            IndexConfig::default(),
            NoopIndexBuilder,
            path_provider,
            &mut metrics,
        )
        .await;

        let write_opts = WriteOptions {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            row_group_size: args.row_group_size,
            max_file_size: None,
        };

        let sst_infos = match target_sst_format {
            FormatType::PrimaryKey => {
                writer
                    .write_all_flat_as_primary_key(source, None, &write_opts)
                    .await
            }
            FormatType::Flat => writer.write_all_flat(source, None, &write_opts).await,
        }
        .unwrap_or_else(|e| {
            eprintln!("ERROR: SST write failed for file {}: {e}", i);
            std::process::exit(1);
        });

        for info in sst_infos {
            let file_meta = file_meta_from_sst_info(&info, region_id, sequence);
            if files.contains_key(&info.file_id) {
                eprintln!(
                    "ERROR: duplicate file id generated by SST writer: {}",
                    info.file_id
                );
                std::process::exit(1);
            }
            println!(
                "  SST file {}: id={} rows={} size={} time_range=({}, {})",
                i,
                info.file_id,
                info.num_rows,
                info.file_size,
                info.time_range.0.to_iso8601_string(),
                info.time_range.1.to_iso8601_string(),
            );
            files.insert(info.file_id, file_meta);
        }
    }

    // -- Build manifest --
    let overall_max_seq = files
        .values()
        .filter_map(|m| m.sequence)
        .map(|s| s.get())
        .max()
        .unwrap_or(0);

    // In seed mode, preserve seed's region-level metadata fields (truncated_entry_id,
    // compaction_time_window, sst_format, append_mode) and write generated SSTs in the
    // same format, but replace files.
    // removed_files are emptied: conservative for MVP, recorded in summary.
    let (sst_format, compaction_time_window, truncated_entry_id, append_mode) =
        if let Some(ref seed_result) = seed {
            let sm = &seed_result.manifest;
            (
                sm.sst_format,
                sm.compaction_time_window,
                sm.truncated_entry_id,
                sm.append_mode,
            )
        } else {
            (FormatType::PrimaryKey, None, None, None)
        };

    let seed_files_replaced = seed
        .as_ref()
        .map(|s| s.existing_file_count > 0)
        .unwrap_or(false);

    let manifest = RegionManifest {
        metadata: metadata.clone(),
        files,
        removed_files: RemovedFilesRecord::default(), // conservative: no removed files
        flushed_entry_id: overall_max_seq,
        flushed_sequence: overall_max_seq,
        committed_sequence: Some(overall_max_seq),
        manifest_version: args.checkpoint_version,
        truncated_entry_id,
        compaction_time_window,
        sst_format,
        append_mode,
    };

    let file_count = manifest.files.len();

    let checkpoint = RegionCheckpoint {
        last_version: args.checkpoint_version,
        compacted_actions: file_count,
        checkpoint: Some(manifest.clone()),
    };

    // -- Write checkpoint --
    let checkpoint_bytes = checkpoint.encode().unwrap_or_else(|e| {
        eprintln!("ERROR: checkpoint encode failed: {e}");
        std::process::exit(1);
    });

    let checkpoint_path = manifest_dir.join(format!("{:020}.checkpoint", args.checkpoint_version));
    fs::write(&checkpoint_path, &checkpoint_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write checkpoint {:?}: {e}", checkpoint_path);
        std::process::exit(1);
    });
    println!("  Wrote checkpoint: {}", checkpoint_path.display());

    // -- Write _last_checkpoint --
    let last_checkpoint_json = serde_json::json!({
        "size": checkpoint_bytes.len(),
        "version": args.checkpoint_version,
        "checksum": null,
        "extend_metadata": {},
    });
    let last_checkpoint_bytes = serde_json::to_vec_pretty(&last_checkpoint_json).unwrap();
    let last_checkpoint_path = manifest_dir.join("_last_checkpoint");
    fs::write(&last_checkpoint_path, &last_checkpoint_bytes).unwrap_or_else(|e| {
        eprintln!(
            "ERROR: cannot write _last_checkpoint {:?}: {e}",
            last_checkpoint_path
        );
        std::process::exit(1);
    });
    println!(
        "  Wrote _last_checkpoint: {}",
        last_checkpoint_path.display()
    );

    // -- Write files.jsonl --
    let files_jsonl_path = args.out_dir.join("files.jsonl");
    {
        let f = fs::File::create(&files_jsonl_path).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot create {:?}: {e}", files_jsonl_path);
            std::process::exit(1);
        });
        let mut writer = BufWriter::new(f);
        let mut file_entries: Vec<_> = manifest.files.iter().collect();
        file_entries.sort_by_key(|(file_id, _)| file_id.to_string());
        for (file_id, meta) in file_entries {
            let line = serde_json::json!({
                "file_id": file_id.to_string(),
                "region_id": meta.region_id.as_u64(),
                "object_path": mito2::sst::location::sst_file_path(
                    &args.table_dir,
                    RegionFileId::new(meta.region_id, *file_id),
                    PathType::Bare,
                ),
                "time_range_start": meta.time_range.0.value(),
                "time_range_end": meta.time_range.1.value(),
                "num_rows": meta.num_rows,
                "num_row_groups": meta.num_row_groups,
                "file_size": meta.file_size,
                "num_series": meta.num_series,
                "sequence": meta.sequence.map(|s| s.get()),
            });
            writeln!(writer, "{}", serde_json::to_string(&line).unwrap()).unwrap();
        }
    }
    println!("  Wrote files.jsonl: {} entries", file_count);

    // -- Readback validation (optional) --
    let readback_ok = if !args.skip_readback {
        match validate_readback(&args.table_dir, &manifest.files, ostorage.clone()).await {
            Ok(()) => {
                println!("  Readback: PASSED ✓");
                true
            }
            Err(e) => {
                eprintln!("  Readback: FAILED — {e}");
                false
            }
        }
    } else {
        println!("  Readback: SKIPPED");
        false // not applicable
    };

    // -- Write summary.json --
    let mut summary = serde_json::json!({
        "out_dir": args.out_dir.to_string_lossy(),
        "region_id": region_id.as_u64(),
        "table_id": region_id.table_id(),
        "region_number": region_id.region_number(),
        "table_dir": args.table_dir,
        "region_dir": region_dir,
        "sst_count": args.sst_count,
        "rows_per_sst": args.rows_per_sst,
        "row_group_size": args.row_group_size,
        "version": args.checkpoint_version,
        "file_count": file_count,
        "total_rows": file_count as u64 * args.rows_per_sst as u64,
        "object_store_dir": obj_store_dir.to_string_lossy(),
        "checkpoint_path": checkpoint_path.to_string_lossy(),
        "last_checkpoint_path": last_checkpoint_path.to_string_lossy(),
        "files_jsonl_path": files_jsonl_path.to_string_lossy(),
        "readback_validated": !args.skip_readback,
        "readback_passed": readback_ok,
        "metadata_source": metadata_source,
    });

    if let Some(ref seed_result) = seed {
        summary["seed_source"] = serde_json::json!(match &seed_result.source {
            SeedSource::Checkpoint(p) => p.to_string_lossy(),
            SeedSource::DeltaDir(p) => p.to_string_lossy(),
        });
        summary["seed_last_version"] = serde_json::json!(seed_result.last_version);
        summary["seed_existing_file_count"] = serde_json::json!(seed_result.existing_file_count);
        summary["seed_files_replaced"] = serde_json::json!(seed_files_replaced);
        summary["removed_files_policy"] =
            serde_json::json!("empty — seed removed_files discarded for MVP safety");
    }

    let summary_bytes = serde_json::to_vec_pretty(&summary).unwrap();
    let summary_path = args.out_dir.join("summary.json");
    fs::write(&summary_path, &summary_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write summary {:?}: {e}", summary_path);
        std::process::exit(1);
    });
    println!("  Wrote summary: {}", summary_path.display());

    println!();
    println!("Done.");
}
