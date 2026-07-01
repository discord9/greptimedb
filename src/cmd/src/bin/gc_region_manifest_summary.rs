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

//! Read-only region manifest summary scanner for GC correctness tooling.
//!
//! Replays uncompressed `.json` region manifest deltas or decodes a checkpoint
//! and outputs a JSON summary including per-file FileMeta information.
//!
//! Used by `run_repartition_gc_correctness_smoke.py` to prove cross-region refs
//! in destination manifests after repartition.
//!
//! Intended as a lab-only tool; does not contact Kubernetes or MinIO.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use clap::Parser;
use mito2::manifest::action::{
    RegionCheckpoint, RegionManifest, RegionManifestBuilder, RegionMetaAction,
    RegionMetaActionList, RemovedFile,
};
use store_api::storage::RegionId;

/// CLI for scanning region manifests and extracting FileMeta summary.
#[derive(Parser, Debug)]
#[command(name = "gc_region_manifest_summary")]
#[command(about = "Scan region manifest deltas/checkpoint and output JSON FileMeta summary")]
struct Args {
    /// Region ID (required).
    #[arg(long, value_name = "ID")]
    region_id: u64,

    /// Directory containing delta JSON seed files (e.g. `00000000000000000000.json`).
    /// `.json.gz` files are not supported.
    #[arg(long, value_name = "DIR")]
    seed_delta_dir: Option<PathBuf>,

    /// Path to a seed checkpoint file (uncompressed `.checkpoint`).
    #[arg(long, value_name = "PATH")]
    seed_checkpoint: Option<PathBuf>,

    /// Optional output file path for JSON summary. If not provided, prints to stdout.
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Seed loading (minimal fork from gc_readable_sst_fixture)
// ---------------------------------------------------------------------------

fn load_seed_checkpoint(seed_path: &PathBuf, region_id: u64) -> RegionManifest {
    let seed_bytes = fs::read(seed_path).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot read seed checkpoint {:?}: {e}", seed_path);
        std::process::exit(1);
    });
    let seed_checkpoint = RegionCheckpoint::decode(&seed_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to decode seed checkpoint: {e}");
        std::process::exit(1);
    });
    let manifest = seed_checkpoint.checkpoint.as_ref().unwrap_or_else(|| {
        eprintln!("ERROR: seed checkpoint has no inner manifest");
        std::process::exit(1);
    });
    let expected = RegionId::from(region_id);
    if manifest.metadata.region_id != expected {
        eprintln!(
            "ERROR: region_id mismatch in seed checkpoint: expected {}, got {}",
            expected.as_u64(),
            manifest.metadata.region_id.as_u64(),
        );
        std::process::exit(1);
    }
    manifest.clone()
}

fn parse_manifest_version(path: &std::path::Path, suffix: &str) -> u64 {
    let fname = path.file_name().unwrap_or_default().to_string_lossy();
    let version_str = fname.trim_end_matches(suffix);
    version_str.parse().unwrap_or_else(|_| {
        eprintln!("ERROR: cannot parse version from manifest filename '{fname}'");
        std::process::exit(1);
    })
}

fn apply_action(
    builder: &mut RegionManifestBuilder,
    delta_version: u64,
    path: &PathBuf,
    action: RegionMetaAction,
) {
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
            eprintln!("ERROR: delta {:?} contains Remove — unsupported", path);
            std::process::exit(1);
        }
    }
}

fn replay_delta_dir(dir: &PathBuf, region_id: u64) -> RegionManifest {
    let mut delta_paths: Vec<PathBuf> = Vec::new();
    let mut checkpoint_paths: Vec<PathBuf> = Vec::new();
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
        } else if path.is_file() && fname.ends_with(".checkpoint") {
            checkpoint_paths.push(path);
        } else if fname.ends_with(".json.gz") {
            eprintln!(
                "ERROR: gzip-compressed delta found ({}) — .json.gz not supported",
                fname,
            );
            std::process::exit(1);
        }
    }
    if delta_paths.is_empty() && checkpoint_paths.is_empty() {
        eprintln!(
            "ERROR: no .json delta files or .checkpoint files found in {:?}",
            dir
        );
        std::process::exit(1);
    }
    delta_paths.sort();
    checkpoint_paths.sort_by_key(|path| parse_manifest_version(path, ".checkpoint"));

    let checkpoint_manifest = checkpoint_paths
        .last()
        .map(|path| load_seed_checkpoint(path, region_id));
    let checkpoint_version = checkpoint_manifest
        .as_ref()
        .map(|manifest| manifest.manifest_version);
    let mut builder = RegionManifestBuilder::with_checkpoint(checkpoint_manifest);
    for path in &delta_paths {
        let delta_version = parse_manifest_version(path, ".json");
        if checkpoint_version.is_some_and(|version| delta_version <= version) {
            continue;
        }
        let raw = fs::read(path).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot read delta file {:?}: {e}", path);
            std::process::exit(1);
        });
        let action_list = RegionMetaActionList::decode(&raw).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot decode delta {:?}: {e}", path);
            std::process::exit(1);
        });
        for action in action_list.actions {
            apply_action(&mut builder, delta_version, path, action);
        }
    }
    if !builder.contains_metadata() {
        eprintln!("ERROR: after replaying deltas, metadata is still not set");
        std::process::exit(1);
    }
    let manifest = builder.try_build().unwrap_or_else(|e| {
        eprintln!("ERROR: RegionManifestBuilder::try_build failed: {e}");
        std::process::exit(1);
    });
    let expected = RegionId::from(region_id);
    if manifest.metadata.region_id != expected {
        eprintln!(
            "ERROR: region_id mismatch in replayed manifest: expected {}, got {}",
            expected.as_u64(),
            manifest.metadata.region_id.as_u64(),
        );
        std::process::exit(1);
    }
    manifest
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args = Args::parse();

    // Validate: exactly one seed source
    if args.seed_delta_dir.is_some() && args.seed_checkpoint.is_some() {
        eprintln!("ERROR: cannot specify both --seed-delta-dir and --seed-checkpoint");
        std::process::exit(1);
    }
    if args.seed_delta_dir.is_none() && args.seed_checkpoint.is_none() {
        eprintln!("ERROR: must specify exactly one of --seed-delta-dir or --seed-checkpoint");
        std::process::exit(1);
    }

    let _region_id = RegionId::from(args.region_id);
    let manifest = if let Some(ref dir) = args.seed_delta_dir {
        replay_delta_dir(dir, args.region_id)
    } else {
        load_seed_checkpoint(args.seed_checkpoint.as_ref().unwrap(), args.region_id)
    };

    // Build summary
    let current_region_id = manifest.metadata.region_id.as_u64();
    let mut file_region_id_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_region_file_count: u64 = 0;
    let mut files_array: Vec<(String, Option<u64>, serde_json::Value)> = Vec::new();

    for (file_id, meta) in &manifest.files {
        let origin_region = meta.region_id.as_u64();
        let key = origin_region.to_string();
        *file_region_id_counts.entry(key).or_insert(0) += 1;

        if origin_region != current_region_id {
            cross_region_file_count += 1;
        }

        let index_version = meta.index_version();
        files_array.push((
            file_id.to_string(),
            index_version,
            serde_json::json!({
            "file_id": file_id.to_string(),
            "index_version": index_version,
            "file_region_id": origin_region,
            "current_region_id": current_region_id,
            "is_cross_region": origin_region != current_region_id,
            "time_range_start": meta.time_range.0.value(),
            "time_range_end": meta.time_range.1.value(),
            "file_size": meta.file_size,
            "num_rows": meta.num_rows,
            "num_row_groups": meta.num_row_groups,
            "level": meta.level,
            }),
        ));
    }
    files_array.sort_by(|left, right| (left.0.as_str(), left.1).cmp(&(right.0.as_str(), right.1)));
    let files_array: Vec<_> = files_array.into_iter().map(|(_, _, value)| value).collect();

    let mut removed_file_count: u64 = 0;
    let mut removed_index_count: u64 = 0;
    let mut removed_files_array: Vec<(String, Option<u64>, &'static str, i64, serde_json::Value)> =
        Vec::new();
    for removed_files in &manifest.removed_files.removed_files {
        for removed in &removed_files.files {
            let (kind, index_version) = match removed {
                RemovedFile::File(_, index_version) => {
                    removed_file_count += 1;
                    ("File", *index_version)
                }
                RemovedFile::Index(_, index_version) => {
                    removed_index_count += 1;
                    ("Index", Some(*index_version))
                }
            };
            let file_id = removed.file_id().to_string();
            removed_files_array.push((
                file_id.clone(),
                index_version,
                kind,
                removed_files.removed_at,
                serde_json::json!({
                    "kind": kind,
                    "file_id": file_id,
                    "index_version": index_version,
                    "removed_at": removed_files.removed_at,
                    "manifest_region_id": current_region_id,
                }),
            ));
        }
    }
    removed_files_array.sort_by(|left, right| {
        (left.0.as_str(), left.1, left.2, left.3).cmp(&(
            right.0.as_str(),
            right.1,
            right.2,
            right.3,
        ))
    });
    let removed_files_array: Vec<_> = removed_files_array
        .into_iter()
        .map(|(_, _, _, _, value)| value)
        .collect();
    let removed_count = removed_file_count + removed_index_count;

    let summary = serde_json::json!({
        "manifest_version": manifest.manifest_version,
        "region_id": current_region_id,
        "sst_format": format!("{:?}", manifest.sst_format),
        "file_count": manifest.files.len(),
        "file_region_id_counts": file_region_id_counts,
        "cross_region_file_count": cross_region_file_count,
        "removed_file_count": removed_file_count,
        "removed_index_count": removed_index_count,
        "removed_count": removed_count,
        "files": files_array,
        "removed_files": removed_files_array,
    });

    let output_json = serde_json::to_string_pretty(&summary).unwrap();

    if let Some(ref output_path) = args.output {
        fs::write(output_path, &output_json).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot write output to {:?}: {e}", output_path);
            std::process::exit(1);
        });
        println!("Summary written to {}", output_path.display());
    } else {
        println!("{}", output_json);
    }
}
