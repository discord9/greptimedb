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

//! Offline synthetic checkpoint generator for Test C2 (active-filemeta lab).
//!
//! Reads a seed checkpoint **or** replays delta JSON files,
//! generates synthetic active [`FileMeta`] entries,
//! and writes a new checkpoint + `_last_checkpoint` to an output directory.
//!
//! Intended as a lab-only tool; does not contact Kubernetes or MinIO.

use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;

use clap::Parser;
use mito2::manifest::action::{
    RegionCheckpoint, RegionManifest, RegionManifestBuilder, RegionMetaAction,
    RegionMetaActionList, RemovedFilesRecord,
};
use mito2::sst::file::{FileMeta, FileTimeRange};
use store_api::storage::{FileId, RegionId};

/// CLI for generating synthetic checkpoint fixtures with active FileMeta pressure.
#[derive(Parser, Debug)]
#[command(name = "gc_synthetic_manifest")]
#[command(about = "Generate synthetic checkpoint with active FileMeta entries for GC lab testing")]
struct Args {
    /// Path to a local seed checkpoint JSON file (uncompressed `.checkpoint`).
    #[arg(long, value_name = "PATH")]
    seed_checkpoint: Option<PathBuf>,

    /// Directory containing delta JSON seed files (e.g. `00000000000000000000.json`).
    /// Files are read in filename-sorted order and replayed to reconstruct the seed manifest.
    /// `.json.gz` files are not supported.
    #[arg(long, value_name = "DIR")]
    seed_delta_dir: Option<PathBuf>,

    /// Output directory where checkpoint files will be written.
    #[arg(long, value_name = "DIR")]
    out_dir: PathBuf,

    /// Number of synthetic active FileMeta entries to generate.
    #[arg(long, value_name = "N")]
    count: usize,

    /// Region ID that must match the seed manifest metadata.
    #[arg(long, value_name = "ID")]
    region_id: u64,

    /// Manifest/checkpoint version to set (default: 1).
    #[arg(long, default_value = "1")]
    version: u64,

    /// File size to set on each generated FileMeta (default: 0).
    #[arg(long, default_value = "0")]
    file_size: u64,

    /// Number of rows per generated FileMeta (default: 100).
    #[arg(long, default_value = "100")]
    num_rows: u64,

    /// Base sequence number — each file gets sequence = sequence_base + i (default: 1).
    #[arg(long, default_value = "1")]
    sequence_base: u64,

    /// Dry-run: decode seed, print summary, but do not write any output files.
    #[arg(long)]
    dry_run: bool,

    /// Required safety flag when count exceeds 100_000.
    #[arg(long)]
    allow_large: bool,
}

fn main() {
    let args = Args::parse();

    // Safety gate: refuse large synthetic manifests without explicit --allow-large.
    if args.count == 0 {
        eprintln!("ERROR: count must be greater than 0");
        std::process::exit(1);
    }
    if args.count > 100_000 && !args.allow_large {
        eprintln!(
            "ERROR: count={} exceeds 100_000. Specify --allow-large to acknowledge memory risk.",
            args.count
        );
        std::process::exit(1);
    }

    // Exactly one of --seed-checkpoint or --seed-delta-dir must be provided.
    match (&args.seed_checkpoint, &args.seed_delta_dir) {
        (Some(_), Some(_)) => {
            eprintln!("ERROR: cannot specify both --seed-checkpoint and --seed-delta-dir");
            std::process::exit(1);
        }
        (None, None) => {
            eprintln!("ERROR: must specify exactly one of --seed-checkpoint or --seed-delta-dir");
            std::process::exit(1);
        }
        _ => {}
    }

    // ------------------------------------------------------------------
    // 1. Load seed manifest (checkpoint mode or delta-replay mode)
    // ------------------------------------------------------------------
    let is_delta_mode = args.seed_delta_dir.is_some();

    let seed_manifest = if is_delta_mode {
        replay_delta_dir(args.seed_delta_dir.as_ref().unwrap(), args.region_id)
    } else {
        load_seed_checkpoint(args.seed_checkpoint.as_ref().unwrap(), args.region_id)
    };

    let region_id = RegionId::from(args.region_id);

    let existing_files = seed_manifest.files.len();
    let total_after = args.count;
    // Rough memory estimate: FileMeta + HashMap overhead ≈ 512 bytes each
    let estimate_mb = (total_after as f64 * 512.0) / (1024.0 * 1024.0);

    // ------------------------------------------------------------------
    // 2. Print summary
    // ------------------------------------------------------------------
    println!("=== gc_synthetic_manifest ===");
    if is_delta_mode {
        println!(
            "seed_source:     delta-dir {:?}",
            args.seed_delta_dir.as_ref().unwrap()
        );
    } else {
        println!(
            "seed_source:     checkpoint {:?}",
            args.seed_checkpoint.as_ref().unwrap()
        );
    }
    println!("  seed_last_version: {}", seed_manifest.manifest_version);
    println!("  existing files:    {}", existing_files);
    println!(
        "region_id:      {} (0x{:016x})",
        region_id.as_u64(),
        region_id.as_u64()
    );
    println!("count:          {}", args.count);
    println!("total after:    {}", total_after);
    println!("version:        {}", args.version);
    println!("file_size:      {}", args.file_size);
    println!("num_rows:       {}", args.num_rows);
    println!(
        "sequence range: {}..{}",
        args.sequence_base,
        args.sequence_base + args.count as u64
    );
    println!("dry_run:        {}", args.dry_run);
    println!("est memory:     ~{:.1} MB", estimate_mb);

    // Guard: generated version must not be less than seed last_version
    let seed_last_version = seed_manifest.manifest_version;
    if args.version < seed_last_version {
        eprintln!(
            "ERROR: --version {} is less than seed_last_version {}. \
             The generated checkpoint would be overwritten by existing delta replay.",
            args.version, seed_last_version,
        );
        std::process::exit(1);
    }

    if args.dry_run {
        println!("DRY-RUN complete — no files written.");
        return;
    }

    // Ensure out-dir exists before writing any output.
    fs::create_dir_all(&args.out_dir).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot create out-dir {:?}: {e}", args.out_dir);
        std::process::exit(1);
    });

    // ------------------------------------------------------------------
    // 3. Build synthetic files
    // ------------------------------------------------------------------
    let mut new_files: HashMap<FileId, FileMeta> = HashMap::with_capacity(args.count);

    let max_new_seq = args.sequence_base + args.count as u64 - 1;
    let overall_max_seq = max_new_seq;

    for i in 0..args.count {
        let file_id = FileId::random();
        let seq_val = args.sequence_base + i as u64;

        let file_meta = FileMeta {
            region_id,
            file_id,
            time_range: FileTimeRange::default(), // zero timestamps
            level: 0,
            file_size: args.file_size,
            max_row_group_uncompressed_size: 0,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: args.num_rows,
            num_row_groups: 1,
            sequence: NonZeroU64::new(seq_val), // None if 0, Some if >0
            partition_expr: None,
            num_series: 1,
        };

        new_files.insert(file_id, file_meta);
    }

    // ------------------------------------------------------------------
    // 3b. Emit active file reference list (for C2b object materialization)
    // ------------------------------------------------------------------
    let files_jsonl_path = args.out_dir.join("files.jsonl");
    {
        let f = fs::File::create(&files_jsonl_path).unwrap_or_else(|e| {
            eprintln!(
                "ERROR: cannot create files.jsonl {:?}: {e}",
                files_jsonl_path
            );
            std::process::exit(1);
        });
        let mut writer = BufWriter::new(f);
        for file_id in new_files.keys() {
            let line = serde_json::json!({
                "file_id": file_id.to_string(),
                "region_id": region_id.as_u64(),
            });
            writeln!(
                writer,
                "{}",
                serde_json::to_string(&line).unwrap_or_else(|e| {
                    eprintln!("ERROR: JSON encode failed for file_id {}: {e}", file_id);
                    std::process::exit(1);
                })
            )
            .unwrap_or_else(|e| {
                eprintln!("ERROR: write files.jsonl failed: {e}");
                std::process::exit(1);
            });
        }
        // Buffer will flush on drop.
    }
    println!(
        "  wrote {} file refs → {}",
        args.count,
        files_jsonl_path.display()
    );

    // ------------------------------------------------------------------
    // 4. Build new manifest
    // ------------------------------------------------------------------
    let new_manifest = RegionManifest {
        metadata: seed_manifest.metadata.clone(),
        files: new_files,
        removed_files: RemovedFilesRecord::default(),
        flushed_entry_id: overall_max_seq,
        flushed_sequence: overall_max_seq,
        committed_sequence: Some(overall_max_seq),
        manifest_version: args.version,
        truncated_entry_id: None,
        compaction_time_window: seed_manifest.compaction_time_window,
        sst_format: seed_manifest.sst_format,
        append_mode: seed_manifest.append_mode,
    };

    let new_checkpoint = RegionCheckpoint {
        last_version: args.version,
        compacted_actions: args.count,
        checkpoint: Some(new_manifest),
    };

    // ------------------------------------------------------------------
    // 5. Encode and write
    // ------------------------------------------------------------------

    let checkpoint_bytes = new_checkpoint.encode().unwrap_or_else(|e| {
        eprintln!("ERROR: checkpoint encode failed: {e}");
        std::process::exit(1);
    });

    let checkpoint_path = args
        .out_dir
        .join(format!("{:020}.checkpoint", args.version));
    fs::write(&checkpoint_path, &checkpoint_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write checkpoint {:?}: {e}", checkpoint_path);
        std::process::exit(1);
    });

    // Manually build _last_checkpoint JSON (private CheckpointMetadata struct).
    let last_checkpoint_json = serde_json::json!({
        "size": checkpoint_bytes.len(),
        "version": args.version,
        "checksum": null,
        "extend_metadata": {},
    });
    let last_checkpoint_bytes =
        serde_json::to_vec_pretty(&last_checkpoint_json).unwrap_or_else(|e| {
            eprintln!("ERROR: last_checkpoint JSON encode failed: {e}");
            std::process::exit(1);
        });
    let last_checkpoint_path = args.out_dir.join("_last_checkpoint");
    fs::write(&last_checkpoint_path, &last_checkpoint_bytes).unwrap_or_else(|e| {
        eprintln!(
            "ERROR: cannot write _last_checkpoint {:?}: {e}",
            last_checkpoint_path
        );
        std::process::exit(1);
    });

    // ------------------------------------------------------------------
    // 6. Write summary.json
    // ------------------------------------------------------------------
    let summary = serde_json::json!({
        "count": args.count,
        "region_id": region_id.as_u64(),
        "table_id": region_id.table_id(),
        "region_number": region_id.region_number(),
        "version": args.version,
        "checkpoint_bytes": checkpoint_bytes.len(),
        "last_checkpoint_bytes": last_checkpoint_bytes.len(),
        "file_size": args.file_size,
        "num_rows": args.num_rows,
        "sequence_base": args.sequence_base,
        "sequence_max": overall_max_seq,
        "existing_files_before": existing_files,
        "total_files_after": total_after,
        "seed_source": if is_delta_mode { "delta-dir" } else { "checkpoint" },
        "seed_last_version": seed_last_version,
        "output_checkpoint": checkpoint_path.to_string_lossy(),
        "output_last_checkpoint": last_checkpoint_path.to_string_lossy(),
        "output_files_jsonl": files_jsonl_path.to_string_lossy(),
        "files_jsonl_count": args.count,
        "est_memory_mb": format!("{:.1}", estimate_mb),
    });
    let summary_bytes = serde_json::to_vec_pretty(&summary).unwrap_or_else(|e| {
        eprintln!("ERROR: summary JSON encode failed: {e}");
        std::process::exit(1);
    });
    let summary_path = args.out_dir.join("summary.json");
    fs::write(&summary_path, &summary_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write summary {:?}: {e}", summary_path);
        std::process::exit(1);
    });

    println!();
    println!("Wrote checkpoint: {}", checkpoint_path.display());
    println!("  bytes: {}", checkpoint_bytes.len());
    println!("Wrote last_checkpoint: {}", last_checkpoint_path.display());
    println!("Wrote files.jsonl: {}", files_jsonl_path.display());
    println!("  entries: {}", args.count);
    println!("Wrote summary: {}", summary_path.display());
    println!("Done.");
}

/// Load seed manifest from a checkpoint file.
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
        "  seed checkpoint last_version={} compacted_actions={}",
        seed_checkpoint.last_version, seed_checkpoint.compacted_actions
    );

    manifest.clone()
}

/// Load seed manifest by replaying delta JSON files from a directory.
fn replay_delta_dir(dir: &PathBuf, region_id: u64) -> RegionManifest {
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
                         Seed regions that have been removed cannot be used for C2 testing.",
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

    manifest
}
