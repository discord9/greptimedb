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

//! Offline cross-region-reference GC fixture generator.
//!
//! Generates coordinated synthetic checkpoints for two regions (A and B)
//! where region A has expired `removed_files` containing protected overlap
//! file X and unprotected Y, while region B still has X active in its
//! manifest with `FileMeta.region_id = A`.
//!
//! This fixture enables a fast-GC smoke test: GC on A must protect X
//! (because B holds a cross-region reference) but may delete Y.
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
    RegionMetaActionList, RemovedFile, RemovedFilesRecord,
};
use mito2::sst::file::{FileMeta, FileTimeRange};
use store_api::storage::{FileId, RegionId};

/// CLI for the cross-region reference GC fixture generator.
#[derive(Parser, Debug)]
#[command(name = "gc_cross_region_ref_fixture")]
#[command(about = "Generate cross-region GC fixture checkpoints for lab testing")]
struct Args {
    // ---- Region A seed ----
    #[arg(long, value_name = "PATH")]
    seed_a_checkpoint: Option<PathBuf>,
    #[arg(long, value_name = "DIR")]
    seed_a_delta_dir: Option<PathBuf>,

    // ---- Region B seed ----
    #[arg(long, value_name = "PATH")]
    seed_b_checkpoint: Option<PathBuf>,
    #[arg(long, value_name = "DIR")]
    seed_b_delta_dir: Option<PathBuf>,

    /// Output directory (creates region-a/ and region-b/ subdirs).
    #[arg(long, value_name = "DIR")]
    out_dir: PathBuf,

    /// Region ID for source region A (must match seed A metadata).
    #[arg(long, value_name = "ID")]
    region_a_id: u64,

    /// Region ID for destination/cross-ref region B (must match seed B metadata).
    #[arg(long, value_name = "ID")]
    region_b_id: u64,

    /// Number of protected overlap FileMeta entries (X files, in A.removed_files + B.files).
    #[arg(long, default_value = "10")]
    overlap_count: usize,

    /// Number of unprotected FileMeta entries (Y files, in A.removed_files only).
    #[arg(long, default_value = "10")]
    unprotected_count: usize,

    /// Manifest/checkpoint version (default: 1).
    #[arg(long, default_value = "1")]
    version: u64,

    /// File size per generated FileMeta (default: 0).
    #[arg(long, default_value = "0")]
    file_size: u64,

    /// Dry-run: decode seeds, print summary, no output files.
    #[arg(long)]
    dry_run: bool,

    /// Safety flag for large counts.
    #[arg(long)]
    allow_large: bool,
}

fn main() {
    let args = Args::parse();

    // Validate exactly one seed per region.
    validate_seed_pair(&args.seed_a_checkpoint, &args.seed_a_delta_dir, "region A");
    validate_seed_pair(&args.seed_b_checkpoint, &args.seed_b_delta_dir, "region B");

    if args.overlap_count == 0 && args.unprotected_count == 0 {
        eprintln!("ERROR: at least one of overlap_count or unprotected_count must be >0");
        std::process::exit(1);
    }

    let region_id_a = RegionId::from(args.region_a_id);
    let region_id_b = RegionId::from(args.region_b_id);

    // ---- 1. Load seed manifests ----
    let seed_a = load_seed(
        &args.seed_a_checkpoint,
        &args.seed_a_delta_dir,
        args.region_a_id,
        "A",
    );
    let seed_b = load_seed(
        &args.seed_b_checkpoint,
        &args.seed_b_delta_dir,
        args.region_b_id,
        "B",
    );

    // ---- 2. Generate FileMeta ----
    // Overlap (protected) X: region_id = A, active in B
    let overlap_files: Vec<(FileId, FileMeta)> =
        gen_file_meta_vec(region_id_a, args.overlap_count, args.file_size);

    // Unprotected Y: region_id = A, NOT active anywhere
    let unprotected_files: Vec<(FileId, FileMeta)> =
        gen_file_meta_vec(region_id_a, args.unprotected_count, args.file_size);

    // ---- 3. Print summary ----
    println!("=== gc_cross_region_ref_fixture ===");
    println!(
        "region A:       {} (0x{:016x})",
        region_id_a.as_u64(),
        region_id_a.as_u64()
    );
    println!(
        "region B:       {} (0x{:016x})",
        region_id_b.as_u64(),
        region_id_b.as_u64()
    );
    println!("overlap (X):    {}", args.overlap_count);
    println!("unprotected (Y):{}", args.unprotected_count);
    println!("version:        {}", args.version);
    println!("dry_run:        {}", args.dry_run);

    if args.dry_run {
        println!("DRY-RUN complete — no files written.");
        return;
    }

    // ---- 4. Build manifests ----
    // A.files = empty
    // A.removed_files = X ∪ Y (timestamp 0)
    let mut a_removed = RemovedFilesRecord::default();
    let mut a_removed_list: Vec<RemovedFile> = Vec::new();
    for (fid, _) in &overlap_files {
        a_removed_list.push(RemovedFile::File(*fid, None));
    }
    for (fid, _) in &unprotected_files {
        a_removed_list.push(RemovedFile::File(*fid, None));
    }
    a_removed.add_removed_files(a_removed_list, 0);

    let manifest_a = RegionManifest {
        metadata: seed_a.metadata.clone(),
        files: HashMap::new(), // empty – overlap X and unprotected Y are removed
        removed_files: a_removed,
        flushed_entry_id: seed_a.flushed_entry_id,
        flushed_sequence: seed_a.flushed_sequence,
        committed_sequence: seed_a.committed_sequence,
        manifest_version: args.version,
        truncated_entry_id: seed_a.truncated_entry_id,
        compaction_time_window: seed_a.compaction_time_window,
        sst_format: seed_a.sst_format,
        append_mode: seed_a.append_mode,
    };

    // B.files = X (with region_id = A)
    // B.removed_files = empty
    let mut b_files_map: HashMap<FileId, FileMeta> = HashMap::with_capacity(args.overlap_count);
    for (fid, fmeta) in &overlap_files {
        b_files_map.insert(*fid, fmeta.clone());
    }

    let manifest_b = RegionManifest {
        metadata: seed_b.metadata.clone(),
        files: b_files_map,
        removed_files: RemovedFilesRecord::default(),
        flushed_entry_id: seed_b.flushed_entry_id,
        flushed_sequence: seed_b.flushed_sequence,
        committed_sequence: seed_b.committed_sequence,
        manifest_version: args.version,
        truncated_entry_id: seed_b.truncated_entry_id,
        compaction_time_window: seed_b.compaction_time_window,
        sst_format: seed_b.sst_format,
        append_mode: seed_b.append_mode,
    };

    // ---- 5. Write outputs ----
    let region_a_dir = args.out_dir.join("region-a");
    let region_b_dir = args.out_dir.join("region-b");
    fs::create_dir_all(&region_a_dir).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot create {:?}: {e}", region_a_dir);
        std::process::exit(1);
    });
    fs::create_dir_all(&region_b_dir).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot create {:?}: {e}", region_b_dir);
        std::process::exit(1);
    });

    write_region_checkpoint(
        &manifest_a,
        args.version,
        args.overlap_count + args.unprotected_count,
        &region_a_dir,
    );
    write_region_checkpoint(&manifest_b, args.version, args.overlap_count, &region_b_dir);

    // ---- 6. Write objects.jsonl ----
    let objects_path = args.out_dir.join("objects.jsonl");
    {
        let f = fs::File::create(&objects_path).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot create {:?}: {e}", objects_path);
            std::process::exit(1);
        });
        let mut writer = BufWriter::new(f);
        for (fid, _) in &overlap_files {
            let line = serde_json::json!({
                "file_id": fid.to_string(),
                "kind": "protected",
                "region_id": region_id_a.as_u64(),
            });
            writeln!(writer, "{}", serde_json::to_string(&line).unwrap()).unwrap();
        }
        for (fid, _) in &unprotected_files {
            let line = serde_json::json!({
                "file_id": fid.to_string(),
                "kind": "unprotected",
                "region_id": region_id_a.as_u64(),
            });
            writeln!(writer, "{}", serde_json::to_string(&line).unwrap()).unwrap();
        }
    }
    println!(
        "  wrote {} objects → {}",
        args.overlap_count + args.unprotected_count,
        objects_path.display()
    );

    // ---- 7. Write summary.json ----
    let summary = serde_json::json!({
        "overlap_count": args.overlap_count,
        "unprotected_count": args.unprotected_count,
        "region_a_id": region_id_a.as_u64(),
        "region_b_id": region_id_b.as_u64(),
        "version": args.version,
        "output_region_a": region_a_dir.to_string_lossy(),
        "output_region_b": region_b_dir.to_string_lossy(),
        "output_objects": objects_path.to_string_lossy(),
    });
    let summary_path = args.out_dir.join("summary.json");
    let summary_bytes = serde_json::to_vec_pretty(&summary).unwrap();
    fs::write(&summary_path, &summary_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write {:?}: {e}", summary_path);
        std::process::exit(1);
    });
    println!("Wrote summary: {}", summary_path.display());
    println!("Done.");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn validate_seed_pair(checkpoint: &Option<PathBuf>, delta_dir: &Option<PathBuf>, label: &str) {
    match (checkpoint, delta_dir) {
        (Some(_), Some(_)) => {
            eprintln!(
                "ERROR: cannot specify both seed checkpoint and delta-dir for {}",
                label
            );
            std::process::exit(1);
        }
        (None, None) => {
            eprintln!(
                "ERROR: must specify exactly one of seed checkpoint or delta-dir for {}",
                label
            );
            std::process::exit(1);
        }
        _ => {}
    }
}

fn load_seed(
    checkpoint: &Option<PathBuf>,
    delta_dir: &Option<PathBuf>,
    region_id: u64,
    _label: &str,
) -> RegionManifest {
    if let Some(dir) = delta_dir {
        replay_delta_dir(dir, region_id)
    } else if let Some(ck) = checkpoint {
        load_seed_checkpoint(ck, region_id)
    } else {
        unreachable!()
    }
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
        eprintln!("ERROR: seed checkpoint has no inner manifest");
        std::process::exit(1);
    });
    validate_region_id(manifest, region_id);
    manifest.clone()
}

/// Load seed manifest by replaying delta JSON files from a directory.
fn replay_delta_dir(dir: &PathBuf, region_id: u64) -> RegionManifest {
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
            eprintln!("ERROR: gzip delta found ({}) — not supported.", fname);
            std::process::exit(1);
        }
    }
    if delta_paths.is_empty() {
        eprintln!("ERROR: no .json delta files in {:?}", dir);
        std::process::exit(1);
    }
    delta_paths.sort();

    let mut builder = RegionManifestBuilder::default();
    for path in &delta_paths {
        let fname = path.file_name().unwrap_or_default().to_string_lossy();
        let version_str = fname.trim_end_matches(".json");
        let delta_version: u64 = version_str.parse().unwrap_or_else(|_| {
            eprintln!("ERROR: cannot parse version from '{}'", fname);
            std::process::exit(1);
        });
        let raw = fs::read(path).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot read delta {:?}: {e}", path);
            std::process::exit(1);
        });
        let action_list = RegionMetaActionList::decode(&raw).unwrap_or_else(|e| {
            eprintln!("ERROR: cannot decode delta {:?}: {e}", path);
            std::process::exit(1);
        });
        for action in action_list.actions {
            match action {
                RegionMetaAction::Change(change) => builder.apply_change(delta_version, change),
                RegionMetaAction::PartitionExprChange(change) => {
                    builder.apply_partition_expr_change(delta_version, change)
                }
                RegionMetaAction::Edit(edit) => builder.apply_edit(delta_version, edit),
                RegionMetaAction::Truncate(truncate) => {
                    builder.apply_truncate(delta_version, truncate)
                }
                RegionMetaAction::Remove(_) => {
                    eprintln!(
                        "ERROR: delta {:?} contains Remove — seed regions must not be removed.",
                        path
                    );
                    std::process::exit(1);
                }
            }
        }
    }
    if !builder.contains_metadata() {
        eprintln!("ERROR: after replaying deltas, metadata is still not set.");
        std::process::exit(1);
    }
    let manifest = builder.try_build().unwrap_or_else(|e| {
        eprintln!("ERROR: RegionManifestBuilder::try_build failed: {e}");
        std::process::exit(1);
    });
    validate_region_id(&manifest, region_id);
    manifest
}

fn validate_region_id(manifest: &RegionManifest, expected: u64) {
    let exp = RegionId::from(expected);
    let act = manifest.metadata.region_id;
    if act != exp {
        eprintln!(
            "ERROR: region_id mismatch: expected {} (CLI), got {} (manifest metadata)",
            exp.as_u64(),
            act.as_u64(),
        );
        eprintln!("  full IDs: expected={exp:?}, actual={act:?}");
        std::process::exit(1);
    }
}

/// Generate a Vec of (FileId, FileMeta) with the given region_id.
fn gen_file_meta_vec(region_id: RegionId, count: usize, file_size: u64) -> Vec<(FileId, FileMeta)> {
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let file_id = FileId::random();
        let fm = FileMeta {
            region_id,
            file_id,
            time_range: FileTimeRange::default(),
            level: 0,
            file_size,
            max_row_group_uncompressed_size: 0,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 100,
            num_row_groups: 1,
            sequence: NonZeroU64::new(1),
            partition_expr: None,
            num_series: 1,
        };
        out.push((file_id, fm));
    }
    out
}

/// Write a single region checkpoint, _last_checkpoint, and _last_checkpoint.
fn write_region_checkpoint(
    manifest: &RegionManifest,
    version: u64,
    compacted_actions: usize,
    dir: &PathBuf,
) {
    let checkpoint = RegionCheckpoint {
        last_version: version,
        compacted_actions,
        checkpoint: Some(manifest.clone()),
    };

    let checkpoint_bytes = checkpoint.encode().unwrap_or_else(|e| {
        eprintln!("ERROR: checkpoint encode failed: {e}");
        std::process::exit(1);
    });

    let ck_path = dir.join(format!("{:020}.checkpoint", version));
    fs::write(&ck_path, &checkpoint_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write {:?}: {e}", ck_path);
        std::process::exit(1);
    });
    println!("  Wrote {}", ck_path.display());

    let lc = serde_json::json!({
        "size": checkpoint_bytes.len(),
        "version": version,
        "checksum": null,
        "extend_metadata": {},
    });
    let lc_bytes = serde_json::to_vec_pretty(&lc).unwrap();
    let lc_path = dir.join("_last_checkpoint");
    fs::write(&lc_path, &lc_bytes).unwrap_or_else(|e| {
        eprintln!("ERROR: cannot write {:?}: {e}", lc_path);
        std::process::exit(1);
    });
    println!("  Wrote {}", lc_path.display());
}
