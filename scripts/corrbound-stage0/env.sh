# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Environment for the corrbound-stage0 probe. Source it before any cargo
# command, e.g.
#
#     source scripts/corrbound-stage0/env.sh
#     cargo run --manifest-path scripts/corrbound-stage0/Cargo.toml --locked
#
# Notes on the two deviations from the generic experiment environment:
#
#  * toolchain: the repository pins `nightly-2026-03-21` in rust-toolchain.toml
#    and several crates it contains (`query`, `table`, `sql`, `mito2`, ...) use
#    `#![feature(...)]` gates, so a stable toolchain cannot compile them at all.
#  * RUSTFLAGS: left empty so that prebuilt artifacts from an earlier build of
#    this workspace (same toolchain, same empty RUSTFLAGS) can be reused from
#    the cargo cache; `-C debuginfo=0` would invalidate every cached crate.

export PATH=/home/discord9/.rustup/toolchains/nightly-2026-03-21-x86_64-unknown-linux-gnu/bin:$PATH
export RUSTUP_TOOLCHAIN=nightly-2026-03-21-x86_64-unknown-linux-gnu
export CARGO_HOME=/mnt/nvme_rust/cargo-home
export CARGO_TARGET_DIR=/mnt/nvme_rust/rust-targets/paper-corrbound-stage0
export RUSTC_WRAPPER=''
export RUSTFLAGS=''
export CARGO_BUILD_JOBS=2
