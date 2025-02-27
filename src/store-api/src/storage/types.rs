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

//! Common types.

/// Represents a sequence number of data in storage. The offset of logstore can be used
/// as a sequence number.
pub type SequenceNumber = u64;

/// seqs should be greater or equal to `start` and lesser than `end`
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SequenceRange {
    pub start: Option<SequenceNumber>,
    pub end: Option<SequenceNumber>,
}

impl SequenceRange {
    pub fn from_max_seq(seq: SequenceNumber) -> Self {
        Self {
            start: None,
            end: Some(seq),
        }
    }
}
