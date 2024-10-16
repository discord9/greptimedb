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

use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

use crate::ir::create_expr::CreateTableExprBuilderError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unexpected, violated: {violated}"))]
    Unexpected {
        violated: String,
        location: Location,
    },

    #[snafu(display("Failed to build create table expr"))]
    BuildCreateTableExpr {
        #[snafu(source)]
        error: CreateTableExprBuilderError,
        location: Location,
    },

    #[snafu(display("No droppable columns"))]
    DroppableColumns { location: Location },
}
