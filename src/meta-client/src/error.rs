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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_error::ErrorInfoHeader;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tonic::Status;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{}", msg))]
    MetaServer {
        code: StatusCode,
        msg: String,
        stack_errors: Vec<String>,
        tonic_code: tonic::Code,
    },

    #[snafu(display("No leader, should ask leader first"))]
    NoLeader {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Ask leader timeout"))]
    AskLeaderTimeout {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: tokio::time::error::Elapsed,
    },

    #[snafu(display("Failed to create gRPC channel"))]
    CreateChannel {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("{} not started", name))]
    NotStarted {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to send heartbeat: {}", err_msg))]
    SendHeartbeat {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed create heartbeat stream to server"))]
    CreateHeartbeatStream {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid response header"))]
    InvalidResponseHeader {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv request"))]
    ConvertMetaRequest {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv response"))]
    ConvertMetaResponse {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Retry exceeded max times({}), message: {}", times, msg))]
    RetryTimesExceeded { times: usize, msg: String },
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalGrpcClientState { .. }
            | Error::NoLeader { .. }
            | Error::AskLeaderTimeout { .. }
            | Error::NotStarted { .. }
            | Error::SendHeartbeat { .. }
            | Error::CreateHeartbeatStream { .. }
            | Error::CreateChannel { .. }
            | Error::RetryTimesExceeded { .. } => StatusCode::Internal,

            Error::MetaServer { code, .. } => *code,

            Error::InvalidResponseHeader { source, .. }
            | Error::ConvertMetaRequest { source, .. }
            | Error::ConvertMetaResponse { source, .. } => source.status_code(),
        }
    }
}

impl Error {
    pub fn is_exceeded_size_limit(&self) -> bool {
        matches!(
            self,
            Error::MetaServer {
                tonic_code: tonic::Code::OutOfRange,
                ..
            }
        )
    }
}

// FIXME(dennis): partial duplicated with src/client/src/error.rs
impl From<Status> for Error {
    fn from(e: Status) -> Self {
        let headers = e.metadata().clone().into_headers();

        match ErrorInfoHeader::from_header_map(&headers) {
            Some(info) => {
                let code = StatusCode::from_u32(info.code).unwrap_or(StatusCode::Internal);
                let msg = info.msg;
                Self::MetaServer {
                    code,
                    msg,
                    stack_errors: info.stack_errors,
                    tonic_code: e.code(),
                }
            }
            None => {
                common_telemetry::error!("Failed to decode error info header, header: {headers:?}");
                let code = StatusCode::Internal;
                let msg = format!(
                    "Failed to decode error info header, tonic message: {}",
                    e.message()
                );
                Self::MetaServer {
                    code,
                    msg,
                    stack_errors: Vec::new(),
                    tonic_code: e.code(),
                }
            }
        }
    }
}
