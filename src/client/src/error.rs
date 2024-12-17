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

use std::any::Any;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_error::ErrorInfoHeader;
use common_macro::stack_trace_debug;
use snafu::{location, Location, Snafu};
use tonic::{Code, Status};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal Flight messages, reason: {}", reason))]
    IllegalFlightMessages {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to do Flight get, code: {}", tonic_code))]
    FlightGet {
        addr: String,
        tonic_code: Code,
        source: BoxedError,
    },

    #[snafu(display("Failed to convert FlightData"))]
    ConvertFlightData {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::Error,
    },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField {
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel, peer address: {}", addr))]
    CreateChannel {
        addr: String,
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to create Tls channel manager"))]
    CreateTlsChannel {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to request RegionServer {}, code: {}", addr, code))]
    RegionServer {
        addr: String,
        code: Code,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to request FlowServer {}, code: {}", addr, code))]
    FlowServer {
        addr: String,
        code: Code,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    // Server error carried in Tonic Status's metadata.
    #[snafu(display("{}", msg))]
    Server {
        code: StatusCode,
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Illegal Database response: {err_msg}"))]
    IllegalDatabaseResponse {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse ascii string: {}", value))]
    InvalidAscii {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalFlightMessages { .. }
            | Error::MissingField { .. }
            | Error::IllegalDatabaseResponse { .. } => StatusCode::Internal,

            Error::Server { code, .. } => *code,
            Error::FlightGet { source, .. }
            | Error::RegionServer { source, .. }
            | Error::FlowServer { source, .. } => source.status_code(),
            Error::CreateChannel { source, .. }
            | Error::ConvertFlightData { source, .. }
            | Error::CreateTlsChannel { source, .. } => source.status_code(),
            Error::IllegalGrpcClientState { .. } => StatusCode::Unexpected,

            Error::InvalidAscii { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Status> for Error {
    fn from(e: Status) -> Self {
        let headers = e.metadata().clone().into_headers();

        match ErrorInfoHeader::from_header_map(&headers) {
            Some(info) => {
                common_telemetry::info!("Error info: {info:#?}");
                let code = StatusCode::from_u32(info.code).unwrap_or(StatusCode::Unknown);
                let msg = info.msg;

                common_telemetry::info!("Code: {code}, Msg: {msg}");
                Self::Server {
                    code,
                    msg,
                    location: location!(),
                }
            }
            None => {
                common_telemetry::error!("Failed to decode error info header, header: {headers:?}");
                let code = StatusCode::Internal;
                let msg = format!(
                    "Failed to decode error info header, tonic message: {}",
                    e.message()
                );
                Self::Server {
                    code,
                    msg,
                    location: location!(),
                }
            }
        }
    }
}

impl Error {
    pub fn should_retry(&self) -> bool {
        // TODO(weny): figure out each case of these codes.
        matches!(
            self,
            Self::RegionServer {
                code: Code::Cancelled,
                ..
            } | Self::RegionServer {
                code: Code::DeadlineExceeded,
                ..
            } | Self::RegionServer {
                code: Code::Unavailable,
                ..
            } | Self::RegionServer {
                code: Code::Unknown,
                ..
            }
        )
    }
}
