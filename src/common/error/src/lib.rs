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

#![feature(error_iter)]

pub mod ext;
pub mod mock;
pub mod status_code;

use ext::{ErrorExt, StackError};
pub use headers::{self, Header, HeaderMapExt};
use http::{HeaderMap, HeaderName, HeaderValue};
pub use snafu;
use status_code::StatusCode;
use unescaper::unescape;

pub const ERROR_INFO_HEADER_NAME: &str = "x-greptime-err-info";

pub static GREPTIME_DB_HEADER_ERROR_INFO: HeaderName =
    HeaderName::from_static(ERROR_INFO_HEADER_NAME);

/// Remote stack error, hold error stack from remote datanode/metasrv etc.
/// can be carried in http header and is human-readable in header
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStackError {
    pub code: StatusCode,
    pub msg: String,
    pub stack_error: Vec<String>,
}

impl RemoteStackError {
    pub fn new(code: StatusCode, msg: String, stack_error: Vec<String>) -> Self {
        RemoteStackError {
            code,
            msg,
            stack_error,
        }
    }
    pub fn from_stack_error(err: &impl ErrorExt) -> Self {
        let code = err.status_code();
        let msg = err.output_msg();
        let mut buf = Vec::new();
        err.debug_fmt(0, &mut buf);
        let mut cur: &dyn StackError = err;
        while let Some(nxt) = cur.next() {
            cur.debug_fmt(0, &mut buf);
            cur = nxt;
        }
        RemoteStackError {
            code,
            msg,
            stack_error: buf,
        }
    }
}

impl std::fmt::Display for RemoteStackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for RemoteStackError {}

impl StackError for RemoteStackError {
    fn debug_fmt(&self, _layer: usize, buf: &mut Vec<String>) {
        buf.extend([format!("RemoteStackError: {}", self)]);
        buf.extend(self.stack_error.clone());
    }

    /// Remote stack error has no next as it's the "leaf error"
    fn next(&self) -> Option<&dyn StackError> {
        None
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ErrorInfoHeader {
    pub code: u32,
    pub msg: String,
    /// Stack trace of errors
    pub stack_errors: Vec<String>,
}

impl Header for ErrorInfoHeader {
    fn name() -> &'static http::HeaderName {
        &GREPTIME_DB_HEADER_ERROR_INFO
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, axum::headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        let code = values
            .next()
            .ok_or_else(axum::headers::Error::invalid)?
            .to_str()
            .map_err(|_| axum::headers::Error::invalid())?
            .parse()
            .map_err(|_| axum::headers::Error::invalid())?;

        let msg = values
            .next()
            .ok_or_else(axum::headers::Error::invalid)?
            .as_bytes();

        let stack_errors = {
            let mut stack_errors = Vec::new();
            for value in values {
                let msg = value.as_bytes();
                let msg = String::from_utf8_lossy(msg);
                let msg = unescape(&msg).map_err(|_| axum::headers::Error::invalid())?;
                stack_errors.push(msg);
            }
            stack_errors
        };

        let msg = String::from_utf8_lossy(msg);

        let msg = unescape(&msg).map_err(|_| axum::headers::Error::invalid())?;

        Ok(ErrorInfoHeader {
            code,
            msg,
            stack_errors,
        })
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let msg = HeaderValue::from_bytes(self.msg.escape_default().to_string().as_bytes())
            .expect("Already escaped string should be valid ascii");

        values.extend([HeaderValue::from(self.code), msg]);

        for err_msg in self.stack_errors.iter() {
            let msg = HeaderValue::from_bytes(err_msg.escape_default().to_string().as_bytes())
                .expect("Already escaped string should be valid ascii");
            values.extend([msg]);
        }
    }
}

impl ErrorInfoHeader {
    pub fn from_error(error: &impl ErrorExt) -> Self {
        let code = error.status_code() as u32;
        let msg = error.output_msg();
        let stack_errors = from_stacked_errors_to_list(error);
        ErrorInfoHeader {
            code,
            msg,
            stack_errors,
        }
    }

    pub fn from_header_map(header: &HeaderMap) -> Option<ErrorInfoHeader> {
        let mut values = header.get_all(ErrorInfoHeader::name()).iter();

        match ErrorInfoHeader::decode(&mut values) {
            Ok(info) => Some(info),
            Err(_err) => None,
        }
    }

    pub fn to_header_map(&self) -> HeaderMap {
        let mut header = HeaderMap::new();
        header.typed_insert(self.clone());
        header
    }
}

/// Create a http header map from error code and message.
/// using `GREPTIME_DB_HEADER_ERROR_INFO` as header name
pub fn from_err_code_msg_stack_to_header(
    code: u32,
    msg: &str,
    stack_errors: Vec<String>,
) -> HeaderMap {
    let mut header = HeaderMap::new();

    let error_info = ErrorInfoHeader {
        code,
        msg: msg.to_string(),
        stack_errors,
    };
    header.typed_insert(error_info);
    header
}

pub fn from_stacked_errors_to_list(err: &impl StackError) -> Vec<String> {
    let mut buf = Vec::new();
    err.debug_fmt(0, &mut buf);
    buf
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_back_and_forth() {
        let testcases = [
            (1003, "test", vec![]),
            (
                1002,
                "Unexpected, violated: Invalid database name: ㊙️database",
                vec![],
            ),
            (
                1003,
                "test",
                vec![
                    "0: test".to_string(),
                    "1: stack error 1".to_string(),
                    "2: stack error 2".to_string(),
                ],
            ),
        ];
        for (code, msg, stack_errors) in &testcases[1..] {
            let info = ErrorInfoHeader {
                code: *code,
                msg: msg.to_string(),
                stack_errors: stack_errors.clone(),
            };
            let mut header = HeaderMap::new();
            header.typed_insert(info.clone());
            let info2 = ErrorInfoHeader::from_header_map(&header).unwrap();

            assert_eq!(info, info2);
        }
    }
}
