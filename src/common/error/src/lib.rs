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

pub use headers::{self, Header, HeaderMapExt};
use http::{HeaderMap, HeaderName, HeaderValue};
pub use snafu;
use unescaper::unescape;

pub const ERROR_INFO_HEADER_NAME: &str = "x-greptime-err-info";

pub static GREPTIME_DB_HEADER_ERROR_INFO: HeaderName =
    HeaderName::from_static(ERROR_INFO_HEADER_NAME);

#[derive(Debug)]
pub struct ErrorInfoHeader {
    pub code: u32,
    pub msg: String,
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
            .ok_or_else(|| axum::headers::Error::invalid())?
            .to_str()
            .map_err(|_| axum::headers::Error::invalid())?
            .parse()
            .map_err(|_| axum::headers::Error::invalid())?;
        let need_escape_val = values
            .next()
            .ok_or_else(|| axum::headers::Error::invalid())?;

        let need_escape = if need_escape_val == "1" {
            true
        } else if need_escape_val == "0" {
            false
        } else {
            return Err(axum::headers::Error::invalid());
        };

        let msg = values
            .next()
            .ok_or_else(|| axum::headers::Error::invalid())?
            .to_str()
            .map_err(|_| axum::headers::Error::invalid())?
            .to_string();

        let msg = if need_escape {
            unescape(&msg).map_err(|_| axum::headers::Error::invalid())?
        } else {
            msg
        };

        Ok(ErrorInfoHeader { code, msg })
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let mut need_escape = false;
        let msg = HeaderValue::from_str(&self.msg).unwrap_or_else(|_| {
            need_escape = true;
            HeaderValue::from_bytes(
                &self
                    .msg
                    .as_bytes()
                    .iter()
                    .flat_map(|b| std::ascii::escape_default(*b))
                    .collect::<Vec<u8>>(),
            )
            .expect("Already escaped string should be valid ascii")
        });
        let need_escape = if need_escape { "1" } else { "0" };
        values.extend([
            HeaderValue::from(self.code),
            HeaderValue::from_static(need_escape),
            msg,
        ]);
    }
}

impl ErrorInfoHeader {
    pub fn from_header_map(header: &HeaderMap) -> Option<ErrorInfoHeader> {
        let mut values = header.get_all(ErrorInfoHeader::name()).iter();

        match ErrorInfoHeader::decode(&mut values) {
            Ok(info) => Some(info),
            Err(_err) => None,
        }
    }
}

/// Create a http header map from error code and message.
/// using `GREPTIME_DB_HEADER_ERROR_INFO` as header name
pub fn from_err_code_msg_to_header(code: u32, msg: &str) -> HeaderMap {
    let mut header = HeaderMap::new();

    let error_info = ErrorInfoHeader {
        code,
        msg: msg.to_string(),
    };
    header.typed_insert(error_info);
    header
}

/// Get error info from header, return None if header is invalid
pub fn from_header_map_to_err_info(header: &HeaderMap) -> Option<ErrorInfoHeader> {
    ErrorInfoHeader::from_header_map(header)
}
