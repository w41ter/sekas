// Copyright 2024-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

mod ast;
mod parse;
mod token;

pub use self::ast::*;
pub use self::parse::*;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("unknown {0} at {1:?}")]
    Unknown(String, Coord),
    #[error("unexpected token at {0:?}")]
    Unexpected(Coord),
    #[error("unexpected end of stream, {0} is required")]
    UnexpectedEOS(String),
    #[error("unexpected token at {1:?}, {0} is required")]
    Expect(String, Coord),
    #[error("invalid encoding at {0:?}, utf8 is required")]
    InvalidEncoding(Coord),
    #[error("unexpected token at {1:?}, {0} is required")]
    UnexpectedToken(String, Coord),
}

pub type ParseResult<T, E = ParseError> = std::result::Result<T, E>;

#[derive(Debug, Default, Clone, Copy)]
pub struct Coord {
    row: usize,
    col: usize,
    pos: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExecuteResult {
    Data(ColumnResult),
    Msg(String),
    None,
}
