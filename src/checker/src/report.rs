// Copyright 2026-present The Sekas Authors.
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

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::history::History;
use crate::linear::CheckReport;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    pub name: String,
    pub seed: u64,
    pub history: History,
    pub check: CheckReport,
}

impl RunReport {
    pub fn read_json(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let data = std::fs::read(path)?;
        serde_json::from_slice(&data).map_err(std::io::Error::other)
    }

    pub fn write_json(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let data = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, data)
    }
}
