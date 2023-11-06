// Copyright 2023-present The Sekas Authors.
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

//! A mod to extend `std::fs`.
use std::io::{ErrorKind, Result};
use std::path::Path;

/// Like `std::fs::create_dir_all` but ignore the already exists error.
pub fn create_dir_all_if_not_exists<P: AsRef<Path>>(dir: &P) -> Result<()> {
    match std::fs::create_dir_all(dir.as_ref()) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err),
    }
}
