// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::path::{Path, PathBuf};

/// Compatibility wrapper used while keeping test call sites concise.
pub struct TempDir(tempfile::TempDir);

impl TempDir {
    pub fn new(prefix: &str) -> std::io::Result<Self> {
        tempfile::Builder::new().prefix(prefix).tempdir().map(Self)
    }

    pub fn path(&self) -> &Path {
        self.0.path()
    }

    #[allow(deprecated)]
    pub fn into_path(self) -> PathBuf {
        self.0.into_path()
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}
