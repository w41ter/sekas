// Copyright 2022 The Engula Authors.
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

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    std::env::set_var("PROTOC", protoc_build::PROTOC);
    std::env::set_var("PROTOC_INCLUDE", protoc_build::PROTOC_INCLUDE);

    let mut config = prost_build::Config::default();
    config.extern_path(".sekas.server.v1", "::sekas_api::server::v1");
    config.extern_path(".sekas.v1", "::sekas_api::v1");
    config.extern_path(".eraftpb", "::raft::eraftpb");
    tonic_build::configure().compile_with_config(
        config,
        &["proto/v1/metadata.proto", "proto/v1/raft.proto", "proto/v1/schedule.proto"],
        &["proto", "proto/include", "../api/"],
    )?;
    Ok(())
}
