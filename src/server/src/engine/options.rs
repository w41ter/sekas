// Copyright 2024 The Sekas Authors.
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

use rocksdb::{BlockBasedIndexType, BlockBasedOptions, Cache, Options};

use crate::engine::properties::SplitKeyCollectorFactory;
use crate::DbConfig;

pub fn to_rocksdb_options(cfg: &DbConfig) -> rocksdb::Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    opts.set_max_background_jobs(cfg.max_background_jobs);
    opts.set_max_subcompactions(cfg.max_sub_compactions);
    opts.set_max_manifest_file_size(cfg.max_manifest_file_size);
    opts.set_bytes_per_sync(cfg.bytes_per_sync);
    opts.set_compaction_readahead_size(cfg.compaction_readahead_size);
    opts.set_use_direct_reads(cfg.use_direct_read);
    opts.set_use_direct_io_for_flush_and_compaction(cfg.use_direct_io_for_flush_and_compaction);
    opts.set_avoid_unnecessary_blocking_io(cfg.avoid_unnecessary_blocking_io);

    opts.set_write_buffer_size(cfg.write_buffer_size);
    opts.set_max_write_buffer_number(cfg.max_write_buffer_number);
    opts.set_min_write_buffer_number_to_merge(cfg.min_write_buffer_number_to_merge);

    opts.set_num_levels(cfg.num_levels);
    opts.set_compression_per_level(&cfg.compression_per_level);

    opts.set_level_zero_file_num_compaction_trigger(cfg.level0_file_num_compaction_trigger);
    opts.set_target_file_size_base(cfg.target_file_size_base);
    opts.set_max_bytes_for_level_base(cfg.max_bytes_for_level_base);
    opts.set_max_bytes_for_level_multiplier(cfg.max_bytes_for_level_multiplier);
    opts.set_max_compaction_bytes(cfg.max_compaction_bytes);
    opts.set_level_compaction_dynamic_level_bytes(true);

    opts.set_level_zero_slowdown_writes_trigger(cfg.level0_slowdown_writes_trigger);
    opts.set_level_zero_stop_writes_trigger(cfg.level0_slowdown_writes_trigger);
    opts.set_soft_pending_compaction_bytes_limit(cfg.soft_pending_compaction_bytes_limit);
    opts.set_hard_pending_compaction_bytes_limit(cfg.hard_pending_compaction_bytes_limit);

    if cfg.rate_limiter_auto_tuned {
        opts.set_auto_tuned_ratelimiter(
            cfg.rate_limiter_bytes_per_sec,
            cfg.rate_limiter_refill_period,
            10,
        );
    }

    let cache = Cache::new_lru_cache(cfg.block_cache_size);

    let mut blk_opts = BlockBasedOptions::default();
    blk_opts.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    blk_opts.set_block_size(cfg.block_size);
    blk_opts.set_block_cache(&cache);
    blk_opts.set_cache_index_and_filter_blocks(true);
    blk_opts.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&blk_opts);

    opts.add_table_properties_collector_factory(SplitKeyCollectorFactory);

    opts
}
