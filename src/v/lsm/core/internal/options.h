// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "absl/time/time.h"
#include "base/format_to.h"
#include "base/units.h"
#include "base/vassert.h"
#include "lsm/core/compression.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/probe.h"

#include <seastar/core/shared_ptr.hh>

#include <cstddef>

namespace lsm::internal {

struct options {
    // The epoch of the database this time it was opened.
    //
    // See the direct description of what an epoch is, but allows for shared
    // storage.
    internal::database_epoch database_epoch;

    // The probe for the database to record stats
    ss::lw_shared_ptr<probe> probe = ss::make_lw_shared<lsm::probe>();

    // If the database is opened in readonly mode. Readonly mode causes all
    // write operations to fail. However, read operations can be performed.
    bool readonly = false;

    // The scheduling group for background operations in the LSM tree, such as
    // memtable flushing and compaction. Note that this is unused if the
    // database is in readonly mode.
    ss::scheduling_group compaction_scheduling_group
      = ss::default_scheduling_group();

    struct level_config {
        // The level number in the database.
        internal::level number;

        // The maximum number of total bytes in this level.
        size_t max_total_bytes;

        // Write up to this amount of bytes to a file in this level before
        // switching to a new one.
        //
        // Increasing this provides better file system
        // efficiency with larger files, but the downside of increasing this is
        // longer compactions and longer latency/performance hiccups.
        size_t max_file_size;

        // The compression type for SST blocks created in this level. In some
        // cases this setting may be ignored, for example if there is a trivial
        // compaction move and a file moves between levels without needing to be
        // rewritten.
        compression_type compression = compression_type::none;

        fmt::iterator format_to(fmt::iterator) const;
    };

    // Make a configuration of levels based on the initial level configuration,
    // and a multiplier between each level.
    static constexpr std::vector<level_config> make_levels(
      level_config base_config, size_t multiplier, internal::level max_level) {
        vassert(
          max_level >= 1_level,
          "there must be at least 2 levels in the LSM tree");
        std::vector<level_config> levels;
        levels.reserve(max_level + 1_level);
        // Level0 and level1 we want to be similar in size because usually
        // compaction between the levels are the bottleneck. This is also the
        // recommendation in RocksDB. Note that generally level0 is special in
        // that the system uses the write buffer size and write triggers to
        // determine the number of files and size of files in level0
        levels.emplace_back(
          level_config{
            .number = 0_level,
            .max_total_bytes = base_config.max_total_bytes,
            .max_file_size = base_config.max_file_size,
          });
        auto* last_level = &levels.emplace_back(
          level_config{
            .number = 1_level,
            .max_total_bytes = base_config.max_total_bytes,
            .max_file_size = base_config.max_file_size,
          });
        for (auto lvl = 2_level; lvl <= max_level; ++lvl) {
            last_level = &levels.emplace_back(
              level_config{
                .number = lvl,
                .max_total_bytes = last_level->max_total_bytes * multiplier,
                .max_file_size = last_level->max_file_size * multiplier,
              });
        }
        return levels;
    }
    // The levels and their configuration in the database,
    // this will be sorted by level number and also will be monotonically
    // increasing from level 0 to level N (configurable).
    constexpr static auto default_max_level = 6_level;
    // The default size multiplier between levels
    constexpr static size_t default_level_multipler = 10;
    std::vector<level_config> levels = make_levels(
      {
        .max_total_bytes = default_level_zero_stop_writes_trigger
                           * default_write_buffer_size,
        .max_file_size = default_write_buffer_size,
      },
      default_level_multipler,
      default_max_level);
    internal::level max_level() const { return levels.back().number; }

    // At what point do we start throttling writes in terms of the number of L0
    // files
    constexpr static size_t default_level_zero_slowdown_writes_trigger = 8;
    size_t level_zero_slowdown_writes_trigger
      = default_level_zero_slowdown_writes_trigger;

    // At what point do we halt writes in terms of number of L0 files
    constexpr static size_t default_level_zero_stop_writes_trigger = 12;
    size_t level_zero_stop_writes_trigger
      = default_level_zero_stop_writes_trigger;

    // How big to let memtable accumulate in bytes before flushing.
    constexpr static size_t default_write_buffer_size = 16_MiB;
    size_t write_buffer_size = default_write_buffer_size;

    // When do we trigger compaction into L1 in terms of number of L0 files
    constexpr static size_t default_level_one_compaction_trigger = 4;
    size_t level_one_compaction_trigger = default_level_one_compaction_trigger;

    // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
    // stop building a single file in a level->level+1 compaction.
    size_t max_grandparent_overlap_bytes(internal::level lvl) const {
        static constexpr size_t multiplier = 10;
        return multiplier * levels[lvl].max_file_size;
    }

    // Maximum number of bytes in all compacted files. We avoid expanding
    // the lower level file set of a compaction if it would make the
    // total compaction cover more than this many bytes.
    size_t expanded_compaction_byte_size_limit(internal::level lvl) const {
        static constexpr size_t multiplier = 25;
        return multiplier * levels[lvl].max_file_size;
    }

    // The approximate max number of SST files that should be opened at one
    // time.
    constexpr static size_t default_max_open_files = 1000;
    size_t max_open_files = default_max_open_files;

    // If non-zero, the number of fibers to use to pre-open all the files in the
    // database, which populates the table cache.
    //
    // This allows ensuring files are all present and readable at database open
    // time.
    uint32_t max_pre_open_fibers = 0;

    // The size of the cache that stores uncompressed blocks.
    constexpr static size_t default_block_cache_size = 10_MiB;
    size_t block_cache_size = default_block_cache_size;

    // The size of a single block within an SST file.
    constexpr static size_t default_sst_block_size = 16_KiB;
    size_t sst_block_size = default_sst_block_size;

    // The amount of extra data to read ahead when reading SST data blocks
    // during compaction. Buffered data is served to subsequent sequential
    // reads without I/O. Set to 0 to disable readahead.
    constexpr static size_t default_compaction_readahead_size = 256_KiB;
    size_t compaction_readahead_size = default_compaction_readahead_size;

    // The frequency at which to generate a new bloom filter.
    //
    // If set to 0 then no bloom filters will be generated.
    //
    // REQUIRED: this value must be a power of two
    constexpr static size_t default_sst_filter_period = 2_KiB;
    size_t sst_filter_period = default_sst_filter_period;

    // We arrange to automatically compact after a file after a certain
    // number of seeks. Let's assume:
    // (1) One seek costs 200us
    // (2) Writing or reading 1MiB costs 1ms (1GiB/s)
    // (3) A compaction of 1MiB does 25MiB of IO:
    //       1MiB read from this level
    //       10-12MiB read from next level (boundaries my be
    //       misaligned)
    //       10-12MiB written to next level
    // This imples that 125 seeks cost the same as the compaction of
    // 1MB of data. I.e., one seek costs approximately the same as
    // the compaction of 8KiB of data.
    constexpr static size_t default_seek_compaction_bytes_cost_ratio = 8_KiB;
    size_t seek_compaction_bytes_cost_ratio
      = default_seek_compaction_bytes_cost_ratio;

    // Approximate gap in bytes between samples of data read during iteration.
    constexpr static size_t default_read_bytes_period = 10_MiB;
    size_t read_bytes_period = default_read_bytes_period;

    // The delay at which to wait until actually deleting files.
    //
    // If set to zero, then files are immediately GC'd after new manifest files
    // are written.
    absl::Duration file_deletion_delay;

    fmt::iterator format_to(fmt::iterator) const;
};
} // namespace lsm::internal
