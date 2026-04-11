// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "container/chunked_vector.h"

#include <cstdint>
#include <string>

namespace lsm {

/// \brief Summary information about a single SST file.
struct file_info {
    uint64_t epoch;
    uint64_t id;
    uint64_t size_bytes;

    // NOTE: these are formatted, human-readable representations of the keys,
    // including additional metadata like sequence number and key type.
    std::string smallest_key_info;
    std::string largest_key_info;
};

/// \brief Per-level SST file information for an LSM tree database.
struct level_info {
    int32_t level_number;
    chunked_vector<file_info> files;
};

/// \brief Data statistics for an LSM tree database.
struct data_stats {
    size_t active_memtable_bytes;
    size_t immutable_memtable_bytes;
    size_t total_size_bytes;
    std::vector<level_info> levels;
};

} // namespace lsm
