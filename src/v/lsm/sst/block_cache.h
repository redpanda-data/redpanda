// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "lsm/block/handle.h"
#include "lsm/block/reader.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/probe.h"

#include <seastar/core/future.hh>

namespace lsm::sst {

// The block cache is a global cache of uncompressed blocks read from the
// persistence layer for individual SST files.
class block_cache {
public:
    class impl;

    // A handle to an entry in the block cache.
    class handle {
    public:
        handle(const handle&) = delete;
        handle& operator=(const handle&) = delete;
        handle(handle&& other) noexcept
          : _cache(std::exchange(other._cache, nullptr))
          , _id(other._id)
          , _handle(other._handle) {}
        handle& operator=(handle&& other) noexcept = delete;
        ~handle() noexcept;

        // Insert the reader into the block cache
        void insert(block::reader);

        // Read the entry from the block cache
        std::optional<block::reader> get();

    private:
        friend class block_cache;
        explicit handle(
          block_cache::impl*, internal::file_id, block::handle) noexcept;
        block_cache::impl* _cache{};
        internal::file_id _id;
        block::handle _handle;
    };

    // Create a new block cache that holds the given number of blocks.
    explicit block_cache(size_t max_entries, ss::lw_shared_ptr<probe>);
    block_cache(const block_cache&) = delete;
    block_cache& operator=(const block_cache&) = delete;
    block_cache(block_cache&&) = default;
    block_cache& operator=(block_cache&&) = default;
    ~block_cache();

    // Get a handle to a cache entry for a given file and block handle.
    ss::future<handle> get(internal::file_id, block::handle);

private:
    std::unique_ptr<impl> _impl;
};

} // namespace lsm::sst
