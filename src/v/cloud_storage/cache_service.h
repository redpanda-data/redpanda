/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>

#include <filesystem>

namespace cloud_storage {

struct cache_item {
    ss::input_stream<char> body;
    size_t size;
};

class cache {
public:
    /// C-tor.
    ///
    /// \param cache_dir is a directory where cached data is stored
    explicit cache(std::filesystem::path cache_dir) noexcept;

    ss::future<> start();
    ss::future<> stop();

    /// Get cached value as a stream if it exists on disk
    ss::future<std::optional<cache_item>> get(std::filesystem::path key);

    /// Add new value to the cache, overwrite if it's already exist
    ss::future<> put(std::filesystem::path key, ss::input_stream<char>& data);

    /// Return true if the following object is already in the cache
    ss::future<bool> is_cached(const std::filesystem::path& key);

    /// Remove element from cache by key
    ss::future<> invalidate(const std::filesystem::path& key);

private:
    std::filesystem::path _cache_dir;
    ss::gate _gate;
    uint64_t _cnt;
};

} // namespace cloud_storage
