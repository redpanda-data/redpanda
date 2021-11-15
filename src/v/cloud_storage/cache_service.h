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

#include "cloud_storage/recursive_directory_walker.h"
#include "resource_mgmt/io_priority.h"
#include "seastarx.h"
#include "units.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>

#include <filesystem>
#include <set>

namespace cloud_storage {

static constexpr size_t default_read_buffer_size = 128_KiB;
static constexpr unsigned default_readahead = 10;
static constexpr size_t default_write_buffer_size = 128_KiB;
static constexpr unsigned default_writebehind = 10;

struct cache_item {
    ss::input_stream<char> body;
    size_t size;
};

enum class cache_element_status { available, not_available, in_progress };
std::ostream& operator<<(std::ostream& o, cache_element_status);

class cache {
public:
    /// C-tor.
    ///
    /// \param cache_dir is a directory where cached data is stored
    cache(
      std::filesystem::path cache_dir,
      size_t _max_cache_size,
      ss::lowres_clock::duration _check_period) noexcept;

    ss::future<> start();
    ss::future<> stop();

    /// Get cached value as a stream if it exists on disk
    ///
    /// \param key is a cache key
    /// \param file_pos is an offset to start from in the returned stream
    /// \param io_priority is an io priority of the resulting stream
    /// \param read_buffer_size is a buffer size of the returned stream
    /// \param readahead is a readahead parameter of the returned stream
    ss::future<std::optional<cache_item>> get(
      std::filesystem::path key,
      size_t file_pos = 0,
      ss::io_priority_class io_priority
      = priority_manager::local().shadow_indexing_priority(),
      size_t read_buffer_size = default_read_buffer_size,
      unsigned int readahead = default_readahead);

    /// Add new value to the cache, overwrite if it's already exist
    ///
    /// \param key is a cache key
    /// \param io_priority is an io priority of disk write operation
    /// \param data is an input stream containing data
    /// \param write_buffer_size is a write buffer size for disk write
    /// \param write_behind number of pages that can be written asynchronously
    ss::future<> put(
      std::filesystem::path key,
      ss::input_stream<char>& data,
      ss::io_priority_class io_priority
      = priority_manager::local().shadow_indexing_priority(),
      size_t write_buffer_size = default_write_buffer_size,
      unsigned int write_behind = default_write_buffer_size);

    /// \brief Checks if the value is cached
    ///
    /// \note returned value \c cache_element_status::in_progress is
    /// shard-local, it indicates that the object is being written by this
    /// shard. If the value is being written by another shard, this function
    /// returns only committed result. The value
    /// \c cache_element_status::in_progress can be used as a hint since ntp are
    /// stored on the same shard most of the time.
    ss::future<cache_element_status>
    is_cached(const std::filesystem::path& key);

    /// Remove element from cache by key
    ss::future<> invalidate(const std::filesystem::path& key);

    // Total cleaned is exposed for better testability of eviction
    uint64_t get_total_cleaned();

private:
    /// Triggers directory walker, creates a list of files to delete and deletes
    /// them until cache size <= _cache_size_low_watermark * max_cache_size
    ss::future<> clean_up_cache();

    /// Triggers directory walker, creates a list of files to delete and deletes
    /// only tmp files that are left from previous Red Panda run
    ss::future<> clean_up_at_start();

    std::filesystem::path _cache_dir;
    size_t _max_cache_size;
    ss::lowres_clock::duration _check_period;

    ss::gate _gate;
    uint64_t _cnt;
    static constexpr double _cache_size_low_watermark{0.8};
    ss::timer<ss::lowres_clock> _timer;
    cloud_storage::recursive_directory_walker _walker;
    uint64_t _total_cleaned;
    std::set<std::filesystem::path> _files_in_progress;
};

} // namespace cloud_storage
