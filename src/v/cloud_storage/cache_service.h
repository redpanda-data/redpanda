/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/cache_probe.h"
#include "cloud_storage/recursive_directory_walker.h"
#include "resource_mgmt/io_priority.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "units.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>

#include <filesystem>
#include <set>
#include <string_view>

namespace cloud_storage {

static constexpr size_t default_write_buffer_size = 128_KiB;
static constexpr unsigned default_writebehind = 10;
static constexpr const char* access_time_tracker_file_name = "accesstime";

struct cache_item {
    ss::file body;
    size_t size;
};

enum class cache_element_status { available, not_available, in_progress };
std::ostream& operator<<(std::ostream& o, cache_element_status);

class cache : public ss::peering_sharded_service<cache> {
public:
    /// C-tor.
    ///
    /// \param cache_dir is a directory where cached data is stored
    cache(std::filesystem::path cache_dir, size_t _max_cache_size) noexcept;

    ss::future<> start();
    ss::future<> stop();

    /// Get cached value as a stream if it exists on disk ///
    /// \param key is a cache key
    ss::future<std::optional<cache_item>> get(std::filesystem::path key);

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
    /// Load access time tracker from file
    ss::future<> load_access_time_tracker();

    /// Save access time tracker to file
    ss::future<> save_access_time_tracker();

    /// Save access time tracker state to the file if needed
    ss::future<> maybe_save_access_time_tracker();

    /// Triggers directory walker, creates a list of files to delete and deletes
    /// them until cache size <= _cache_size_low_watermark * max_cache_size
    ss::future<> clean_up_cache();

    /// Triggers directory walker, creates a list of files to delete and deletes
    /// only tmp files that are left from previous Red Panda run
    ss::future<> clean_up_at_start();

    /// Deletes a file and then recursively goes up and deletes a directory
    /// until it meet a non-empty directory.
    ///
    /// \param key if a path to a file what should be deleted
    ss::future<> recursive_delete_empty_directory(const std::string_view& key);

    /// This method is called on shard 0 by other shards to report disk
    /// space changes.
    ss::future<> consume_cache_space(size_t);

    std::filesystem::path _cache_dir;
    size_t _max_cache_size;

    ss::gate _gate;
    uint64_t _cnt;
    static constexpr double _cache_size_low_watermark{0.8};
    cloud_storage::recursive_directory_walker _walker;
    uint64_t _total_cleaned;
    /// Current size of the cache directory (only used on shard 0)
    uint64_t _current_cache_size{0};
    ssx::semaphore _cleanup_sm{1, "cloud/cache"};
    std::set<std::filesystem::path> _files_in_progress;
    cache_probe probe;
    access_time_tracker _access_time_tracker;
    ss::timer<ss::lowres_clock> _tracker_timer;
};

} // namespace cloud_storage
