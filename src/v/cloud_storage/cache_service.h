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
#include "config/property.h"
#include "resource_mgmt/io_priority.h"
#include "resource_mgmt/storage.h"
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

class cache_test_fixture;

struct cache_item {
    ss::file body;
    size_t size;
};

enum class cache_element_status { available, not_available, in_progress };
std::ostream& operator<<(std::ostream& o, cache_element_status);

class cache;

class space_reservation_guard {
public:
    space_reservation_guard(cache& cache, size_t bytes) noexcept
      : _cache(cache)
      , _bytes(bytes) {}

    space_reservation_guard(const space_reservation_guard&) = delete;
    space_reservation_guard() = delete;
    space_reservation_guard(space_reservation_guard&& rhs) noexcept
      : _cache(rhs._cache)
      , _bytes(rhs._bytes) {
        rhs._bytes = 0;
    }

    ~space_reservation_guard();

private:
    cache& _cache;
    size_t _bytes;
};

class cache : public ss::peering_sharded_service<cache> {
public:
    /// C-tor.
    ///
    /// \param cache_dir is a directory where cached data is stored
    cache(std::filesystem::path cache_dir, config::binding<uint64_t>) noexcept;

    cache(const cache&) = delete;
    cache(cache&& rhs) = delete;

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
      unsigned int write_behind = default_writebehind);

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

    // Call this before starting a download, to trim the cache if necessary
    // and wait until enough free space is available.
    ss::future<space_reservation_guard> reserve_space(size_t);

    // Release capacity acquired via `reserve_space`.  This spawns
    // a background fiber in order to be callable from the guard destructor.
    void reserve_space_release(size_t);

    static ss::future<> initialize(std::filesystem::path);

    /// Shard 0 only.  Update the utilization status of local disk.  Will
    /// call onwards to other shards as needed.
    void notify_disk_status(
      uint64_t total_space,
      uint64_t free_space,
      storage::disk_space_alert alert);

private:
    /// Load access time tracker from file
    ss::future<> load_access_time_tracker();

    /// Save access time tracker to file
    ss::future<> save_access_time_tracker();
    ss::future<> _save_access_time_tracker(ss::file);

    /// Save access time tracker state to the file if needed
    ss::future<> maybe_save_access_time_tracker();

    /// Triggers directory walker, creates a list of files to delete and deletes
    /// them until cache size <= _cache_size_low_watermark * max_bytes
    ss::future<> trim();

    /// Invoke trim, waiting if not enough time passed since the last trim
    ss::future<> trim_throttled();

    /// Triggers directory walker, creates a list of files to delete and deletes
    /// only tmp files that are left from previous Red Panda run
    ss::future<> clean_up_at_start();

    /// Deletes a file and then recursively goes up and deletes a directory
    /// until it meet a non-empty directory.
    ///
    /// \param key if a path to a file what should be deleted
    ss::future<> delete_file_and_empty_parents(const std::string_view& key);

    /// This method is called on shard 0 by other shards to report disk
    /// space changes.
    void consume_cache_space(size_t);

    /// Block until enough space is available to commit to a reservation
    /// (only runs on shard 0)
    ss::future<> do_reserve_space(size_t bytes);

    /// Return true if the sum of used space and reserved space is far enough
    /// below max size to accommodate a new reservation of `bytes`
    /// (only runs on shard 0)
    bool may_reserve_space(size_t bytes);

    /// Release units from _reserved_cache_size: the inner part of
    /// `reserve_space_release`
    /// (only runs on shard 0)
    void do_reserve_space_release(size_t bytes);

    /// Update _block_puts and kick _block_puts_cond if necessary.  This is
    /// called on all shards by shard 0 when handling a disk space status
    /// update.
    void set_block_puts(bool);

    std::filesystem::path _cache_dir;
    config::binding<uint64_t> _max_bytes;

    ss::abort_source _as;
    ss::gate _gate;
    uint64_t _cnt;

    // When trimming, trim to this fraction of the target size to leave some
    // slack free space and thereby avoid continuously trimming.
    static constexpr double _cache_size_low_watermark{0.8};

    cloud_storage::recursive_directory_walker _walker;
    uint64_t _total_cleaned;
    /// Current size of the cache directory (only used on shard 0)
    uint64_t _current_cache_size{0};

    /// Bytes reserved by downloads in progress, owned by instances of
    /// space_reservation_guard.
    uint64_t _reserved_cache_size{0};

    /// Bytes waiting to be reserved when the next cache trim completes: a
    /// hint to clean_up_cache on how much extra space to try and free
    uint64_t _reservations_pending{0};

    /// A _lazily updated_ record of physical space on the drive.  This is only
    /// for use in corner cases when we e.g. cannot trim the cache far enough
    /// and have to decide whether to block writes, or exceed our configured
    /// limit.
    /// (shard 0 only)
    uint64_t _free_space{0};

    ssx::semaphore _cleanup_sm{1, "cloud/cache"};
    std::set<std::filesystem::path> _files_in_progress;
    cache_probe probe;
    access_time_tracker _access_time_tracker;
    ss::timer<ss::lowres_clock> _tracker_timer;

    /// Remember when we last finished clean_up_cache, in order to
    /// avoid wastefully running it again soon after.
    ss::lowres_clock::time_point _last_clean_up;

    // If true, no space reservation requests will be granted: this is used to
    // block cache promotions when critically low on disk space.
    bool _block_puts{false};

    // Kick this cond var when block_puts goes true->false to wake up blocked
    // fibers.
    ss::condition_variable _block_puts_cond;

    friend class cache_test_fixture;
};

} // namespace cloud_storage
