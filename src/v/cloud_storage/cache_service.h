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

#include "base/seastarx.h"
#include "base/units.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/cache_probe.h"
#include "cloud_storage/recursive_directory_walker.h"
#include "config/configuration.h"
#include "config/property.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/semaphore.h"
#include "storage/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/thread.hh>

#include <filesystem>
#include <iterator>
#include <optional>
#include <set>
#include <string_view>

namespace cloud_storage {

// These timeout/backoff settings are for S3 requests
using namespace std::chrono_literals;
inline const ss::lowres_clock::duration cache_hydration_timeout = 60s;
inline const ss::lowres_clock::duration cache_hydration_backoff = 250ms;

// This backoff is for failure of the local cache to retain recently
// promoted data (i.e. highly stressed cache)
inline const ss::lowres_clock::duration cache_thrash_backoff = 5000ms;

class cache_test_fixture;

using cache_item = cloud_io::cache_item;

using cache_element_status = cloud_io::cache_element_status;

class cache;

/// RAII guard for bytes reserved in the cache: constructed prior to a call
/// to cache::put, and may be destroyed afterwards.
using space_reservation_guard
  = cloud_io::basic_space_reservation_guard<ss::lowres_clock>;

class cache
  : public cloud_io::basic_cache_service_api<ss::lowres_clock>
  , public ss::peering_sharded_service<cache> {
public:
    /// C-tor.
    ///
    /// \param cache_dir is a directory where cached data is stored
    cache(
      std::filesystem::path cache_dir,
      size_t disk_size,
      config::binding<double> disk_reservation,
      config::binding<uint64_t> max_bytes_cfg,
      config::binding<std::optional<double>> max_percent,
      config::binding<uint32_t> max_objects,
      config::binding<uint16_t> walk_concurrency) noexcept;

    cache(const cache&) = delete;
    cache(cache&& rhs) = delete;

    ss::future<> start();
    ss::future<> stop();

    /// Get cached value as a stream if it exists on disk
    ///
    /// \param key is a cache key
    ss::future<std::optional<cache_item>> get(std::filesystem::path key);

    ss::future<std::optional<cloud_io::cache_item_stream>> get(
      std::filesystem::path key,
      ss::io_priority_class io_priority,
      size_t read_buffer_size = cloud_io::default_read_buffer_size,
      unsigned int read_ahead = cloud_io::default_read_ahead) override;

    /// Add new value to the cache, overwrite if it's already exist
    ///
    /// \param key is a cache key
    /// \param io_priority is an io priority of disk write operation
    /// \param data is an input stream containing data
    /// \param write_buffer_size is a write buffer size for disk write
    /// \param write_behind number of pages that can be written asynchronously
    /// \param reservation caller must have reserved cache space before
    /// proceeding with put
    ss::future<> put(
      std::filesystem::path key,
      ss::input_stream<char>& data,
      space_reservation_guard& reservation,
      ss::io_priority_class io_priority
      = priority_manager::local().shadow_indexing_priority(),
      size_t write_buffer_size = cloud_io::default_write_buffer_size,
      unsigned int write_behind = cloud_io::default_write_behind) override;

    /// \brief Checks if the value is cached
    ///
    /// \note returned value \c cache_element_status::in_progress is
    /// shard-local, it indicates that the object is being written by this
    /// shard. If the value is being written by another shard, this function
    /// returns only committed result. The value
    /// \c cache_element_status::in_progress can be used as a hint since ntp are
    /// stored on the same shard most of the time.
    ss::future<cache_element_status>
    is_cached(const std::filesystem::path& key) override;

    /// Remove element from cache by key
    ss::future<> invalidate(const std::filesystem::path& key);

    // Total cleaned is exposed for better testability of eviction
    uint64_t get_total_cleaned();

    // Call this before starting a download, to trim the cache if necessary
    // and wait until enough free space is available.
    ss::future<space_reservation_guard>
      reserve_space(uint64_t, size_t) override;

    // Release capacity acquired via `reserve_space`.  This spawns
    // a background fiber in order to be callable from the guard destructor.
    void reserve_space_release(uint64_t, size_t, uint64_t, size_t) override;

    static ss::future<> initialize(std::filesystem::path);

    /// Shard 0 only.  Update the utilization status of local disk.  Will
    /// call onwards to other shards as needed.
    void notify_disk_status(
      uint64_t total_space,
      uint64_t free_space,
      storage::disk_space_alert alert);

    size_t get_usage_objects() { return _current_cache_objects; }

    uint64_t get_usage_bytes() { return _current_cache_size; }

    uint64_t get_max_bytes() { return _max_bytes; }

    uint64_t get_max_objects() { return _max_objects(); }

    /// Administrative trim, that specifies its own limits instead of using
    /// the configured limits (skips throttling, and can e.g. trim to zero bytes
    /// if they want to)
    ss::future<> trim_manually(
      std::optional<uint64_t> size_limit_override,
      std::optional<size_t> object_limit_override);

    std::filesystem::path
    get_local_path(const std::filesystem::path& key) const {
        return _cache_dir / key;
    }

    // Checks if a cluster configuration is valid for the properties
    // `cloud_storage_cache_size` and `cloud_storage_cache_size_percent`.
    // Two cases are invalid: 1. the case in which both are 0, 2. the case in
    // which `cache_size` is 0 while `cache_size_percent` is `std::nullopt`.
    //
    // Returns `std::nullopt` if the passed configuration is valid, or an
    // `ss::sstring` explaining the misconfiguration otherwise.
    static std::optional<ss::sstring>
    validate_cache_config(const config::configuration& conf);

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
    ss::future<> trim(
      std::optional<uint64_t> size_limit_override = std::nullopt,
      std::optional<size_t> object_limit_override = std::nullopt);

    struct trim_result {
        uint64_t deleted_size{0};
        size_t deleted_count{0};
        bool trim_missed_tmp_files{false};
    };

    ss::future<std::optional<cache_item>> _get(std::filesystem::path key);

    /// Remove object from cache
    ss::future<> _invalidate(const std::filesystem::path& key);

    ss::future<cache_element_status>
    _is_cached(const std::filesystem::path& key);

    /// Ordinary trim: prioritze trimming data chunks, only delete indices etc
    /// if all their chunks are dropped.
    ss::future<trim_result> trim_fast(
      const fragmented_vector<file_list_item>& candidates,
      uint64_t delete_bytes,
      size_t delete_objects);

    ss::future<trim_result> do_trim(
      const fragmented_vector<file_list_item>& candidates,
      uint64_t delete_bytes,
      size_t delete_objects);

    /// Exhaustive trim: walk all files including indices, remove whatever is
    /// least recently accessed.
    ss::future<trim_result>
    trim_exhaustive(uint64_t delete_bytes, size_t delete_objects);

    /// If trimming may proceed immediately, return nullopt.  Else return
    /// how long the caller should wait before trimming to respect the
    /// rate limit.
    std::optional<std::chrono::milliseconds> get_trim_delay() const;

    /// Invoke trim, waiting if not enough time passed since the last trim
    ss::future<> trim_throttled_unlocked(
      std::optional<uint64_t> size_limit_override = std::nullopt,
      std::optional<size_t> object_limit_override = std::nullopt);

    // Take the cleanup semaphore before calling trim_throttled
    ss::future<> trim_throttled(
      std::optional<uint64_t> size_limit_override = std::nullopt,
      std::optional<size_t> object_limit_override = std::nullopt);

    void maybe_background_trim();

    /// Whether an objects path makes it impervious to pinning, like
    /// the access time tracker.
    bool is_trim_exempt(const ss::sstring&) const;

    ss::future<std::optional<uint64_t>> access_time_tracker_size() const;

    /// Triggers directory walker, creates a list of files to delete and deletes
    /// only tmp files that are left from previous Red Panda run
    ss::future<> clean_up_at_start();

    /// Deletes a file and then recursively goes up and deletes a directory
    /// until it meet a non-empty directory.
    ///
    /// \param key if a path to a file what should be deleted
    /// \return true if any parents were deleted
    ss::future<bool> delete_file_and_empty_parents(const std::string_view& key);

    /// Block until enough space is available to commit to a reservation
    /// (only runs on shard 0)
    ss::future<> do_reserve_space(uint64_t, size_t);

    /// Trim cache using results from the previous recursive directory walk
    ss::future<trim_result>
    trim_carryover(uint64_t delete_bytes, uint64_t delete_objects);

    /// Return true if the sum of used space and reserved space is far enough
    /// below max size to accommodate a new reservation of `bytes`
    /// (only runs on shard 0)
    bool may_reserve_space(uint64_t, size_t);

    /// Return true if it is safe to overshoot the configured limits, even if
    /// may_reserve_space returned false.  This enables selectively exceeding
    /// our configured limits to enable progress if trimming is not possible.
    bool may_exceed_limits(uint64_t, size_t);

    /// Release units from _reserved_cache_size: the inner part of
    /// `reserve_space_release`
    /// (only runs on shard 0)
    void do_reserve_space_release(uint64_t, size_t, uint64_t, size_t);

    /// Update _block_puts and kick _block_puts_cond if necessary.  This is
    /// called on all shards by shard 0 when handling a disk space status
    /// update.
    void set_block_puts(bool);

    /// Remove segment or chunk subdirectory with all its auxilary files (tx,
    /// index)
    ss::future<cache::trim_result>
    remove_segment_full(const file_list_item& file_stat);

    ss::future<> sync_access_time_tracker(
      access_time_tracker::add_entries_t add_entries
      = access_time_tracker::add_entries_t::no);
    std::filesystem::path _cache_dir;
    size_t _disk_size;
    config::binding<double> _disk_reservation;
    config::binding<uint64_t> _max_bytes_cfg;
    config::binding<std::optional<double>> _max_percent;
    uint64_t _max_bytes;
    config::binding<uint32_t> _max_objects;
    config::binding<uint16_t> _walk_concurrency;
    void update_max_bytes();

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

    size_t _current_cache_objects{0};

    /// Bytes reserved by downloads in progress, owned by instances of
    /// space_reservation_guard.
    uint64_t _reserved_cache_size{0};
    size_t _reserved_cache_objects{0};

    /// Bytes waiting to be reserved when the next cache trim completes: a
    /// hint to clean_up_cache on how much extra space to try and free
    uint64_t _reservations_pending{0};
    size_t _reservations_pending_objects{0};

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
    ssx::semaphore _access_tracker_writer_sm{
      1, "cloud/cache/access_tracker_writer"};

    /// Remember when we last finished clean_up_cache, in order to
    /// avoid wastefully running it again soon after.
    ss::lowres_clock::time_point _last_clean_up;

    /// 'failed' in the sense that we were unable to release sufficient space to
    /// enable a may_reserve_space() call to return true.
    bool _last_trim_failed{false};

    // If true, no space reservation requests will be granted: this is used to
    // block cache promotions when critically low on disk space.
    bool _block_puts{false};

    // Kick this cond var when block_puts goes true->false to wake up blocked
    // fibers.
    ss::condition_variable _block_puts_cond;

    friend class cache_test_fixture;

    // List of probable deletion candidates from the last trim.
    std::optional<fragmented_vector<file_list_item>> _last_trim_carryover;

    ss::timer<ss::lowres_clock> _tracker_sync_timer;
    ssx::semaphore _tracker_sync_timer_sem{
      1, "cloud/cache/access_tracker_sync"};
};

} // namespace cloud_storage
