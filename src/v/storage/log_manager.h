/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "random/simple_time_jitter.h"
#include "seastarx.h"
#include "storage/batch_cache.h"
#include "storage/log.h"
#include "storage/log_housekeeping_meta.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"
#include "storage/types.h"
#include "storage/version.h"
#include "units.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <array>
#include <chrono>
#include <optional>

namespace storage {

struct log_config {
    enum class storage_type { memory, disk };
    log_config(
      storage_type type,
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should,
      ss::io_priority_class compaction_priority,
      with_cache with) noexcept
      : stype(type)
      , base_dir(std::move(directory))
      , max_segment_size(segment_size)
      , sanitize_fileops(should)
      , compaction_priority(compaction_priority)
      , cache(with) {}

    log_config(
      storage_type type,
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should,
      ss::io_priority_class compaction_priority
      = ss::default_priority_class()) noexcept
      : stype(type)
      , base_dir(std::move(directory))
      , max_segment_size(segment_size)
      , sanitize_fileops(should)
      , compaction_priority(compaction_priority) {}
    log_config(
      storage_type type,
      ss::sstring directory,
      size_t segment_size,
      size_t compacted_segment_size,
      size_t max_compacted_segment_size,
      debug_sanitize_files should,
      ss::io_priority_class compaction_priority,
      std::optional<size_t> ret_bytes,
      std::chrono::milliseconds compaction_ival,
      std::chrono::milliseconds del_ret,
      with_cache c,
      batch_cache::reclaim_options recopts,
      std::chrono::milliseconds rdrs_cache_eviction_timeout,
      ss::scheduling_group compaction_sg) noexcept
      : stype(type)
      , base_dir(std::move(directory))
      , max_segment_size(segment_size)
      , compacted_segment_size(compacted_segment_size)
      , max_compacted_segment_size(max_compacted_segment_size)
      , sanitize_fileops(should)
      , compaction_priority(compaction_priority)
      , retention_bytes(ret_bytes)
      , compaction_interval(compaction_ival)
      , delete_retention(del_ret)
      , cache(c)
      , reclaim_opts(recopts)
      , readers_cache_eviction_timeout(rdrs_cache_eviction_timeout)
      , compaction_sg(compaction_sg) {}

    ~log_config() noexcept = default;
    // must be enabled so that we can do ss::sharded<>.start(config);
    log_config(const log_config&) = default;
    // must be enabled so that we can do ss::sharded<>.start(config);
    log_config& operator=(const log_config&) = default;
    log_config(log_config&&) noexcept = default;
    log_config& operator=(log_config&&) noexcept = default;

    storage_type stype;
    ss::sstring base_dir;
    size_t max_segment_size;

    // compacted segment size
    size_t compacted_segment_size = 256_MiB;
    size_t max_compacted_segment_size = 5_GiB;
    // used for testing: keeps a backtrace of operations for debugging
    debug_sanitize_files sanitize_fileops = debug_sanitize_files::no;
    ss::io_priority_class compaction_priority;
    // same as retention.bytes in kafka
    std::optional<size_t> retention_bytes = std::nullopt;
    std::chrono::milliseconds compaction_interval = std::chrono::minutes(10);
    // same as delete.retention.ms in kafka - default 1 week
    std::chrono::milliseconds delete_retention = std::chrono::minutes(10080);
    with_cache cache = with_cache::yes;
    batch_cache::reclaim_options reclaim_opts{
      .growth_window = std::chrono::seconds(3),
      .stable_window = std::chrono::seconds(10),
      .min_size = 128_KiB,
      .max_size = 4_MiB,
    };
    std::chrono::milliseconds readers_cache_eviction_timeout
      = std::chrono::seconds(30);
    ss::scheduling_group compaction_sg;
    friend std::ostream& operator<<(std::ostream& o, const log_config&);
}; // namespace storage

/**
 * \brief Create, track, and manage log instances.
 *
 * The log manager is the access point for creating, obtaining, and
 * managing the lifecycle of references to log instances each identified
 * by a model::ntp.
 *
 * Before a log may be accessed it must be brought under management using
 * the interface `manage(ntp)`. This will open the log if it exists on
 * disk. Otherwise, a new log will be initialized and then opened.
 *
 * The log manager uses the file system to organize log storage. All log
 * data (e.g. segments) for a given ntp is managed under a single
 * directory:
 *
 *    <base>/<namespace>/<topic>/<partition>/
 *
 * where <base> is configured for each server (e.g.
 * /var/lib/redpanda/data). Log segments are stored in the ntp directory
 * with the naming convention:
 *
 *   <base offset>-<raft term>-<format version>.log
 *
 * where <base offset> is the smallest offset (inclusive) that maps to /
 * is managed by the segment, <format version> is the binary format of
 * the segment, and <raft term> is special metadata specified by raft as
 * it interacts with the log.
 *
 * Generally the log manager is instantiated as part of a sharded service
 * where each core manages a distinct set of logs. When the service is
 * shut down, calling `stop` on the log manager will close all of the
 * logs currently being managed.
 */
class log_manager {
public:
    explicit log_manager(log_config, kvstore& kvstore) noexcept;

    ss::future<log> manage(ntp_config);

    ss::future<> shutdown(model::ntp);

    /**
     * Remove an ntp and clean-up its storage.
     *
     * NOTE: if removal of an ntp causes the parent topic directory to become
     * empty then it is also removed. Currently topic deletion is the only
     * action that drives partition removal, so this makes sense. This must be
     * revisted when we start removing partitions for other reasons, like
     * rebalancing partitions across the cluster, etc...
     */
    ss::future<> remove(model::ntp);

    ss::future<> stop();

    ss::future<ss::lw_shared_ptr<segment>> make_log_segment(
      const ntp_config&,
      model::offset,
      model::term_id,
      ss::io_priority_class pc,
      record_version_type = record_version_type::v1,
      size_t buffer_size = default_segment_readahead_size);

    const log_config& config() const { return _config; }

    /// Returns the number of managed logs.
    size_t size() const { return _logs.size(); }

    /// Returns the log for the specified ntp.
    std::optional<log> get(const model::ntp& ntp) {
        if (auto it = _logs.find(ntp); it != _logs.end()) {
            return it->second.handle;
        }
        return std::nullopt;
    }

    /// Returns all ntp's managed by this instance
    absl::flat_hash_set<model::ntp> get_all_ntps() const;

    int64_t compaction_backlog() const;

private:
    using logs_type = absl::flat_hash_map<model::ntp, log_housekeeping_meta>;

    ss::future<log> do_manage(ntp_config);

    /**
     * \brief delete old segments and trigger compacted segments
     *        runs inside a seastar thread
     */
    void trigger_housekeeping();
    void arm_housekeeping();
    ss::future<> housekeeping();

    std::optional<batch_cache_index> create_cache(with_cache);

    ss::future<> dispatch_topic_dir_deletion(ss::sstring dir);
    ss::future<> recover_log_state(const ntp_config&);

    log_config _config;
    kvstore& _kvstore;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _compaction_timer;
    logs_type _logs;
    batch_cache _batch_cache;
    ss::gate _open_gate;
    ss::abort_source _abort_source;

    friend std::ostream& operator<<(std::ostream&, const log_manager&);
};
std::ostream& operator<<(std::ostream& o, log_config::storage_type t);
} // namespace storage
