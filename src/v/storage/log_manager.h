/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "random/simple_time_jitter.h"
#include "seastarx.h"
#include "storage/batch_cache.h"
#include "storage/log.h"
#include "storage/log_housekeeping_meta.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"
#include "storage/storage_resources.h"
#include "storage/types.h"
#include "storage/version.h"
#include "units.h"
#include "utils/intrusive_list_helpers.h"
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

// class log_config {
struct log_config {
    log_config(
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should,
      ss::io_priority_class compaction_priority
      = ss::default_priority_class()) noexcept;
    log_config(
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should,
      ss::io_priority_class compaction_priority,
      with_cache with) noexcept;
    log_config(
      ss::sstring directory,
      config::binding<size_t> segment_size,
      config::binding<size_t> compacted_segment_size,
      config::binding<size_t> max_compacted_segment_size,
      jitter_percents segment_size_jitter,
      debug_sanitize_files should,
      ss::io_priority_class compaction_priority,
      config::binding<std::optional<size_t>> ret_bytes,
      config::binding<std::chrono::milliseconds> compaction_ival,
      config::binding<std::optional<std::chrono::milliseconds>> del_ret,
      with_cache c,
      batch_cache::reclaim_options recopts,
      std::chrono::milliseconds rdrs_cache_eviction_timeout,
      ss::scheduling_group compaction_sg) noexcept;

    ~log_config() noexcept = default;
    // must be enabled so that we can do ss::sharded<>.start(config);
    log_config(const log_config&) = default;
    // must be enabled so that we can do ss::sharded<>.start(config);
    log_config& operator=(const log_config&) = default;
    log_config(log_config&&) noexcept = default;
    log_config& operator=(log_config&&) noexcept = default;

    ss::sstring base_dir;
    config::binding<size_t> max_segment_size;

    // Default 5% jitter on segment size thresholds
    jitter_percents segment_size_jitter;

    // compacted segment size
    config::binding<size_t> compacted_segment_size;
    config::binding<size_t> max_compacted_segment_size;
    // used for testing: keeps a backtrace of operations for debugging
    debug_sanitize_files sanitize_fileops = debug_sanitize_files::no;
    ss::io_priority_class compaction_priority;
    // same as retention.bytes in kafka
    config::binding<std::optional<size_t>> retention_bytes;
    config::binding<std::chrono::milliseconds> compaction_interval;
    // same as delete.retention.ms in kafka - default 1 week
    config::binding<std::optional<std::chrono::milliseconds>> delete_retention;
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
    explicit log_manager(
      log_config,
      kvstore& kvstore,
      storage_resources&,
      ss::sharded<features::feature_table>&) noexcept;

    ss::future<log> manage(ntp_config);

    ss::future<> shutdown(model::ntp);

    /**
     * Remove an ntp and clean-up its storage.
     *
     * NOTE: if removal of an ntp causes the parent topic directory to become
     * empty then it is also removed. Currently topic deletion is the only
     * action that drives partition removal, so this makes sense. This must be
     * revisited when we start removing partitions for other reasons, like
     * rebalancing partitions across the cluster, etc...
     */
    ss::future<> remove(model::ntp);

    ss::future<> stop();

    ss::future<ss::lw_shared_ptr<segment>> make_log_segment(
      const ntp_config&,
      model::offset,
      model::term_id,
      ss::io_priority_class pc,
      size_t read_buffer_size,
      unsigned read_ahead,
      record_version_type = record_version_type::v1);

    const log_config& config() const { return _config; }

    /// Returns the number of managed logs.
    size_t size() const { return _logs.size(); }

    /// Returns the log for the specified ntp.
    std::optional<log> get(const model::ntp& ntp) {
        if (auto it = _logs.find(ntp); it != _logs.end()) {
            return it->second->handle;
        }
        return std::nullopt;
    }

    /// Returns all ntp's managed by this instance
    absl::flat_hash_set<model::ntp> get_all_ntps() const;

    int64_t compaction_backlog() const;

    storage_resources& resources() { return _resources; }

private:
    using logs_type
      = absl::flat_hash_map<model::ntp, std::unique_ptr<log_housekeeping_meta>>;
    using compaction_list_type
      = intrusive_list<log_housekeeping_meta, &log_housekeeping_meta::link>;

    ss::future<log> do_manage(ntp_config);
    ss::future<> clean_close(storage::log&);

    /**
     * \brief delete old segments and trigger compacted segments
     *        runs inside a seastar thread
     */
    void trigger_housekeeping();
    ss::future<> housekeeping();

    std::optional<batch_cache_index> create_cache(with_cache);

    ss::future<> dispatch_topic_dir_deletion(ss::sstring dir);
    ss::future<> recover_log_state(const ntp_config&);
    ss::future<> async_clear_logs();

    ss::future<> housekeeping_scan(model::timestamp);

    log_config _config;
    kvstore& _kvstore;
    storage_resources& _resources;
    ss::sharded<features::feature_table>& _feature_table;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _housekeeping_timer;
    logs_type _logs;
    compaction_list_type _logs_list;
    batch_cache _batch_cache;
    ss::gate _open_gate;
    ss::abort_source _abort_source;

    friend std::ostream& operator<<(std::ostream&, const log_manager&);
};

} // namespace storage
