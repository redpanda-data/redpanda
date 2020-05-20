#pragma once

#include "model/fundamental.h"
#include "random/simple_time_jitter.h"
#include "seastarx.h"
#include "storage/batch_cache.h"
#include "storage/log.h"
#include "storage/log_housekeeping_meta.h"
#include "storage/segment.h"
#include "storage/types.h"
#include "storage/version.h"
#include "units.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <array>
#include <chrono>
#include <optional>

namespace storage {

static constexpr size_t default_read_buffer_size = 128 * 1024;

struct log_config {
    enum class storage_type { memory, disk };
    using debug_sanitize_files
      = ss::bool_class<struct debug_sanitize_files_tag>;
    using with_cache = ss::bool_class<struct log_cache_tag>;
    log_config(
      storage_type type,
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should,
      with_cache with) noexcept
      : stype(type)
      , base_dir(std::move(directory))
      , max_segment_size(segment_size)
      , sanitize_fileops(should)
      , cache(with) {}
    log_config(
      storage_type type,
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should) noexcept
      : stype(type)
      , base_dir(std::move(directory))
      , max_segment_size(segment_size)
      , sanitize_fileops(should) {}
    log_config(
      storage_type type,
      ss::sstring directory,
      size_t segment_size,
      debug_sanitize_files should,
      std::optional<size_t> ret_bytes,
      std::chrono::milliseconds compaction_ival,
      std::chrono::milliseconds del_ret,
      with_cache c,
      batch_cache::reclaim_options recopts) noexcept
      : stype(type)
      , base_dir(std::move(directory))
      , max_segment_size(segment_size)
      , sanitize_fileops(should)
      , retention_bytes(ret_bytes)
      , compaction_interval(compaction_ival)
      , delete_retention(del_ret)
      , cache(c)
      , reclaim_opts(recopts) {}

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

    // used for testing: keeps a backtrace of operations for debugging
    debug_sanitize_files sanitize_fileops
      = log_config::debug_sanitize_files::no;
    // same as retention.bytes in kafka
    std::optional<size_t> retention_bytes = std::nullopt;
    std::chrono::milliseconds compaction_interval = std::chrono::minutes(10);
    // same as delete.retention.ms in kafka - default 1 week
    std::chrono::milliseconds delete_retention = std::chrono::minutes(10080);
    with_cache cache = log_config::with_cache::yes;
    batch_cache::reclaim_options reclaim_opts{
      .growth_window = std::chrono::seconds(3),
      .stable_window = std::chrono::seconds(10),
      .min_size = 128_KiB,
      .max_size = 4_MiB,
    };

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
    explicit log_manager(log_config) noexcept;

    ss::future<log> manage(ntp_config);
    ss::future<> remove(model::ntp);

    ss::future<> stop();

    ss::future<ss::lw_shared_ptr<segment>> make_log_segment(
      const ntp_config&,
      model::offset,
      model::term_id,
      ss::io_priority_class pc,
      record_version_type = record_version_type::v1,
      size_t buffer_size = default_read_buffer_size);

    size_t max_segment_size() const { return _config.max_segment_size; }
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

private:
    using logs_type = absl::flat_hash_map<model::ntp, log_housekeeping_meta>;

    ss::future<log> do_manage(ntp_config);
    ss::future<ss::lw_shared_ptr<segment>> do_make_log_segment(
      const ntp_config&,
      model::offset,
      model::term_id,
      ss::io_priority_class pc,
      record_version_type,
      size_t buffer_size);

    /**
     * \brief Create a segment reader for the specified file.
     *
     * Returns an exceptional future if the segment cannot be opened.
     * This may occur due to many reasons such as a file system error, or
     * because the segment is corrupt or is stored in an unsupported
     * format.
     *
     * Returns a ready future containing a nullptr value if the specified
     * file is not a segment file.
     *
     * Returns an open segment if the segment was successfully opened.
     * Including a valid index and recovery for the index if one does not
     * exist
     */
    ss::future<ss::lw_shared_ptr<segment>> open_segment(
      const std::filesystem::path& path,
      size_t buf_size = default_read_buffer_size);

    /**
     * \brief Open all segments in a directory.
     *
     * Returns an exceptional future if any error occured opening a
     * segment. Otherwise all open segment readers are returned.
     */
    ss::future<ss::circular_buffer<ss::lw_shared_ptr<segment>>>
    open_segments(ss::sstring path);

    /**
     * \brief delete old segments and trigger compacted segments
     *        runs inside a seastar thread
     */
    void trigger_housekeeping();
    void arm_housekeeping();
    ss::future<> housekeeping();

    std::optional<batch_cache_index> create_cache();

    log_config _config;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _compaction_timer;
    logs_type _logs;
    batch_cache _batch_cache;
    ss::gate _open_gate;

    friend std::ostream& operator<<(std::ostream&, const log_manager&);
};
std::ostream& operator<<(std::ostream& o, log_config::storage_type t);
} // namespace storage
