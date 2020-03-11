#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/batch_cache.h"
#include "storage/log.h"
#include "storage/segment.h"
#include "storage/version.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>

#include <array>
#include <chrono>
#include <optional>

namespace storage {

static constexpr size_t default_read_buffer_size = 128 * 1024;

struct log_config {
    ss::sstring base_dir;
    size_t max_segment_size;
    // used for testing: keeps a backtrace of operations for debugging
    using sanitize_files = ss::bool_class<struct sanitize_files_tag>;
    using disable_batch_cache = ss::bool_class<struct disable_cache_tag>;
    sanitize_files should_sanitize;
    std::chrono::milliseconds compaction_interval = std::chrono::minutes(1);
    disable_batch_cache disable_cache = disable_batch_cache::no;
};

/**
 * \brief Create, track, and manage log instances.
 *
 * The log manager is the access point for creating, obtaining, and managing the
 * lifecycle of references to log instances each identified by a model::ntp.
 *
 * Before a log may be accessed it must be brought under management using the
 * interface `manage(ntp)`. This will open the log if it exists on disk.
 * Otherwise, a new log will be initialized and then opened.
 *
 * The log manager uses the file system to organize log storage. All log data
 * (e.g. segments) for a given ntp is managed under a single directory:
 *
 *    <base>/<namespace>/<topic>/<partition>/
 *
 * where <base> is configured for each server (e.g. /var/lib/redpanda/data). Log
 * segments are stored in the ntp directory with the naming convention:
 *
 *   <base offset>-<raft term>-<format version>.log
 *
 * where <base offset> is the smallest offset (inclusive) that maps to / is
 * managed by the segment, <format version> is the binary format of the segment,
 * and <raft term> is special metadata specified by raft as it interacts with
 * the log.
 *
 * Generally the log manager is instantiated as part of a sharded service where
 * each core manages a distinct set of logs. When the service is shut down,
 * calling `stop` on the log manager will close all of the logs currently being
 * managed.
 */
class log_manager {
public:
    // TODO: move storage_type into log_config
    enum class storage_type { memory, disk };

    explicit log_manager(log_config) noexcept;

    ss::future<log> manage(model::ntp, storage_type type = storage_type::disk);

    ss::future<> stop();

    ss::future<ss::lw_shared_ptr<segment>> make_log_segment(
      const model::ntp&,
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
            return it->second;
        }
        return std::nullopt;
    }

private:
    using logs_type = absl::flat_hash_map<model::ntp, log>;

    ss::future<log> do_manage(model::ntp, storage_type type);
    ss::future<ss::lw_shared_ptr<segment>> do_make_log_segment(
      const model::ntp&,
      model::offset,
      model::term_id,
      ss::io_priority_class pc,
      record_version_type,
      size_t buffer_size);

    /**
     * \brief Create a segment reader for the specified file.
     *
     * Returns an exceptional future if the segment cannot be opened. This may
     * occur due to many reasons such as a file system error, or because the
     * segment is corrupt or is stored in an unsupported format.
     *
     * Returns a ready future containing a nullptr value if the specified file
     * is not a segment file.
     *
     * Returns an open segment if the segment was successfully opened.
     * Including a valid index and recovery for the index if one does not exist
     */
    ss::future<ss::lw_shared_ptr<segment>> open_segment(
      const std::filesystem::path& path,
      size_t buf_size = default_read_buffer_size);

    /**
     * \brief Open all segments in a directory.
     *
     * Returns an exceptional future if any error occured opening a segment.
     * Otherwise all open segment readers are returned.
     */
    ss::future<ss::circular_buffer<ss::lw_shared_ptr<segment>>>
    open_segments(ss::sstring path);

    /**
     * \brief delete old segments and trigger compacted segments
     *        runs inside a seastar thread
     */
    void trigger_housekeeping();
    ss::future<> housekeeping();

    std::optional<batch_cache_index> create_cache();
    
    log_config _config;
    ss::timer<> _compaction_timer;
    logs_type _logs;
    batch_cache _batch_cache;
    ss::gate _open_gate;
};
std::ostream& operator<<(std::ostream& o, log_manager::storage_type t);
} // namespace storage
