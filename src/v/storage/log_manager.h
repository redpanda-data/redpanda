#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/version.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <array>
#include <unordered_map>

namespace storage {

static constexpr size_t default_read_buffer_size = 128 * 1024;

struct log_config {
    ss::sstring base_dir;
    size_t max_segment_size;
    // used for testing: keeps a backtrace of operations for debugging
    using sanitize_files = ss::bool_class<struct sanitize_files_tag>;
    sanitize_files should_sanitize;
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
    explicit log_manager(log_config) noexcept;

    ss::future<log_ptr> manage(model::ntp);

    ss::future<> stop();

    struct log_handles {
        segment_reader_ptr reader;
        segment_appender_ptr appender;
    };
    ss::future<log_handles> make_log_segment(
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
    log_ptr log(const model::ntp& ntp) {
        if (auto it = _logs.find(ntp); it != _logs.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    using logs_type = std::unordered_map<model::ntp, log_ptr>;

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
     * Returns an open segment reader if the segment was successfully opened.
     */
    ss::future<segment_reader_ptr> open_segment(
      const std::filesystem::path& path,
      size_t buf_size = default_read_buffer_size);

    /**
     * \brief Open all segments in a directory.
     *
     * Returns an exceptional future if any error occured opening a segment.
     * Otherwise all open segment readers are returned.
     */
    ss::future<std::vector<segment_reader_ptr>> open_segments(ss::sstring path);

    log_config _config;
    logs_type _logs;
};

} // namespace storage
