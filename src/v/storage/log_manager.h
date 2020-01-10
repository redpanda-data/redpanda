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
     * If the segment cannot be opened the future will resolve to a nullptr.
     * Otherwise it will resolve to the segment reader instance.
     */
    ss::future<segment_reader_ptr>
    open_segment(const ss::sstring& dir, const ss::sstring& name);

    log_config _config;
    logs_type _logs;
};

} // namespace storage
