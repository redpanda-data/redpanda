#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/log.h"
#include "storage/log_segment.h"
#include "storage/version.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <array>
#include <unordered_map>

namespace storage {

static constexpr size_t default_read_buffer_size = 128 * 1024;

struct log_config {
    sstring base_dir;
    size_t max_segment_size;
    // used for testing: keeps a backtrace of operations for debugging
    using sanitize_files = bool_class<struct sanitize_files_tag>;
    sanitize_files should_sanitize;
};

class log_manager {
    using logs_type
      = std::unordered_map<model::namespaced_topic_partition, log_ptr>;

public:
    log_manager(log_config) noexcept;

    future<> load_logs();

    future<> stop();

    future<log_segment_ptr> make_log_segment(
      const model::namespaced_topic_partition&,
      model::offset,
      model::term_id,
      record_version_type = record_version_type::v1,
      size_t buffer_size = default_read_buffer_size);

    size_t max_segment_size() const {
        return _config.max_segment_size;
    }
    const log_config& config() const {
        return _config;
    }

    logs_type& logs() {
        return _logs;
    }

private:
    future<> do_load_logs(
      sstring path, std::array<sstring, 3> components, unsigned level);
    future<> load_segments(sstring path, model::namespaced_topic_partition);

private:
    log_config _config;
    logs_type _logs;
};

} // namespace storage
