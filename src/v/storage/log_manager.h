#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
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
    using sanitize_files = bool_class<struct sanitize_files_tag>;
    sanitize_files should_sanitize;
};

class log_manager {
public:
    log_manager(log_config) noexcept;

    future<> start();

    future<> stop();

    future<log_segment_ptr> make_log_segment(
      const model::namespaced_topic_partition& ntp,
      model::offset base_offset,
      int64_t term,
      record_version_type = record_version_type::v1,
      size_t buffer_size = default_read_buffer_size);

    size_t max_segment_size() const {
        return _config.max_segment_size;
    }

private:
    log_config _config;
};

} // namespace storage
