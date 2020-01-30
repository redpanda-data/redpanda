#pragma once

#include "model/fundamental.h"
#include "model/limits.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/file.hh>
#include <seastar/util/bool_class.hh>

#include <vector>

namespace storage {
using log_clock = ss::lowres_clock;

struct log_append_config {
    using fsync = ss::bool_class<class skip_tag>;
    fsync should_fsync;
    ss::io_priority_class io_priority;
    model::timeout_clock::time_point timeout;
};
struct append_result {
    log_clock::time_point append_time;
    model::offset base_offset;
    model::offset last_offset;
    size_t byte_size;
};
/**
 * Log reader configuration.
 *
 * The default reader configuration will read all batch types. To filter batches
 * by type add the types of interest to the type_filter set.
 *
 * The type filter is sorted before a segment scan, and a linear search is
 * performed. This will generally perform better than something like a binary
 * search when the size of the filter set is small (e.g. < 5). If you need to
 * use a larger filter then this design should be revisited.
 */
struct log_reader_config {
    model::offset start_offset;
    size_t max_bytes;
    size_t min_bytes;
    ss::io_priority_class prio;
    std::vector<model::record_batch_type> type_filter;
    model::offset max_offset
      = model::model_limits<model::offset>::max(); // inclusive
    size_t bytes_consumed{0};
};

inline std::ostream& operator<<(std::ostream& o, const log_reader_config& cfg) {
    return o << "{start_offset:" << cfg.start_offset
             << ", max_offset:" << cfg.max_offset
             << ", min_bytes:" << cfg.min_bytes
             << ", max_bytes:" << cfg.max_bytes
             << ", type_filter_size:" << cfg.type_filter.size() << "}";
}

inline std::ostream& operator<<(std::ostream& o, const append_result& a) {
    return o << "{append_time:"
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                  a.append_time.time_since_epoch())
                  .count()
             << ", base_offset:" << a.base_offset
             << ", last_offset:" << a.last_offset
             << ", byte_size:" << a.byte_size << "}";
}

} // namespace storage
