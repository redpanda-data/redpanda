#pragma once

#include "model/fundamental.h"
#include "model/limits.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "storage/ntp_config.h"
#include "tristate.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh> //io_priority
#include <seastar/util/bool_class.hh>

#include <optional>
#include <vector>

namespace storage {
using log_clock = ss::lowres_clock;
using debug_sanitize_files = ss::bool_class<struct debug_sanitize_files_tag>;

/// returns base_offset's from batches. Not max_offsets
struct offset_stats {
    model::offset start_offset;
    model::term_id start_offset_term;

    model::offset committed_offset;
    model::term_id committed_offset_term;

    model::offset dirty_offset;
    model::term_id dirty_offset_term;
    // Base offset of the first batch in the most recent term stored in log
    //
    // The last_term_base offset will be used by the followers to inform the
    // raft group leader about the next possible index where truncation should
    // happen while recovering node log.
    model::offset last_term_start_offset;

    friend std::ostream& operator<<(std::ostream&, const offset_stats&);
};

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
    model::term_id last_term;

    friend std::ostream& operator<<(std::ostream& o, const append_result&);
};

using opt_abort_source_t
  = std::optional<std::reference_wrapper<ss::abort_source>>;

struct timequery_config {
    timequery_config(
      model::timestamp t,
      model::offset o,
      ss::io_priority_class iop,
      opt_abort_source_t as = std::nullopt) noexcept
      : time(t)
      , max_offset(o)
      , prio(iop)
      , abort_source(as) {}
    model::timestamp time;
    model::offset max_offset;
    ss::io_priority_class prio;
    opt_abort_source_t abort_source;

    friend std::ostream& operator<<(std::ostream& o, const timequery_config&);
};
struct timequery_result {
    timequery_result(model::offset o, model::timestamp t) noexcept
      : offset(o)
      , time(t) {}
    model::offset offset;
    model::timestamp time;

    friend std::ostream& operator<<(std::ostream& o, const timequery_result&);
};

struct truncate_config {
    truncate_config(model::offset o, ss::io_priority_class p)
      : base_offset(o)
      , prio(p) {}
    model::offset base_offset;
    ss::io_priority_class prio;
    friend std::ostream& operator<<(std::ostream&, const truncate_config&);
};

struct truncate_prefix_config {
    using sloppy_prefix = ss::bool_class<struct sloppy_prefix_truncation>;

    truncate_prefix_config(model::offset o, ss::io_priority_class p)
      : max_offset(o)
      , prio(p) {}
    model::offset max_offset;
    ss::io_priority_class prio;
    sloppy_prefix sloppy = sloppy_prefix::yes;

    friend std::ostream&
    operator<<(std::ostream&, const truncate_prefix_config&);
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
 *
 * Start and max offset are inclusive.
 */
struct log_reader_config {
    model::offset start_offset;
    model::offset max_offset;
    size_t min_bytes;
    size_t max_bytes;
    ss::io_priority_class prio;
    std::optional<model::record_batch_type> type_filter;

    /// \brief gurantees first_timestamp >= record_batch.first_timestamp
    /// it is the std::lower_bound
    std::optional<model::timestamp> first_timestamp;

    /// abort source for read operations
    opt_abort_source_t abort_source;

    // used by log reader
    size_t bytes_consumed{0};

    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      ss::io_priority_class prio,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> time,
      opt_abort_source_t as)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , prio(prio)
      , type_filter(type_filter)
      , first_timestamp(time)
      , abort_source(as) {}

    /**
     * Read offsets [start, end].
     */
    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      ss::io_priority_class prio,
      opt_abort_source_t as = std::nullopt)
      : log_reader_config(
        start_offset,
        max_offset,
        0,
        std::numeric_limits<size_t>::max(),
        prio,
        std::nullopt,
        std::nullopt,
        as) {}

    friend std::ostream& operator<<(std::ostream& o, const log_reader_config&);
};

struct compaction_config {
    explicit compaction_config(
      model::timestamp upper,
      std::optional<size_t> max_bytes_in_log,
      ss::io_priority_class p,
      ss::abort_source& as)
      : eviction_time(upper)
      , max_bytes(max_bytes_in_log)
      , iopc(p)
      , as(as) {}
    // remove everything below eviction time
    model::timestamp eviction_time;
    // remove one segment if log is > max_bytes
    std::optional<size_t> max_bytes;
    // priority for all IO in compaction
    ss::io_priority_class iopc;
    // abort source for compaction task
    ss::abort_source& as;
};

} // namespace storage
