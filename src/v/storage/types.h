#pragma once

#include "model/fundamental.h"
#include "model/limits.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "tristate.h"

#include <seastar/core/file.hh>
#include <seastar/util/bool_class.hh>

#include <optional>
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
    model::term_id last_term;

    friend std::ostream& operator<<(std::ostream& o, const append_result&);
};
struct timequery_config {
    timequery_config(
      model::timestamp t, model::offset o, ss::io_priority_class iop) noexcept
      : time(t)
      , max_offset(o)
      , prio(iop) {}
    model::timestamp time;
    model::offset max_offset;
    ss::io_priority_class prio;

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

    // used by log reader
    size_t bytes_consumed{0};

    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      ss::io_priority_class prio,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> time)
      : start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , prio(prio)
      , type_filter(type_filter)
      , first_timestamp(time) {}

    /**
     * Read offsets [start, end].
     */
    log_reader_config(
      model::offset start_offset,
      model::offset max_offset,
      ss::io_priority_class prio)
      : log_reader_config(
        start_offset,
        max_offset,
        0,
        std::numeric_limits<size_t>::max(),
        prio,
        std::nullopt,
        std::nullopt) {}

    friend std::ostream& operator<<(std::ostream& o, const log_reader_config&);
};

struct ntp_config {
    struct default_overrides {
        // if not set use the log_manager's configuration
        std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
        // if not set use the log_manager's configuration
        std::optional<model::compaction_strategy> compaction_strategy;
        // if not set, use the log_manager's configuration
        std::optional<size_t> segment_size;

        // partition retention settings. If tristate is disabled the feature
        // will be disabled if there is no value set the default will be used
        tristate<size_t> retention_bytes{std::nullopt};
        tristate<std::chrono::milliseconds> retention_time{std::nullopt};
        friend std::ostream&
        operator<<(std::ostream&, const default_overrides&);
    };

    ntp_config(model::ntp n, ss::sstring base_dir) noexcept
      : ntp(std::move(n))
      , base_dir(std::move(base_dir)) {}

    ntp_config(
      model::ntp n,
      ss::sstring base_dir,
      std::unique_ptr<default_overrides> overrides) noexcept
      : ntp(std::move(n))
      , base_dir(std::move(base_dir))
      , overrides(std::move(overrides)) {}

    model::ntp ntp;
    /// \brief currently this is the basedir. In the future
    /// this will be used to load balance on devices so that there is no
    /// implicit hierarchy, simply directories with data
    ss::sstring base_dir;

    std::unique_ptr<default_overrides> overrides;

    ss::sstring work_directory() const {
        return fmt::format("{}/{}", base_dir, ntp.path());
    }

    friend std::ostream& operator<<(std::ostream&, const ntp_config&);
};
} // namespace storage
