/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "model/transform.h"
#include "ssx/semaphore.h"
#include "transform/logging/fwd.h"
#include "transform/logging/io.h"
#include "utils/absl_sstring_hash.h"

#include <seastar/core/lowres_clock.hh>

#include <absl/container/btree_map.h>

namespace transform::logging {

namespace detail {
struct buffer_entry;
template<typename ClockType>
class flusher;
} // namespace detail

/**
 * A class for collecting and publishing log messages emitted by Redpanda Data
 * Transforms.
 *
 * Clients (data transforms) may pass messages to the manager via `enqueue_log`,
 * where they are sanitized stored in outgoing buffers in-memory. This interface
 * is synchronous and does not perform any I/O.
 *
 * Periodically, buffered log messages are JSON serialized, re-collected by
 * output partition ID (computed based on the source transform_name), and
 * published to the transform_logs topic.
 *
 * Constraints:
 *   Single instance per core
 *   Owning service must call manager::start to enable flushing
 *   Buffer occupancy is manager-global and configurable by cluster config
 *     - data_transforms_logging_buffer_capacity_bytes (requires restart)
 *   Messages containing invalid UTF-8 will be dropped
 *   Messages which would otherwise overflow buffer limits will be dropped
 */
template<typename ClockType = ss::lowres_clock>
class manager {
    static_assert(
      std::is_same_v<ClockType, ss::lowres_clock>
        || std::is_same_v<ClockType, ss::manual_clock>,
      "Only lowres or manual clocks are supported");

public:
    manager() = delete;
    ~manager();
    manager(const manager&) = delete;
    manager& operator=(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;

    explicit manager(
      model::node_id,
      std::unique_ptr<client>,
      size_t buffer_cap,
      config::binding<size_t> line_limit,
      config::binding<std::chrono::milliseconds> flush_interval);

    ss::future<> start();
    ss::future<> stop();

    /**
     * Enqueue one log message (along with the source transform_name) for later
     * production to the transform_logs topic.
     *
     * Total log occupancy will not exceed [1]
     * data_transforms_logging_buffer_capacity_bytes with capacity occupied by
     * a certain log message not released until that message has been published
     * to the cluster.
     *
     * Incoming messages are sanitized as follows:
     *   - Messages which do not pass UTF-8 validation are dropped.
     *   - Control characters will be replaced with a control picture
     *   - Messages are truncated to data_transforms_logging_line_max_bytes
     *
     * Buffers are flushed asynchronously on a dedicated, separate fiber. In
     * general, this occurs at a regular, configurable interval. However, if
     * total buffer occupancy meets or exceeds 90% of configured capacity, the
     * flushing fiber is woken up explicitly at the end of the call to
     * enqueue_log.
     *
     * [1] This is a soft limit in the sense that truncation occurs prior to
     *     control character escaping. In the common case (no truncation or
     *     control chars present) the limit is respected strictly overflowing
     *     entries immediately dropped.
     */
    void enqueue_log(
      ss::log_level lvl, model::transform_name_view, std::string_view message);

private:
    // TODO(oren): make configurable?
    static constexpr double lwm_denom = 10;
    bool check_lwm() const;

    model::node_id _self;
    std::unique_ptr<client> _client;
    config::binding<size_t> _line_limit_bytes;
    size_t _buffer_limit_bytes;
    ssize_t _buffer_low_water_mark;
    ssx::semaphore _buffer_sem;
    config::binding<std::chrono::milliseconds> _flush_interval;

    ss::abort_source _as{};

    // per @rockwood
    // TODO(oren): Evaluate (and probably substitute) `chunked_vector` once it
    // lands
    using buffer_t = ss::chunked_fifo<detail::buffer_entry>;
    absl::btree_map<ss::sstring, buffer_t, sstring_less> _log_buffers;
    using probe_map_t = absl::
      btree_map<ss::sstring, std::unique_ptr<logger_probe>, sstring_less>;
    probe_map_t _logger_probes;

    std::unique_ptr<manager_probe> _probe{};
    std::unique_ptr<detail::flusher<ClockType>> _flusher{};
};

} // namespace transform::logging
