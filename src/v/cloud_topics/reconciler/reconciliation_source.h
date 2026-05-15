/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/reconciler/source_probe.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <expected>
#include <optional>

namespace cluster {
class metadata_cache;
class partition;
struct topic_properties;
} // namespace cluster

namespace cloud_topics::reconciler {

// An abstraction for what the reconciler reads from, as well as the source for
// bookkeeping progress that has been made.
class source {
public:
    // Error codes for a reconcilation source.
    enum class errc : uint8_t {
        timeout,
        not_leader,
        shutdown,
        failure,
    };

    source(model::ntp ntp, model::topic_id_partition tidp)
      : _ntp(std::move(ntp))
      , _tidp(tidp)
      , _probe(_ntp, *this) {}
    source(const source&) = delete;
    source(source&&) = delete;
    source& operator=(const source&) = delete;
    source& operator=(source&&) = delete;
    virtual ~source() = default;

    // Deregister metrics so a new source for the same partition can be
    // created while this shared_ptr is still alive.
    void deregister_metrics() { _probe.clear(); }

    // The NTP for this source
    const model::ntp& ntp() const { return _ntp; }
    // The topic ID + partition for the source
    const model::topic_id_partition& topic_id_partition() const {
        return _tidp;
    }

    // Returns true if there may be new data to reconcile (LSO > LRO).
    virtual bool has_pending_data() = 0;

    // Returns the number of offsets pending reconciliation (LSO -
    // next_offset(LRO)), or 0 if there is no pending data or LSO is
    // unavailable.
    virtual int64_t pending_offset_lag() = 0;

    // Get the last reconciled offset for this source, or kafka::offset::min()
    // if none.
    virtual kafka::offset last_reconciled_offset() = 0;

    // Set the last reconciled offset for this source.
    //
    // This operation is idempotent, the last reconciled offset never moves
    // back.
    virtual ss::future<std::expected<void, errc>>
    set_last_reconciled_offset(kafka::offset, ss::abort_source&) = 0;

    struct reader_config {
        // The offset to start reading at.
        kafka::offset start_offset;
        // The soft limit for number of bytes to read.
        size_t max_bytes;
        // The abort source for when to stop the reader. The abort source must
        // live as long as the returned reader from `make_reader`.
        ss::abort_source* as;
    };

    // Create a reader for the reconciliation source, data should only be read
    // above `last_reconciled_offset`.
    //
    // It *is* valid for this reader to outlive `source`.
    virtual ss::future<model::record_batch_reader>
      make_reader(reader_config) = 0;

    // Compute the target value for the allowed_local_start_offset hint.
    //
    // Returns:
    // - std::nullopt outer: the hint should not be evaluated/published (e.g.,
    //   not leader, topic config missing, or this isn't a tiered_cloud topic).
    // - std::optional<kafka::offset> inner: when present, the target value of
    //   the hint that should be published via ctp_stm:
    //     * Some(offset): clamp local log to this kafka offset.
    //     * nullopt: clear the hint (no local-retention behavior).
    //
    // The outer optional disambiguates "do not touch the hint at all" from
    // "publish a value (possibly nullopt)".
    virtual ss::future<std::optional<std::optional<kafka::offset>>>
    compute_local_retention_target(const cluster::topic_properties&) = 0;

    // Replicate the allowed_local_start_offset hint value via ctp_stm.
    virtual ss::future<std::expected<void, errc>>
    publish_local_retention_target(
      std::optional<kafka::offset>, ss::abort_source&) = 0;

    // Per-partition bookkeeping for the local-retention evaluator.
    size_t local_retention_bytes_since_eval() const noexcept {
        return _local_retention_bytes_since_eval;
    }
    void add_local_retention_bytes(size_t n) noexcept {
        _local_retention_bytes_since_eval += n;
    }
    void reset_local_retention_eval_counter() noexcept {
        _local_retention_bytes_since_eval = 0;
    }

    std::optional<ss::lowres_clock::time_point>
    local_retention_last_eval_time() const noexcept {
        return _local_retention_last_eval_time;
    }
    void set_local_retention_last_eval_time(
      ss::lowres_clock::time_point t) noexcept {
        _local_retention_last_eval_time = t;
    }

    std::optional<std::optional<kafka::offset>>
    local_retention_last_published() const noexcept {
        return _local_retention_last_published;
    }
    void set_local_retention_last_published(
      std::optional<kafka::offset> o) noexcept {
        _local_retention_last_published = o;
    }

private:
    model::ntp _ntp;
    model::topic_id_partition _tidp;
    source_probe _probe;

    size_t _local_retention_bytes_since_eval{0};
    std::optional<ss::lowres_clock::time_point> _local_retention_last_eval_time;
    std::optional<std::optional<kafka::offset>> _local_retention_last_published;
};

// Make a reconciliation source from L0 components (data plane) and the cluster
// partition (containing metadata).
ss::shared_ptr<source> make_source(
  model::ntp,
  model::topic_id_partition,
  data_plane_api*,
  ss::lw_shared_ptr<cluster::partition>);

} // namespace cloud_topics::reconciler

template<>
struct fmt::formatter<cloud_topics::reconciler::source::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      const cloud_topics::reconciler::source::errc&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
