/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "config/property.h"
#include "config/throughput_control_group.h"
#include "kafka/server/atomic_token_bucket.h"
#include "metrics/metrics.h"
#include "ssx/sharded_ptr.h"
#include "utils/log_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/shared_token_bucket.hh>

#include <chrono>
#include <optional>
#include <string_view>

namespace kafka {

/// Represents a homogenous pair of values that correspond to
/// ingress and egress side of the same entity
template<class T>
struct ingress_egress_state {
    T in;
    T eg;
};

class snc_quotas_probe {
public:
    snc_quotas_probe() = default;
    snc_quotas_probe(const snc_quotas_probe&) = delete;
    snc_quotas_probe& operator=(const snc_quotas_probe&) = delete;
    snc_quotas_probe(snc_quotas_probe&&) = delete;
    snc_quotas_probe& operator=(snc_quotas_probe&&) = delete;
    ~snc_quotas_probe() noexcept = default;

    void rec_traffic_in(const size_t bytes) noexcept { _traffic.in += bytes; }
    void rec_traffic_eg(const size_t bytes) noexcept { _traffic.eg += bytes; }

    void setup_metrics();

    auto get_traffic_in() const { return _traffic.in; }
    auto get_traffic_eg() const { return _traffic.eg; }

    auto record_throttle_time(std::chrono::microseconds t) {
        return _throttle_time_us.record(t.count());
    }

    auto get_throttle_time() const {
        return _throttle_time_us.public_histogram_logform();
    }

private:
    metrics::internal_metric_groups _metrics;
    ingress_egress_state<size_t> _traffic = {};
    log_hist_public _throttle_time_us;
};

class snc_quota_context {
public:
    explicit snc_quota_context(std::optional<std::string_view> client_id)
      : _client_id(client_id) {}

private:
    friend class snc_quota_manager;

    // Indexing
    std::optional<ss::sstring> _client_id;

    // Configuration

    /// Whether the connection belongs to an exempt tput control group
    bool _exempt{false};
};

/// Isolates \ref quota_manager functionality related to
/// shard/node/cluster (SNC) wide quotas and limits
class snc_quota_manager
  : public ss::peering_sharded_service<snc_quota_manager> {
public:
    using clock = ss::lowres_clock;
    using bucket_t = atomic_token_bucket;
    using buckets_t = kafka::ingress_egress_state<
      ssx::sharded_ptr<kafka::snc_quota_manager::bucket_t>>;

    explicit snc_quota_manager(buckets_t& node_quota);
    snc_quota_manager(const snc_quota_manager&) = delete;
    snc_quota_manager& operator=(const snc_quota_manager&) = delete;
    snc_quota_manager(snc_quota_manager&&) = delete;
    snc_quota_manager& operator=(snc_quota_manager&&) = delete;
    ~snc_quota_manager() noexcept = default;

    ss::future<> start();
    ss::future<> stop();

    /// @p request delay to request from the client via throttle_ms
    struct delays_t {
        clock::duration request{0};
    };

    /// Depending on the other arguments, create or actualize or keep the
    /// existing \p ctx. The context object is supposed to be stored
    /// in the connection context, and created only once per connection
    /// lifetime. However since the kafka API allows changing client_id of a
    /// connection on the fly, we may need to replace the existing context with
    /// a new one if that happens (actualize).
    /// \post (bool)ctx == true
    void get_or_create_quota_context(
      std::unique_ptr<snc_quota_context>& ctx,
      std::optional<std::string_view> client_id);

    /// Determine throttling required by shard level TP quotas.
    delays_t get_shard_delays(const snc_quota_context&) const;

    /// Record the request size when it has arrived from the transport.
    /// This should be done before calling \ref get_shard_delays because the
    /// recorded request size is used to calculate throttling parameters.
    void record_request_receive(
      const snc_quota_context&,
      size_t request_size,
      clock::time_point now = clock::now()) noexcept;

    /// Record the request size when the request data is about to be consumed.
    /// This data is used to represent throttled throughput.
    void record_request_intake(
      const snc_quota_context&, size_t request_size) noexcept;

    /// Record the response size for all purposes
    void record_response(
      const snc_quota_context&,
      size_t request_size,
      clock::time_point now = clock::now()) noexcept;

    /// Metrics probe object
    const snc_quotas_probe& get_snc_quotas_probe() const noexcept {
        return _probe;
    };

private:
    void update_balance_config();
    // configuration
    config::binding<std::chrono::milliseconds> _max_kafka_throttle_delay;
    ingress_egress_state<config::binding<std::optional<int64_t>>>
      _kafka_throughput_limit_node_bps;
    config::binding<std::vector<config::throughput_control_group>>
      _kafka_throughput_control;

    // operational, only used in the balancer shard
    ss::gate _balancer_gate;

    buckets_t& _node_quota;

    // service
    mutable snc_quotas_probe _probe;
};

} // namespace kafka
