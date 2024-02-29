// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/snc_quota_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/future-util.h"
#include "ssx/sharded_ptr.h"
#include "utils/tristate.h"

#include <seastar/core/metrics.hh>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <chrono>
#include <iterator>
#include <memory>
#include <numeric>

using namespace std::chrono_literals;

namespace kafka {

static constexpr ss::shard_id quota_balancer_shard = 0;

template<std::integral T>
ingress_egress_state<T> operator+(
  const ingress_egress_state<T>& lhs, const ingress_egress_state<T>& rhs) {
    return {.in = lhs.in + rhs.in, .eg = lhs.eg + rhs.eg};
}
template<std::integral T>
ingress_egress_state<T> operator-(
  const ingress_egress_state<T>& lhs, const ingress_egress_state<T>& rhs) {
    return {.in = lhs.in - rhs.in, .eg = lhs.eg - rhs.eg};
}
template<std::integral T>
ingress_egress_state<T> operator-(const ingress_egress_state<T>& v) {
    return {.in = -v.in, .eg = -v.eg};
}

void snc_quotas_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    std::vector<ss::metrics::impl::metric_definition_impl> metric_defs;
    static const auto direction_label = ss::metrics::label("direction");
    static const auto label_ingress = direction_label("ingress");
    static const auto label_egress = direction_label("egress");

    {
        static const char* name = "quota_effective";
        static const auto desc = sm::description(
          "Currently effective quota, in bytes/s");
        static constexpr auto calc_quota = [](const std::optional<int64_t>& q) {
            if (q.has_value()) {
                return q.value() / ss::smp::count;
            }
            constexpr int64_t max_without_conversion_error
              = std::numeric_limits<int64_t>::max() / 1024 * 1024;
            return max_without_conversion_error;
        };
        metric_defs.emplace_back(sm::make_counter(
          name,
          [] {
              return calc_quota(
                config::shard_local_cfg().kafka_throughput_limit_node_in_bps());
          },
          desc,
          {label_ingress}));
        metric_defs.emplace_back(sm::make_counter(
          name,
          [] {
              return calc_quota(config::shard_local_cfg()
                                  .kafka_throughput_limit_node_out_bps());
          },
          desc,
          {label_egress}));
    }
    metric_defs.emplace_back(sm::make_counter(
      "traffic_intake",
      _traffic.in,
      sm::description("Amount of Kafka traffic received from the clients "
                      "that is taken into processing, in bytes")));

    metric_defs.emplace_back(sm::make_counter(
      "traffic_egress",
      _traffic.eg,
      sm::description("Amount of Kafka traffic published to the clients "
                      "that was taken into processing, in bytes")));

    metric_defs.emplace_back(sm::make_histogram(
      "throttle_time",
      [this] { return get_throttle_time(); },
      sm::description("Throttle time histogram (in seconds)")));

    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:quotas"),
      metric_defs,
      {},
      {sm::shard_label});
}

namespace {

auto update_node_bucket(
  ssx::sharded_ptr<kafka::snc_quota_manager::bucket_t>& b,
  const config::binding<std::optional<int64_t>>& cfg) {
    if (!cfg().has_value()) {
        return b.reset();
    }
    uint64_t rate = *cfg();
    if (b && b->rate() == rate) {
        return ss::make_ready_future();
    }
    uint64_t limit = rate;
    uint64_t threshold = config::shard_local_cfg()
                           .kafka_throughput_replenish_threshold()
                           .value_or(1);
    return b.reset(rate, limit, threshold, false);
};

} // namespace

snc_quota_manager::snc_quota_manager(buckets_t& node_quota)
  : _max_kafka_throttle_delay(
    config::shard_local_cfg().max_kafka_throttle_delay_ms.bind())
  , _kafka_throughput_limit_node_bps{
      config::shard_local_cfg().kafka_throughput_limit_node_in_bps.bind(),
      config::shard_local_cfg().kafka_throughput_limit_node_out_bps.bind()}
  , _kafka_throughput_control(config::shard_local_cfg().kafka_throughput_control.bind())
  , _node_quota{node_quota}
  , _probe()
{
    _kafka_throughput_limit_node_bps.in.watch([this] {
        if (ss::this_shard_id() == quota_balancer_shard) {
            ssx::spawn_with_gate(_balancer_gate, [this] {
                return update_node_bucket(
                  _node_quota.in, _kafka_throughput_limit_node_bps.in);
            });
        }
    });
    _kafka_throughput_limit_node_bps.eg.watch([this] {
        if (ss::this_shard_id() == quota_balancer_shard) {
            ssx::spawn_with_gate(_balancer_gate, [this] {
                return update_node_bucket(
                  _node_quota.eg, _kafka_throughput_limit_node_bps.eg);
            });
        }
    });

    _probe.setup_metrics();
}

ss::future<> snc_quota_manager::start() {
    if (ss::this_shard_id() == quota_balancer_shard) {
        co_await update_node_bucket(
          _node_quota.in, _kafka_throughput_limit_node_bps.in);
        co_await update_node_bucket(
          _node_quota.eg, _kafka_throughput_limit_node_bps.eg);
    }
}

ss::future<> snc_quota_manager::stop() {
    if (ss::this_shard_id() == quota_balancer_shard) {
        co_await _balancer_gate.close();
        if (_node_quota.in) {
            co_await _node_quota.in.stop();
        }
        if (_node_quota.eg) {
            co_await _node_quota.eg.stop();
        }
    }
}

namespace {

using delay_t = std::chrono::milliseconds;

/// Evaluate throttling delay required based on the state of a token bucket
delay_t eval_node_delay(
  const ssx::sharded_ptr<snc_quota_manager::bucket_t>& tbp) noexcept {
    if (!tbp) {
        return delay_t::zero();
    }
    auto& tb = *tbp;
    return tb.calculate_delay<delay_t>();
}

} // namespace

void snc_quota_manager::get_or_create_quota_context(
  std::unique_ptr<snc_quota_context>& ctx,
  std::optional<std::string_view> client_id) {
    if (likely(ctx)) {
        // NB: comparing string_view to sstring might be suboptimal
        if (likely(ctx->_client_id == client_id)) {
            // the context is the right one
            return;
        }

        // either of the context indexing propeties have changed on the client
        // within the same connection. This is an unexpected path, quotas may
        // misbehave if we ever get here. The design is based on assumption that
        // this should not happen. If it does happen with a supported client, we
        // probably should start supporting multiple quota contexts per
        // connection
        vlog(
          klog.warn,
          "qm - client_id has changed on the connection. Quotas are reset now. "
          "Old client_id: {}, new client_id: {}",
          ctx->_client_id,
          client_id);
    }

    ctx = std::make_unique<snc_quota_context>(client_id);
    const auto tcgroup_it = config::find_throughput_control_group(
      _kafka_throughput_control().cbegin(),
      _kafka_throughput_control().cend(),
      client_id);
    if (tcgroup_it == _kafka_throughput_control().cend()) {
        ctx->_exempt = false;
        vlog(klog.debug, "qm - No throughput control group assigned");
    } else {
        ctx->_exempt = true;
        if (tcgroup_it->is_noname()) {
            vlog(
              klog.debug,
              "qm - Assigned throughput control group #{}",
              std::distance(_kafka_throughput_control().cbegin(), tcgroup_it));
        } else {
            vlog(
              klog.debug,
              "qm - Assigned throughput control group: {}",
              tcgroup_it->name);
        }
    }
}

snc_quota_manager::delays_t
snc_quota_manager::get_shard_delays(const snc_quota_context& ctx) const {
    delays_t res;

    if (ctx._exempt) {
        return res;
    }

    // throttling delay the connection should be requested to throttle
    // this time
    res.request = std::min(
      _max_kafka_throttle_delay(),
      std::max(
        eval_node_delay(_node_quota.in), eval_node_delay(_node_quota.eg)));

    _probe.record_throttle_time(
      std::chrono::duration_cast<std::chrono::microseconds>(res.request));

    return res;
}

void snc_quota_manager::record_request_receive(
  const snc_quota_context& ctx,
  const size_t request_size,
  const clock::time_point now) noexcept {
    if (ctx._exempt) {
        return;
    }
    if (_node_quota.in) {
        _node_quota.in->replenish(now);
        _node_quota.in->record(request_size);
    }
}

void snc_quota_manager::record_request_intake(
  const snc_quota_context& ctx, const size_t request_size) noexcept {
    if (ctx._exempt) {
        return;
    }
    _probe.rec_traffic_in(request_size);
}

void snc_quota_manager::record_response(
  const snc_quota_context& ctx,
  const size_t request_size,
  const clock::time_point now) noexcept {
    if (ctx._exempt) {
        return;
    }
    if (_node_quota.eg) {
        _node_quota.eg->replenish(now);
        _node_quota.eg->record(request_size);
    }
    _probe.rec_traffic_eg(request_size);
}

} // namespace kafka
