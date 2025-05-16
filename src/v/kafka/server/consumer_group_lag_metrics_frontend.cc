/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "consumer_group_lag_metrics_frontend.h"

#include "cluster/metadata_cache.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/server/consumer_group_lag_metrics_rpc_service.h"
#include "kafka/server/consumer_group_lag_metrics_rpc_types.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"
#include "rpc/connection_cache.h"

#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shard_id.hh>

#include <absl/container/flat_hash_map.h>

#include <algorithm>

namespace {
constexpr auto timeout = 5s;

struct ktp_view {
    ktp_view(const model::topic& topic, model::partition_id partition)
      : _tp{topic}
      , _partition(partition) {}
    model::ktp ktp() const { return model::ktp{tp(), partition()}; }
    model::ntp ntp() const { return ktp().to_ntp(); }
    model::topic_view tp() const { return model::topic_view{_tp}; }
    model::partition_id partition() const { return _partition; }

    model::topic_view _tp;
    model::partition_id _partition;
};

inline auto request_as_ktps(const kafka::partition_offsets_request& req) {
    return req.data | std::views::transform([](const auto& topic) {
               return topic.second | std::views::transform([&topic](auto part) {
                          return ktp_view{topic.first, part};
                      });
           })
           | std::views::join;
};

} // namespace

namespace kafka {

ss::future<> consumer_group_lag_metrics_frontend::start() { co_return; }
ss::future<> consumer_group_lag_metrics_frontend::stop() {
    co_await _gate.close();
}

ss::future<partition_offsets_reply>
consumer_group_lag_metrics_frontend::get_partition_offsets(
  partition_offsets_request req) {
    absl::flat_hash_map<model::node_id, decltype(req.data)> requests;
    for (const auto& ktp : request_as_ktps(req)) {
        auto leader_id = _metadata.local().get_leader_id(ktp.ntp());
        if (leader_id.has_value()) {
            requests[*leader_id][ktp.tp()].emplace(ktp.partition());
        } else {
            vlog(
              klog.debug,
              "consumer_group_lag_metrics_frontend::get_partition_offsets: "
              "no leader for {}",
              ktp.ntp());
        }
    }

    auto hold_gate = _gate.hold();

    co_return co_await ss::map_reduce(
      requests,
      [this](auto& data) {
          auto leader_id = data.first;
          auto req = partition_offsets_request{.data = std::move(data).second};
          if (leader_id == _self) {
              return get_local_partition_offsets(std::move(req));
          }
          return _rpc_connections.local()
            .with_node_client<consumer_group_lag_metrics_rpc_client_protocol>(
              _self,
              ss::this_shard_id(),
              leader_id,
              timeout,
              [req{std::move(req)}](
                consumer_group_lag_metrics_rpc_client_protocol proto) mutable {
                  return proto.partition_offsets(
                    std::move(req), rpc::client_opts{timeout});
              })
            .then(&rpc::get_ctx_data<partition_offsets_reply>)
            .then([](result<partition_offsets_reply> res) {
                if (res.has_value()) {
                    return std::move(res).value();
                }
                vlog(
                  klog.info,
                  "consumer_group_lag_metrics_frontend::get_partition_offsets: "
                  "error: {}",
                  res.error());
                return partition_offsets_reply{};
            });
      },
      partition_offsets_reply{},
      [](partition_offsets_reply acc, const partition_offsets_reply& res) {
          for (const auto& [topic, data] : res.data) {
              acc.data[topic].insert(data.begin(), data.end());
          }
          return acc;
      });
}

ss::future<partition_offsets_reply>
consumer_group_lag_metrics_frontend::get_local_partition_offsets(
  partition_offsets_request req) {
    // const because don't share mutable state between threads.
    const auto ktps = request_as_ktps(req);

    auto gate_holder = _gate.hold();

    // gather all cores
    co_return co_await container().map_reduce0(
      [ktps](auto& me) {
          // take a copy of ktps since it is an input_range, which requires it
          // to be mutated by for_each, and this lambda cannot be mutable.
          auto copy = ktps;
          partition_offsets_reply reply;
          std::ranges::for_each(copy, [&](ktp_view ktp) {
              auto part = make_partition_proxy(
                ktp.ktp(), me._partition_manager.local(), me._ct_app);
              if (part.has_value()) {
                  reply.data[ktp.tp()][ktp.partition()] = kafka::offset{
                    part->high_watermark()()};
              }
          });
          return reply;
      },
      partition_offsets_reply{},
      [](partition_offsets_reply acc, const partition_offsets_reply& rep) {
          for (const auto& [t, ps] : rep.data) {
              for (const auto [p, o] : ps) {
                  auto& acc_off = acc.data[t][p];
                  acc_off = std::max(acc_off, o);
              }
          }
          return acc;
      });
}

} // namespace kafka
