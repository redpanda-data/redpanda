// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/write_txn_markers.h"

#include "cluster/errc.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/write_txn_markers_request.h"
#include "kafka/protocol/schemata/write_txn_markers_response.h"
#include "model/record.h"
#include "security/acl.h"

#include <seastar/core/coroutine.hh>

namespace kafka {

namespace {

ss::future<cluster::tx_errc> expire_tx_for_ntp(
  request_context& ctx, const model::ntp& ntp, model::producer_identity pid) {
    auto& shard_table = ctx.connection()->server().shard_table();
    auto shard = shard_table.shard_for(ntp);

    if (!shard) {
        return ss::make_ready_future<cluster::tx_errc>(
          cluster::tx_errc::shard_not_found);
    }

    auto& partition_manager = ctx.connection()->server().partition_manager();

    return partition_manager.invoke_on(
      *shard,
      [pid, ntp](cluster::partition_manager& mgr) mutable
      -> ss::future<cluster::tx_errc> {
          auto partition = mgr.get(ntp);
          if (!partition) {
              return ss::make_ready_future<cluster::tx_errc>(
                cluster::tx_errc::partition_not_found);
          }

          auto stm = partition->rm_stm();

          if (!stm) {
              vlog(
                cluster::txlog.warn,
                "can't get tx stm of the {}' partition",
                ntp);
              return ss::make_ready_future<cluster::tx_errc>(
                cluster::tx_errc::stm_not_found);
          }

          return stm->mark_expired(pid);
      });
}

} // namespace

template<>
ss::future<response_ptr> write_txn_markers_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    write_txn_markers_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    write_txn_markers_response resp;

    if (!ctx.authorized(
          security::acl_operation::cluster_action,
          security::default_cluster_name)) {
        for (const auto& marker : request.data.markers) {
            model::producer_identity pid;
            pid.id = marker.producer_id;
            pid.epoch = marker.producer_epoch;

            writable_txn_marker_result marker_res;
            marker_res.producer_id = marker.producer_id;
            for (const auto& topic : marker.topics) {
                writable_txn_marker_topic_result topic_res;
                topic_res.name = topic.name;
                for (const auto& partition : topic.partition_indexes) {
                    writable_txn_marker_partition_result partition_res;
                    partition_res.partition_index = partition;
                    partition_res.error_code
                      = kafka::error_code::cluster_authorization_failed;
                    topic_res.partitions.emplace_back(std::move(partition_res));
                }
                marker_res.topics.emplace_back(std::move(topic_res));
            }
            resp.data.markers.emplace_back(std::move(marker_res));
        }

        co_return co_await ctx.respond(std::move(resp));
    }

    for (const auto& marker : request.data.markers) {
        model::producer_identity pid;
        pid.id = marker.producer_id;
        pid.epoch = marker.producer_epoch;

        writable_txn_marker_result marker_res;
        marker_res.producer_id = marker.producer_id;
        for (const auto& topic : marker.topics) {
            writable_txn_marker_topic_result topic_res;
            topic_res.name = topic.name;
            for (const auto& partition : topic.partition_indexes) {
                model::ntp ntp(model::kafka_namespace, topic.name, partition);
                auto ec = co_await expire_tx_for_ntp(ctx, ntp, pid);
                writable_txn_marker_partition_result partition_res;
                partition_res.partition_index = partition;
                switch (ec) {
                case cluster::tx_errc::none:
                    break;
                case cluster::tx_errc::shard_not_found:
                case cluster::tx_errc::partition_not_found: {
                    partition_res.error_code
                      = kafka::error_code::not_leader_for_partition;
                    break;
                }
                default:
                    partition_res.error_code
                      = kafka::error_code::unknown_server_error;
                }
                topic_res.partitions.emplace_back(std::move(partition_res));
            }
            marker_res.topics.emplace_back(std::move(topic_res));
        }
        resp.data.markers.emplace_back(std::move(marker_res));
    }

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
