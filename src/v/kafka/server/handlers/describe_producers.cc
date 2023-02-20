// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_producers.h"

#include "cluster/errc.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_stm.h"
#include "cluster/shard_table.h"
#include "kafka/protocol/describe_producers.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/describe_producers_response.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include <chrono>

namespace kafka {

namespace {

using ret_res = checked<cluster::rm_stm::transaction_set, kafka::error_code>;

int64_t
convert_rm_stm_time_to_milliseconds(cluster::rm_stm::time_point_type time) {
    auto diff = cluster::rm_stm::clock_type::now() - time;
    auto now = std::chrono::steady_clock::now();
    return now.time_since_epoch() / 1ms - diff / 1ms;
}

ss::future<checked<cluster::rm_stm::transaction_set, kafka::error_code>>
get_producers_for_partition(request_context& ctx, model::ntp ntp) {
    auto shard = ctx.shards().shard_for(ntp);
    if (!shard) {
        vlog(
          klog.debug,
          "Can not find shard for ntp({}) to get info about producers",
          ntp);
        return ss::make_ready_future<ret_res>(
          kafka::error_code::not_leader_for_partition);
    }

    return ctx.partition_manager().invoke_on(
      *shard, [ntp](cluster::partition_manager& pm) -> ss::future<ret_res> {
          auto partition = pm.get(ntp);
          if (!partition) {
              return ss::make_ready_future<ret_res>(
                kafka::error_code::unknown_topic_or_partition);
          }

          if (!partition->is_leader()) {
              return ss::make_ready_future<ret_res>(
                kafka::error_code::not_leader_for_partition);
          }

          auto rm_stm_ptr = partition->rm_stm();

          if (!rm_stm_ptr) {
              vlog(
                klog.error,
                "Can not get rm_stm for ntp({}). Looks like transaction are "
                "disabled",
                ntp);

              return ss::make_ready_future<ret_res>(
                kafka::error_code::unknown_server_error);
          }

          return rm_stm_ptr->get_transactions().then(
            [ntp](auto tx) -> ss::future<ret_res> {
                if (!tx.has_value()) {
                    if (tx.error() == cluster::errc::not_leader) {
                        return ss::make_ready_future<ret_res>(
                          kafka::error_code::not_leader_for_partition);
                    }

                    vlog(
                      klog.warn,
                      "get_transactions returned unexpected error: {}",
                      tx.error());
                    return ss::make_ready_future<ret_res>(
                      kafka::error_code::unknown_server_error);
                }

                return ss::make_ready_future<ret_res>(tx.value());
            });
      });
}

} // namespace

template<>
ss::future<response_ptr>
describe_producers_handler::handle(request_context ctx, ss::smp_service_group) {
    describe_producers_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_producers_response response;

    auto unauthorized_it = std::partition(
      request.data.topics.begin(),
      request.data.topics.end(),
      [&ctx](const topic_request& topic) {
          return ctx.authorized(security::acl_operation::read, topic.name);
      });

    std::vector<topic_request> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topics.end()));

    request.data.topics.erase(unauthorized_it, request.data.topics.end());

    for (const auto& topic : request.data.topics) {
        topic_response topic_resp;
        topic_resp.name = topic.name;
        for (const auto& partition : topic.partition_indexes) {
            partition_response partition_resp;
            partition_resp.partition_index = partition;
            model::ntp ntp(model::kafka_namespace, topic.name, partition);
            auto txs = co_await get_producers_for_partition(ctx, ntp);
            if (txs.has_value()) {
                for (const auto& tx : txs.value()) {
                    producer_state producer_info;
                    producer_info.producer_epoch = tx.first.epoch;
                    producer_info.producer_id = tx.first.id;

                    if (tx.second.info) {
                        producer_info.last_timestamp
                          = convert_rm_stm_time_to_milliseconds(
                            tx.second.info->last_update);
                    }

                    // TODO: We do not store coordinator epoch
                    producer_info.coordinator_epoch = -1;
                    producer_info.current_txn_start_offset
                      = tx.second.lso_bound;
                    producer_info.last_sequence = tx.second.seq.value_or(-1);

                    partition_resp.active_producers.push_back(
                      std::move(producer_info));
                }
            } else {
                partition_resp.error_code = txs.error();
            }
            topic_resp.partitions.push_back(std::move(partition_resp));
        }
        response.data.topics.push_back(std::move(topic_resp));
    }

    for (auto& topic_req : unauthorized) {
        topic_response topic_resp;
        topic_resp.name = topic_req.name;
        for (const auto& partition_id : topic_req.partition_indexes) {
            partition_response partition_resp;
            partition_resp.partition_index = partition_id;
            partition_resp.error_code = error_code::topic_authorization_failed;
            topic_resp.partitions.push_back(std::move(partition_resp));
        }

        response.data.topics.push_back(std::move(topic_resp));
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
