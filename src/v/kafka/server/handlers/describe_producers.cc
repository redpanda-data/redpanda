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
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/describe_producers_response.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/namespace.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include <chrono>

namespace kafka {

namespace {

using ret_res = checked<cluster::tx::partition_transactions, kafka::error_code>;

partition_response
make_error_response(model::partition_id id, kafka::error_code ec) {
    return partition_response{.partition_index = id, .error_code = ec};
}

partition_response
do_get_producers_for_partition(cluster::partition_manager& pm, model::ktp ntp) {
    auto partition = pm.get(ntp);
    if (!partition || !partition->is_leader()) {
        return make_error_response(
          ntp.get_partition(), kafka::error_code::not_leader_for_partition);
    }

    auto rm_stm_ptr = partition->rm_stm();

    if (!rm_stm_ptr) {
        vlog(
          klog.error,
          "Can not get rm_stm for ntp: {}. Looks like transaction are "
          "disabled",
          ntp);
        return make_error_response(
          ntp.get_partition(), kafka::error_code::unknown_server_error);
    }
    const auto& producers = rm_stm_ptr->get_producers();
    auto log_start = partition->raft_start_offset();
    partition_response resp;
    resp.error_code = error_code::none;
    resp.partition_index = ntp.get_partition();
    resp.active_producers.reserve(producers.size());
    for (const auto& [pid, state] : producers) {
        auto tx_start = state->get_current_tx_start_offset();
        auto kafka_tx_start = -1;
        if (tx_start && tx_start.value() >= log_start) {
            kafka_tx_start = partition->log()->from_log_offset(
              tx_start.value());
        }
        resp.active_producers.push_back(producer_state{
          .producer_id = pid,
          .producer_epoch = state->id().get_epoch(),
          .last_sequence = state->last_sequence_number().value_or(-1),
          .last_timestamp = state->last_update_timestamp().value(),
          .coordinator_epoch = -1,
          .current_txn_start_offset = kafka_tx_start,
        });
    }
    return resp;
}

ss::future<partition_response>
get_producers_for_partition(request_context& ctx, model::ktp ntp) {
    auto shard = ctx.shards().shard_for(ntp);
    if (!shard) {
        vlog(
          klog.debug,
          "Can not find shard for ntp({}) to get info about producers",
          ntp);
        co_return make_error_response(
          ntp.get_partition(), kafka::error_code::not_leader_for_partition);
    }

    co_return co_await ctx.partition_manager().invoke_on(
      *shard, [ntp = std::move(ntp)](cluster::partition_manager& pm) mutable {
          return do_get_producers_for_partition(pm, std::move(ntp));
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

    if (!ctx.audit()) {
        response.data.topics.reserve(request.data.topics.size());
        std::transform(
          request.data.topics.begin(),
          request.data.topics.end(),
          std::back_inserter(response.data.topics),
          [](const topic_request& r) {
              topic_response resp;
              resp.name = r.name;
              resp.partitions.reserve(r.partition_indexes.size());
              for (const auto& part_id : r.partition_indexes) {
                  partition_response part_resp;
                  part_resp.partition_index = part_id;
                  part_resp.error_code = error_code::broker_not_available;
                  part_resp.error_message
                    = "Broker not available - audit system failure";
                  resp.partitions.emplace_back(std::move(part_resp));
              }

              return resp;
          });

        co_return co_await ctx.respond(std::move(response));
    }

    std::vector<topic_request> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topics.end()));

    request.data.topics.erase_to_end(unauthorized_it);

    for (const auto& topic : request.data.topics) {
        topic_response topic_resp;
        topic_resp.name = topic.name;
        for (const auto& partition : topic.partition_indexes) {
            auto partition_resp = co_await get_producers_for_partition(
              ctx, model::ktp(topic.name, partition));

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
