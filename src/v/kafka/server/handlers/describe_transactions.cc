// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_transactions.h"

#include "cluster/errc.h"
#include "cluster/tx_gateway_frontend.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/describe_transactions_request.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include <chrono>
#include <unordered_map>

namespace kafka {

namespace {

ss::future<> fill_info_about_tx(
  cluster::tx_gateway_frontend& tx_frontend,
  describe_transactions_response& response,
  kafka::transactional_id tx_id) {
    describe_transaction_state tx_info_resp;
    tx_info_resp.transactional_id = tx_id;

    auto tx_info = co_await tx_frontend.describe_tx(tx_id);
    if (tx_info.has_value()) {
        auto tx = tx_info.value();
        tx_info_resp.producer_id = kafka::producer_id(tx.pid.id);
        tx_info_resp.producer_epoch = tx.pid.get_epoch();
        tx_info_resp.transaction_state = ss::sstring(tx.get_kafka_status());
        tx_info_resp.transaction_timeout_ms = tx.get_timeout() / 1ms;
        // RP doesn't store transaction_start_time so we use last_update
        // insteadget_timeout
        tx_info_resp.transaction_start_time_ms
          = tx.last_update_ts.time_since_epoch() / 1ms;

        std::unordered_map<model::topic, std::vector<model::partition_id>>
          partitions;
        for (const auto& ntp : tx.partitions) {
            partitions[ntp.ntp.tp.topic].push_back(ntp.ntp.tp.partition);
        }

        for (const auto& [topic, partitions] : partitions) {
            topic_data topic_info;
            topic_info.topic = topic;
            for (auto& partition : partitions) {
                topic_info.partitions.push_back(partition);
            }
            tx_info_resp.topics.push_back(std::move(topic_info));
        }
    } else {
        switch (tx_info.error()) {
        case cluster::tx::errc::not_coordinator:
        case cluster::tx::errc::partition_not_found:
        case cluster::tx::errc::stm_not_found:
            tx_info_resp.error_code = kafka::error_code::not_coordinator;
            break;
        case cluster::tx::errc::tx_id_not_found:
            tx_info_resp.error_code
              = kafka::error_code::transactional_id_not_found;
            break;
        default:
            vlog(
              klog.warn,
              "Can not find transaction with tx_id:({}) for "
              "describe_transactions request. Got error (tx::errc): {}",
              tx_id,
              tx_info.error());
            tx_info_resp.error_code = kafka::error_code::unknown_server_error;
            break;
        }
    }

    response.data.transaction_states.push_back(std::move(tx_info_resp));
}

ss::future<> fill_info_about_transactions(
  cluster::tx_gateway_frontend& tx_frontend,
  describe_transactions_response& response,
  chunked_vector<kafka::transactional_id> tx_ids) {
    return ss::max_concurrent_for_each(
      tx_ids, 32, [&response, &tx_frontend](const auto tx_id) -> ss::future<> {
          return fill_info_about_tx(tx_frontend, response, tx_id);
      });
}

} // namespace

template<>
ss::future<response_ptr> describe_transactions_handler::handle(
  request_context ctx, ss::smp_service_group) {
    describe_transactions_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    describe_transactions_response response;

    auto unauthorized_it = std::partition(
      request.data.transactional_ids.begin(),
      request.data.transactional_ids.end(),
      [&ctx](const kafka::transactional_id& tx_id) {
          return ctx.authorized(security::acl_operation::describe, tx_id);
      });

    if (!ctx.audit()) {
        response.data.transaction_states.reserve(
          request.data.transactional_ids.size());
        std::transform(
          request.data.transactional_ids.begin(),
          request.data.transactional_ids.end(),
          std::back_inserter(response.data.transaction_states),
          [](const kafka::transactional_id& id) {
              return describe_transaction_state{
                .error_code = error_code::broker_not_available,
                .transactional_id = id};
          });
        co_return co_await ctx.respond(std::move(response));
    }

    std::vector<kafka::transactional_id> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.transactional_ids.end()));

    request.data.transactional_ids.erase_to_end(unauthorized_it);

    auto& tx_frontend = ctx.tx_gateway_frontend();
    co_await fill_info_about_transactions(
      tx_frontend, response, std::move(request.data.transactional_ids));

    for (auto& tx_id : unauthorized) {
        response.data.transaction_states.push_back(describe_transaction_state{
          .error_code = error_code::transactional_id_authorization_failed,
          .transactional_id = tx_id,
        });
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
