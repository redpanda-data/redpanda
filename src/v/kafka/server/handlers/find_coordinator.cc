// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/find_coordinator.h"

#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "config/configuration.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/rm_group_frontend.h"
#include "model/metadata.h"
#include "model/namespace.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <expected>
#include <iterator>

namespace kafka {

namespace {

constexpr size_t max_concurrency = 10;

// maps a given leader id to its relevant coordinator response struct
template<typename KeyType>
kafka::coordinator_response leader_to_coordinator(
  request_context* ctx, const KeyType& key, model::node_id leader) {
    auto broker = ctx->metadata_cache().get_node_metadata(leader);
    if (broker) {
        auto& b = *broker;
        for (const auto& listener : b.broker.kafka_advertised_listeners()) {
            if (listener.name == ctx->listener()) {
                return kafka::coordinator_response{
                  key,
                  b.broker.id(),
                  listener.address.host(),
                  listener.address.port()};
            }
        }
    }
    return kafka::coordinator_response{
      key, kafka::error_code::coordinator_not_available};
}

// given a partition id, find its leader, then turn that leader into the
// coordinator response struct
ss::future<kafka::coordinator_response> partition_to_coordinator(
  group_id group_id,
  request_context* ctx,
  std::optional<model::partition_id> maybe_partition) {
    kafka::coordinator_response return_element{
      group_id, error_code::coordinator_not_available};

    if (!maybe_partition) {
        co_return return_element;
    }

    auto timeout
      = ss::lowres_clock::now()
        + config::shard_local_cfg().internal_rpc_request_timeout_ms();

    model::ntp consumer_offsets_ntp{
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      *maybe_partition};

    auto leader_future = co_await ss::coroutine::as_future(
      ctx->metadata_cache().get_leader(consumer_offsets_ntp, timeout));
    if (leader_future.failed()) {
        vlog(
          klog.info,
          "exception while waiting for leader on ntp {}: {}",
          consumer_offsets_ntp,
          leader_future.get_exception());
        co_return return_element;
    }
    co_return leader_to_coordinator(ctx, group_id, leader_future.get());
}

template<typename KeyType>
struct audit_success {
    chunked_vector<KeyType> authorized_keys;
    chunked_vector<KeyType> unauthorized_keys;
};

template<typename KeyType>
struct audit_failure {
    chunked_vector<KeyType> all_keys;
};

// carve an input key list into a list of authorized keys and a list of
// unauthorized keys. On failure of the auth system, hand all keys back in the
// error result
template<typename KeyType>
std::expected<audit_success<KeyType>, audit_failure<KeyType>>
check_authorization(request_context* ctx, chunked_vector<KeyType> all_keys) {
    // split the vector into subsegments [unautorized, authorized]
    auto authorized_range = std::ranges::partition(
      all_keys, [&ctx](const KeyType& key) {
          return !ctx->authorized(security::acl_operation::describe, key);
      });

    // audit system failure, return all keys as err value
    if (!ctx->audit()) {
        // error result
        return std::unexpected<audit_failure<KeyType>>(std::move(all_keys));
    }

    chunked_vector<KeyType> authorized_keys{};
    authorized_keys.reserve(authorized_range.size());
    std::ranges::move(authorized_range, std::back_inserter(authorized_keys));

    // truncate the authorized keys off of the original
    auto authorized_it = all_keys.end() - authorized_keys.size();
    all_keys.erase_to_end(authorized_it);

    // value result
    return audit_success<KeyType>{
      .authorized_keys = std::move(authorized_keys),
      .unauthorized_keys = std::move(all_keys)};
}

template<typename KeyType>
chunked_vector<kafka::coordinator> map_errored_keys(
  chunked_vector<KeyType> errored_keys,
  kafka::error_code error_code,
  const std::optional<ss::sstring>& error_message = std::nullopt) {
    chunked_vector<coordinator> unauthorized_key_responses{};
    unauthorized_key_responses.reserve(errored_keys.size());

    std::ranges::transform(
      std::move(errored_keys),
      std::back_inserter(unauthorized_key_responses),
      [error_code, error_message](const KeyType& key) {
          kafka::coordinator_response response_element{
            key, error_code, error_message};
          return response_element;
      });

    return unauthorized_key_responses;
}

// handler for the subset of authorized transaction ids
ss::future<chunked_vector<kafka::coordinator>> handle_authorized_txn_id(
  request_context* ctx, chunked_vector<transactional_id> authorized_keys) {
    chunked_vector<kafka::coordinator> out_vector{};
    co_await ss::max_concurrent_for_each(
      std::move(authorized_keys),
      max_concurrency,
      [&ctx, &out_vector](auto authorized_key) {
          return ctx->tx_gateway_frontend()
            .find_coordinator(authorized_key)
            .then([&ctx, &out_vector, authorized_key](const auto& response) {
                kafka::coordinator_response response_element{
                  authorized_key, error_code::coordinator_not_available};
                auto maybe_leader = response.coordinator;
                if (maybe_leader) {
                    response_element = leader_to_coordinator(
                      ctx, authorized_key, *maybe_leader);
                }
                out_vector.emplace_back(std::move(response_element));
            });
      });
    co_return out_vector;
}

// handler for the subset of authorized group ids
ss::future<chunked_vector<kafka::coordinator>> handle_authorized_group_id(
  request_context* ctx, chunked_vector<group_id> authorized_keys) {
    chunked_vector<kafka::coordinator> out_vector{};

    if (!ctx->coordinator_mapper().topic_exists()) {
        bool success = co_await ctx->group_initializer().assure_topic_exists();
        if (!success) {
            co_return map_errored_keys(
              std::move(authorized_keys),
              kafka::error_code::coordinator_not_available);
        }
    }

    auto loop_body = [ctx, &out_vector](const group_id& group_id) {
        auto partition_id = ctx->coordinator_mapper().partition_for(group_id);
        return partition_to_coordinator(group_id, ctx, partition_id)
          .then(
            [&out_vector, group_id](kafka::coordinator coordinator) mutable {
                out_vector.emplace_back(std::move(coordinator));
            });
    };

    co_await ss::max_concurrent_for_each(
      std::move(authorized_keys), max_concurrency, std::move(loop_body));

    co_return out_vector;
}

template<typename KeyType>
kafka::error_code unauthorized_key_ec() {
    if constexpr (std::is_same_v<KeyType, transactional_id>) {
        return kafka::error_code::transactional_id_authorization_failed;
    } else if constexpr (std::is_same_v<KeyType, group_id>) {
        return kafka::error_code::group_authorization_failed;
    } else {
        static_assert(false, "Unimplemented type");
    }
};

template<typename KeyType>
ss::future<chunked_vector<kafka::coordinator>> handle_authorized_keys(
  request_context* ctx, chunked_vector<KeyType> authorized_keys) {
    if constexpr (std::is_same_v<KeyType, transactional_id>) {
        return handle_authorized_txn_id(ctx, std::move(authorized_keys));
    } else if constexpr (std::is_same_v<KeyType, group_id>) {
        return handle_authorized_group_id(ctx, std::move(authorized_keys));
    } else {
        static_assert(false, "Unimplemented type");
    }
}

// routes a request to its relevant key type.
// Generically this does
//   1. transmute sstring keys into the relevant key type
//   2. check auth
//      a. fail all if auth system failure
//      b. split keys into authed and unauthed
//   3. error unauthed keys
//   4. fetch response for authed keys
//   5. glue all together into response vector
template<typename KeyType>
ss::future<find_coordinator_response> handle_generic_request(
  request_context* ctx, chunked_vector<ss::sstring> generic_keys) {
    find_coordinator_response return_value{};
    auto& return_vector = return_value.data.coordinators;

    // 1. transmute sstring keys into relevant key type
    auto keys = std::ranges::to<chunked_vector<KeyType>>(
      std::move(generic_keys) | std::views::as_rvalue);

    // 2. check auth
    auto auth_result = check_authorization<KeyType>(ctx, std::move(keys));
    if (!auth_result.has_value()) {
        auto error_result = std::move(auth_result).error();
        return_vector = map_errored_keys(
          std::move(error_result.all_keys),
          kafka::error_code::broker_not_available,
          "Broker not available - audit system failure");
        co_return return_value;
    }
    auto [authorized_keys, unauthorized_keys] = *std::move(auth_result);

    // 3. error unauthed keys
    return_vector = map_errored_keys(
      std::move(unauthorized_keys), unauthorized_key_ec<KeyType>());

    // 4. fetch response for authed keys
    auto authorized_coordinators = co_await handle_authorized_keys<KeyType>(
      ctx, std::move(authorized_keys));

    // 5. all keys are emplace_back'd onto the response vector
    std::ranges::move(
      std::move(authorized_coordinators), std::back_inserter(return_vector));

    co_return return_value;
}

// on some validation failure do for each key, make an error coordinator
// response struct
template<typename KeyType>
find_coordinator_response error_entire_request(
  chunked_vector<KeyType> keys, kafka::error_code error_code) {
    find_coordinator_response failed_responses{};
    auto& response_vector = failed_responses.data.coordinators;
    response_vector = map_errored_keys(std::move(keys), error_code);
    return failed_responses;
}

// multiple key handler
ss::future<find_coordinator_response>
handle_multiple_keys(find_coordinator_request request, request_context* ctx) {
    auto& keys = request.data.coordinator_keys;

    switch (request.data.key_type) {
    case kafka::coordinator_type::transaction: {
        if (!ctx->are_transactions_enabled()) {
            co_return error_entire_request(
              std::move(keys), kafka::error_code::unsupported_version);
        }
        co_return co_await handle_generic_request<transactional_id>(
          ctx, std::move(keys));
    }
    case kafka::coordinator_type::group:
        co_return co_await handle_generic_request<group_id>(
          ctx, std::move(keys));
    }
    co_return error_entire_request(
      std::move(keys), kafka::error_code::unsupported_version);
}

} // namespace

// v4+ is multi key, v3- simply gets packed into a vector to be handled
template<>
ss::future<response_ptr> find_coordinator_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    find_coordinator_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    bool is_single_element_request = ctx.header().version <= api_version(3);

    auto& keys = request.data.coordinator_keys;

    // pack v1-3 requests into list form
    if (is_single_element_request) {
        if (!keys.empty()) {
            co_return co_await ctx.respond(
              find_coordinator_response(kafka::error_code::invalid_request));
        }
        keys.emplace_back(std::move(request.data.key));
        request.data.key = "";
    }

    auto response = co_await handle_multiple_keys(std::move(request), &ctx);

    // if needed, unpack a multi-key, v4+ style response back into a v1-3 single
    // key response
    if (is_single_element_request) {
        auto& response_vector = response.data.coordinators;
        vassert(
          response_vector.size() == 1,
          "legacy requests should only yield one response, had {}",
          response_vector.size());
        auto output_element = std::move(response_vector[0]);

        // reset response vector
        response_vector.clear();

        // repack
        response.data.host = std::move(output_element.host);
        response.data.port = output_element.port;
        response.data.node_id = output_element.node_id;
        response.data.error_code = output_element.error_code;
        response.data.error_message = std::move(output_element.error_message);
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
