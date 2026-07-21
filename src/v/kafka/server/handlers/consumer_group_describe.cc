/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/handlers/consumer_group_describe.h"

#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/protocol/consumer_group_describe.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <iterator>

namespace kafka {

template<>
ss::future<response_ptr> consumer_group_describe_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    consumer_group_describe_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (
      unlikely(
        !config::shard_local_cfg().group_consumer_protocol_enabled()
        || !ctx.feature_table().local().is_active(
          features::feature::topic_ids_api))) {
        consumer_group_describe_response resp;
        resp.data.groups.reserve(request.data.group_ids.size());
        for (auto& group_id : request.data.group_ids) {
            resp.data.groups.push_back(make_described_group_error(
              std::move(group_id),
              error_code::unsupported_version,
              "The next generation consumer group protocol is not enabled"));
        }
        co_return co_await ctx.respond(std::move(resp));
    }

    auto unauthorized_it = std::partition(
      request.data.group_ids.begin(),
      request.data.group_ids.end(),
      [&ctx](const group_id& id) {
          return ctx.authorized(security::acl_operation::describe, id);
      });

    if (!ctx.audit()) {
        consumer_group_describe_response resp;
        resp.data.groups.reserve(request.data.group_ids.size());
        for (auto& group_id : request.data.group_ids) {
            resp.data.groups.push_back(make_described_group_error(
              std::move(group_id),
              error_code::broker_not_available,
              "Broker not available - audit system failure"));
        }
        co_return co_await ctx.respond(std::move(resp));
    }

    std::vector<group_id> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.group_ids.end()));

    request.data.group_ids.erase_to_end(unauthorized_it);

    consumer_group_describe_response response;

    if (likely(!request.data.group_ids.empty())) {
        chunked_vector<ss::future<consumer_group_described_group>> described;
        described.reserve(request.data.group_ids.size());
        for (auto& group_id : request.data.group_ids) {
            described.push_back(
              ctx.groups().consumer_group_describe(group_id).then(
                [&ctx, &request, group_id](auto res) {
                    if (request.data.include_authorized_operations) {
                        res.authorized_operations = details::to_bit_field(
                          details::authorized_operations<kafka::group_id>(
                            [&ctx](
                              security::acl_operation op,
                              const kafka::group_id& resource,
                              authz_quiet q,
                              audit_authz_check c) {
                                return ctx.authorized(op, resource, q, c);
                            },
                            group_id));
                    }
                    return res;
                }));
        }
        auto group_v = co_await ssx::when_all_succeed<
          chunked_vector<consumer_group_described_group>>(std::move(described));

        response.data.groups = std::move(group_v);
    }

    for (auto& group : unauthorized) {
        response.data.groups.push_back(make_described_group_error(
          std::move(group), error_code::group_authorization_failed, ""));
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
