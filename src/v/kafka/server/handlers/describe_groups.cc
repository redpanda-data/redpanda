// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_groups.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/namespace.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

namespace kafka {

template<>
ss::future<response_ptr>
describe_groups_handler::handle(request_context ctx, ss::smp_service_group) {
    describe_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    auto unauthorized_it = std::partition(
      request.data.groups.begin(),
      request.data.groups.end(),
      [&ctx](const group_id& id) {
          return ctx.authorized(security::acl_operation::describe, id);
      });

    std::vector<group_id> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.groups.end()));

    request.data.groups.erase(unauthorized_it, request.data.groups.end());

    describe_groups_response response;

    if (likely(!request.data.groups.empty())) {
        std::vector<ss::future<described_group>> described;
        described.reserve(request.data.groups.size());
        for (auto& group_id : request.data.groups) {
            described.push_back(ctx.groups().describe_group(group_id).then(
              [&ctx, &request, group_id](auto res) {
                  if (request.data.include_authorized_operations) {
                      res.authorized_operations = details::to_bit_field(
                        details::authorized_operations(ctx, group_id));
                  }
                  return res;
              }));
        }
        response.data.groups = co_await ss::when_all_succeed(
          described.begin(), described.end());
    }

    for (auto& group : unauthorized) {
        response.data.groups.push_back(described_group{
          .error_code = error_code::group_authorization_failed,
          .group_id = std::move(group),
        });
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
