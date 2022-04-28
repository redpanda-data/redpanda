/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/handlers/delete_groups.h"

#include "kafka/protocol/schemata/delete_groups_response.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/print.hh>

namespace kafka {

template<>
ss::future<response_ptr> delete_groups_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    delete_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(kgrouplog.debug, "Handling delete groups: {}", request);

    auto unauthorized_it = std::partition(
      request.data.groups_names.begin(),
      request.data.groups_names.end(),
      [&ctx](const kafka::group_id& group) {
          return ctx.authorized(security::acl_operation::remove, group);
      });

    std::vector<kafka::group_id> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.groups_names.end()));

    request.data.groups_names.erase(
      unauthorized_it, request.data.groups_names.end());

    std::vector<deletable_group_result> results;

    if (!request.data.groups_names.empty()) {
        results = co_await ctx.groups().delete_groups(
          std::move(request.data.groups_names));
    }

    for (auto& group : unauthorized) {
        results.push_back(deletable_group_result{
          .group_id = std::move(group),
          .error_code = error_code::group_authorization_failed,
        });
    }

    co_return co_await ctx.respond(delete_groups_response(std::move(results)));
}

} // namespace kafka
