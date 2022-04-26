// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/list_groups.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "model/metadata.h"

#include <seastar/core/coroutine.hh>

namespace kafka {

template<>
ss::future<response_ptr> list_groups_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    list_groups_request request{};
    request.decode(ctx.reader(), ctx.header().version);
    vlog(kgrouplog.trace, "Handling request {}", request);

    auto&& [error, groups] = co_await ctx.groups().list_groups();

    list_groups_response resp;
    resp.data.error_code = error;
    resp.data.groups = std::move(groups);

    if (ctx.authorized(
          security::acl_operation::describe, security::default_cluster_name)) {
        co_return co_await ctx.respond(std::move(resp));
    }

    // remove groups from response that should not be visible
    auto non_visible_it = std::partition(
      resp.data.groups.begin(),
      resp.data.groups.end(),
      [&ctx](const listed_group& group) {
          return ctx.authorized(
            security::acl_operation::describe, group.group_id);
      });

    resp.data.groups.erase(non_visible_it, resp.data.groups.end());

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
