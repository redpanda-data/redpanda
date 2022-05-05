// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/heartbeat.h"

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
ss::future<response_ptr> heartbeat_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    heartbeat_request request;
    request.first_byte_ts = ctx.header().first_byte_ts;
    request.decode(ctx.reader(), ctx.header().version);

    if (!ctx.authorized(security::acl_operation::read, request.data.group_id)) {
        co_return co_await ctx.respond(
          heartbeat_response(error_code::group_authorization_failed));
    }

    auto resp = co_await ctx.groups().heartbeat(std::move(request));
    co_return co_await ctx.respond(resp);
}

} // namespace kafka
