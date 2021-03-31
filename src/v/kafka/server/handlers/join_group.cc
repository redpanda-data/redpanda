// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/join_group.h"

#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>

namespace kafka {

std::ostream& operator<<(std::ostream& o, const member_protocol& p) {
    return ss::fmt_print(o, "{}:{}", p.name, p.metadata.size());
}

void join_group_request::decode(request_context& ctx) {
    data.decode(ctx.reader(), ctx.header().version);
    version = ctx.header().version;
    if (ctx.header().client_id) {
        client_id = ss::sstring(*ctx.header().client_id);
    }
}

void join_group_response::encode(const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

template<>
ss::future<response_ptr> join_group_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    join_group_request request(ctx);

    if (request.data.group_instance_id) {
        co_return co_await ctx.respond(
          join_group_response(error_code::unsupported_version));
    }

    if (!ctx.authorized(security::acl_operation::read, request.data.group_id)) {
        co_return co_await ctx.respond(
          join_group_response(error_code::group_authorization_failed));
    }

    auto resp = co_await ctx.groups().join_group(std::move(request));
    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
