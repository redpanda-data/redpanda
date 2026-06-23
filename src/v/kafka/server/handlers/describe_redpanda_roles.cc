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
#include "kafka/server/handlers/describe_redpanda_roles.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace kafka {

template<>
ss::future<response_ptr> describe_redpanda_roles_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_redpanda_roles_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    // Stub proving reserved-range dispatch end to end: returns an empty,
    // well-formed response. Role enumeration and the authorization that guards
    // it are added in the follow-on PR, once the API returns real data.
    describe_redpanda_roles_response response;
    response.data.error_code = error_code::none;
    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
