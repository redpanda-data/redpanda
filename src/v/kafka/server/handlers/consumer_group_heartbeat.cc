// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/consumer_group_heartbeat.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

namespace kafka {

template<>
ss::future<response_ptr> consumer_group_heartbeat_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    consumer_group_heartbeat_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    co_return co_await ctx.respond(
      consumer_group_heartbeat_response(error_code::unsupported_version));
}

} // namespace kafka
