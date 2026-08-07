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
#include "kafka/server/handlers/consumer_group_heartbeat.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/details/consumer_group_protocol.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace kafka {

template<>
ss::future<response_ptr> consumer_group_heartbeat_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    consumer_group_heartbeat_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    consumer_group_heartbeat_response resp;

    if (!details::consumer_group_protocol_enabled(
          ctx.feature_table().local())) {
        resp.data.error_code = error_code::unsupported_version;
        resp.data.error_message
          = "ConsumerGroupHeartbeat is not available: the consumer group "
            "protocol is not enabled on this cluster";
        co_return co_await ctx.respond(std::move(resp));
    }

    // TODO(kip-848): replace with the live path through to the group routing
    // code. Unreachable until then: the gate above is always closed.
    resp.data.error_code = error_code::unsupported_version;
    resp.data.error_message = "ConsumerGroupHeartbeat is not implemented";
    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
