// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/consumer_group_heartbeat.h"

#include "config/configuration.h"
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

    if (!config::shard_local_cfg()
           .enable_kip848_next_gen_consumer_group_protocol()) {
        co_return co_await ctx.respond(
          consumer_group_heartbeat_response(error_code::unsupported_version));
    }

    vlog(
      klog.debug,
      "kip848: heartbeat group={} member={}",
      request.data.group_id,
      request.data.member_id);

    consumer_group_heartbeat_response response;
    response.data.error_code = error_code::none;
    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
