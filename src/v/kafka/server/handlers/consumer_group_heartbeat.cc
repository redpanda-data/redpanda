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

    ctx.connection()->attributes().last_group_id.update(request.data.group_id);
    ctx.connection()->attributes().last_group_instance_id.update(
      request.data.instance_id);
    ctx.connection()->attributes().last_group_member_id.update(
      kafka::member_id(request.data.member_id));

    consumer_group_heartbeat_response resp;

    if (!details::consumer_group_protocol_enabled(
          ctx.feature_table().local())) {
        resp.data.error_code = error_code::unsupported_version;
        resp.data.error_message
          = "ConsumerGroupHeartbeat is not available: the consumer group "
            "protocol is not enabled on this cluster";
        co_return co_await ctx.respond(std::move(resp));
    }

    if (unlikely(ctx.recovery_mode_enabled())) {
        co_return co_await ctx.respond(consumer_group_heartbeat_response(
          request, error_code::policy_violation));
    }

    // authorized() first, so the audit event carries its outcome
    auto authz = ctx.authorized(
      security::acl_operation::read, request.data.group_id);

    if (!ctx.audit()) {
        co_return co_await ctx.respond(consumer_group_heartbeat_response(
          request, error_code::broker_not_available));
    }

    if (!authz) {
        co_return co_await ctx.respond(consumer_group_heartbeat_response(
          request, error_code::group_authorization_failed));
    }

    // The group manager resolves the coordinator and rejects a group-id the
    // classic protocol owns. It cannot yet serve a heartbeat, so a request
    // that passes those checks still comes back unimplemented.
    co_return co_await ctx.respond(
      co_await ctx.groups().consumer_group_heartbeat(std::move(request)));
}

} // namespace kafka
