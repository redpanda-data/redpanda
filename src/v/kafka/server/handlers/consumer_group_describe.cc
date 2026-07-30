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
#include "kafka/server/handlers/consumer_group_describe.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/details/consumer_group_protocol.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

namespace kafka {

namespace {

/// The response carries no top-level error code, so a rejection has to answer
/// each requested group id.
consumer_group_describe_response reject_all(
  const consumer_group_describe_request& request,
  error_code ec,
  std::string_view message) {
    consumer_group_describe_response resp;
    resp.data.groups.reserve(request.data.group_ids.size());
    for (const auto& group_id : request.data.group_ids) {
        resp.data.groups.push_back(
          consumer_group_describe_response_described_group{
            .error_code = ec,
            .error_message = ss::sstring{message},
            .group_id = group_id});
    }
    return resp;
}

} // namespace

template<>
ss::future<response_ptr> consumer_group_describe_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    consumer_group_describe_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!details::consumer_group_protocol_enabled(
          ctx.feature_table().local())) {
        co_return co_await ctx.respond(reject_all(
          request,
          error_code::unsupported_version,
          "ConsumerGroupDescribe is not available: the consumer group "
          "protocol is not enabled on this cluster"));
    }

    // TODO(kip-848): replace with the live path through to the group routing
    // code. Unreachable until then: the gate above is always closed.
    co_return co_await ctx.respond(reject_all(
      request,
      error_code::unsupported_version,
      "ConsumerGroupDescribe is not implemented"));
}

} // namespace kafka
