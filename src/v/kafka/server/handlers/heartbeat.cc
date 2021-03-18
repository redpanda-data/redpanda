// Copyright 2020 Vectorized, Inc.
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

#include <seastar/core/print.hh>

namespace kafka {

void heartbeat_response::encode(const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

template<>
ss::future<response_ptr> heartbeat_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        heartbeat_request request;
        request.decode(ctx.reader(), ctx.header().version);
        return ctx.groups()
          .heartbeat(std::move(request))
          .then([&ctx](heartbeat_response&& reply) {
              return ctx.respond(std::move(reply));
          });
    });
}

} // namespace kafka
