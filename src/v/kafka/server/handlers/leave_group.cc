// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/leave_group.h"

#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

void leave_group_response::encode(const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

template<>
ss::future<response_ptr> leave_group_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(
      remote(std::move(ctx)), [](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          leave_group_request request;
          request.decode(ctx.reader(), ctx.header().version);
          return ctx.groups()
            .leave_group(std::move(request))
            .then([&ctx](leave_group_response&& reply) {
                auto resp = std::make_unique<response>();
                reply.encode(ctx, *resp.get());
                return ss::make_ready_future<response_ptr>(std::move(resp));
            });
      });
}

} // namespace kafka
