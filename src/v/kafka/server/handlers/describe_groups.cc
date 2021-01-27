// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/describe_groups.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/namespace.h"
#include "resource_mgmt/io_priority.h"

namespace kafka {

void describe_groups_response::encode(
  const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

struct describe_groups_ctx {
    request_context rctx;
    describe_groups_request request;
    describe_groups_response response;
    ss::smp_service_group ssg;

    describe_groups_ctx(
      request_context&& rctx,
      describe_groups_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

template<>
ss::future<response_ptr> describe_groups_handler::handle(
  request_context&& ctx, ss::smp_service_group ssg) {
    describe_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    return ss::do_with(
      describe_groups_ctx(std::move(ctx), std::move(request), ssg),
      [](describe_groups_ctx& octx) {
          return ss::parallel_for_each(
                   octx.request.data.groups.begin(),
                   octx.request.data.groups.end(),
                   [&octx](kafka::group_id id) {
                       return octx.rctx.groups()
                         .describe_group(std::move(id))
                         .then([&octx](described_group g) {
                             octx.response.data.groups.push_back(std::move(g));
                         });
                   })
            .then(
              [&octx] { return octx.rctx.respond(std::move(octx.response)); });
      });
}

} // namespace kafka
