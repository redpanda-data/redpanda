// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/offset_fetch.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/metadata.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

std::ostream& operator<<(std::ostream& os, const offset_fetch_request& r) {
    os << r.data;
    return os;
}

std::ostream& operator<<(std::ostream& os, const offset_fetch_response& r) {
    os << r.data;
    return os;
}

void offset_fetch_response::encode(const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

struct offset_fetch_ctx {
    request_context rctx;
    offset_fetch_request request;
    ss::smp_service_group ssg;

    offset_fetch_ctx(
      request_context&& rctx,
      offset_fetch_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

template<>
ss::future<response_ptr>
offset_fetch_handler::handle(request_context&& ctx, ss::smp_service_group ssg) {
    offset_fetch_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);
    return ss::do_with(
      offset_fetch_ctx(std::move(ctx), std::move(request), ssg),
      [](offset_fetch_ctx& octx) {
          return octx.rctx.groups()
            .offset_fetch(std::move(octx.request))
            .then([&octx](offset_fetch_response resp) {
                return octx.rctx.respond(std::move(resp));
            });
      });
}

} // namespace kafka
