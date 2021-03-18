// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/list_groups.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/request_context.h"
#include "model/metadata.h"

#include <seastar/core/coroutine.hh>

namespace kafka {

void list_groups_response::encode(const request_context& ctx, response& resp) {
    data.encode(resp.writer(), ctx.header().version);
}

template<>
ss::future<response_ptr> list_groups_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    list_groups_request request{};
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    auto groups = co_await ctx.groups().list_groups();
    // group listing is still returned even if some partitions are
    // still in the process of loading/recovering.
    list_groups_response resp;
    resp.data.error_code = groups.first
                             ? error_code::coordinator_load_in_progress
                             : error_code::none;
    resp.data.groups = std::move(groups.second);
    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
