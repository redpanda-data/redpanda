/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/handlers/describe_acls.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"

#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

namespace kafka {

template<>
ss::future<response_ptr> describe_acls_handler::handle(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_acls_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    // we do not current implement acls, so we do not have any resources. so
    // instead of returning an error, we'll return success and an empty set.
    describe_acls_response_data data;
    data.error_code = error_code::none;

    describe_acls_response response{
      .data = std::move(data),
    };

    return ctx.respond(std::move(response));
}

} // namespace kafka
