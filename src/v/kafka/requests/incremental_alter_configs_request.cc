// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "kafka/requests/incremental_alter_configs_request.h"

#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "model/metadata.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

ss::future<response_ptr> incremental_alter_configs_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    incremental_alter_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    return ss::do_with(
      std::move(ctx),
      std::move(request),
      [](request_context& ctx, incremental_alter_configs_request& request) {
          incremental_alter_configs_response response;
          for (auto& resource : request.data.resources) {
              response.data.responses.push_back(
                incremental_alter_configs_resource_response{
                  .error_code = error_code::invalid_request,
                  .error_message = "Cannot alter config resource",
                  .resource_type = resource.resource_type,
                  .resource_name = std::move(resource.resource_name),
                });
          }
          return ctx.respond(std::move(response));
      });
}

} // namespace kafka
