/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

namespace kafka {

static void fill_response(
  request_context& ctx,
  security::acl_binding_filter& filter,
  describe_acls_response_data& response) {
    /*
     * collapse common acls by pattern
     */
    absl::flat_hash_map<
      security::resource_pattern,
      std::vector<security::acl_entry>>
      entries;

    auto bindings = ctx.authorizer().acls(filter);

    for (const auto& binding : bindings) {
        entries[binding.pattern()].push_back(binding.entry());
    }

    for (const auto& entry : entries) {
        describe_acls_resource resource;

        // resource pattern
        resource.type = details::to_kafka_resource_type(entry.first.resource());
        resource.name = entry.first.name();
        resource.pattern_type = details::to_kafka_pattern_type(
          entry.first.pattern());

        // acl entries
        for (auto& acl : entry.second) {
            // ignore ephemeral_users
            auto ephemeral_user = security::principal_type::ephemeral_user;
            if (acl.principal().type() == ephemeral_user) {
                continue;
            }
            acl_description desc{
              .principal = details::to_kafka_principal(acl.principal()),
              .host = details::to_kafka_host(acl.host()),
              .operation = details::to_kafka_operation(acl.operation()),
              .permission_type = details::to_kafka_permission(acl.permission()),
            };
            resource.acls.push_back(std::move(desc));
        }
        response.resources.push_back(std::move(resource));
    }
}

template<>
ss::future<response_ptr> describe_acls_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    describe_acls_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    if (!ctx.authorized(
          security::acl_operation::describe, security::default_cluster_name)) {
        describe_acls_response resp;
        resp.data.error_code = error_code::cluster_authorization_failed;
        co_return co_await ctx.respond(std::move(resp));
    }

    describe_acls_response_data data;

    try {
        auto filter = details::to_acl_binding_filter(request.data);
        fill_response(ctx, filter, data);
    } catch (const details::acl_conversion_error& e) {
        vlog(klog.debug, "Error describing ACLs: {}", e.what());
        data.error_code = error_code::invalid_request;
        data.error_message = e.what();
    }

    describe_acls_response response{
      .data = std::move(data),
    };

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
