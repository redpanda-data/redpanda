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

#include "cluster/topics_frontend.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/errors.h"
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
  const std::vector<cluster::acl_resource>& entries,
  describe_acls_response_data& data) {
    for (const auto& entry : entries) {
        describe_acls_resource resource;
        resource.type = details::to_kafka_resource_type(entry.type);
        resource.name = entry.name;
        resource.pattern_type = details::to_kafka_pattern_type(
          entry.pattern_type);

        // acl entries
        for (const auto& acl : entry.acls) {
            // ignore ephemeral_users
            auto ephemeral_user = security::principal_type::ephemeral_user;
            if (acl.principal.type() == ephemeral_user) {
                continue;
            }
            acl_description desc{
              .principal = details::to_kafka_principal(acl.principal),
              .host = details::to_kafka_host(acl.host),
              .operation = details::to_kafka_operation(acl.operation),
              .permission_type = details::to_kafka_permission(
                acl.permission_type),
            };
            resource.acls.push_back(std::move(desc));
        }
        data.resources.push_back(std::move(resource));
    }
}

template<>
ss::future<response_ptr> describe_acls_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    static const auto default_describe_acls_timeout = 3s;
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
        auto result = co_await ctx.security_frontend().describe_acls(
          filter, default_describe_acls_timeout);
        if (result.error != cluster::errc::success) {
            vlog(klog.debug, "Error describing ACLs: {}", result.error);
            data.error_code = map_topic_error_code(result.error);
        } else {
            fill_response(result.acl_resources, data);
        }
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
