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
#include "kafka/server/handlers/create_acls.h"

#include "kafka/protocol/errors.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "security/acl.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <variant>

using namespace std::chrono_literals;

namespace kafka {

template<>
ss::future<response_ptr> create_acls_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    create_acls_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    if (!ctx.authorized(
          security::acl_operation::alter, security::default_cluster_name)) {
        creatable_acl_result result;
        result.error_code = error_code::cluster_authorization_failed;
        create_acls_response resp;
        resp.data.results.assign(request.data.creations.size(), result);
        co_return co_await ctx.respond(std::move(resp));
    }

    // <bindings index> | error
    std::vector<std::variant<size_t, creatable_acl_result>> result_index;
    result_index.reserve(request.data.creations.size());

    // bindings to create. optimized for common case
    std::vector<security::acl_binding> bindings;
    bindings.reserve(request.data.creations.size());

    for (const auto& acl : request.data.creations) {
        try {
            auto binding = details::to_acl_binding(acl);
            result_index.emplace_back(bindings.size());
            bindings.push_back(std::move(binding));

        } catch (const details::acl_conversion_error& e) {
            result_index.emplace_back(creatable_acl_result{
              .error_code = error_code::invalid_request,
              .error_message = e.what(),
            });
        }
    }

    const auto num_bindings = bindings.size();

    auto results = co_await ctx.security_frontend().create_acls(
      std::move(bindings), 5s);

    // kafka: put things back in the same order :(
    create_acls_response response;
    response.data.results.reserve(result_index.size());

    // this is an important step because the loop below that constructs the
    // response unconditionally indexes into the result set.
    if (unlikely(results.size() != num_bindings)) {
        vlog(
          klog.error,
          "Unexpected result size creating ACLs: {} (expected {})",
          results.size(),
          num_bindings);

        response.data.results.assign(
          result_index.size(),
          creatable_acl_result{.error_code = error_code::unknown_server_error});

        co_return co_await ctx.respond(std::move(response));
    }

    for (auto& result : result_index) {
        ss::visit(
          result,
          [&response, &results](size_t i) {
              auto ec = map_topic_error_code(results[i]);
              response.data.results.push_back(
                creatable_acl_result{.error_code = ec});
          },
          [&response](creatable_acl_result r) {
              response.data.results.push_back(std::move(r));
          });
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
