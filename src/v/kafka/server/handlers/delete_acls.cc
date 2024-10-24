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
#include "kafka/server/handlers/delete_acls.h"

#include "cluster/security_frontend.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/delete_acls_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

using namespace std::chrono_literals;

namespace kafka {

static std::vector<delete_acls_matching_acl>
bindings_to_delete_result(const std::vector<security::acl_binding>& bindings) {
    std::vector<delete_acls_matching_acl> res;

    for (auto& binding : bindings) {
        delete_acls_matching_acl m;
        m.error_code = error_code::none;

        // resource pattern
        m.resource_type = details::to_kafka_resource_type(
          binding.pattern().resource());
        m.resource_name = binding.pattern().name();
        m.pattern_type = details::to_kafka_pattern_type(
          binding.pattern().pattern());

        // acl entry
        m.principal = details::to_kafka_principal(binding.entry().principal());
        m.host = details::to_kafka_host(binding.entry().host());
        m.operation = details::to_kafka_operation(binding.entry().operation());
        m.permission_type = details::to_kafka_permission(
          binding.entry().permission());

        res.push_back(std::move(m));
    }

    return res;
}

template<>
ss::future<response_ptr> delete_acls_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    delete_acls_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    // <filter index> | error
    std::vector<std::variant<size_t, delete_acls_filter_result>> result_index;
    result_index.reserve(request.data.filters.size());

    // binding filters to delete. optimized for common case
    std::vector<security::acl_binding_filter> filters;
    filters.reserve(request.data.filters.size());

    for (const auto& request_filter : request.data.filters) {
        try {
            auto filter = details::to_acl_binding_filter(request_filter);
            result_index.emplace_back(filters.size());
            filters.push_back(std::move(filter));
        } catch (const details::acl_conversion_error& e) {
            result_index.emplace_back(delete_acls_filter_result{
              .error_code = error_code::invalid_request,
              .error_message = e.what(),
            });
        }
    }

    auto return_filters = [&filters]() { return filters; };

    auto authz = ctx.authorized(
      security::acl_operation::alter,
      security::default_cluster_name,
      std::move(return_filters));

    if (!ctx.audit()) {
        delete_acls_filter_result result;
        result.error_code = error_code::broker_not_available;
        result.error_message = "Broker not available - audit system failure";
        delete_acls_response resp;
        resp.data.filter_results.reserve(request.data.filters.size());
        std::fill_n(
          std::back_inserter(resp.data.filter_results),
          request.data.filters.size(),
          result);
        co_return co_await ctx.respond(std::move(resp));
    }

    if (!authz) {
        delete_acls_filter_result result;
        result.error_code = error_code::cluster_authorization_failed;
        delete_acls_response resp;
        resp.data.filter_results.reserve(request.data.filters.size());
        std::fill_n(
          std::back_inserter(resp.data.filter_results),
          request.data.filters.size(),
          result);
        co_return co_await ctx.respond(std::move(resp));
    }

    const auto num_filters = filters.size();

    auto results = co_await ctx.security_frontend().delete_acls(
      std::move(filters), 5s);

    delete_acls_response response;
    response.data.filter_results.reserve(result_index.size());

    // this is an important step because the loop below that constructs the
    // response unconditionally indexes into the result set.
    if (unlikely(results.size() != num_filters)) {
        vlog(
          klog.error,
          "Unexpected result size deleting ACLs: {} (expected {})",
          results.size(),
          num_filters);

        response.data.filter_results.reserve(result_index.size());
        for ([[maybe_unused]] auto _ :
             boost::irange<size_t>(result_index.size())) {
            response.data.filter_results.push_back(delete_acls_filter_result{
              .error_code = error_code::unknown_server_error});
        }

        co_return co_await ctx.respond(std::move(response));
    }

    for (auto& result : result_index) {
        ss::visit(
          result,
          [&response, &results](size_t i) {
              auto ec = map_topic_error_code(results[i].error);
              response.data.filter_results.push_back(delete_acls_filter_result{
                .error_code = ec,
                .matching_acls = bindings_to_delete_result(results[i].bindings),
              });
          },
          [&response](delete_acls_filter_result& r) {
              response.data.filter_results.push_back(std::move(r));
          });
    }

    co_return co_await ctx.respond(std::move(response));
}

} // namespace kafka
