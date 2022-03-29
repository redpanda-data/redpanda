// Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/core/coroutine.hh>
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

template<>
ss::future<response_ptr>
offset_fetch_handler::handle(request_context ctx, ss::smp_service_group) {
    offset_fetch_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    if (!ctx.authorized(
          security::acl_operation::describe, request.data.group_id)) {
        co_return co_await ctx.respond(
          offset_fetch_response(error_code::group_authorization_failed));
    }

    /*
     * request is for all group offsets
     */
    if (!request.data.topics) {
        auto resp = co_await ctx.groups().offset_fetch(std::move(request));
        if (resp.data.error_code != error_code::none) {
            co_return co_await ctx.respond(std::move(resp));
        }

        // remove unauthorized topics from response
        auto unauthorized = std::partition(
          resp.data.topics.begin(),
          resp.data.topics.end(),
          [&ctx](const offset_fetch_response_topic& topic) {
              /*
               * quiet authz failures. this is checking for visibility across
               * all topics not specifically requested topics.
               */
              return ctx.authorized(
                security::acl_operation::describe,
                topic.name,
                authz_quiet{true});
          });
        resp.data.topics.erase(unauthorized, resp.data.topics.end());

        co_return co_await ctx.respond(std::move(resp));
    }

    /*
     * pre-filter authorized topics in request
     */
    auto unauthorized_it = std::partition(
      request.data.topics->begin(),
      request.data.topics->end(),
      [&ctx](const offset_fetch_request_topic& topic) {
          return ctx.authorized(security::acl_operation::describe, topic.name);
      });

    std::vector<offset_fetch_request_topic> unauthorized(
      std::make_move_iterator(unauthorized_it),
      std::make_move_iterator(request.data.topics->end()));

    // remove unauthorized topics from request
    request.data.topics->erase(unauthorized_it, request.data.topics->end());
    auto resp = co_await ctx.groups().offset_fetch(std::move(request));

    // add requested (but unauthorized) topics into response
    for (auto& req_topic : unauthorized) {
        auto& topic = resp.data.topics.emplace_back();
        topic.name = std::move(req_topic.name);
        topic.partitions.reserve(req_topic.partition_indexes.size());
        for (auto partition_index : req_topic.partition_indexes) {
            auto& partition = topic.partitions.emplace_back();
            partition.partition_index = partition_index;
            partition.error_code = error_code::group_authorization_failed;
            topic.partitions.push_back(std::move(partition));
        }
    }

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
