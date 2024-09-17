
// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/create_partitions.h"

#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/create_partitions_request.h"
#include "kafka/protocol/schemata/create_partitions_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/fwd.h"
#include "kafka/server/quota_manager.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/node_hash_map.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <vector>

namespace kafka {

create_partitions_topic_result
make_result(const create_partitions_topic& tp, error_code ec) {
    return {
      .name = tp.name,
      .error_code = ec,
    };
}

using request_iterator = chunked_vector<create_partitions_topic>::iterator;

template<typename ResultIter>
request_iterator validate_range_duplicates(
  request_iterator begin, request_iterator end, ResultIter out_it) {
    using type = create_partitions_topic;
    absl::node_hash_map<model::topic_view, uint32_t> freq;

    freq.reserve(std::distance(begin, end));
    for (const auto& r : boost::make_iterator_range(begin, end)) {
        freq[r.name]++;
    }
    auto valid_range_end = std::partition(
      begin, end, [&freq](const type& item) { return freq[item.name] == 1; });

    std::transform(valid_range_end, end, out_it, [](const type& req) {
        return create_partitions_topic_result{
          .name = req.name,
          .error_code = error_code::invalid_request,
          .error_message = "request contains duplicated topics",
        };
    });
    return valid_range_end;
}
template<typename ResultIter, typename Predicate>
request_iterator validate_range(
  request_iterator begin,
  request_iterator end,
  ResultIter out_it,
  error_code ec,
  const ss::sstring& error_message,
  Predicate&& p) {
    auto valid_range_end = std::partition(
      begin, end, std::forward<Predicate>(p));

    std::transform(
      valid_range_end,
      end,
      out_it,
      [ec, &error_message](const create_partitions_topic& req) {
          return create_partitions_topic_result{
            .name = req.name,
            .error_code = ec,
            .error_message = error_message,
          };
      });
    return valid_range_end;
}

template<typename ResultIter, typename Predicate>
ss::future<request_iterator> validate_range_async(
  request_iterator begin,
  request_iterator end,
  ResultIter out_it,
  error_code ec,
  const ss::sstring& error_message,
  Predicate&& p) {
    auto valid_range_end = co_await ssx::partition(
      begin, end, std::forward<Predicate>(p));

    std::transform(
      valid_range_end,
      end,
      out_it,
      [ec, &error_message](const create_partitions_topic& req) {
          return create_partitions_topic_result{
            .name = req.name,
            .error_code = ec,
            .error_message = error_message,
          };
      });
    co_return valid_range_end;
}

ss::future<std::vector<cluster::topic_result>> do_create_partitions(
  request_context& ctx,
  request_iterator begin,
  request_iterator end,
  model::timeout_clock::time_point timeout) {
    const auto sz = std::distance(begin, end);
    if (sz == 0) {
        co_return std::vector<cluster::topic_result>{};
    }
    std::vector<cluster::create_partitions_configuration> partitions;
    partitions.reserve(sz);

    std::transform(
      begin,
      end,
      std::back_inserter(partitions),
      [](create_partitions_topic& tp) {
          return cluster::create_partitions_configuration(
            model::topic_namespace(model::kafka_namespace, std::move(tp.name)),
            tp.count);
      });

    co_return co_await ctx.topics_frontend().create_partitions(
      std::move(partitions), timeout);
}

template<>
ss::future<response_ptr> create_partitions_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    create_partitions_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);
    create_partitions_response resp;

    if (request.data.topics.empty()) {
        co_return co_await ctx.respond(std::move(resp));
    }

    resp.data.results.reserve(request.data.topics.size());

    if (ctx.recovery_mode_enabled()) {
        for (const auto& t : request.data.topics) {
            resp.data.results.push_back(create_partitions_topic_result{
              .name = t.name,
              .error_code = error_code::policy_violation,
              .error_message = "Forbidden in recovery mode",
            });
        }

        co_return co_await ctx.respond(std::move(resp));
    }

    // authorize
    auto valid_range_end = validate_range(
      request.data.topics.begin(),
      request.data.topics.end(),
      std::back_inserter(resp.data.results),
      error_code::topic_authorization_failed,
      "Topic authorization failed",
      [&ctx](const create_partitions_topic& tp) {
          return ctx.authorized(security::acl_operation::alter, tp.name);
      });

    if (!ctx.audit()) {
        auto distance = std::distance(
          request.data.topics.begin(), valid_range_end);

        co_return co_await ctx.respond(create_partitions_response(
          error_code::broker_not_available,
          "Broker not available - audit system failure",
          std::move(resp),
          std::move(request),
          distance));
    }

    // check duplicates
    valid_range_end = validate_range_duplicates(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results));

    // validate topics existence
    valid_range_end = validate_range(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results),
      error_code::invalid_topic_exception,
      "Topic does not exist",
      [&ctx](const create_partitions_topic& tp) {
          return ctx.metadata_cache().contains(
            model::topic_namespace(model::kafka_namespace, tp.name));
      });

    valid_range_end = validate_range(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results),
      error_code::invalid_request,
      "Partition count must be greater then current number of partitions",
      [&ctx](const create_partitions_topic& tp) {
          return tp.count > ctx.metadata_cache()
                              .get_topic_cfg(model::topic_namespace_view(
                                model::kafka_namespace, tp.name))
                              ->partition_count;
      });

    // validate custom assignment
    valid_range_end = validate_range(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results),
      error_code::invalid_request,
      "Partition count has to be greater than 0",
      [](const create_partitions_topic& tp) { return tp.count > 0; });

    // validate custom assignment
    valid_range_end = validate_range(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results),
      error_code::invalid_request,
      "Redpanda does not yet support custom partitions assignment",
      [](const create_partitions_topic& tp) {
          return !tp.assignments.has_value();
      });

    // check for inprogress reassignments
    valid_range_end = validate_range(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results),
      error_code::reassignment_in_progress,
      "A partition reassignment is in progress.",
      [&ctx](const create_partitions_topic& tp) {
          const auto& updates_in_progress
            = ctx.metadata_cache().updates_in_progress();
          return std::none_of(
            updates_in_progress.begin(),
            updates_in_progress.end(),
            [name{tp.name}](auto& iter) {
                return iter.first.tp.topic == name;
            });
      });

    if (request.data.validate_only) {
        std::transform(
          request.data.topics.begin(),
          valid_range_end,
          std::back_inserter(resp.data.results),
          [](const create_partitions_topic& tp) {
              return create_partitions_topic_result{
                .name = tp.name,
                .error_code = error_code::none,
              };
          });
        co_return co_await ctx.respond(std::move(resp));
    }

    valid_range_end = co_await validate_range_async(
      request.data.topics.begin(),
      valid_range_end,
      std::back_inserter(resp.data.results),
      ((ctx.header().version >= api_version(3))
         ? error_code::throttling_quota_exceeded
         : error_code::unknown_server_error),
      "Too many partition mutations requested",
      [&ctx, &resp](const create_partitions_topic& tp) {
          const auto cfg = ctx.metadata_cache().get_topic_cfg(
            model::topic_namespace_view(model::kafka_namespace, tp.name));
          vassert(cfg, "Topic exist check has already occurred");
          vassert(
            tp.count > cfg->partition_count,
            "Sanity check for request increase partition count failed");
          const auto mutations = (tp.count - cfg->partition_count);
          return ctx.quota_mgr()
            .record_partition_mutations(ctx.header().client_id, mutations)
            .then([&resp](std::chrono::milliseconds delay) {
                resp.data.throttle_time_ms = std::max(
                  resp.data.throttle_time_ms, delay);
                return delay == 0ms;
            });
      });

    auto results = co_await do_create_partitions(
      ctx,
      request.data.topics.begin(),
      valid_range_end,
      model::timeout_clock::now()
        + std::chrono::milliseconds(request.data.timeout_ms));

    std::transform(
      results.begin(),
      results.end(),
      std::back_inserter(resp.data.results),
      [](cluster::topic_result& r) {
          return create_partitions_topic_result{
            .name = std::move(r.tp_ns.tp),
            .error_code = map_topic_error_code(r.ec),
            .error_message = cluster::make_error_code(r.ec).message(),
          };
      });

    co_return co_await ctx.respond(std::move(resp));
}

} // namespace kafka
