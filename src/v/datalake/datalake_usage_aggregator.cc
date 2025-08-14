/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/datalake_usage_aggregator.h"

#include "cluster/controller.h"
#include "cluster/topic_table.h"
#include "datalake/coordinator/frontend.h"
#include "datalake/logger.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>

#include <ranges>

namespace {

ss::future<chunked_vector<datalake::coordinator::usage_stats_reply>>
dispatch_requests(
  datalake::coordinator::frontend& frontend, int partition_count) {
    chunked_vector<ss::future<datalake::coordinator::usage_stats_reply>>
      replies;
    for (auto i = 0; i < partition_count; ++i) {
        datalake::coordinator::usage_stats_request request{
          model::partition_id(i)};
        replies.push_back(frontend.get_usage_stats(request));
    }
    co_return co_await ss::when_all_succeed(replies.begin(), replies.end())
      | std::views::as_rvalue
      | std::ranges::to<
        chunked_vector<datalake::coordinator::usage_stats_reply>>();
}
} // namespace

namespace datalake {

disabled_datalake_usage_api_impl::disabled_datalake_usage_api_impl(
  cluster::controller* controller)
  : _controller(controller) {
    vassert(
      _controller,
      "Controller must not be null for disabled datalake usage API");
}

ss::future<kafka::datalake_usage_api::usage_stats>
disabled_datalake_usage_api_impl::compute_usage(ss::abort_source&) {
    usage_stats stats;
    if (!_controller->is_raft0_leader()) {
        stats.missing_reason = kafka::datalake_usage_api::stats_missing_reason::
          not_controller_leader;
    } else {
        stats.missing_reason
          = kafka::datalake_usage_api::stats_missing_reason::feature_disabled;
        vlog(
          datalake_log.debug,
          "Datalake usage API is disabled, returning empty stats");
    }
    return ss::make_ready_future<usage_stats>(std::move(stats));
}

static constexpr ss::lowres_clock::duration usage_aggregation_timeout = 5s;
static constexpr ss::lowres_clock::duration usage_aggregation_backoff = 100ms;

default_datalake_usage_api_impl::default_datalake_usage_api_impl(
  cluster::controller* controller,
  ss::sharded<cluster::topic_table>* topic_table,
  ss::sharded<datalake::coordinator::frontend>* coordinator_fe)
  : _controller(controller)
  , _topics(topic_table)
  , _frontend(coordinator_fe) {
    vassert(_controller, "Controller must not be null");
}

ss::future<kafka::datalake_usage_api::usage_stats>
default_datalake_usage_api_impl::compute_usage(ss::abort_source& as) {
    if (!_controller->is_raft0_leader()) {
        kafka::datalake_usage_api::usage_stats stats;
        stats.missing_reason = kafka::datalake_usage_api::stats_missing_reason::
          not_controller_leader;
        co_return stats;
    }
    const auto& topics = _topics->local();
    auto topic = topics.get_topic_metadata(model::datalake_coordinator_nt);
    if (!topic) {
        vlog(
          datalake_log.debug,
          "Datalake coordinator topic {} not found, nothing to report",
          model::datalake_coordinator_nt);
        // The absence of the coordinator topic suggests the datalake feature
        // was never used, as any coordinator operation would have created it
        // initially. Hence nothing to report.
        kafka::datalake_usage_api::usage_stats stats;
        stats.missing_reason
          = kafka::datalake_usage_api::stats_missing_reason::none;
        stats.topic_stats.emplace();
        co_return stats;
    }

    retry_chain_node rcn(
      as,
      usage_aggregation_timeout,
      usage_aggregation_backoff,
      retry_strategy::polling);
    std::exception_ptr last_exception = nullptr;
    while (true) {
        auto coordinator_partitions
          = topic->get_configuration().partition_count;
        auto usage_results_f = co_await ss::coroutine::as_future(
          dispatch_requests(_frontend->local(), coordinator_partitions));

        bool succeeded = true;
        chunked_vector<kafka::datalake_usage_api::topic_usage> topic_stats;
        if (usage_results_f.failed()) {
            last_exception = usage_results_f.get_exception();
            vlog(
              datalake_log.warn,
              "Failed to collect datalake usage stats: {}",
              last_exception);
            succeeded = false;
        } else {
            for (const auto& result : usage_results_f.get()) {
                if (result.errc != coordinator::errc::ok) {
                    vlog(
                      datalake_log.warn,
                      "Failed to collect datalake usage stats: {}",
                      result.errc);
                    succeeded = false;
                    break;
                }
                for (const auto& topic_usage : result.stats.topic_usages) {
                    kafka::datalake_usage_api::topic_usage usage;
                    usage.topic = topic_usage.topic,
                    usage.revision = topic_usage.revision,
                    usage.kafka_bytes_processed
                      = topic_usage.total_kafka_bytes_processed;
                    topic_stats.push_back(std::move(usage));
                }
                co_await ss::maybe_yield();
            }
        }
        if (succeeded) {
            // If we succeeded, we can return the stats
            usage_stats stats;
            stats.missing_reason
              = kafka::datalake_usage_api::stats_missing_reason::none;
            stats.topic_stats = std::move(topic_stats);
            vlog(
              datalake_log.debug, "Collected datalake usage stats: {}", stats);
            co_return stats;
        }
        // check if we should retry
        auto retry = rcn.retry();
        if (!retry.is_allowed) {
            break;
        }
        last_exception = nullptr;
        co_await ss::sleep_abortable(retry.delay, as);
    }
    rcn.check_abort();
    if (last_exception) {
        // last retry resulted in an exception, let the caller handle it
        std::rethrow_exception(last_exception);
    }
    usage_stats stats;
    stats.missing_reason
      = kafka::datalake_usage_api::stats_missing_reason::collection_error;
    co_return stats;
}

} // namespace datalake
