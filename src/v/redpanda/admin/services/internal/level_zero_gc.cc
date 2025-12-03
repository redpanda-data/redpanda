/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/level_zero_gc.h"

#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

#include <fmt/format.h>

namespace {
using namespace std::chrono_literals;
constexpr auto health_report_query_timeout = 10s;
} // namespace

namespace admin {

seastar::future<proto::admin::level_zero_gc::advance_epoch_response>
level_zero_gc_service_impl::advance_epoch(
  serde::pb::rpc::context,
  proto::admin::level_zero_gc::advance_epoch_request req) {
    using namespace proto::admin::level_zero_gc;
    advance_epoch_response response;

    // TODO: Implement epoch advancement logic
    // For each partition in req.get_partitions():
    //   1. Look up the topic in _topic_table
    //   2. Validate the partition exists
    //   3. Call the appropriate GC epoch advancement method
    //   4. Create result entry with success/failure status

    // Stub: Return empty response for now
    chunked_vector<topic_partition_advance_epoch_result> results;
    for (const auto& partition_epoch : req.get_partitions()) {
        topic_partition_advance_epoch_result result;
        const auto& tp = partition_epoch.get_partition();

        proto::common::topic_partition result_tp;
        result_tp.set_topic(ss::sstring{tp.get_topic()});
        result_tp.set_partition(tp.get_partition());

        auto cfg = _topic_table->local().get_topic_cfg(
          model::topic_namespace_view{
            model::kafka_namespace, model::topic_view{tp.get_topic()}});

        if (!cfg.has_value()) {
            result.set_error(error::l0_gc_error_topic_not_found);
        } else if (auto p = tp.get_partition();
                   p < 0 || p >= cfg.value().partition_count) {
            result.set_error(error::l0_gc_error_invalid_partition);
        } else if (!cfg.value().is_cloud_topic()) {
            result.set_error(error::l0_gc_error_not_cloud_topic);
        } else {
            result.set_error(error::l0_gc_error_failed);
        }

        results.push_back(std::move(result));
    }

    response.set_partitions(std::move(results));

    co_return response;
}

auto level_zero_gc_service_impl::populate_epochs(
  topic_partition_epoch_map tp_epochs)
  -> ss::future<topic_partition_epoch_map> {
    auto health_report = co_await _health_monitor->local().get_cluster_health(
      cluster::cluster_report_filter{},
      cluster::force_refresh::yes,
      model::timeout_clock::now() + health_report_query_timeout);

    if (!health_report.has_value()) {
        throw serde::pb::rpc::unavailable_exception(
          fmt::format(
            "Error retrieving cluster health report: {}",
            health_report.error()));
    }
    fmt::print(std::cerr, "POPULATE EPOCHS: {}\n", tp_epochs.size());

    for (const auto& node_health : health_report.value().node_reports) {
        for (const auto& [tp_ns, partition_statuses] : node_health->topics) {
            auto tp_it = tp_epochs.find(tp_ns.tp);
            if (tp_it == tp_epochs.end()) {
                fmt::print(std::cerr, "{}: NOT FOUND\n", tp_ns);
                continue;
            }
            for (const auto& [pid, p_status] : partition_statuses) {
                const auto maybe_max_gc_epoch
                  = p_status.cloud_topic_max_gc_eligible_epoch;
                auto p_it = tp_it->second.find(pid);
                if (p_it == tp_it->second.end()) {
                    fmt::print(
                      std::cerr, "{}/{}: PARTITION NOT FOUND\n", tp_ns, pid);
                    continue;
                }
                if (!maybe_max_gc_epoch.has_value()) {
                    fmt::print(std::cerr, "{}/{}: NO EPOCH\n", tp_ns, pid);
                    continue;
                }
                p_it->second = std::max(p_it->second, maybe_max_gc_epoch);
                fmt::print(
                  std::cerr, "{}/{}: EPOCH: {}\n", tp_ns, pid, p_it->second);
            }
        }
        co_await ss::maybe_yield();
    }

    co_return std::move(tp_epochs);
}

seastar::future<proto::admin::level_zero_gc::get_epoch_response>
level_zero_gc_service_impl::get_epoch(
  serde::pb::rpc::context,
  proto::admin::level_zero_gc::get_epoch_request req) {
    using namespace proto::admin::level_zero_gc;
    get_epoch_response response;

    // TODO: Implement epoch retrieval logic
    // For each partition in req.get_partitions():
    //   1. Look up the topic in _topic_table
    //   2. Validate the partition exists
    //   3. Get the current GC epoch for the partition
    //   4. Create result entry with success/failure status

    chunked_vector<topic_partition_get_epoch_result> results;
    topic_partition_epoch_map epochs;
    for (const auto& tp : req.get_partitions()) {
        auto cfg = _topic_table->local().get_topic_cfg(
          model::topic_namespace_view{
            model::kafka_namespace, model::topic_view{tp.get_topic()}});

        topic_partition_get_epoch_result result;
        proto::common::topic_partition result_tp;
        result_tp.set_topic(ss::sstring{tp.get_topic()});
        result_tp.set_partition(tp.get_partition());
        result.set_partition(std::move(result_tp));

        if (!cfg.has_value()) {
            result.set_error(error::l0_gc_error_topic_not_found);
        } else if (auto p = tp.get_partition();
                   p < 0 || p >= cfg.value().partition_count) {
            result.set_error(error::l0_gc_error_invalid_partition);
        } else if (!cfg.value().is_cloud_topic()) {
            result.set_error(error::l0_gc_error_not_cloud_topic);
        } else {
            // if the requested tp is good, stage it for a health report scan
            epochs[model::topic_view{result.get_partition().get_topic()}]
              .try_emplace(
                model::partition_id{result.get_partition().get_partition()},
                std::nullopt);
        }

        results.push_back(std::move(result));
    }

    if (!epochs.empty()) {
        epochs = co_await populate_epochs(std::move(epochs));
        for (auto& r : results) {
            auto tp_it = epochs.find(
              model::topic_view{r.get_partition().get_topic()});
            if (tp_it == epochs.end()) {
                vassert(
                  r.has_error(),
                  "Epoch not populated for {}, expected error!",
                  r.get_partition().get_topic());
                continue;
            }
            auto p_it = tp_it->second.find(
              model::partition_id{r.get_partition().get_partition()});
            if (p_it == tp_it->second.end()) {
                vassert(
                  r.has_error(),
                  "Epoch not populated for {}/{}, expected error!",
                  r.get_partition().get_topic(),
                  r.get_partition().get_partition());
                continue;
            }
            if (!p_it->second.has_value()) {
                // TODO(oren): better error code i guess
                r.set_error(error::l0_gc_error_failed);
            } else {
                r.set_epoch(p_it->second.value());
            }
        }
    }

    response.set_partitions(std::move(results));

    co_return response;
}

} // namespace admin
