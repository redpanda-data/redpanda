/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/cluster_link/frontend.h"
#include "cluster/controller.h"
#include "cluster/controller_stm.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster_link/deps.h"
#include "cluster_link/link.h"
#include "cluster_link/link_probe.h"
#include "cluster_link/logger.h"
#include "cluster_link/manager.h"
#include "cluster_link/model/types.h"
#include "cluster_link/replication/deps.h"
#include "cluster_link/replication/mux_remote_consumer.h"
#include "cluster_link/replication/types.h"
#include "cluster_link/service.h"
#include "cluster_link/shadow_linking_rpc_service.h"
#include "config/node_config.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/server/group_router.h"

#include <seastar/coroutine/switch_to.hh>

namespace {
struct shard_report_reducer {
    using result_t = ::cluster_link::rpc::shadow_topic_report_response;
    void operator()(result_t shard_result) {
        if (!result) {
            result = std::move(shard_result);
            return;
        }
        if (result->err_code != ::cluster_link::errc::success) {
            return;
        }
        if (shard_result.err_code != ::cluster_link::errc::success) {
            result->err_code = shard_result.err_code;
            return;
        }
        result->link_update_revision = std::min(
          result->link_update_revision, shard_result.link_update_revision);
        for (auto& leader : shard_result.leaders) {
            result->leaders.push_back(std::move(leader));
        }
        return;
    }

    std::optional<result_t> get() && {
        if (!result) {
            return std::nullopt;
        }
        if (result->err_code != ::cluster_link::errc::success) {
            result->leaders.clear();
            result->link_update_revision = {};
        }
        return std::move(result);
    }
    std::optional<result_t> result;
};

struct shard_link_report_reducer {
    using result_t = ::cluster_link::rpc::shadow_link_status_report_response;
    void operator()(result_t shard_result) {
        if (!result) {
            result = std::move(shard_result);
            return;
        }
        if (
          shard_result.err_code != ::cluster_link::errc::success
          && result->err_code == ::cluster_link::errc::success) {
            result->err_code = shard_result.err_code;
        }
        for (auto& [topic, response] : shard_result.topic_responses) {
            auto& existing = result->topic_responses[topic];
            existing.status = response.status;
            for (auto& [pid, report] : response.partition_reports) {
                existing.partition_reports.emplace(pid, std::move(report));
            }
        }
        for (auto& [task_name, reports] : shard_result.task_status_reports) {
            auto& existing = result->task_status_reports[task_name];
            for (auto& report : reports) {
                existing.push_back(std::move(report));
            }
        }
    }
    std::optional<result_t> get() && {
        if (!result) {
            return std::nullopt;
        }
        return std::move(result);
    }
    std::optional<result_t> result;
};
} // namespace

namespace cluster_link {

ss::future<rpc::shadow_topic_report_response>
service::node_local_shadow_topic_report(
  rpc::shadow_topic_report_request request) {
    auto h = _gate.hold();
    if (auto err = check_manager_state(); err != errc::success) {
        co_return rpc::shadow_topic_report_response{.err_code = err};
    }
    shard_report_reducer reducer{};
    const auto& link_id = request.link_id;
    const auto& topic = request.topic_name;
    co_await container().map_reduce(
      reducer,
      [](
        service& s,
        const ::cluster_link::model::id_t& link_id,
        const ::model::topic& topic) {
          if (!s.container().local_is_initialized()) {
              return rpc::shadow_topic_report_response{
                .err_code = errc::service_not_ready};
          }
          return s.shard_local_topic_report(link_id, topic);
      },
      link_id,
      topic);
    auto result = std::move(reducer).get();
    if (result) {
        result->node_id = _self;
        co_return std::move(*result);
    }
    vlog(
      cllog.error,
      "No result from shard report reducer for topic: {}, this should never "
      "happen, returning {}",
      topic,
      errc::link_id_not_found);
    // This is effectively unreachable because the reducer always produces a
    // result aggregated from all shards. Here we return a blanket
    // link_id_not_found
    co_return ::cluster_link::rpc::shadow_topic_report_response{
      .err_code = errc::link_id_not_found};
}

ss::future<::cluster_link::rpc::shadow_topic_report_response>
service::shadow_topic_report(
  ::model::node_id node_id, rpc::shadow_topic_report_request request) {
    if (auto err = check_manager_state(); err != errc::success) {
        co_return rpc::shadow_topic_report_response{.err_code = err};
    }
    using resp_t = ::cluster_link::rpc::shadow_topic_report_response;
    if (node_id == _self) {
        co_return co_await node_local_shadow_topic_report(std::move(request));
    }
    static constexpr auto rpc_timeout = 5s;
    co_return co_await _connections->local()
      .with_node_client<rpc::shadow_linking_rpc_client_protocol>(
        _self,
        ss::this_shard_id(),
        node_id,
        ::model::timeout_clock::now() + rpc_timeout,
        [request = std::move(request)](
          rpc::shadow_linking_rpc_client_protocol client) mutable {
            return client
              .shadow_topic_report(
                std::move(request), ::rpc::client_opts(rpc_timeout))
              .then(&::rpc::get_ctx_data<resp_t>);
        })
      .then(
        [](result<::cluster_link::rpc::shadow_topic_report_response> result) {
            if (result.has_error()) {
                vlog(
                  cllog.warn,
                  "Error getting shadow topic report from remote node: {}",
                  result.error());
                return ss::make_ready_future<resp_t>(
                  resp_t{.err_code = ::cluster_link::errc::rpc_error});
            }
            return ss::make_ready_future<resp_t>(std::move(result.value()));
        });
}

ss::future<model::report_result_t>
service::shadow_topic_report(model::id_t link_id, const ::model::topic& topic) {
    auto h = _gate.hold();
    // farms out requests to all nodes with replicas of the topic
    // and then aggregates the results
    // generate a list of brokers with replicas of the topic
    if (auto err = check_manager_state(); err != errc::success) {
        co_return std::unexpected<errc>(err);
    }
    absl::flat_hash_set<::model::node_id> topic_nodes;
    const auto& md_cache = _metadata_cache->local();
    const auto& maybe_tp_md = md_cache.get_topic_metadata_ref(
      ::model::topic_namespace_view{::model::kafka_namespace, topic});
    if (!maybe_tp_md) {
        co_return std::unexpected<errc>(errc::topic_does_not_exist);
    }
    const auto& tp_md = maybe_tp_md.value().get();
    // no scheduling points while looping through partitions
    auto num_partitions = tp_md.get_configuration().partition_count;
    const auto& assignments = tp_md.get_assignments();
    for (const auto& [_, p_assignment] : assignments) {
        for (const auto& r : p_assignment.replicas) {
            topic_nodes.insert(r.node_id);
        }
    }
    if (topic_nodes.empty()) {
        co_return std::unexpected<errc>(errc::topic_metadata_stale);
    }
    ::cluster_link::model::aggregated_shadow_topic_report result;
    result.total_partitions = num_partitions;
    result.brokers.reserve(topic_nodes.size());
    try {
        co_await ss::max_concurrent_for_each(
          topic_nodes,
          32,
          [this, link_id, &topic, &result](::model::node_id node_id) {
              ::cluster_link::rpc::shadow_topic_report_request request;
              request.link_id = link_id;
              request.topic_name = topic;
              return shadow_topic_report(node_id, std::move(request))
                .then([node_id, &result](
                        ::cluster_link::rpc::shadow_topic_report_response r) {
                    if (r.err_code != ::cluster_link::errc::success) {
                        vlog(
                          cllog.warn,
                          "Error getting shadow topic report from node {}: {}",
                          node_id,
                          r.err_code);
                        return ss::now();
                    }
                    ::cluster_link::model::aggregated_shadow_topic_report::
                      broker_report broker_report;
                    broker_report.broker = node_id;
                    broker_report.link_update_revision = r.link_update_revision;
                    for (auto& leader : r.leaders) {
                        broker_report.leaders.push_back(
                          {.partition = leader.partition});
                    }
                    result.brokers.push_back(std::move(broker_report));
                    return ss::now();
                });
          });
    } catch (...) {
        vlog(
          cllog.warn,
          "Exception during shadow topic reporting {}",
          std::current_exception());
        co_return std::unexpected<errc>(errc::rpc_error);
    }
    co_return result;
}

ss::future<rpc::shadow_link_status_report_response>
service::node_local_shadow_link_report(
  rpc::shadow_link_status_report_request req) {
    shard_link_report_reducer reducer{};
    const auto& link_id = req.link_id;

    co_await container().map_reduce(
      reducer,
      [](service& s, const model::id_t& id) {
          if (!s.container().local_is_initialized()) {
              return rpc::shadow_link_status_report_response{
                .err_code = errc::service_not_ready};
          }
          return s.shard_local_shadow_link_report(id);
      },
      link_id);
    auto result = std::move(reducer).get();
    if (result) {
        vlog(cllog.trace, "shadow link report for node {}: {}", _self, *result);
        co_return std::move(*result);
    }
    vlog(
      cllog.error,
      "No result from shard link report reducer for link {}",
      link_id);

    co_return rpc::shadow_link_status_report_response{
      .err_code = errc::link_id_not_found, .link_id = link_id};
}

rpc::shadow_link_status_report_response
service::shard_local_shadow_link_report(model::id_t id) {
    if (auto err = check_manager_state(); err != errc::success) {
        return rpc::shadow_link_status_report_response{.err_code = err};
    }
    auto on_controller_leader = [this]() -> bool {
        if (ss::this_shard_id() != cluster::controller_stm_shard) {
            return false;
        }
        return _self
               == _partition_leaders_table->local().get_leader(
                 ::model::controller_ntp);
    }();
    rpc::shadow_link_status_report_response result;
    result.link_id = id;

    auto res = _manager->get_partition_offsets_report_for_link(id);
    if (!res.has_value()) {
        vlog(
          cllog.warn,
          "Failed to get shard local shadow link report for link {}: {} ({})",
          id,
          res.assume_error().code(),
          res.assume_error().message());
        result.err_code = res.assume_error().code();
        return result;
    }
    auto value = std::move(res.assume_value());
    result.err_code = errc::success;

    for (const auto& [topic, offsets] : value) {
        auto& report = result.topic_responses[topic.tp.topic]
                         .partition_reports[topic.tp.partition];
        report.partition = topic.tp.partition;
        report.shadow_partition_high_watermark = offsets.shadow_hwm;
        // If last update time is default time_point, then we have not received
        // a new offset report from the source cluster for this partition
        if (offsets.update_time != ss::lowres_clock::time_point{}) {
            report.source_partition_start_offset = offsets.source_start_offset;
            report.source_partition_high_watermark = offsets.source_hwm;
            report.source_partition_last_stable_offset = offsets.source_lso;
            report.last_update_time
              = std::chrono::duration_cast<std::chrono::milliseconds>(
                offsets.update_time.time_since_epoch());
        }
    }

    auto task_report_res = _manager->get_task_status_report(id);
    if (!task_report_res.has_value()) {
        vlog(
          cllog.warn,
          "Failed to get shard local shadow link task report for link {}: {} "
          "({})",
          id,
          task_report_res.assume_error().code(),
          task_report_res.assume_error().message());
        result.err_code = task_report_res.assume_error().code();
        return result;
    }

    auto task_report = std::move(task_report_res).assume_value();
    for (auto& [name, report] : task_report.task_status_reports) {
        if (
          !on_controller_leader && report.is_controller_locked_task
          && report.task_state == model::task_state::stopped) {
            // skip reporting stopped controller locked tasks on non-leader
            // nodes
            continue;
        }
        result.task_status_reports[name].emplace_back(std::move(report));
    }

    vlog(
      cllog.trace,
      "shadow link report for shard {}/{}: {}",
      _self,
      ss::this_shard_id(),
      result);

    return result;
}

ss::future<model::status_report_ret_t>
service::shadow_link_report(model::name_t name) {
    vlog(cllog.trace, "Generating shadow link report for link {}", name);
    auto link_id = _plf->local().find_link_id_by_name(name);
    if (!link_id.has_value()) {
        co_return std::unexpected<errc>(errc::link_id_not_found);
    }
    auto& members_table = _controller->get_members_table();
    const auto& node_ids = members_table.local().node_ids();
    vlog(cllog.trace, "Issuing rpcs to nodes {}", node_ids);
    model::shadow_link_status_report results;
    results.link_id = link_id.value();
    try {
        co_await ss::max_concurrent_for_each(
          node_ids,
          32,
          [this, &results, link_id = link_id.value()](::model::node_id node) {
              rpc::shadow_link_status_report_request request{
                .link_id = link_id};
              return shadow_link_report(node, std::move(request))
                .then([&results](rpc::shadow_link_status_report_response resp) {
                    for (const auto& [topic, topic_response] :
                         resp.topic_responses) {
                        auto& existing = results.topic_responses[topic];
                        for (const auto& [pid, report] :
                             topic_response.partition_reports) {
                            existing.partition_reports.emplace(pid, report);
                        }
                    }
                    for (auto& [task_name, reports] :
                         resp.task_status_reports) {
                        auto& existing_reports
                          = results.task_status_reports[task_name];
                        for (auto& r : reports) {
                            existing_reports.push_back(std::move(r));
                        }
                    }
                });
          });
    } catch (const std::exception& e) {
        vlog(cllog.warn, "Exception during shadow link reporting: {}", e);
        co_return std::unexpected<errc>(errc::rpc_error);
    }

    co_return results;
}

ss::future<rpc::shadow_link_status_report_response> service::shadow_link_report(
  ::model::node_id node, rpc::shadow_link_status_report_request req) {
    using resp_t = rpc::shadow_link_status_report_response;
    if (node == _self) {
        co_return co_await node_local_shadow_link_report(std::move(req));
    }

    static constexpr auto rpc_timeout = 5s;
    vlog(cllog.trace, "Issuing rpc to node {}", node);
    auto resp = co_await _connections->local()
                  .with_node_client<rpc::shadow_linking_rpc_client_protocol>(
                    _self,
                    ss::this_shard_id(),
                    node,
                    ::model::timeout_clock::now() + rpc_timeout,
                    [request = std::move(req)](
                      rpc::shadow_linking_rpc_client_protocol client) mutable {
                        return client
                          .shadow_link_report(
                            std::move(request), ::rpc::client_opts(rpc_timeout))
                          .then(&::rpc::get_ctx_data<resp_t>);
                    });
    if (resp.has_error()) {
        vlog(
          cllog.warn,
          "Error getting shadow link report for node {}: {}",
          node,
          resp.error());
        co_return resp_t{.err_code = errc::rpc_error};
    }
    co_return std::move(resp.value());
}

} // namespace cluster_link
