// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cluster/client_quota_frontend.h"
#include "cluster/client_quota_serde.h"
#include "cluster/cluster_link/frontend.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/plugin_frontend.h"
#include "cluster/service.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "rpc/errc.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/switch_to.hh>

namespace cluster {

cluster::errc map_health_monitor_error_code(std::error_code e);

ss::future<get_node_health_reply> service::collect_node_health_report(
  get_node_health_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_collect_node_health_report(std::move(req));
      });
}

ss::future<get_cluster_health_reply> service::get_cluster_health_report(
  get_cluster_health_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_get_cluster_health_report(std::move(req));
      });
}

ss::future<get_node_health_reply>
service::do_collect_node_health_report(get_node_health_request req) {
    // validate if the receiving node is the one that that the request is
    // addressed to
    if (
      req.get_target_node_id() != get_node_health_request::node_id_not_set
      && req.get_target_node_id() != _controller->self()) {
        vlog(
          clusterlog.debug,
          "Received a get_node_health request addressed to different node. "
          "Requested node id: {}, current node id: {}",
          req.get_target_node_id(),
          _controller->self());
        co_return get_node_health_reply{.error = errc::invalid_target_node_id};
    }

    auto res = co_await _hm_frontend.local().get_current_node_health();
    if (res.has_error()) {
        co_return get_node_health_reply{
          .error = map_health_monitor_error_code(res.error())};
    }
    co_return get_node_health_reply{
      .error = errc::success,
      .report = node_health_report_serde{*res.value()},
    };
}

ss::future<get_cluster_health_reply>
service::do_get_cluster_health_report(get_cluster_health_request req) {
    auto tout = config::shard_local_cfg().health_monitor_max_metadata_age()
                + model::timeout_clock::now();
    auto res = co_await _hm_frontend.local().get_cluster_health(
      req.filter, req.refresh, tout);

    if (res.has_error()) {
        co_return get_cluster_health_reply{
          .error = map_health_monitor_error_code(res.error())};
    }
    auto report = std::move(res.value());

    co_return get_cluster_health_reply{
      .error = errc::success,
      .report = std::move(report),
    };
}

ss::future<feature_action_response>
service::feature_action(feature_action_request req, rpc::streaming_context&) {
    co_await _feature_manager.invoke_on(
      feature_manager::backend_shard,
      [req = std::move(req)](feature_manager& fm) {
          return fm.write_action(req.action);
      });

    co_return feature_action_response{
      .error = errc::success,
    };
}

ss::future<feature_barrier_response>
service::feature_barrier(feature_barrier_request req, rpc::streaming_context&) {
    auto result = co_await _feature_manager.invoke_on(
      feature_manager::backend_shard,
      [req = std::move(req)](feature_manager& fm) {
          return fm.update_barrier(req.tag, req.peer, req.entered);
      });

    co_return feature_barrier_response{
      .entered = result.entered, .complete = result.complete};
}

ss::future<cancel_partition_movements_reply>
service::cancel_all_partition_movements(
  cancel_all_partition_movements_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this, req]() {
        return do_cancel_all_partition_movements(req);
    });
}
ss::future<cancel_partition_movements_reply>
service::cancel_node_partition_movements(
  cancel_node_partition_movements_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this, req]() {
        return do_cancel_node_partition_movements(req);
    });
}

ss::future<cancel_partition_movements_reply>
service::do_cancel_all_partition_movements(
  cancel_all_partition_movements_request) {
    auto ret
      = co_await _topics_frontend.local().cancel_moving_all_partition_replicas(
        default_move_interruption_timeout + model::timeout_clock::now());

    if (ret.has_error()) {
        co_return cancel_partition_movements_reply{
          .general_error = map_update_interruption_error_code(ret.error())};
    }
    co_return cancel_partition_movements_reply{
      .general_error = errc::success,
      .partition_results = std::move(ret.value())};
}

ss::future<cancel_partition_movements_reply>
service::do_cancel_node_partition_movements(
  cancel_node_partition_movements_request req) {
    auto ret
      = co_await _topics_frontend.local().cancel_moving_partition_replicas_node(
        req.node_id,
        req.direction,
        default_move_interruption_timeout + model::timeout_clock::now());

    if (ret.has_error()) {
        co_return cancel_partition_movements_reply{
          .general_error = map_update_interruption_error_code(ret.error())};
    }
    co_return cancel_partition_movements_reply{
      .general_error = errc::success,
      .partition_results = std::move(ret.value())};
}

ss::future<transfer_leadership_reply> service::transfer_leadership(
  transfer_leadership_request r, rpc::streaming_context&) {
    auto shard_id = _api.local().shard_for(r.group);
    if (!shard_id.has_value()) {
        co_return transfer_leadership_reply{
          .success = false, .result = raft::errc::group_not_exists};
    } else {
        auto errc = co_await _partition_manager.invoke_on(
          shard_id.value(),
          [r = std::move(r)](
            partition_manager& pm) mutable -> ss::future<std::error_code> {
              auto partition_ptr = pm.partition_for(r.group);
              if (!partition_ptr) {
                  return ss::make_ready_future<std::error_code>(
                    raft::errc::group_not_exists);
              } else {
                  return partition_ptr->transfer_leadership(std::move(r));
              }
          });
        co_return transfer_leadership_reply{
          .success = (errc == raft::make_error_code(raft::errc::success)),
          .result = raft::errc{int16_t(errc.value())}};
    }
}

ss::future<producer_id_lookup_reply> service::highest_producer_id(
  producer_id_lookup_request, rpc::streaming_context&) {
    producer_id_lookup_reply reply;
    auto highest_pid = co_await _partition_manager.map_reduce0(
      [](const partition_manager& pm) {
          model::producer_id pid{};
          for (const auto& [_, p] : pm.partitions()) {
              pid = std::max(pid, p->highest_producer_id());
          }
          vlog(clusterlog.debug, "Found producer id {}", pid);
          return pid;
      },
      model::producer_id{},
      [](model::producer_id acc, model::producer_id pid) {
          return std::max(acc, pid);
      });
    vlog(clusterlog.debug, "Returning highest producer id {}", highest_pid);
    reply.highest_producer_id = highest_pid;
    co_return reply;
}

ss::future<cloud_storage_usage_reply> service::cloud_storage_usage(
  cloud_storage_usage_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this, req]() {
        return do_cloud_storage_usage(req);
    });
}

ss::future<cloud_storage_usage_reply>
service::do_cloud_storage_usage(cloud_storage_usage_request req) {
    struct res_type {
        uint64_t total_size{0};
        std::vector<model::ntp> missing_partitions;
    };

    std::vector<model::ntp> missing_ntps;

    absl::flat_hash_map<ss::shard_id, std::vector<model::ntp>> ntps_by_shard;
    for (const auto& ntp : req.partitions) {
        auto shard = _api.local().shard_for(ntp);
        if (!shard) {
            missing_ntps.push_back(ntp);
        } else {
            ntps_by_shard[*shard].push_back(ntp);
        }
    }

    res_type result = co_await _partition_manager.map_reduce0(
      [&partitions = ntps_by_shard](const partition_manager& pm) {
          auto iter = partitions.find(ss::this_shard_id());
          if (iter == partitions.end()) {
              return res_type{};
          }

          const auto& ntps_for_shard = iter->second;

          std::vector<model::ntp> missing_partitions_on_shard;
          uint64_t size_on_shard = 0;
          for (const auto& ntp : ntps_for_shard) {
              auto partition = pm.get(ntp);

              if (!partition) {
                  missing_partitions_on_shard.push_back(ntp);
              } else {
                  size_on_shard += partition->cloud_log_size().value_or(0);
              }
          }
          return res_type{
            .total_size = size_on_shard,
            .missing_partitions = std::move(missing_partitions_on_shard)};
      },
      res_type{.missing_partitions = std::move(missing_ntps)},
      [](res_type acc, res_type map_result) {
          acc.total_size += map_result.total_size;
          acc.missing_partitions.insert(
            acc.missing_partitions.end(),
            std::make_move_iterator(map_result.missing_partitions.begin()),
            std::make_move_iterator(map_result.missing_partitions.end()));

          return acc;
      });

    co_return cloud_storage_usage_reply{
      .total_size_bytes = result.total_size,
      .missing_partitions = std::move(result.missing_partitions)};
}

ss::future<partition_state_reply> service::get_partition_state(
  partition_state_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this, req]() {
        return do_get_partition_state(req);
    });
}

ss::future<controller_committed_offset_reply>
service::get_controller_committed_offset(
  controller_committed_offset_request, rpc::streaming_context&) {
    return ss::with_scheduling_group(get_scheduling_group(), [this]() {
        return ss::smp::submit_to(controller_stm_shard, [this]() {
            if (!_controller->is_raft0_leader()) {
                return ss::make_ready_future<controller_committed_offset_reply>(
                  controller_committed_offset_reply{
                    .result = errc::not_leader_controller});
            }
            return _controller->linearizable_barrier().then([](auto r) {
                if (r.has_error()) {
                    return controller_committed_offset_reply{
                      .last_committed = model::offset{},
                      .result = errc::not_leader_controller};
                }
                return controller_committed_offset_reply{
                  .last_committed = r.value(), .result = errc::success};
            });
        });
    });
}

ss::future<partition_state_reply>
service::do_get_partition_state(partition_state_request req) {
    const auto ntp = req.ntp;
    const auto shard = _api.local().shard_for(ntp);
    partition_state_reply reply{};
    if (!shard) {
        reply.error_code = errc::partition_not_exists;
        co_return reply;
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [req = std::move(req),
       reply = std::move(reply)](cluster::partition_manager& pm) mutable {
          auto partition = pm.get(req.ntp);
          if (!partition) {
              reply.error_code = errc::partition_not_exists;
              return ss::make_ready_future<partition_state_reply>(reply);
          }
          reply.state = ::cluster::get_partition_state(partition);
          reply.error_code = errc::success;
          return ss::make_ready_future<partition_state_reply>(reply);
      });
}

ss::future<upsert_plugin_response>
service::upsert_plugin(upsert_plugin_request req, rpc::streaming_context&) {
    // Capture the request values in this coroutine
    auto transform = std::move(req.transform);
    auto deadline = model::timeout_clock::now() + req.timeout;
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto ec = co_await _plugin_frontend.local().upsert_transform(
      std::move(transform), deadline);
    co_return upsert_plugin_response{.ec = ec};
}

ss::future<remove_plugin_response>
service::remove_plugin(remove_plugin_request req, rpc::streaming_context&) {
    // Capture the request values in this coroutine
    auto name = std::move(req.name);
    auto deadline = model::timeout_clock::now() + req.timeout;
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto result = co_await _plugin_frontend.local().remove_transform(
      name, deadline);
    co_return remove_plugin_response{.uuid = result.uuid, .ec = result.ec};
}

ss::future<delete_topics_reply>
service::delete_topics(delete_topics_request req, rpc::streaming_context&) {
    // Capture the request values in this coroutine
    auto topics = req.topics_to_delete;
    auto timeout = req.timeout;
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto result = co_await _topics_frontend.local().delete_topics(
      std::move(topics), model::timeout_clock::now() + timeout);

    co_return delete_topics_reply{.results = std::move(result)};
}

ss::future<set_partition_shard_reply> service::set_partition_shard(
  set_partition_shard_request req, rpc::streaming_context&) {
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto ec = co_await _topics_frontend.local().set_local_partition_shard(
      req.ntp, req.shard);
    co_return set_partition_shard_reply{.ec = ec};
}

ss::future<client_quota::alter_quotas_response> service::alter_client_quotas(
  client_quota::alter_quotas_request req, rpc::streaming_context&) {
    co_await ss::coroutine::switch_to(get_scheduling_group());
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto ec = co_await _quotas_frontend.local().alter_quotas(
      std::move(req.cmd_data), deadline);
    co_return client_quota::alter_quotas_response{.ec = ec};
}

ss::future<upsert_cluster_link_response> service::upsert_cluster_link(
  upsert_cluster_link_request req, rpc::streaming_context&) {
    auto meta = std::move(req.metadata);
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result = co_await _cluster_link_frontend.local().upsert_cluster_link(
      std::move(meta), deadline);
    co_return upsert_cluster_link_response{.ec = result};
}

ss::future<remove_cluster_link_response> service::remove_cluster_link(
  remove_cluster_link_request req, rpc::streaming_context&) {
    auto name = std::move(req.cmd.link_name);
    auto force = req.cmd.force;
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result = co_await _cluster_link_frontend.local().remove_cluster_link(
      std::move(name), force, deadline);
    co_return remove_cluster_link_response{.ec = result};
}

ss::future<add_mirror_topic_response> service::add_mirror_topic(
  add_mirror_topic_request req, rpc::streaming_context&) {
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result = co_await _cluster_link_frontend.local().add_mirror_topic(
      req.link_id, std::move(req.cmd), deadline);
    co_return add_mirror_topic_response{.ec = result};
}

ss::future<update_mirror_topic_status_response>
service::update_mirror_topic_status(
  update_mirror_topic_status_request req, rpc::streaming_context&) {
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result
      = co_await _cluster_link_frontend.local().update_mirror_topic_status(
        req.link_id, std::move(req.cmd), deadline);
    co_return update_mirror_topic_status_response{.ec = result};
}

ss::future<update_mirror_topic_properties_response>
service::update_mirror_topic_properties(
  update_mirror_topic_properties_request req, rpc::streaming_context&) {
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result
      = co_await _cluster_link_frontend.local().update_mirror_topic_properties(
        req.link_id, std::move(req.cmd), deadline);
    co_return update_mirror_topic_properties_response{.ec = result};
}

ss::future<update_cluster_link_configuration_response>
service::update_cluster_link_configuration(
  update_cluster_link_configuration_request req, rpc::streaming_context&) {
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result = co_await _cluster_link_frontend.local()
                    .update_cluster_link_configuration(
                      req.link_id, std::move(req.cmd), deadline);
    co_return update_cluster_link_configuration_response{.ec = result};
}

ss::future<delete_mirror_topic_response> service::delete_mirror_topic(
  delete_mirror_topic_request req, rpc::streaming_context&) {
    auto deadline = model::timeout_clock::now() + req.timeout;
    auto result = co_await _cluster_link_frontend.local().delete_mirror_topic(
      req.link_id, std::move(req.cmd), deadline);
    co_return delete_mirror_topic_response{.ec = result};
}

ss::future<get_current_cluster_epoch_response>
service::get_current_cluster_epoch(
  get_current_cluster_epoch_request, ::rpc::streaming_context&) {
    auto result = co_await _controller->get_cluster_epoch_generator().invoke_on(
      controller_stm_shard, &cluster_epoch_service<>::get_current_epoch);
    co_return get_current_cluster_epoch_response{
      .ec = result ? errc::success : errc::not_leader_controller,
      .epoch = result.value_or(-1),
    };
}

} // namespace cluster
