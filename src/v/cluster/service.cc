// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/service.h"

#include "base/vlog.h"
#include "cluster/client_quota_frontend.h"
#include "cluster/client_quota_serde.h"
#include "cluster/config_frontend.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/metadata_cache.h"
#include "cluster/node_status_backend.h"
#include "cluster/partition_manager.h"
#include "cluster/plugin_frontend.h"
#include "cluster/security_frontend.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "rpc/errc.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/coroutine/switch_to.hh>

namespace cluster {
service::service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  controller* controller,
  ss::sharded<topics_frontend>& tf,
  ss::sharded<plugin_frontend>& pf,
  ss::sharded<members_manager>& mm,
  ss::sharded<metadata_cache>& cache,
  ss::sharded<security_frontend>& sf,
  ss::sharded<controller_api>& api,
  ss::sharded<members_frontend>& members_frontend,
  ss::sharded<config_frontend>& config_frontend,
  ss::sharded<config_manager>& config_manager,
  ss::sharded<feature_manager>& feature_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<health_monitor_frontend>& hm_frontend,
  ss::sharded<rpc::connection_cache>& conn_cache,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<node_status_backend>& node_status_backend,
  ss::sharded<client_quota::frontend>& quotas_frontend)
  : controller_service(sg, ssg)
  , _controller(controller)
  , _topics_frontend(tf)
  , _members_manager(mm)
  , _md_cache(cache)
  , _security_frontend(sf)
  , _api(api)
  , _members_frontend(members_frontend)
  , _config_frontend(config_frontend)
  , _config_manager(config_manager)
  , _feature_manager(feature_manager)
  , _feature_table(feature_table)
  , _hm_frontend(hm_frontend)
  , _conn_cache(conn_cache)
  , _partition_manager(partition_manager)
  , _plugin_frontend(pf)
  , _node_status_backend(node_status_backend)
  , _quotas_frontend(quotas_frontend) {}

ss::future<join_node_reply>
service::join_node(join_node_request req, rpc::streaming_context&) {
    cluster_version expect_version
      = _feature_table.local().get_active_version();
    if (expect_version == invalid_version) {
        // Feature table isn't initialized, fall back to requiring that
        // joining node is as recent as this node.
        expect_version = features::feature_table::get_latest_logical_version();
    }

    if (
      (req.earliest_logical_version != cluster::invalid_version
       && req.earliest_logical_version > expect_version)
      || (req.latest_logical_version != cluster::invalid_version && req.latest_logical_version < expect_version)) {
        // Our active version is outside the range of versions the
        // joining node is compatible with.
        bool permit_join = config::node().upgrade_override_checks();
        vlog(
          clusterlog.warn,
          "{}join request from incompatible node {}, our version {} vs "
          "their {}-{}",
          permit_join ? "" : "Rejecting ",
          req.node,
          expect_version,
          req.earliest_logical_version,
          req.latest_logical_version);
        if (!permit_join) {
            return ss::make_ready_future<join_node_reply>(join_node_reply{
              join_node_reply::status_code::incompatible, model::node_id{-1}});
        }
    }

    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req]() mutable {
          return _members_manager
            .invoke_on(
              members_manager::shard,
              get_smp_service_group(),
              [req](members_manager& mm) mutable {
                  return mm.handle_join_request(req);
              })
            .then([](result<join_node_reply> r) {
                if (!r) {
                    auto status = join_node_reply::status_code::error;
                    if (r.error() == errc::update_in_progress) {
                        status = join_node_reply::status_code::busy;
                    }

                    return join_node_reply{status, model::node_id{-1}};
                }
                return std::move(r.value());
            });
      });
}

ss::future<create_topics_reply>
service::create_topics(create_topics_request r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
             get_scheduling_group(),
             [this, r = std::move(r)]() mutable {
                 return _topics_frontend.local().create_topics(
                   without_custom_assignments(std::move(r.topics)),
                   model::timeout_clock::now() + r.timeout);
             })
      .then([this](std::vector<topic_result> res) {
          // Fetch metadata for successfully created topics
          auto [md, cfg] = fetch_metadata_and_cfg(res);
          return create_topics_reply{
            std::move(res), std::move(md), std::move(cfg)};
      });
}

ss::future<purged_topic_reply>
service::purged_topic(purged_topic_request r, rpc::streaming_context&) {
    return ss::with_scheduling_group(
             get_scheduling_group(),
             [this, r = std::move(r)]() mutable {
                 return _topics_frontend.local().do_purged_topic(
                   std::move(r.topic), model::timeout_clock::now() + r.timeout);
             })
      .then(
        [](topic_result res) { return purged_topic_reply(std::move(res)); });
}

std::pair<std::vector<model::topic_metadata>, topic_configuration_vector>
service::fetch_metadata_and_cfg(const std::vector<topic_result>& res) {
    std::vector<model::topic_metadata> md;
    topic_configuration_vector cfg;
    md.reserve(res.size());
    for (const auto& r : res) {
        if (r.ec == errc::success) {
            auto topic_md = _md_cache.local().get_model_topic_metadata(r.tp_ns);
            auto topic_cfg = _md_cache.local().get_topic_cfg(r.tp_ns);
            if (topic_md && topic_cfg) {
                md.push_back(std::move(topic_md.value()));
                cfg.push_back(std::move(topic_cfg.value()));
            }
        }
    }
    return {std::move(md), std::move(cfg)};
}

ss::future<configuration_update_reply> service::update_node_configuration(
  configuration_update_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return _members_manager
            .invoke_on(
              members_manager::shard,
              get_smp_service_group(),
              [req = std::move(req)](members_manager& mm) mutable {
                  return mm.handle_configuration_update_request(std::move(req));
              })
            .then([](result<configuration_update_reply> r) {
                if (!r) {
                    return configuration_update_reply{false};
                }
                return r.value();
            });
      });
}

ss::future<finish_partition_update_reply> service::finish_partition_update(
  finish_partition_update_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_finish_partition_update(std::move(req));
      });
}

ss::future<finish_partition_update_reply>
service::do_finish_partition_update(finish_partition_update_request req) {
    auto ec
      = co_await _topics_frontend.local().finish_moving_partition_replicas(
        req.ntp,
        req.new_replica_set,
        config::shard_local_cfg().replicate_append_timeout_ms()
          + model::timeout_clock::now(),
        topics_frontend::dispatch_to_leader::no);

    finish_partition_update_reply reply{.result = errc::success};
    if (ec) {
        if (ec.category() == cluster::error_category()) {
            reply.result = errc(ec.value());
        } else {
            reply.result = errc::not_leader;
        }
    }

    co_return reply;
}

ss::future<update_topic_properties_reply> service::update_topic_properties(
  update_topic_properties_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_topic_properties(std::move(req));
      });
}

ss::future<update_topic_properties_reply>
service::do_update_topic_properties(update_topic_properties_request req) {
    // local topic frontend instance will eventually dispatch request to _raft0
    // core
    auto res = co_await _topics_frontend.local().update_topic_properties(
      std::move(req).updates,
      config::shard_local_cfg().replicate_append_timeout_ms()
        + model::timeout_clock::now());

    co_return update_topic_properties_reply{.results = std::move(res)};
}

ss::future<create_acls_reply>
service::create_acls(create_acls_request request, rpc::streaming_context&) {
    return ss::with_scheduling_group(
             get_scheduling_group(),
             [this, r = std::move(request)]() mutable {
                 return _security_frontend.local().create_acls(
                   std::move(r.data.bindings), r.timeout);
             })
      .then([](std::vector<errc> results) {
          return create_acls_reply{.results = std::move(results)};
      });
}

ss::future<delete_acls_reply>
service::delete_acls(delete_acls_request request, rpc::streaming_context&) {
    return ss::with_scheduling_group(
             get_scheduling_group(),
             [this, r = std::move(request)]() mutable {
                 return _security_frontend.local().delete_acls(
                   std::move(r.data.filters), r.timeout);
             })
      .then([](std::vector<delete_acls_result> results) {
          return delete_acls_reply{.results = std::move(results)};
      });
}

ss::future<reconciliation_state_reply> service::get_reconciliation_state(
  reconciliation_state_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_get_reconciliation_state(std::move(req));
      });
}

ss::future<reconciliation_state_reply>
service::do_get_reconciliation_state(reconciliation_state_request req) {
    auto result = co_await _api.local().get_reconciliation_state(req.ntps);

    co_return reconciliation_state_reply{.results = std::move(result)};
}

ss::future<decommission_node_reply> service::decommission_node(
  decommission_node_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req]() mutable {
          return _members_frontend.local().decommission_node(req.id).then(
            [](std::error_code ec) {
                if (!ec) {
                    return decommission_node_reply{.error = errc::success};
                }
                if (ec.category() == cluster::error_category()) {
                    return decommission_node_reply{.error = errc(ec.value())};
                }
                return decommission_node_reply{
                  .error = errc::replication_error};
            });
      });
}

ss::future<recommission_node_reply> service::recommission_node(
  recommission_node_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req]() mutable {
          return _members_frontend.local().recommission_node(req.id).then(
            [](std::error_code ec) {
                if (!ec) {
                    return recommission_node_reply{.error = errc::success};
                }
                if (ec.category() == cluster::error_category()) {
                    return recommission_node_reply{.error = errc(ec.value())};
                }
                return recommission_node_reply{
                  .error = errc::replication_error};
            });
      });
}

ss::future<finish_reallocation_reply> service::finish_reallocation(
  finish_reallocation_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(),
      [this, req]() mutable { return do_finish_reallocation(req); });
}

ss::future<revert_cancel_partition_move_reply>
service::revert_cancel_partition_move(
  revert_cancel_partition_move_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(),
      [this, req]() mutable { return do_revert_cancel_partition_move(req); });
}

ss::future<set_maintenance_mode_reply> service::set_maintenance_mode(
  set_maintenance_mode_request req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req]() mutable {
          return _members_frontend.local()
            .set_maintenance_mode(req.id, req.enabled)
            .then([](std::error_code ec) {
                if (!ec) {
                    return set_maintenance_mode_reply{.error = errc::success};
                }
                if (ec.category() == cluster::error_category()) {
                    return set_maintenance_mode_reply{
                      .error = errc(ec.value())};
                }
                return set_maintenance_mode_reply{
                  .error = errc::replication_error};
            });
      });
}

/*
 * A hello message from a peer means that the peer just booted up and setup a
 * connection to this node. An important optimization is to resume sending
 * heartbeats to the peer, which might not be active because the connection is
 * in a back-off state. Resuming heartbeats is important because raft groups on
 * the peer are likely idle waiting for signs of life without which they will
 * start calling for votes which may cause disruption.
 */
ss::future<hello_reply>
service::hello(hello_request req, rpc::streaming_context&) {
    vlog(
      clusterlog.debug,
      "Handling hello request from node {} with start time {}",
      req.peer,
      req.start_time.count());
    co_await _conn_cache.invoke_on_all(
      [peer = req.peer](rpc::connection_cache& cache) {
          if (cache.contains(peer)) {
              vlog(
                clusterlog.debug,
                "Resetting backoff for node {} on current shard",
                peer);
              cache.get(peer)->reset_backoff();
          }
      });
    co_await _node_status_backend.invoke_on(
      0, [peer = req.peer](node_status_backend& backend) {
          backend.reset_node_backoff(peer);
      });
    co_return hello_reply{.error = errc::success};
}

ss::future<config_status_reply>
service::config_status(config_status_request req, rpc::streaming_context&) {
    // Move to stack to avoid referring to reference argument after a
    // scheduling point
    auto status = std::move(req.status);

    // Peer should not be sending us status messages unless their status
    // differs from the content of the controller log, but they might
    // be out of date and send us multiple messages: avoid writing to
    // our controller log by pre-checking if their request is actually a
    // change.
    auto needs_update = co_await _config_manager.invoke_on(
      config_manager::shard,
      [status](config_manager& mgr) { return mgr.needs_update(status); });
    if (!needs_update) {
        vlog(clusterlog.debug, "Ignoring status update {}, is a no-op", status);
        co_return config_status_reply{.error = errc::success};
    }

    auto ec = co_await _config_frontend.local().set_status(
      status,
      config::shard_local_cfg().replicate_append_timeout_ms()
        + model::timeout_clock::now());

    if (ec.category() == error_category()) {
        co_return config_status_reply{.error = errc(ec.value())};
    } else {
        co_return config_status_reply{.error = errc::replication_error};
    }
}

ss::future<config_update_reply>
service::config_update(config_update_request req, rpc::streaming_context&) {
    auto patch_result = co_await _config_frontend.local().patch(
      std::move(req),
      config::shard_local_cfg().replicate_append_timeout_ms()
        + model::timeout_clock::now());

    if (patch_result.errc.category() == error_category()) {
        co_return config_update_reply{
          .error = errc(patch_result.errc.value()),
          .latest_version = patch_result.version};
    } else {
        co_return config_update_reply{
          .error = errc::replication_error,
          .latest_version = patch_result.version};
    }
}

ss::future<finish_reallocation_reply>
service::do_finish_reallocation(finish_reallocation_request req) {
    auto ec = co_await _members_frontend.local().finish_node_reallocations(
      req.id);
    if (ec) {
        if (ec.category() == error_category()) {
            co_return finish_reallocation_reply{.error = errc(ec.value())};
        } else {
            co_return finish_reallocation_reply{
              .error = errc::replication_error};
        }
    }

    co_return finish_reallocation_reply{.error = errc::success};
}

ss::future<revert_cancel_partition_move_reply>
service::do_revert_cancel_partition_move(
  revert_cancel_partition_move_request req) {
    auto ec = co_await _topics_frontend.local().revert_cancel_partition_move(
      std::move(req.ntp),
      config::shard_local_cfg().replicate_append_timeout_ms()
        + model::timeout_clock::now());
    if (ec) {
        if (ec.category() == error_category()) {
            co_return revert_cancel_partition_move_reply{
              .result = errc(ec.value())};
        } else {
            co_return revert_cancel_partition_move_reply{
              .result = errc::replication_error};
        }
    }

    co_return revert_cancel_partition_move_reply{.result = errc::success};
}

cluster::errc map_health_monitor_error_code(std::error_code e) {
    if (e.category() == cluster::error_category()) {
        return cluster::errc(e.value());
    } else if (e.category() == rpc::error_category()) {
        switch (rpc::errc(e.value())) {
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        default:
            return cluster::errc::error_collecting_health_report;
        }
    }

    return cluster::errc::error_collecting_health_report;
}

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
            partition_manager& pm) -> ss::future<std::error_code> {
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

} // namespace cluster
