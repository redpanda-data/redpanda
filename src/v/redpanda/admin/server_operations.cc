/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/vlog.h"
#include "cloud_io/cache_service.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/spillover_manifest.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_balancer_backend.h"
#include "cluster/partition_balancer_rpc_service.h"
#include "cluster/partition_manager.h"
#include "cluster/self_test_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topic_recovery_service.h"
#include "cluster/topic_recovery_status_frontend.h"
#include "cluster/topic_recovery_status_rpc_handler.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "container/lw_shared_container.h"
#include "finjector/hbadger.h"
#include "finjector/stress_fiber.h"
#include "json/document.h"
#include "json/validator.h"
#include "json/writer.h"
#include "kafka/data/partition_proxy.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/schema_registry/api.h"
#include "redpanda/admin/api-doc/broker.json.hh"
#include "redpanda/admin/api-doc/cluster.json.hh"
#include "redpanda/admin/api-doc/debug.json.hh"
#include "redpanda/admin/api-doc/hbadger.json.hh"
#include "redpanda/admin/api-doc/partition.json.hh"
#include "redpanda/admin/api-doc/shadow_indexing.json.hh"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"
#include "rpc/rpc_utils.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "strings/string_switch.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/json_path.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/short_streams.hh>

#include <boost/lexical_cast.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <ranges>
#include <vector>

using namespace std::chrono_literals;

using admin::apply_validator;
using admin::get_boolean_query_param;

namespace {
ss::httpd::broker_json::maintenance_status fill_maintenance_status(
  const cluster::broker_state& b_state,
  const cluster::drain_manager::drain_status& s) {
    ss::httpd::broker_json::maintenance_status ret;
    ret.draining = b_state.get_maintenance_state()
                   == model::maintenance_state::active;

    ret.finished = s.finished;
    ret.errors = s.errors;
    ret.partitions = s.partitions.value_or(0);
    ret.transferring = s.transferring.value_or(0);
    ret.eligible = s.eligible.value_or(0);
    ret.failed = s.failed.value_or(0);

    return ret;
}
ss::httpd::broker_json::maintenance_status
fill_maintenance_status(const cluster::broker_state& b_state) {
    ss::httpd::broker_json::maintenance_status ret;

    ret.draining = b_state.get_maintenance_state()
                   == model::maintenance_state::active;

    return ret;
}

ss::httpd::broker_json::broker get_broker_info(
  model::node_id node_id,
  const cluster::node_metadata& node_metadata,
  const cluster::health_monitor_frontend& health_monitor,
  const cluster::members_table& members_table,
  const cluster::cluster_health_report& health_report) {
    ss::httpd::broker_json::broker b;

    b.node_id = node_id;
    b.num_cores = node_metadata.broker.properties().cores;
    if (node_metadata.broker.rack()) {
        b.rack = *node_metadata.broker.rack();
    }
    b.membership_status = fmt::format(
      "{}", node_metadata.state.get_membership_state());
    b.is_alive = health_monitor.is_alive(node_id) == cluster::alive::yes;
    b.maintenance_status = fill_maintenance_status(node_metadata.state);
    b.internal_rpc_address = node_metadata.broker.rpc_address().host();
    b.internal_rpc_port = node_metadata.broker.rpc_address().port();
    b.in_fips_mode = fmt::format(
      "{}", node_metadata.broker.properties().in_fips_mode);

    auto node_report_it = std::ranges::find_if(
      health_report.node_reports.begin(),
      health_report.node_reports.end(),
      [node_id](const auto& report) { return report->id == node_id; });

    if (node_report_it != health_report.node_reports.end()) {
        const auto& node_report = *node_report_it;
        b.version = node_report->local_state.redpanda_version;
        b.recovery_mode_enabled
          = node_report->local_state.recovery_mode_enabled;

        auto nm = members_table.get_node_metadata_ref(node_id);
        if (nm && node_report->drain_status) {
            b.maintenance_status = fill_maintenance_status(
              nm.value().get().state, node_report->drain_status.value());
        }

        auto add_disk = [&ds_list = b.disk_space](const storage::disk& ds) {
            ss::httpd::broker_json::disk_space_info dsi;
            dsi.path = ds.path;
            dsi.free = ds.free;
            dsi.total = ds.total;
            ds_list.push(dsi);
        };
        add_disk(node_report->local_state.data_disk);
        if (!node_report->local_state.shared_disk()) {
            add_disk(node_report->local_state.get_cache_disk());
        }
    }

    return b;
}

ss::future<std::vector<ss::httpd::broker_json::broker>>
get_brokers(cluster::controller* const controller) {
    cluster::node_report_filter filter;
    filter.include_partitions = cluster::include_partitions_info::no;

    return controller->get_health_monitor()
      .local()
      .get_cluster_health(
        cluster::cluster_report_filter{
          .node_report_filter = std::move(filter),
        },
        cluster::force_refresh::no,
        model::no_timeout)
      .then([controller](result<cluster::cluster_health_report> h_report) {
          if (h_report.has_error()) {
              throw ss::httpd::base_exception(
                fmt::format(
                  "Unable to get cluster health: {}",
                  h_report.error().message()),
                ss::http::reply::status_type::service_unavailable);
          }

          std::vector<ss::httpd::broker_json::broker> brokers;
          auto& members_table = controller->get_members_table().local();
          auto& health_monitor = controller->get_health_monitor().local();

          brokers.reserve(members_table.nodes().size());

          for (auto& [id, nm] : members_table.nodes()) {
              brokers.push_back(get_broker_info(
                id, nm, health_monitor, members_table, h_report.value()));
          }

          return ssx::now(std::move(brokers));
      });
};
} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_broker_handler(std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);
    auto node_meta = _metadata_cache.local().get_node_metadata(id);
    if (!node_meta) {
        throw ss::httpd::not_found_exception(
          fmt::format("broker with id: {} not found", id));
    }

    cluster::node_report_filter filter;
    filter.include_partitions = cluster::include_partitions_info::no;

    auto h_report
      = co_await _controller->get_health_monitor().local().get_cluster_health(
        cluster::cluster_report_filter{
          .node_report_filter = std::move(filter),
        },
        cluster::force_refresh::no,
        model::time_from_now(5s));

    if (h_report.has_error()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Unable to get cluster health: {}", h_report.error().message()),
          ss::http::reply::status_type::internal_server_error);
    }

    const auto& members_table = _controller->get_members_table().local();
    const auto& health_monitor = _controller->get_health_monitor().local();

    co_return get_broker_info(
      id, *node_meta, health_monitor, members_table, h_report.value());
}

ss::future<ss::json::json_return_type>
admin_server::get_broker_uuids_handler() {
    auto mappings = co_await _controller->get_members_manager().invoke_on(
      cluster::controller_stm_shard, [](cluster::members_manager& mm) {
          std::vector<ss::httpd::broker_json::broker_uuid_mapping> ret;
          const auto& uuid_map = mm.get_id_by_uuid_map();
          ret.reserve(uuid_map.size());
          for (const auto& [uuid, id] : mm.get_id_by_uuid_map()) {
              ss::httpd::broker_json::broker_uuid_mapping mapping;
              mapping.node_id = id();
              mapping.uuid = ssx::sformat("{}", uuid);
              ret.push_back(mapping);
          }
          return ret;
      });
    co_return ss::json::json_return_type(mappings);
}

ss::future<ss::json::json_return_type> admin_server::decomission_broker_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);

    auto ec
      = co_await _controller->get_members_frontend().local().decommission_node(
        id);

    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::get_decommission_progress_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);
    auto res
      = co_await _controller->get_api().local().get_node_decommission_progress(
        id, 5s + model::timeout_clock::now());
    if (!res) {
        if (res.error() == cluster::errc::node_does_not_exists) {
            throw ss::httpd::base_exception(
              fmt::format("Node {} does not exists", id),
              ss::http::reply::status_type::not_found);
        } else if (res.error() == cluster::errc::invalid_node_operation) {
            throw ss::httpd::base_exception(
              fmt::format("Node {} is not decommissioning", id),
              ss::http::reply::status_type::bad_request);
        }

        throw ss::httpd::base_exception(
          fmt::format(
            "Unable to get decommission status for {} - {}",
            id,
            res.error().message()),
          ss::http::reply::status_type::internal_server_error);
    }
    ss::httpd::broker_json::decommission_status ret;
    auto& decommission_progress = res.value();

    ret.replicas_left = decommission_progress.replicas_left;
    ret.finished = decommission_progress.finished;
    ret.reallocation_failure_details._elements.reserve(
      decommission_progress.allocation_failures.size());
    for (const auto& [ntp, details] :
         decommission_progress.allocation_failures) {
        ret.allocation_failures.push(
          fmt::format("{}/{}/{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition));
        using failure_details_t
          = ss::httpd::broker_json::reallocation_failure_details;
        failure_details_t f_details;
        f_details.ns = ntp.ns;
        f_details.topic = ntp.tp.topic;
        f_details.partition = ntp.tp.partition;
        f_details.error = fmt::to_string(details.error);

        ret.reallocation_failure_details.push(f_details);
    }

    for (auto& p : decommission_progress.current_reconfigurations) {
        ss::httpd::broker_json::partition_reconfiguration_status status;
        status.ns = p.ntp.ns;
        status.topic = p.ntp.tp.topic;
        status.partition = p.ntp.tp.partition;
        auto added_replicas = cluster::subtract(
          p.current_assignment, p.previous_assignment);
        // we are only interested in reconfigurations where one replica was
        // added to the node
        if (added_replicas.size() != 1) {
            continue;
        }
        ss::httpd::broker_json::broker_shard moving_to{};
        moving_to.node_id = added_replicas.front().node_id();
        moving_to.core = added_replicas.front().shard;
        status.moving_to = moving_to;
        size_t left_to_move = 0;
        size_t already_moved = 0;
        for (auto replica_status : p.replicas) {
            left_to_move += replica_status.bytes_left;
            already_moved += replica_status.bytes_transferred;
        }
        status.bytes_left_to_move = left_to_move;
        status.bytes_moved = already_moved;
        status.partition_size = p.current_partition_size;
        // if no information from partitions is present yet, we may indicate
        // that everything have to be moved
        if (already_moved == 0 && left_to_move == 0) {
            status.bytes_left_to_move = p.current_partition_size;
        }
        status.reconfiguration_policy = ssx::sformat("{}", p.policy);
        ret.partitions.push(status);
    }

    co_return ret;
}

ss::future<ss::json::json_return_type> admin_server::recomission_broker_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);

    auto ec
      = co_await _controller->get_members_frontend().local().recommission_node(
        id);
    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::start_broker_maintenance_handler(
  std::unique_ptr<ss::http::request> req) {
    if (_controller->get_members_table().local().node_count() < 2) {
        throw ss::httpd::bad_request_exception(
          "Maintenance mode may not be used on a single node "
          "cluster");
    }

    model::node_id id = parse_broker_id(*req);
    auto ec = co_await _controller->get_members_frontend()
                .local()
                .set_maintenance_mode(id, true);
    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::stop_broker_maintenance_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);
    auto ec = co_await _controller->get_members_frontend()
                .local()
                .set_maintenance_mode(id, false);
    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::reset_crash_tracking(std::unique_ptr<ss::http::request>) {
    auto file = config::node().crash_loop_tracker_path().string();
    // we don't need to synchronize access to this file because it is only
    // touched in the very beginning of bootup or very late in shutdown when
    // everything is already cleaned up. This guarantees that there are no
    // concurrent modifications to this file while this API is running.
    co_await ss::remove_file(file);
    co_await ss::sync_directory(config::node().data_directory().as_sstring());
    vlog(adminlog.info, "Deleted crash loop tracker file: {}", file);
    co_return ss::json::json_void();
}

namespace {
void format_affected_partitions(
  const cluster::restart_risk_report::partitions_t& src,
  ss::json::json_list<ss::sstring>& dest) {
    dest = src | std::views::transform([](const model::ntp& ntp) {
               return fmt::format(
                 "{}/{}/{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition());
           });
    dest._set = true; // even if empty
}

std::optional<uint64_t>
get_integer_query_param(const ss::http::request& req, std::string_view key) {
    if (!req.has_query_param(key)) {
        return std::nullopt;
    }

    const ss::sstring& str_param = req.get_query_param(key);
    try {
        return std::stoull(str_param);
    } catch (const std::invalid_argument&) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Parameter {} must be an integer", key));
    }
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::pre_restart_probe(std::unique_ptr<ss::http::request> req) {
    vlog(adminlog.debug, "Requested broker pre-restart probe");
    auto limit = get_integer_query_param(*req, "limit");
    auto maybe_res = co_await _controller->get_health_monitor()
                       .local()
                       .get_current_node_restart_risks(
                         limit.value_or(128),
                         model::time_from_now(std::chrono::seconds(5)));

    if (!maybe_res.has_value()) {
        co_await throw_on_error(*req, maybe_res.error(), model::controller_ntp);
        vunreachable("the line above should have thrown");
    }
    const auto& res = maybe_res.value();
    ss::httpd::broker_json::restart_risks risks;
    format_affected_partitions(res.rf1_offline, risks.rf1_offline);
    format_affected_partitions(
      res.full_acks_produce_unavailable, risks.full_acks_produce_unavailable);
    format_affected_partitions(res.unavailable, risks.unavailable);
    format_affected_partitions(res.acks1_data_loss, risks.acks1_data_loss);

    ss::httpd::broker_json::pre_restart_check_result ret;
    ret.risks = risks;
    co_return ss::json::json_return_type(ret);
}

ss::future<ss::json::json_return_type>
admin_server::post_restart_probe(std::unique_ptr<ss::http::request> req) {
    vlog(adminlog.debug, "Requested broker post-restart probe");
    auto maybe_res = co_await _controller->get_health_monitor()
                       .local()
                       .get_current_node_in_sync_replicas_share(
                         model::time_from_now(std::chrono::seconds(5)));
    if (!maybe_res.has_value()) {
        co_await throw_on_error(*req, maybe_res.error(), model::controller_ntp);
        vunreachable("the line above should have thrown");
    }
    ss::httpd::broker_json::post_restart_check_result ret;
    // placeholder, to be implemented
    ret.load_reclaimed_pc = 100 * maybe_res.value();
    co_return ss::json::json_return_type(ret);
}

void admin_server::register_broker_routes() {
    register_route<user>(
      ss::httpd::broker_json::get_cluster_view,
      [this](std::unique_ptr<ss::http::request>) {
          return get_brokers(_controller)
            .then([this](std::vector<ss::httpd::broker_json::broker> brokers) {
                auto& members_table = _controller->get_members_table().local();

                ss::httpd::broker_json::cluster_view ret;
                ret.version = members_table.version();
                ret.brokers = brokers;

                return ss::json::json_return_type(ret);
            });
      });

    register_route<user>(
      ss::httpd::broker_json::get_brokers,
      [this](std::unique_ptr<ss::http::request>) {
          return get_brokers(_controller)
            .then([](std::vector<ss::httpd::broker_json::broker> brokers) {
                return ss::json::json_return_type(brokers);
            });
      });
    register_route<user>(
      ss::httpd::broker_json::get_broker_uuids,
      [this](std::unique_ptr<ss::http::request>) {
          return get_broker_uuids_handler();
      });

    register_route<user>(
      ss::httpd::broker_json::get_broker,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_broker_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::broker_json::get_decommission,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_decommission_progress_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::decommission,
      [this](std::unique_ptr<ss::http::request> req) {
          return decomission_broker_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::recommission,
      [this](std::unique_ptr<ss::http::request> req) {
          return recomission_broker_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::start_broker_maintenance,
      [this](std::unique_ptr<ss::http::request> req) {
          return start_broker_maintenance_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::stop_broker_maintenance,
      [this](std::unique_ptr<ss::http::request> req) {
          return stop_broker_maintenance_handler(std::move(req));
      });

    /*
     * Unlike start|stop_broker_maintenace, the xxx_local_maintenance
     * versions below operate on local state only and could be used to force
     * a node out of maintenance mode if needed. they don't require the
     * feature flag because the feature is available locally.
     */
    register_route<superuser>(
      ss::httpd::broker_json::start_local_maintenance,
      [this](std::unique_ptr<ss::http::request>) {
          return _controller->get_drain_manager().local().drain().then(
            [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<superuser>(
      ss::httpd::broker_json::stop_local_maintenance,
      [this](std::unique_ptr<ss::http::request>) {
          return _controller->get_drain_manager().local().restore().then(
            [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<superuser>(
      ss::httpd::broker_json::get_local_maintenance,
      [this](std::unique_ptr<ss::http::request>) {
          return _controller->get_drain_manager().local().status().then(
            [](auto status) {
                ss::httpd::broker_json::maintenance_status res;
                res.draining = status.has_value();
                if (status.has_value()) {
                    res.finished = status->finished;
                    res.errors = status->errors;
                    if (status->partitions.has_value()) {
                        res.partitions = status->partitions.value();
                    }
                    if (status->eligible.has_value()) {
                        res.eligible = status->eligible.value();
                    }
                    if (status->transferring.has_value()) {
                        res.transferring = status->transferring.value();
                    }
                    if (status->failed.has_value()) {
                        res.failed = status->failed.value();
                    }
                }
                return ss::json::json_return_type(res);
            });
      });
    register_route<superuser>(
      ss::httpd::broker_json::cancel_partition_moves,
      [this](std::unique_ptr<ss::http::request> req) {
          return cancel_node_partition_moves(
            *req, cluster::partition_move_direction::all);
      });
    register_route<superuser>(
      ss::httpd::broker_json::reset_crash_tracking,
      [this](std::unique_ptr<ss::http::request> req) {
          return reset_crash_tracking(std::move(req));
      });
    register_route<publik>(
      ss::httpd::broker_json::pre_restart_probe,
      [this](std::unique_ptr<ss::http::request> req) {
          return pre_restart_probe(std::move(req));
      });
    register_route<publik>(
      ss::httpd::broker_json::post_restart_probe,
      [this](std::unique_ptr<ss::http::request> req) {
          return post_restart_probe(std::move(req));
      });
}

void admin_server::register_hbadger_routes() {
    /**
     * we always register `v1/failure-probes` route. It will ALWAYS return
     * empty list of probes in production mode, and flag indicating that
     * honey badger is disabled
     */

    if constexpr (!finjector::honey_badger::is_enabled()) {
        register_route<user>(
          ss::httpd::hbadger_json::get_failure_probes,
          [](std::unique_ptr<ss::http::request>) {
              ss::httpd::hbadger_json::failure_injector_status status;
              status.enabled = false;
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(status));
          });
        return;
    }

    register_route<user>(
      ss::httpd::hbadger_json::get_failure_probes,
      [](std::unique_ptr<ss::http::request>) {
          auto modules = finjector::shard_local_badger().modules();
          ss::httpd::hbadger_json::failure_injector_status status;
          status.enabled = true;

          for (auto& m : modules) {
              ss::httpd::hbadger_json::failure_probes pr;
              pr.module = m.first.data();
              for (auto& p : m.second) {
                  pr.points.push(p.data());
              }
              status.probes.push(pr);
          }

          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(status));
      });
    /*
     * Enable failure injector
     */
    static constexpr std::string_view delay_type = "delay";
    static constexpr std::string_view exception_type = "exception";
    static constexpr std::string_view terminate_type = "terminate";

    register_route<superuser>(
      ss::httpd::hbadger_json::set_failure_probe,
      [](std::unique_ptr<ss::http::request> req) {
          auto m = req->get_path_param("module");
          auto p = req->get_path_param("point");
          auto type = req->get_path_param("type");
          vlog(
            adminlog.info,
            "Request to set failure probe of type '{}' in  '{}' at point "
            "'{}'",
            type,
            m,
            p);
          auto f = ss::now();

          if (type == delay_type) {
              f = ss::smp::invoke_on_all(
                [m, p] { finjector::shard_local_badger().set_delay(m, p); });
          } else if (type == exception_type) {
              f = ss::smp::invoke_on_all([m, p] {
                  finjector::shard_local_badger().set_exception(m, p);
              });
          } else if (type == terminate_type) {
              f = ss::smp::invoke_on_all([m, p] {
                  finjector::shard_local_badger().set_termination(m, p);
              });
          } else {
              throw ss::httpd::bad_param_exception(
                fmt::format(
                  "Type parameter has to be one of "
                  "['{}','{}','{}']",
                  delay_type,
                  exception_type,
                  terminate_type));
          }

          return f.then(
            [] { return ss::json::json_return_type(ss::json::json_void()); });
      });
    /*
     * Remove all failure injectors at given point
     */
    register_route<superuser>(
      ss::httpd::hbadger_json::delete_failure_probe,
      [](std::unique_ptr<ss::http::request> req) {
          auto m = req->get_path_param("module");
          auto p = req->get_path_param("point");
          vlog(
            adminlog.info,
            "Request to unset failure probe '{}' at point '{}'",
            m,
            p);
          return ss::smp::invoke_on_all(
                   [m, p] { finjector::shard_local_badger().unset(m, p); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });
}

namespace {
json::validator make_self_test_start_validator() {
    const std::string_view schema = R"(
{
    "type": "object",
    "properties": {
        "nodes": {
            "type": "array",
            "items": {
                "type": "number"
            }
        },
        "tests": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string"
                    }
                },
                "required": ["type"]
            }
        }
    },
    "required": [],
    "additionalProperties": false
}
)";
    return json::validator(schema);
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::self_test_start_handler(std::unique_ptr<ss::http::request> req) {
    static thread_local json::validator self_test_start_validator(
      make_self_test_start_validator());
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        vlog(adminlog.debug, "Need to redirect self_test_start request");
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }
    auto doc = co_await parse_json_body(req.get());
    apply_validator(self_test_start_validator, doc);
    std::vector<model::node_id> ids;
    cluster::start_test_request r;
    if (!doc.IsNull()) {
        if (doc.HasMember("nodes")) {
            const auto& node_ids = doc["nodes"].GetArray();
            for (const auto& element : node_ids) {
                ids.emplace_back(element.GetInt());
            }
        } else {
            /// If not provided, default is to start the test on all nodes
            ids = _controller->get_members_table().local().node_ids();
        }
        if (doc.HasMember("tests")) {
            const auto& params = doc["tests"].GetArray();
            for (const auto& element : params) {
                const auto& obj = element.GetObject();
                const ss::sstring test_type(obj["type"].GetString());
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                element.Accept(writer);
                r.unparsed_checks.push_back(
                  cluster::unparsed_check{
                    .test_type = test_type,
                    .test_json = ss::sstring{
                      buffer.GetString(), buffer.GetSize()}});
            }
            cluster::parse_self_test_checks(r);
        } else {
            /// Default test run is to start 1 disk and 1 network test with
            /// default arguments
            r.dtos.push_back(cluster::diskcheck_opts{});
            r.ntos.push_back(cluster::netcheck_opts{});
            r.ctos.push_back(cluster::cloudcheck_opts{});
        }
    }
    try {
        auto tid = co_await _self_test_frontend.invoke_on(
          cluster::self_test_frontend::shard,
          [r, ids](auto& self_test_frontend) {
              return self_test_frontend.start_test(r, ids);
          });
        vlog(adminlog.info, "Request to start self test succeeded: {}", tid);
        co_return ss::json::json_return_type(tid);
    } catch (const std::exception& ex) {
        throw ss::httpd::base_exception(
          fmt::format("Failed to start self test, reason: {}", ex),
          ss::http::reply::status_type::service_unavailable);
    }
}

ss::future<ss::json::json_return_type>
admin_server::self_test_stop_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        vlog(adminlog.info, "Need to redirect self_test_stop request");
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }
    auto r = co_await _self_test_frontend.invoke_on(
      cluster::self_test_frontend::shard,
      [](auto& self_test_frontend) { return self_test_frontend.stop_test(); });
    if (!r.finished()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Failed to stop one or more self_test jobs: {}",
            r.active_participant_ids()),
          ss::http::reply::status_type::service_unavailable);
    }
    vlog(adminlog.info, "Request to stop self test succeeded");
    co_return ss::json::json_void();
}

namespace {
ss::httpd::debug_json::self_test_result
self_test_result_to_json(const cluster::self_test_result& str) {
    ss::httpd::debug_json::self_test_result r;
    r.test_id = ss::sstring(str.test_id);
    r.name = str.name;
    r.info = str.info;
    r.test_type = str.test_type;
    r.start_time = str.start_time;
    r.end_time = str.end_time;
    r.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                   str.duration)
                   .count();
    r.timeouts = str.timeouts;
    if (str.warning) {
        r.warning = *str.warning;
    }
    if (str.error) {
        r.error = *str.error;
        return r;
    }
    r.p50 = str.p50;
    r.p90 = str.p90;
    r.p99 = str.p99;
    r.p999 = str.p999;
    r.max_latency = str.max;
    r.rps = str.rps;
    r.bps = str.bps;
    return r;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::self_test_get_results_handler(
  std::unique_ptr<ss::http::request>) {
    namespace dbg_ns = ss::httpd::debug_json;
    std::vector<dbg_ns::self_test_node_report> reports;
    auto status = co_await _self_test_frontend.invoke_on(
      cluster::self_test_frontend::shard,
      [](auto& self_test_frontend) { return self_test_frontend.status(); });
    reports.reserve(status.results().size());
    for (const auto& [id, participant] : status.results()) {
        dbg_ns::self_test_node_report nr;
        nr.node_id = id;
        nr.status = cluster::self_test_status_as_string(participant.status());
        nr.stage = cluster::self_test_stage_as_string(participant.stage());
        if (participant.response) {
            for (const auto& r : participant.response->results) {
                nr.results.push(self_test_result_to_json(r));
            }
        }
        reports.push_back(nr);
    }
    co_return ss::json::json_return_type(reports);
}

void admin_server::register_self_test_routes() {
    register_route<superuser>(
      ss::httpd::debug_json::self_test_start,
      [this](std::unique_ptr<ss::http::request> req) {
          return self_test_start_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::debug_json::self_test_stop,
      [this](std::unique_ptr<ss::http::request> req) {
          return self_test_stop_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::debug_json::self_test_status,
      [this](std::unique_ptr<ss::http::request> req) {
          return self_test_get_results_handler(std::move(req));
      });
}

namespace {
storage::node::disk_type resolve_disk_type(std::string_view name) {
    if (name == "data") {
        return storage::node::disk_type::data;
    } else if (name == "cache") {
        return storage::node::disk_type::cache;
    } else {
        throw ss::httpd::bad_param_exception(
          fmt::format("Unknown disk type: {}", name));
    }
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_disk_stat_handler(std::unique_ptr<ss::http::request> req) {
    auto type = resolve_disk_type(req->get_path_param("type"));

    // get effective disk stat
    auto stat = co_await _storage_node.invoke_on(
      0, [type](auto& node) { return node.get_statvfs(type); });

    ss::httpd::debug_json::disk_stat disk;
    disk.total_bytes = stat.stat.f_blocks * stat.stat.f_frsize;
    disk.free_bytes = stat.stat.f_bfree * stat.stat.f_frsize;

    co_return disk;
}

namespace {
json::validator make_disk_stat_overrides_validator() {
    const std::string_view schema = R"(
{
    "type": "object",
    "properties": {
        "total_bytes": {
            "type": "integer"
        },
        "free_bytes": {
            "type": "integer"
        },
        "free_bytes_delta": {
            "type": "integer"
        }
    },
    "additionalProperties": false
}
)";
    return json::validator(schema);
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::put_disk_stat_handler(std::unique_ptr<ss::http::request> req) {
    static thread_local auto disk_stat_validator(
      make_disk_stat_overrides_validator());

    auto doc = co_await parse_json_body(req.get());
    apply_validator(disk_stat_validator, doc);
    auto type = resolve_disk_type(req->get_path_param("type"));

    storage::node::statvfs_overrides overrides;
    if (doc.HasMember("total_bytes")) {
        overrides.total_bytes = doc["total_bytes"].GetUint64();
    }
    if (doc.HasMember("free_bytes")) {
        overrides.free_bytes = doc["free_bytes"].GetUint64();
    }
    if (doc.HasMember("free_bytes_delta")) {
        overrides.free_bytes_delta = doc["free_bytes_delta"].GetInt64();
    }

    co_await _storage_node.invoke_on(
      storage::node::work_shard, [type, overrides](auto& node) {
          node.set_statvfs_overrides(type, overrides);
          return ss::now();
      });

    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::get_partition_balancer_status_handler(
  std::unique_ptr<ss::http::request> req) {
    vlog(adminlog.debug, "Requested partition balancer status");

    using result_t = std::variant<
      cluster::partition_balancer_overview_reply,
      model::node_id,
      cluster::errc>;

    result_t result = co_await _controller->get_partition_balancer().invoke_on(
      cluster::partition_balancer_backend::shard,
      [](cluster::partition_balancer_backend& backend) {
          if (backend.is_leader()) {
              return result_t(backend.overview());
          } else {
              auto leader_id = backend.leader_id();
              if (leader_id) {
                  return result_t(leader_id.value());
              } else {
                  return result_t(cluster::errc::no_leader_controller);
              }
          }
      });

    cluster::partition_balancer_overview_reply overview;
    if (std::holds_alternative<cluster::partition_balancer_overview_reply>(
          result)) {
        overview = std::move(
          std::get<cluster::partition_balancer_overview_reply>(result));
    } else if (std::holds_alternative<model::node_id>(result)) {
        auto node_id = std::get<model::node_id>(result);
        vlog(
          adminlog.debug,
          "proxying the partition_balancer_overview call to node {}",
          node_id);
        auto rpc_result
          = co_await _connection_cache.local()
              .with_node_client<
                cluster::partition_balancer_rpc_client_protocol>(
                _controller->self(),
                ss::this_shard_id(),
                node_id,
                5s,
                [](cluster::partition_balancer_rpc_client_protocol cp) {
                    return cp.overview(
                      cluster::partition_balancer_overview_request{},
                      rpc::client_opts(5s));
                });

        if (rpc_result.has_error()) {
            co_await throw_on_error(
              *req, rpc_result.error(), model::controller_ntp);
        }

        overview = std::move(rpc_result.value().data);
    } else {
        co_await throw_on_error(
          *req, std::get<cluster::errc>(result), model::controller_ntp);
    }

    ss::httpd::cluster_json::partition_balancer_status ret;

    if (overview.error == cluster::errc::feature_disabled) {
        ret.status = "off";
        co_return ss::json::json_return_type(ret);
    } else if (overview.error != cluster::errc::success) {
        co_await throw_on_error(*req, overview.error, model::controller_ntp);
    }

    ret.status = fmt::format("{}", overview.status);

    if (overview.last_tick_time != model::timestamp::missing()) {
        ret.seconds_since_last_tick = (model::timestamp::now().value()
                                       - overview.last_tick_time.value())
                                      / 1000;
    }

    if (overview.violations) {
        ss::httpd::cluster_json::partition_balancer_violations ret_violations;
        for (const auto& n : overview.violations->unavailable_nodes) {
            ret_violations.unavailable_nodes.push(n.id);
        }
        for (const auto& n : overview.violations->full_nodes) {
            ret_violations.over_disk_limit_nodes.push(n.id);
        }
        ret.violations = ret_violations;
    }

    ret.current_reassignments_count
      = _controller->get_topics_state().local().updates_in_progress().size();

    ret.partitions_pending_force_recovery_count
      = overview.partitions_pending_force_recovery_count;
    for (const auto& ntp : overview.partitions_pending_force_recovery_sample) {
        ret.partitions_pending_force_recovery_sample.push(
          fmt::format(
            "{}/{}/{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition()));
    }

    co_return ss::json::json_return_type(ret);
}

namespace {
ss::future<std::vector<ss::httpd::partition_json::partition_result>>
map_partition_results(std::vector<cluster::move_cancellation_result> results) {
    std::vector<ss::httpd::partition_json::partition_result> ret;
    ret.reserve(results.size());

    for (cluster::move_cancellation_result& r : results) {
        ss::httpd::partition_json::partition_result result;
        result.ns = std::move(r.ntp.ns)();
        result.topic = std::move(r.ntp.tp.topic)();
        result.partition = r.ntp.tp.partition;
        result.result = cluster::make_error_code(r.result).message();
        ret.push_back(std::move(result));
        co_await ss::maybe_yield();
    }
    co_return ret;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::cancel_all_partitions_reconfigs_handler(
  std::unique_ptr<ss::http::request> req) {
    vlog(
      adminlog.info,
      "Requested cancellation of all ongoing partition movements");

    auto res = co_await _controller->get_topics_frontend()
                 .local()
                 .cancel_moving_all_partition_replicas(
                   model::timeout_clock::now() + 5s);
    if (res.has_error()) {
        co_await throw_on_error(*req, res.error(), model::controller_ntp);
    }

    co_return ss::json::json_return_type(
      co_await map_partition_results(std::move(res.value())));
}

ss::future<ss::json::json_return_type>
admin_server::get_metrics_uuid(std::unique_ptr<ss::http::request>) {
    vlog(adminlog.debug, "Requested metrics UUID");
    ss::httpd::cluster_json::metrics_uuid ret;
    ret.uuid = co_await _controller->get_controller_stm().invoke_on(
      0, ([](cluster::controller_stm& s) {
          return s.get_metrics_reporter_cluster_info().uuid;
      }));
    co_return ss::json::json_return_type(ret);
}

static json::validator make_post_cluster_partitions_validator() {
    const std::string_view schema = R"(
{
    "type": "object",
    "properties": {
        "disabled": {
            "type": "boolean"
        }
    },
    "additionalProperties": false,
    "required": ["disabled"]
}
)";
    return json::validator(schema);
}

ss::future<ss::json::json_return_type>
admin_server::post_cluster_partitions_topic_handler(
  std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto ns_tp = model::topic_namespace{
      model::ns{req->get_path_param("namespace")},
      model::topic{req->get_path_param("topic")}};

    static thread_local auto body_validator(
      make_post_cluster_partitions_validator());
    auto doc = co_await parse_json_body(req.get());
    apply_validator(body_validator, doc);
    bool disabled = doc["disabled"].GetBool();

    std::error_code err
      = co_await _controller->get_topics_frontend()
          .local()
          .set_topic_partitions_disabled(
            ns_tp, std::nullopt, disabled, model::timeout_clock::now() + 5s);
    if (err) {
        co_await throw_on_error(*req, err, model::controller_ntp);
    }

    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::post_cluster_partitions_topic_partition_handler(
  std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto ntp = parse_ntp_from_request(req->param);

    static thread_local auto body_validator(
      make_post_cluster_partitions_validator());
    auto doc = co_await parse_json_body(req.get());
    apply_validator(body_validator, doc);
    bool disabled = doc["disabled"].GetBool();

    std::error_code err = co_await _controller->get_topics_frontend()
                            .local()
                            .set_topic_partitions_disabled(
                              model::topic_namespace_view{ntp},
                              ntp.tp.partition,
                              disabled,
                              model::timeout_clock::now() + 5s);
    if (err) {
        co_await throw_on_error(*req, err, model::controller_ntp);
    }

    co_return ss::json::json_void();
}

namespace {

struct cluster_partition_info {
    ss::lw_shared_ptr<model::topic_namespace> ns_tp;
    model::partition_id id;
    std::vector<model::broker_shard> replicas;
    std::optional<model::node_id> leader_id;
    bool disabled = false;

    ss::httpd::cluster_json::cluster_partition to_json() const {
        ss::httpd::cluster_json::cluster_partition ret;
        ret.ns = ns_tp->ns();
        ret.topic = ns_tp->tp();
        ret.partition_id = id();
        for (auto& r : replicas) {
            ss::httpd::cluster_json::replica_assignment a;
            a.node_id = r.node_id;
            a.core = r.shard;
            ret.replicas.push(a);
        }
        if (leader_id) {
            ret.leader_id = leader_id.value();
        }
        ret.disabled = disabled;
        return ret;
    }
};

// Use contiguous_range_map for the ease of indexing when joining with the
// health report.
using cluster_partitions_t
  = contiguous_range_map<model::partition_id::type, cluster_partition_info>;

cluster_partitions_t topic2cluster_partitions(
  model::topic_namespace ns_tp,
  const cluster::assignments_set& assignments,
  const cluster::metadata_cache& md_cache,
  const cluster::topic_disabled_partitions_set* disabled_set,
  std::optional<bool> disabled_filter) {
    cluster_partitions_t ret;

    if (disabled_filter) {
        // fast exits
        if (
          disabled_filter.value()
          && (!disabled_set || disabled_set->is_fully_enabled())) {
            return ret;
        }

        if (
          !disabled_filter.value() && disabled_set
          && disabled_set->is_fully_disabled()) {
            return ret;
        }
    }

    auto shared_ns_tp = ss::make_lw_shared<model::topic_namespace>(
      std::move(ns_tp));

    if (
      disabled_filter && disabled_filter.value() && disabled_set
      && disabled_set->partitions) {
        // special handling for disabled=true filter, as we hope that iterating
        // over the disabled set is more optimal.
        for (const auto& id : *disabled_set->partitions) {
            auto as_it = assignments.find(id);
            vassert(
              as_it != assignments.end(),
              "topic: {}, partition {} must be present",
              *shared_ns_tp,
              id);

            ret.emplace(
              id,
              cluster_partition_info{
                .ns_tp = shared_ns_tp,
                .id = id,
                .replicas = as_it->second.replicas,
                .leader_id = md_cache.get_leader_id(*shared_ns_tp, id),
                .disabled = true,
              });
        }
    } else {
        for (const auto& [_, p_as] : assignments) {
            bool disabled = disabled_set && disabled_set->is_disabled(p_as.id);

            if (disabled_filter && *disabled_filter != disabled) {
                continue;
            }

            ret.emplace(
              p_as.id,
              cluster_partition_info{
                .ns_tp = shared_ns_tp,
                .id = p_as.id,
                .replicas = p_as.replicas,
                .leader_id = md_cache.get_leader_id(*shared_ns_tp, p_as.id),
                .disabled = disabled,
              });
        }
    }

    return ret;
}

void collect_shards_from_health_report(
  model::topic_namespace_view ns_tp,
  cluster_partitions_t& partitions,
  const cluster::cluster_health_report& hr) {
    for (const auto& node : hr.node_reports) {
        auto topic_it = node->topics.find(ns_tp);
        if (topic_it == node->topics.end()) {
            continue;
        }

        for (const auto& replica : topic_it->second) {
            auto partition_it = partitions.find(replica.first);
            if (partition_it == partitions.end()) {
                continue;
            }
            auto& part = partition_it->second;

            auto bs_it = std::find_if(
              part.replicas.begin(),
              part.replicas.end(),
              [node_id = node->id](const model::broker_shard& bs) {
                  return bs.node_id == node_id;
              });
            if (bs_it != part.replicas.end()) {
                bs_it->shard = replica.second.shard;
            }
        }
    }
}

} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_cluster_partitions_handler(
  std::unique_ptr<ss::http::request> req) {
    std::optional<bool> disabled_filter;
    if (req->has_query_param("disabled")) {
        disabled_filter = get_boolean_query_param(*req, "disabled");
    }

    bool with_internal = get_boolean_query_param(*req, "with_internal");

    const auto& topics_state = _controller->get_topics_state().local();

    chunked_vector<model::topic_namespace> topics;
    auto fill_topics = [&](const auto& map) {
        for (const auto& [ns_tp, _] : map) {
            if (!with_internal && !model::is_user_topic(ns_tp)) {
                continue;
            }
            topics.push_back(ns_tp);
        }
    };

    if (disabled_filter && *disabled_filter) {
        // optimization: if disabled filter is on, iterate only over disabled
        // topics;
        fill_topics(topics_state.get_disabled_partitions());
    } else {
        fill_topics(topics_state.topics_map());
    }

    std::sort(topics.begin(), topics.end());

    std::optional<cluster::cluster_health_report> health_report;
    if (_controller->get_topics_frontend()
          .local()
          .node_local_core_assignment_enabled()) {
        // We'll need to get core assignments from the health report
        auto hr_result = co_await _controller->get_health_monitor()
                           .local()
                           .get_cluster_health(
                             cluster::cluster_report_filter{},
                             cluster::force_refresh::no,
                             model::timeout_clock::now() + 5s);
        if (hr_result.has_error()) {
            throw ss::httpd::base_exception{
              ssx::sformat(
                "Error getting cluster health report: {}",
                hr_result.error().message()),
              ss::http::reply::status_type::internal_server_error};
        }

        health_report = std::move(hr_result.value());
    }

    ss::chunked_fifo<cluster_partition_info> partitions;
    for (const auto& ns_tp : topics) {
        auto topic_it = topics_state.topics_map().find(ns_tp);
        if (topic_it == topics_state.topics_map().end()) {
            // probably got deleted while we were iterating.
            continue;
        }

        auto topic_partitions = topic2cluster_partitions(
          ns_tp,
          topic_it->second.get_assignments(),
          _metadata_cache.local(),
          topics_state.get_topic_disabled_set(ns_tp),
          disabled_filter);

        if (health_report) {
            collect_shards_from_health_report(
              ns_tp, topic_partitions, health_report.value());
        }

        for (auto& [id, part] : topic_partitions) {
            partitions.push_back(std::move(part));
        }

        co_await ss::coroutine::maybe_yield();
    }

    co_return ss::json::json_return_type(
      ss::json::stream_range_as_array(
        lw_shared_container{std::move(partitions)},
        [](const auto& p) { return p.to_json(); }));
}

ss::future<ss::json::json_return_type>
admin_server::get_cluster_partitions_topic_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ns_tp = model::topic_namespace{
      model::ns{req->get_path_param("namespace")},
      model::topic{req->get_path_param("topic")}};

    std::optional<bool> disabled_filter;
    if (req->has_query_param("disabled")) {
        disabled_filter = get_boolean_query_param(*req, "disabled");
    }

    const auto& topics_state = _controller->get_topics_state().local();

    auto topic_it = topics_state.topics_map().find(ns_tp);
    if (topic_it == topics_state.topics_map().end()) {
        throw ss::httpd::not_found_exception(
          fmt::format("topic {} not found", ns_tp));
    }

    auto partitions = topic2cluster_partitions(
      ns_tp,
      topic_it->second.get_assignments(),
      _metadata_cache.local(),
      topics_state.get_topic_disabled_set(ns_tp),
      disabled_filter);

    if (_controller->get_topics_frontend()
          .local()
          .node_local_core_assignment_enabled()) {
        // We'll need to get core assignments from the health report
        auto hr_result = co_await _controller->get_health_monitor()
                           .local()
                           .get_cluster_health(
                             cluster::cluster_report_filter{},
                             cluster::force_refresh::no,
                             model::timeout_clock::now() + 5s);
        if (hr_result.has_error()) {
            throw ss::httpd::base_exception{
              ssx::sformat(
                "Error getting cluster health report: {}",
                hr_result.error().message()),
              ss::http::reply::status_type::internal_server_error};
        }
        collect_shards_from_health_report(ns_tp, partitions, hr_result.value());
    }

    co_return ss::json::json_return_type(
      ss::json::stream_range_as_array(
        lw_shared_container{std::move(partitions)},
        [](const auto& kv) { return kv.second.to_json(); }));
}

void admin_server::register_cluster_routes() {
    register_route<publik>(
      ss::httpd::cluster_json::get_cluster_health_overview,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(adminlog.debug, "Requested cluster status");
          return _controller->get_health_monitor()
            .local()
            .get_cluster_health_overview(
              model::time_from_now(std::chrono::seconds(5)))
            .then([](auto health_overview) {
                ss::httpd::cluster_json::cluster_health_overview ret;
                ret.is_healthy = health_overview.is_healthy();

                ret.unhealthy_reasons._set = true;
                ret.all_nodes._set = true;
                ret.nodes_down._set = true;
                ret.high_disk_usage_nodes._set = true;
                ret.leaderless_partitions._set = true;
                ret.under_replicated_partitions._set = true;

                ret.unhealthy_reasons = health_overview.unhealthy_reasons;
                ret.all_nodes = health_overview.all_nodes;
                ret.nodes_down = health_overview.nodes_down;
                ret.high_disk_usage_nodes
                  = health_overview.high_disk_usage_nodes;
                ret.nodes_in_recovery_mode
                  = health_overview.nodes_in_recovery_mode;

                ret.leaderless_count = health_overview.leaderless_count;
                ret.under_replicated_count
                  = health_overview.under_replicated_count;

                for (auto& ntp : health_overview.leaderless_partitions) {
                    ret.leaderless_partitions.push(
                      fmt::format(
                        "{}/{}/{}",
                        ntp.ns(),
                        ntp.tp.topic(),
                        ntp.tp.partition));
                }
                for (auto& ntp : health_overview.under_replicated_partitions) {
                    ret.under_replicated_partitions.push(
                      fmt::format(
                        "{}/{}/{}",
                        ntp.ns(),
                        ntp.tp.topic(),
                        ntp.tp.partition));
                }
                if (health_overview.controller_id) {
                    ret.controller_id = health_overview.controller_id.value();
                } else {
                    ret.controller_id = -1;
                }
                if (health_overview.bytes_in_cloud_storage) {
                    ret.bytes_in_cloud_storage
                      = health_overview.bytes_in_cloud_storage.value();
                } else {
                    ret.bytes_in_cloud_storage = -1;
                }

                return ss::json::json_return_type(ret);
            });
      });

    register_route<publik>(
      ss::httpd::cluster_json::get_partition_balancer_status,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_partition_balancer_status_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::cluster_json::cancel_all_partitions_reconfigurations,
      [this](std::unique_ptr<ss::http::request> req) {
          return cancel_all_partitions_reconfigs_handler(std::move(req));
      });

    register_route_sync<publik>(
      ss::httpd::cluster_json::get_cluster_uuid,
      [this](ss::httpd::const_req) -> ss::json::json_return_type {
          vlog(adminlog.debug, "Requested cluster UUID");
          const std::optional<model::cluster_uuid>& cluster_uuid
            = _controller->get_storage().local().get_cluster_uuid();
          if (cluster_uuid) {
              ss::httpd::cluster_json::uuid ret;
              ret.cluster_uuid = ssx::sformat("{}", cluster_uuid.value());
              return ss::json::json_return_type(ret);
          }
          return ss::json::json_return_type(ss::json::json_void());
      });

    register_route<publik>(
      ss::httpd::cluster_json::get_metrics_uuid,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_metrics_uuid(std::move(req));
      });

    register_cluster_partitions_routes();
}

void admin_server::register_cluster_partitions_routes() {
    register_route<superuser>(
      ss::httpd::cluster_json::post_cluster_partitions_topic,
      [this](std::unique_ptr<ss::http::request> req) {
          return post_cluster_partitions_topic_handler(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::cluster_json::post_cluster_partitions_topic_partition,
      [this](std::unique_ptr<ss::http::request> req) {
          return post_cluster_partitions_topic_partition_handler(
            std::move(req));
      });

    // The following GET routes provide APIs for getting high-level partition
    // info known to all cluster nodes.

    register_route<user>(
      ss::httpd::cluster_json::get_cluster_partitions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_cluster_partitions_handler(std::move(req));
      });
    register_route<user>(
      ss::httpd::cluster_json::get_cluster_partitions_topic,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_cluster_partitions_topic_handler(std::move(req));
      });
}

ss::future<ss::json::json_return_type> admin_server::sync_local_state_handler(
  std::unique_ptr<ss::http::request> request) {
    struct manifest_reducer {
        ss::future<>
        operator()(std::optional<cloud_storage::partition_manifest>&& value) {
            _manifest = std::move(value);
            return ss::make_ready_future<>();
        }
        std::optional<cloud_storage::partition_manifest> get() && {
            return std::move(_manifest);
        }
        std::optional<cloud_storage::partition_manifest> _manifest;
    };

    vlog(adminlog.info, "Requested bucket syncup");
    auto ntp = parse_ntp_from_request(request->param, model::kafka_namespace);
    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        vlog(adminlog.info, "Need to redirect bucket syncup request");
        throw co_await redirect_to_leader(*request, ntp);
    } else {
        auto result = co_await _partition_manager.map_reduce(
          manifest_reducer(), [ntp](cluster::partition_manager& p) {
              auto partition = p.get(ntp);
              if (partition) {
                  auto archiver = partition->archiver();
                  if (archiver) {
                      return archiver.value().get().maybe_truncate_manifest();
                  }
              }
              return ss::make_ready_future<
                std::optional<cloud_storage::partition_manifest>>(std::nullopt);
          });
        vlog(adminlog.info, "Requested bucket syncup completed");
        if (result) {
            std::stringstream sts;
            result->serialize_json(sts);
            vlog(adminlog.info, "Requested bucket syncup result {}", sts.str());
        } else {
            vlog(adminlog.info, "Requested bucket syncup result empty");
        }
    }
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::unsafe_reset_metadata(
  std::unique_ptr<ss::http::request> request,
  std::unique_ptr<ss::http::reply> reply) {
    reply->set_content_type("json");

    auto ntp = parse_ntp_from_request(request->param, model::kafka_namespace);
    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        vlog(adminlog.info, "Need to redirect unsafe reset metadata request");
        throw co_await redirect_to_leader(*request, ntp);
    }
    if (request->content_length <= 0) {
        throw ss::httpd::bad_request_exception("Empty request content");
    }

    ss::sstring content = co_await ss::util::read_entire_stream_contiguous(
      *request->content_stream);

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "{} could not be found on the node. Perhaps it has been moved "
            "during the redirect.",
            ntp));
    }

    try {
        co_await _partition_manager.invoke_on(
          *shard,
          [ntp = std::move(ntp), content = std::move(content), shard](
            auto& pm) mutable {
              auto partition = pm.get(ntp);
              if (!partition) {
                  throw ss::httpd::not_found_exception(
                    fmt::format("Could not find {} on shard {}", ntp, *shard));
              }

              iobuf buf;
              buf.append(content.data(), content.size());
              content = {};

              return partition
                ->unsafe_reset_remote_partition_manifest_from_json(
                  std::move(buf));
          });
    } catch (const std::runtime_error& err) {
        throw ss::httpd::server_error_exception(err.what());
    }

    reply->set_status(ss::http::reply::status_type::ok);
    co_return reply;
}

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::initiate_topic_scan_and_recovery(
  std::unique_ptr<ss::http::request> request,
  std::unique_ptr<ss::http::reply> reply) {
    reply->set_content_type("json");

    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*request, model::controller_ntp);
    }

    if (!_topic_recovery_service.local_is_initialized()) {
        throw ss::httpd::bad_request_exception(
          "Topic recovery is not available. is cloud storage enabled?");
    }

    auto result = co_await _topic_recovery_service.invoke_on(
      cloud_storage::topic_recovery_service::shard_id,
      [&request](auto& svc) { return svc.start_recovery(*request); });

    if (result.status_code != ss::http::reply::status_type::accepted) {
        throw ss::httpd::base_exception{result.message, result.status_code};
    }

    auto payload = ss::httpd::shadow_indexing_json::init_recovery_result{};
    payload.status = result.message;

    reply->set_status(result.status_code, payload.to_json());
    co_return reply;
}

namespace {
ss::httpd::shadow_indexing_json::topic_recovery_status
map_status_to_json(cluster::single_status status) {
    ss::httpd::shadow_indexing_json::topic_recovery_status status_json;
    status_json.state = fmt::format("{}", status.state);

    for (const auto& count : status.download_counts) {
        ss::httpd::shadow_indexing_json::topic_download_counts c;
        c.topic_namespace = fmt::format("{}", count.tp_ns);
        c.pending_downloads = count.pending_downloads;
        c.successful_downloads = count.successful_downloads;
        c.failed_downloads = count.failed_downloads;
        status_json.topic_download_counts.push(c);
    }

    ss::httpd::shadow_indexing_json::recovery_request_params r;
    r.topic_names_pattern = status.request.topic_names_pattern.value_or("none");
    r.retention_bytes = status.request.retention_bytes.value_or(-1);
    r.retention_ms = status.request.retention_ms.value_or(-1ms).count();
    status_json.request = r;

    return status_json;
}

ss::json::json_return_type serialize_topic_recovery_status(
  const cluster::status_response& cluster_status, bool extended) {
    if (!extended) {
        return map_status_to_json(cluster_status.status_log.back());
    }

    std::vector<ss::httpd::shadow_indexing_json::topic_recovery_status>
      status_log;
    status_log.reserve(cluster_status.status_log.size());
    for (const auto& entry : cluster_status.status_log) {
        status_log.push_back(map_status_to_json(entry));
    }

    return status_log;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::query_automated_recovery(std::unique_ptr<ss::http::request> req) {
    ss::httpd::shadow_indexing_json::topic_recovery_status ret;
    ret.state = "inactive";

    if (
      !_topic_recovery_status_frontend.local_is_initialized()
      || !_topic_recovery_service.local_is_initialized()) {
        co_return ret;
    }

    auto controller_leader = _metadata_cache.local().get_leader_id(
      model::controller_ntp);

    if (!controller_leader) {
        throw ss::httpd::server_error_exception{
          "Unable to get controller leader, cannot get recovery status"};
    }

    auto extended = get_boolean_query_param(*req, "extended");
    if (controller_leader.value() == config::node().node_id.value()) {
        auto status_log = co_await _topic_recovery_service.invoke_on(
          cloud_storage::topic_recovery_service::shard_id,
          [](auto& svc) { return svc.recovery_status_log(); });
        co_return serialize_topic_recovery_status(
          cluster::map_log_to_response(std::move(status_log)), extended);
    }

    if (auto status = co_await _topic_recovery_status_frontend.local().status(
          controller_leader.value());
        status.has_value()) {
        co_return serialize_topic_recovery_status(status.value(), extended);
    }

    co_return ret;
}

namespace {
ss::httpd::shadow_indexing_json::partition_cloud_storage_status
map_status_to_json(cluster::partition_cloud_storage_status status) {
    ss::httpd::shadow_indexing_json::partition_cloud_storage_status json;

    json.cloud_storage_mode = fmt::format("{}", status.mode);

    if (status.since_last_manifest_upload) {
        json.ms_since_last_manifest_upload
          = status.since_last_manifest_upload->count();
    }
    if (status.since_last_segment_upload) {
        json.ms_since_last_segment_upload
          = status.since_last_segment_upload->count();
    }
    if (status.since_last_manifest_sync) {
        json.ms_since_last_manifest_sync
          = status.since_last_manifest_sync->count();
    }

    json.metadata_update_pending = status.cloud_metadata_update_pending;

    json.total_log_size_bytes = status.total_log_size_bytes;
    json.cloud_log_size_bytes = status.cloud_log_size_bytes;
    json.stm_region_size_bytes = status.stm_region_size_bytes;
    json.archive_size_bytes = status.archive_size_bytes;
    json.local_log_size_bytes = status.local_log_size_bytes;
    json.stm_region_segment_count = status.stm_region_segment_count;
    // TODO: add spillover segments.
    json.cloud_log_segment_count = status.stm_region_segment_count;
    json.local_log_segment_count = status.local_log_segment_count;

    if (status.cloud_log_start_offset) {
        json.cloud_log_start_offset = status.cloud_log_start_offset.value()();
    }
    if (status.stm_region_start_offset) {
        json.stm_region_start_offset = status.stm_region_start_offset.value()();
    }
    if (status.cloud_log_last_offset) {
        json.cloud_log_last_offset = status.cloud_log_last_offset.value()();
    }
    if (status.local_log_start_offset) {
        json.local_log_start_offset = status.local_log_start_offset.value()();
    }
    if (status.local_log_last_offset) {
        json.local_log_last_offset = status.local_log_last_offset.value()();
    }

    return json;
}

ss::httpd::shadow_indexing_json::segment_meta
map_segment_meta_to_json(const cloud_storage::segment_meta& meta) {
    ss::httpd::shadow_indexing_json::segment_meta json;
    json.base_offset = meta.base_offset();
    json.committed_offset = meta.committed_offset();

    if (meta.delta_offset != model::offset_delta{}) {
        json.delta_offset = meta.delta_offset;
    }
    if (meta.delta_offset_end != model::offset_delta{}) {
        json.delta_offset_end = meta.delta_offset_end;
    }

    json.base_timestamp = meta.base_timestamp();
    json.max_timestamp = meta.max_timestamp();

    json.size_bytes = meta.size_bytes;
    json.is_compacted = meta.is_compacted;

    json.archiver_term = meta.archiver_term();
    json.segment_term = meta.segment_term();
    json.ntp_revision = meta.ntp_revision();

    return json;
}

ss::httpd::shadow_indexing_json::metadata_anomaly
map_metadata_anomaly_to_json(const cloud_storage::anomaly_meta& meta) {
    ss::httpd::shadow_indexing_json::metadata_anomaly json;
    switch (meta.type) {
    case cloud_storage::anomaly_type::missing_delta: {
        json.type = "missing_delta";
        json.explanation = "Segment is missing delta offset";
        json.at_segment = map_segment_meta_to_json(meta.at);
        if (meta.previous) {
            json.previous_segment = map_segment_meta_to_json(*meta.previous);
        }

        break;
    }
    case cloud_storage::anomaly_type::non_monotonical_delta: {
        if (!meta.previous) {
            vlog(
              adminlog.error,
              "Invalid anomaly metadata of type {} at {}",
              meta.type,
              meta.at);
            return json;
        }

        json.type = "non_monotonical_delta";
        json.explanation = ssx::sformat(
          "Segment has lower delta than previous: {} < {}",
          meta.at.delta_offset,
          meta.previous->delta_offset);
        json.at_segment = map_segment_meta_to_json(meta.at);
        json.previous_segment = map_segment_meta_to_json(*meta.previous);

        break;
    }
    case cloud_storage::anomaly_type::end_delta_smaller: {
        json.type = "end_delta_smaller";
        json.explanation = ssx::sformat(
          "Segment has end delta offset lower than start delta offset: {} < {}",
          meta.at.delta_offset_end,
          meta.at.delta_offset);
        json.at_segment = map_segment_meta_to_json(meta.at);

        break;
    }
    case cloud_storage::anomaly_type::committed_smaller: {
        json.type = "committed_smaller";
        json.explanation = ssx::sformat(
          "Segment has committed offset lower start offset: {} < {}",
          meta.at.committed_offset,
          meta.at.base_offset);
        json.at_segment = map_segment_meta_to_json(meta.at);

        break;
    }
    case cloud_storage::anomaly_type::offset_gap: {
        if (!meta.previous) {
            vlog(
              adminlog.error,
              "Invalid anomaly metadata of type {} at {}",
              meta.type,
              meta.at);
            return json;
        }

        json.type = "offset_gap";
        json.explanation = ssx::sformat(
          "Gap between offsets in interval ({}, {})",
          meta.previous->committed_offset(),
          meta.at.base_offset());
        json.at_segment = map_segment_meta_to_json(meta.at);
        json.previous_segment = map_segment_meta_to_json(*meta.previous);

        break;
    }
    case cloud_storage::anomaly_type::offset_overlap: {
        if (!meta.previous) {
            vlog(
              adminlog.error,
              "Invalid anomaly metadata of type {} at {}",
              meta.type,
              meta.at);
            return json;
        }

        json.type = "offest_overlap";
        json.explanation = ssx::sformat(
          "Overlapping offset in interval [{}, {}]",
          meta.at.base_offset(),
          meta.previous->committed_offset());
        json.at_segment = map_segment_meta_to_json(meta.at);
        json.previous_segment = map_segment_meta_to_json(*meta.previous);

        break;
    }
    }

    return json;
}

ss::httpd::shadow_indexing_json::cloud_storage_partition_anomalies
map_anomalies_to_json(
  const cloud_storage::remote_path_provider& path_provider,
  const model::ntp& ntp,
  const model::initial_revision_id& initial_rev,
  const cloud_storage::anomalies& detected) {
    ss::httpd::shadow_indexing_json::cloud_storage_partition_anomalies json;
    json.ns = ntp.ns();
    json.topic = ntp.tp.topic();
    json.partition = ntp.tp.partition();
    json.revision_id = initial_rev();

    if (detected.last_complete_scrub) {
        json.last_complete_scrub_at = detected.last_complete_scrub->value();
    }

    if (detected.num_discarded_missing_spillover_manifests) {
        json.num_discarded_missing_spillover_manifests
          = detected.num_discarded_missing_spillover_manifests;
    }

    if (detected.num_discarded_missing_segments) {
        json.num_discarded_missing_segments
          = detected.num_discarded_missing_segments;
    }

    if (detected.num_discarded_metadata_anomalies) {
        json.num_discarded_metadata_anomalies
          = detected.num_discarded_metadata_anomalies;
    }

    if (detected.missing_partition_manifest) {
        json.missing_partition_manifest = true;
    }

    cloud_storage::partition_manifest tmp{ntp, initial_rev};
    if (detected.missing_spillover_manifests.size() > 0) {
        const auto& missing_spills = detected.missing_spillover_manifests;
        for (auto iter = missing_spills.begin(); iter != missing_spills.end();
             ++iter) {
            json.missing_spillover_manifests.push(
              path_provider.spillover_manifest_path(tmp, *iter));
        }
    }

    if (detected.missing_segments.size() > 0) {
        const auto& missing_segs = detected.missing_segments;
        for (auto iter = missing_segs.begin(); iter != missing_segs.end();
             ++iter) {
            json.missing_segments.push(path_provider.segment_path(tmp, *iter));
        }
    }

    if (detected.segment_metadata_anomalies.size() > 0) {
        const auto& segment_meta_anomalies
          = detected.segment_metadata_anomalies;
        for (const auto& a : segment_meta_anomalies) {
            json.segment_metadata_anomalies.push(
              map_metadata_anomaly_to_json(a));
        }
    }

    return json;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_partition_cloud_storage_status(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(
      req->param, model::kafka_namespace);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "{} could not be found on the node. Perhaps it has been moved "
            "during the redirect.",
            ntp));
    }

    auto status = co_await _partition_manager.invoke_on(
      *shard,
      [&ntp](this auto, const auto& pm)
        -> ss::future<std::optional<cluster::partition_cloud_storage_status>> {
          const auto& partitions = pm.partitions();
          auto partition_iter = partitions.find(ntp);

          if (partition_iter == partitions.end()) {
              co_return std::nullopt;
          }
          auto pp = kafka::make_partition_proxy(partition_iter->second);
          co_return co_await pp.get_cloud_storage_status();
      });

    if (!status) {
        throw ss::httpd::not_found_exception(
          fmt::format("{} could not be found on shard {}.", ntp, *shard));
    }

    co_return map_status_to_json(*status);
}

ss::future<ss::json::json_return_type>
admin_server::get_cloud_storage_lifecycle(std::unique_ptr<ss::http::request>) {
    ss::httpd::shadow_indexing_json::get_lifecycle_response response;

    auto& topic_table = _controller->get_topics_state().local();

    chunked_vector<cluster::topic_table::lifecycle_markers_t::value_type>
      markers{std::from_range, topic_table.get_lifecycle_markers()};

    // Hack: persuade json response to always include the field even if empty
    response.markers._set = true;

    for (auto [nt_revision, marker] : markers) {
        ss::httpd::shadow_indexing_json::lifecycle_marker item;
        item.ns = nt_revision.nt.ns;
        item.topic = nt_revision.nt.tp;
        item.revision_id = nt_revision.initial_revision_id;

        // At time of writing, a lifecycle marker's existence implicitly means
        // it is in a purging state.  In future this will change, e.g. when we
        // use lifecycle markers to track offloaded topics that were deleted
        // with remote.delete=false
        item.status = "purging";

        response.markers.push(item);
    }

    co_return response;
}

ss::future<ss::json::json_return_type>
admin_server::delete_cloud_storage_lifecycle(
  std::unique_ptr<ss::http::request> req) {
    auto topic = model::topic(req->get_path_param("topic"));

    model::initial_revision_id revision;
    try {
        revision = model::initial_revision_id(
          std::stoll(req->get_path_param("revision")));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format(
            "Revision id must be an integer: {}",
            req->get_path_param("revision")));
    }

    auto& tp_frontend = _controller->get_topics_frontend();
    cluster::nt_revision ntr{
      .nt = model::topic_namespace(model::kafka_namespace, model::topic{topic}),
      .initial_revision_id = revision};
    auto r = co_await tp_frontend.local().purged_topic(
      ntr, cluster::topic_purge_domain::cloud_storage, 5s);
    co_await throw_on_error(*req, r.ec, model::controller_ntp);

    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::post_cloud_storage_cache_trim(
  std::unique_ptr<ss::http::request> req) {
    co_await ss::smp::submit_to(ss::shard_id{0}, [this] {
        if (!_cloud_storage_cache.local_is_initialized()) {
            throw ss::httpd::bad_request_exception(
              "Cloud Storage Cache is not available. Is cloud storage "
              "enabled?");
        }
    });

    auto max_objects = get_integer_query_param(*req, "objects");
    auto max_bytes = static_cast<std::optional<size_t>>(
      get_integer_query_param(*req, "bytes"));

    co_await _cloud_storage_cache.invoke_on(
      ss::shard_id{0}, [max_objects, max_bytes](auto& c) {
          return c.trim_manually(max_bytes, max_objects);
      });

    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::get_manifest(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    model::ntp ntp = parse_ntp_from_request(req->param, model::kafka_namespace);

    if (!_metadata_cache.local().contains(ntp)) {
        throw ss::httpd::not_found_exception(
          fmt::format("Could not find {} on the cluster", ntp));
    }

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "Could not find {} on node {}",
            ntp,
            config::node().node_id.value()));
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [rep = std::move(rep), ntp = std::move(ntp), shard](auto& pm) mutable {
          auto partition = pm.get(ntp);
          if (!partition) {
              throw ss::httpd::not_found_exception(
                fmt::format("Could not find {} on shard {}", ntp, *shard));
          }

          if (!partition->remote_partition()) {
              throw ss::httpd::bad_request_exception(
                fmt::format("Cluster is not configured for cloud storage"));
          }

          // The 'remote_partition' 'ss::shared_ptr' belongs to the
          // shard with sid '*shard'. Hence, we need to ensure that
          // when Seastar calls into the labmda provided by read body,
          // all access to the pointer happens on its home shard.
          rep->write_body(
            "json",
            [part = std::move(partition),
             sid = *shard](ss::output_stream<char>&& output_stream) mutable {
                return ss::smp::submit_to(
                  sid,
                  [os = std::move(output_stream),
                   part = std::move(part)]() mutable {
                      return ss::do_with(
                        std::move(os),
                        std::move(part),
                        [](auto& os, auto& part) mutable {
                            return part
                              ->serialize_json_manifest_to_output_stream(os)
                              .finally([&os] { return os.close(); });
                        });
                  });
            });

          return ss::make_ready_future<std::unique_ptr<ss::http::reply>>(
            std::move(rep));
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_cloud_storage_anomalies(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    const auto& topic_table = _controller->get_topics_state().local();
    const auto initial_rev = topic_table.get_initial_revision(ntp);
    const auto& tp = topic_table.get_topic_cfg(
      model::topic_namespace{ntp.ns, ntp.tp.topic});
    if (!initial_rev.has_value() || !tp.has_value()) {
        throw ss::httpd::not_found_exception(
          fmt::format("topic {} not found", ntp.tp));
    }
    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "{} could not be found on the node. Perhaps it has been moved "
            "during the redirect.",
            ntp));
    }

    cloud_storage::remote_path_provider path_provider(
      tp->properties.remote_label,
      tp->properties.remote_topic_namespace_override);
    auto status = co_await _partition_manager.invoke_on(
      *shard,
      [&ntp](const auto& pm) -> std::optional<cloud_storage::anomalies> {
          const auto& partitions = pm.partitions();
          auto partition_iter = partitions.find(ntp);

          if (partition_iter == partitions.end()) {
              return std::nullopt;
          }

          return partition_iter->second->get_cloud_storage_anomalies();
      });

    if (!status) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "Cloud partition {} could not be found on shard {}.", ntp, *shard));
    }

    co_return map_anomalies_to_json(path_provider, ntp, *initial_rev, *status);
}

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::unsafe_reset_metadata_from_cloud(
  std::unique_ptr<ss::http::request> request,
  std::unique_ptr<ss::http::reply> reply) {
    reply->set_content_type("json");

    auto ntp = parse_ntp_from_request(request->param);
    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        vlog(
          adminlog.info,
          "Need to redirect unsafe reset metadata from cloud request");
        throw co_await redirect_to_leader(*request, ntp);
    }

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "{} could not be found on the node. Perhaps it has been moved "
            "during the redirect.",
            ntp));
    }

    bool force = get_boolean_query_param(*request, "force");

    try {
        co_await _partition_manager.invoke_on(
          *shard, [ntp = std::move(ntp), shard, force](auto& pm) {
              auto partition = pm.get(ntp);
              if (!partition) {
                  throw ss::httpd::not_found_exception(
                    fmt::format("Could not find {} on shard {}", ntp, *shard));
              }

              return partition
                ->unsafe_reset_remote_partition_manifest_from_cloud(force);
          });
    } catch (const std::runtime_error& err) {
        throw ss::httpd::server_error_exception(err.what());
    }

    reply->set_status(ss::http::reply::status_type::ok);
    co_return reply;
}

ss::future<ss::json::json_return_type>
admin_server::reset_scrubbing_metadata(std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(
      req->param, model::kafka_namespace);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(
          fmt::format(
            "{} could not be found on the node. Perhaps it has been moved "
            "during the redirect.",
            ntp));
    }

    auto status = co_await _partition_manager.invoke_on(
      *shard, [&ntp, shard](const auto& pm) {
          const auto& partitions = pm.partitions();
          auto partition_iter = partitions.find(ntp);

          if (partition_iter == partitions.end()) {
              throw ss::httpd::not_found_exception(
                fmt::format("{} could not be found on shard {}.", ntp, *shard));
          }

          auto archiver = partition_iter->second->archiver();
          if (!archiver) {
              throw ss::httpd::not_found_exception(
                fmt::format("{} has no archiver on shard {}.", ntp, *shard));
          }

          return archiver.value().get().reset_scrubbing_metadata();
      });

    co_await throw_on_error(*req, status, ntp);

    co_return ss::json::json_return_type(ss::json::json_void());
}

void admin_server::register_shadow_indexing_routes() {
    register_route<superuser>(
      ss::httpd::shadow_indexing_json::sync_local_state,
      [this](std::unique_ptr<ss::http::request> req) {
          return sync_local_state_handler(std::move(req));
      });

    request_handler_fn recovery_handler = [this](auto req, auto reply) {
        return initiate_topic_scan_and_recovery(
          std::move(req), std::move(reply));
    };

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::initiate_topic_scan_and_recovery,
      std::move(recovery_handler));

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::query_automated_recovery,
      [this](auto req) { return query_automated_recovery(std::move(req)); });

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::initialize_cluster_recovery,
      request_handler_fn{[this](auto req, auto reply) {
          return initialize_cluster_recovery(std::move(req), std::move(reply));
      }});
    register_route<superuser>(
      ss::httpd::shadow_indexing_json::get_cluster_recovery,
      [this](auto req) { return get_cluster_recovery(std::move(req)); });

    register_route<user>(
      ss::httpd::shadow_indexing_json::get_partition_cloud_storage_status,
      [this](auto req) {
          return get_partition_cloud_storage_status(std::move(req));
      });

    register_route<user>(
      ss::httpd::shadow_indexing_json::get_cloud_storage_lifecycle,
      [this](auto req) { return get_cloud_storage_lifecycle(std::move(req)); });

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::delete_cloud_storage_lifecycle,
      [this](auto req) {
          return delete_cloud_storage_lifecycle(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::post_cloud_storage_cache_trim,
      [this](auto req) {
          return post_cloud_storage_cache_trim(std::move(req));
      });

    register_route_raw_async<user>(
      ss::httpd::shadow_indexing_json::get_manifest,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> rep) {
          return get_manifest(std::move(req), std::move(rep));
      });

    register_route<user>(
      ss::httpd::shadow_indexing_json::get_cloud_storage_anomalies,
      [this](auto req) { return get_cloud_storage_anomalies(std::move(req)); });

    request_handler_fn unsafe_reset_metadata_from_cloud_handler =
      [this](auto req, auto reply) {
          return unsafe_reset_metadata_from_cloud(
            std::move(req), std::move(reply));
      };

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::unsafe_reset_metadata_from_cloud,
      std::move(unsafe_reset_metadata_from_cloud_handler));

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::reset_scrubbing_metadata,
      [this](auto req) { return reset_scrubbing_metadata(std::move(req)); });
}

constexpr std::string_view to_string_view(service_kind kind) {
    switch (kind) {
    case service_kind::schema_registry:
        return "schema-registry";
    case service_kind::http_proxy:
        return "http-proxy";
    }
    return "invalid";
}

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<service_kind>
from_string_view<service_kind>(std::string_view sv) {
    return string_switch<std::optional<service_kind>>(sv)
      .match(
        to_string_view(service_kind::schema_registry),
        service_kind::schema_registry)
      .match(to_string_view(service_kind::http_proxy), service_kind::http_proxy)
      .default_match(std::nullopt);
}

namespace {
template<typename service_t>
ss::future<>
try_service_restart(service_t* svc, std::string_view service_str_view) {
    if (svc == nullptr) {
        throw ss::httpd::server_error_exception(
          fmt::format(
            "{} is undefined. Is it set in the .yaml config file?",
            service_str_view));
    }

    try {
        co_await svc->restart();
    } catch (const std::exception& ex) {
        vlog(
          adminlog.error,
          "Unknown issue restarting {}: {}",
          service_str_view,
          ex.what());
        throw ss::httpd::server_error_exception(
          fmt::format("Unknown issue restarting {}", service_str_view));
    }
}
} // namespace

ss::future<> admin_server::restart_redpanda_service(service_kind service) {
    switch (service) {
    case service_kind::schema_registry:
        co_await try_service_restart(_schema_registry, to_string_view(service));
        break;
    case service_kind::http_proxy:
        co_await try_service_restart(_http_proxy, to_string_view(service));
        break;
    }
}

ss::future<ss::json::json_return_type>
admin_server::restart_service_handler(std::unique_ptr<ss::http::request> req) {
    auto service_param = req->get_query_param("service");
    std::optional<service_kind> service = from_string_view<service_kind>(
      service_param);
    if (!service.has_value()) {
        throw ss::httpd::not_found_exception(
          fmt::format("Invalid service: {}", service_param));
    }

    vlog(
      adminlog.info, "Restart redpanda service: {}", to_string_view(*service));
    co_await container().invoke_on(0, [service](admin_server& server) {
        return server.restart_redpanda_service(*service);
    });
    co_return ss::json::json_return_type(ss::json::json_void());
}
