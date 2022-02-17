/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/health_monitor_backend.h"

#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/log.hh>

namespace cluster {

health_monitor_backend::health_monitor_backend(
  ss::lw_shared_ptr<raft::consensus> raft0,
  ss::sharded<members_table>& mt,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<ss::abort_source>& as)
  : _raft0(std::move(raft0))
  , _members(mt)
  , _connections(connections)
  , _partition_manager(partition_manager)
  , _as(as) {
    _tick_timer.set_callback([this] { tick(); });
    _tick_timer.arm(tick_interval());
}

ss::future<> health_monitor_backend::stop() {
    _tick_timer.cancel();
    co_await _gate.close();
}

cluster_health_report health_monitor_backend::build_cluster_report(
  const cluster_report_filter& filter) {
    std::vector<node_health_report> reports;
    std::vector<node_state> statuses;
    // refreshing node status is not expensive on leader, we can refresh it
    // every time
    if (_raft0->is_leader()) {
        refresh_nodes_status();
    }

    auto nodes = filter.nodes.empty() ? _members.local().all_broker_ids()
                                      : filter.nodes;
    reports.reserve(nodes.size());
    statuses.reserve(nodes.size());
    for (const auto& node_id : nodes) {
        auto r = build_node_report(node_id, filter.node_report_filter);
        if (r) {
            reports.push_back(std::move(r.value()));
        }

        auto it = _status.find(node_id);
        if (it != _status.end()) {
            statuses.push_back(it->second);
        }
    }

    return cluster_health_report{
      .raft0_leader = _raft0->get_leader_id(),
      .node_states = std::move(statuses),
      .node_reports = std::move(reports)};
}

void health_monitor_backend::refresh_nodes_status() {
    // remove all nodes not longer present in members collection
    absl::erase_if(
      _status, [this](auto& e) { return !_members.local().contains(e.first); });

    for (auto& b : _members.local().all_brokers()) {
        node_state status;
        status.id = b->id();
        status.membership_state = b->get_membership_state();

        // current node is always alive
        if (b->id() == _raft0->self().id()) {
            status.is_alive = alive::yes;
        }
        auto res = _raft0->get_follower_metrics(b->id());
        if (res) {
            status.is_alive = alive(res.value().is_live);
        }
        _status.insert_or_assign(b->id(), status);
    }
}

std::vector<topic_status> filter_topic_status(
  const std::vector<topic_status>& topics, const partitions_filter& filter) {
    // empty filter matches all
    if (filter.namespaces.empty()) {
        return topics;
    }

    std::vector<topic_status> filtered;

    for (auto& tl : topics) {
        topic_status filtered_topic_status{.tp_ns = tl.tp_ns};
        for (auto& pl : tl.partitions) {
            if (filter.matches(tl.tp_ns, pl.id)) {
                filtered_topic_status.partitions.push_back(pl);
            }
        }
        if (!filtered_topic_status.partitions.empty()) {
            filtered.push_back(std::move(filtered_topic_status));
        }
    }

    return filtered;
}

std::optional<node_health_report> health_monitor_backend::build_node_report(
  model::node_id id, const node_report_filter& f) {
    auto it = _reports.find(id);
    if (it == _reports.end()) {
        return std::nullopt;
    }

    node_health_report report;
    report.id = id;

    report.disk_space = it->second.disk_space;
    report.redpanda_version = it->second.redpanda_version;
    report.uptime = it->second.uptime;

    if (f.include_partitions) {
        report.topics = filter_topic_status(it->second.topics, f.ntp_filters);
    }

    return report;
}

ss::future<std::error_code>
health_monitor_backend::refresh_cluster_health_cache(force_refresh force) {
    auto leader_id = _raft0->get_leader_id();

    // if we are a leader, do nothing
    if (leader_id == _raft0->self().id()) {
        co_return errc::success;
    }

    auto units = co_await _refresh_mutex.get_units();
    // no leader controller
    if (!leader_id) {
        vlog(
          clusterlog.debug,
          "unable to refresh health metadata, no leader controller");
        // TODO: maybe we should try to ping other members in this case ?
        co_return errc::no_leader_controller;
    }
    // check under semaphore if we need to force refresh, otherwise we will just
    // skip refresh request since current state is 'fresh enough' i.e. not older
    // than max metadata age
    auto now = model::timeout_clock::now();
    if (!force && now - _last_refresh < max_metadata_age()) {
        vlog(
          clusterlog.trace,
          "skipping metadata refresh request current metadata age: {} ms",
          (now - _last_refresh) / 1ms);
        co_return errc::success;
    }

    vlog(clusterlog.debug, "refreshing health cache, leader id: {}", leader_id);
    const auto timeout = now + max_metadata_age();
    auto reply = co_await _connections.local()
                   .with_node_client<controller_client_protocol>(
                     _raft0->self().id(),
                     ss::this_shard_id(),
                     *leader_id,
                     max_metadata_age(),
                     [timeout](controller_client_protocol client) mutable {
                         get_cluster_health_request req{
                           .filter = cluster_report_filter{}};
                         return client.get_cluster_health_report(
                           std::move(req), rpc::client_opts(timeout));
                     })
                   .then(&rpc::get_ctx_data<get_cluster_health_reply>);
    if (!reply) {
        vlog(
          clusterlog.warn,
          "unable to get cluster health metadata from {} - {}",
          leader_id,
          reply.error().message());
        co_return reply.error();
    }

    if (!reply.value().report) {
        vlog(
          clusterlog.warn,
          "unable to get cluster health metadata from {} - {}",
          leader_id,
          reply.value().error);
        co_return reply.value().error;
    }

    _status.clear();
    for (auto& n_status : reply.value().report->node_states) {
        _status.emplace(n_status.id, n_status);
    }

    _reports.clear();
    for (auto& n_report : reply.value().report->node_reports) {
        const auto id = n_report.id;
        _reports.emplace(id, std::move(n_report));
    }

    _last_refresh = ss::lowres_clock::now();
    co_return errc::success;
}

ss::future<result<cluster_health_report>>
health_monitor_backend::get_cluster_health(
  cluster_report_filter filter,
  force_refresh refresh,
  model::timeout_clock::time_point deadline) {
    vlog(
      clusterlog.debug,
      "requesing cluster state report with filter: {}, force refresh: {}",
      filter,
      refresh);
    auto const need_refresh = refresh
                              || _last_refresh + max_metadata_age()
                                   < ss::lowres_clock::now();

    // if current node is not the controller leader and we need a refresh we
    // refresh metadata cache
    if (!_raft0->is_leader() && need_refresh) {
        try {
            auto f = refresh_cluster_health_cache(refresh);
            auto err = co_await ss::with_timeout(deadline, std::move(f));
            if (err) {
                vlog(
                  clusterlog.info,
                  "error refreshing cluster health state - {}",
                  err.message());
                co_return err;
            }
        } catch (const ss::timed_out_error&) {
            vlog(
              clusterlog.info,
              "timed out when refreshing cluster health state, falling back to "
              "previous cluster health snapshot");
            co_return errc::timeout;
        }
    }

    co_return build_cluster_report(filter);
}

cluster_health_report
health_monitor_backend::get_current_cluster_health_snapshot(
  const cluster_report_filter& f) {
    return build_cluster_report(f);
}

void health_monitor_backend::tick() {
    if (!_raft0->is_leader()) {
        vlog(clusterlog.trace, "skipping tick, not leader");
        _tick_timer.arm(tick_interval());
        return;
    }

    (void)ss::with_gate(_gate, [this] {
        // make sure that ticks will have fixed interval
        auto next_tick = ss::lowres_clock::now() + tick_interval();
        return collect_cluster_health().finally([this, next_tick] {
            if (!_as.local().abort_requested()) {
                _tick_timer.arm(next_tick);
            }
        });
    });
}

ss::future<result<node_health_report>>
health_monitor_backend::collect_remote_node_health(model::node_id id) {
    const auto timeout = model::timeout_clock::now() + max_metadata_age();
    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _raft0->self().id(),
        ss::this_shard_id(),
        id,
        max_metadata_age(),
        [timeout](controller_client_protocol client) mutable {
            return client.collect_node_health_report(
              get_node_health_request{.filter = node_report_filter{}},
              rpc::client_opts(timeout));
        })
      .then(&rpc::get_ctx_data<get_node_health_reply>)
      .then([this, id](result<get_node_health_reply> reply) {
          return process_node_reply(id, std::move(reply));
      });
}

result<node_health_report>
map_reply_result(result<get_node_health_reply> reply) {
    if (!reply) {
        return result<node_health_report>(reply.error());
    }
    if (!reply.value().report.has_value()) {
        return result<node_health_report>(reply.value().error);
    }
    return result<node_health_report>(std::move(*reply.value().report));
}

result<node_health_report> health_monitor_backend::process_node_reply(
  model::node_id id, result<get_node_health_reply> reply) {
    auto it = _last_replies.find(id);
    if (it == _last_replies.end()) {
        auto [inserted, _] = _last_replies.emplace(id, reply_status{});
        it = inserted;
    }

    auto res = map_reply_result(reply);
    if (!res) {
        vlog(
          clusterlog.trace,
          "unable to get node health report from {} - {}",
          id,
          res.error().message());
        /**
         * log only once node state transition from alive to down
         */
        if (it->second.is_alive) {
            vlog(
              clusterlog.warn,
              "unable to get node health report from {} - {}, marking node as "
              "down",
              id,
              res.error().message());
        }
        return result<node_health_report>(reply.error());
    }

    it->second.last_reply_timestamp = ss::lowres_clock::now();
    if (!it->second.is_alive && clusterlog.is_enabled(ss::log_level::info)) {
        vlog(
          clusterlog.info,
          "received node {} health report, marking node as up",
          id);
        it->second.is_alive = alive::yes;
    }

    return res;
}

ss::future<> health_monitor_backend::collect_cluster_health() {
    /**
     * We are collecting cluster health on raft 0 leader only
     */
    vlog(clusterlog.trace, "collecting cluster health statistics");
    // collect all reports
    auto ids = _members.local().all_broker_ids();
    auto reports = co_await ssx::async_transform(
      ids.begin(), ids.end(), [this](model::node_id id) {
          if (id == _raft0->self().id()) {
              return collect_current_node_health(node_report_filter{});
          }
          return collect_remote_node_health(id);
      });
    // update nodes reports
    _reports.clear();
    for (auto& r : reports) {
        if (r) {
            const auto id = r.value().id;
            vlog(
              clusterlog.debug,
              "collected node {} health report: {}",
              id,
              r.value());
            _reports.emplace(id, std::move(r.value()));
        }
    }
}

ss::future<result<node_health_report>>
health_monitor_backend::collect_current_node_health(node_report_filter filter) {
    vlog(clusterlog.debug, "collecting health report with filter: {}", filter);
    node_health_report ret;
    ret.id = _raft0->self().id();

    ret.disk_space = get_disk_space();
    ret.redpanda_version = cluster::application_version(
      (std::string)redpanda_version());

    ret.uptime = std::chrono::duration_cast<std::chrono::milliseconds>(
      ss::engine().uptime());

    if (filter.include_partitions) {
        ret.topics = co_await collect_topic_status(
          std::move(filter.ntp_filters));
    }

    co_return ret;
}
namespace {
struct ntp_leader {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;
};

partition_status to_partition_leader(const ntp_leader& ntpl) {
    return partition_status{
      .id = ntpl.ntp.tp.partition,
      .term = ntpl.term,
      .leader_id = ntpl.leader_id,
    };
}

std::vector<ntp_leader> collect_shard_local_leaders(
  partition_manager& pm, const partitions_filter& filters) {
    std::vector<ntp_leader> leaders;
    // empty filter, collect all
    if (filters.namespaces.empty()) {
        leaders.reserve(pm.partitions().size());
        std::transform(
          pm.partitions().begin(),
          pm.partitions().end(),
          std::back_inserter(leaders),
          [](auto& p) {
              return ntp_leader{
                .ntp = p.first,
                .term = p.second->term(),
                .leader_id = p.second->get_leader_id(),
              };
          });
    } else {
        for (const auto& [ntp, partition] : pm.partitions()) {
            if (filters.matches(ntp)) {
                leaders.push_back(ntp_leader{
                  .ntp = ntp,
                  .term = partition->term(),
                  .leader_id = partition->get_leader_id(),
                });
            }
        }
    }

    return leaders;
}
using leaders_acc_t
  = absl::node_hash_map<model::topic_namespace, std::vector<partition_status>>;

leaders_acc_t
reduce_leaders_map(leaders_acc_t acc, std::vector<ntp_leader> current_leaders) {
    for (auto& ntpl : current_leaders) {
        model::topic_namespace tp_ns(
          std::move(ntpl.ntp.ns), std::move(ntpl.ntp.tp.topic));

        acc[tp_ns].push_back(to_partition_leader(ntpl));
    }
    return acc;
}
} // namespace
ss::future<std::vector<topic_status>>
health_monitor_backend::collect_topic_status(partitions_filter filters) {
    auto leaders_map = co_await _partition_manager.map_reduce0(
      [&filters](partition_manager& pm) {
          return collect_shard_local_leaders(pm, filters);
      },
      leaders_acc_t{},
      &reduce_leaders_map);

    std::vector<topic_status> topics;
    topics.reserve(leaders_map.size());
    for (auto& [tp_ns, partitions] : leaders_map) {
        topics.push_back(
          topic_status{.tp_ns = tp_ns, .partitions = std::move(partitions)});
    }

    co_return topics;
}

std::vector<node_disk_space> health_monitor_backend::get_disk_space() {
    auto space_info = std::filesystem::space(
      config::node().data_directory().path);

    return {node_disk_space{
      .path = config::node().data_directory().as_sstring(),
      .free = space_info.free,
      .total = space_info.capacity,
    }};
}

std::chrono::milliseconds health_monitor_backend::tick_interval() {
    return config::shard_local_cfg().health_monitor_tick_interval();
}

std::chrono::milliseconds health_monitor_backend::max_metadata_age() {
    return config::shard_local_cfg().health_monitor_max_metadata_age();
}

} // namespace cluster
