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
#include "cluster/health_monitor_backend.h"

#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/node/local_monitor.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fwd.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/log.hh>

namespace cluster {

health_monitor_backend::health_monitor_backend(
  ss::lw_shared_ptr<raft::consensus> raft0,
  ss::sharded<members_table>& mt,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<storage::node_api>& storage_api,
  ss::sharded<drain_manager>& drain_manager,
  config::binding<size_t> storage_min_bytes_threshold,
  config::binding<unsigned> storage_min_percent_threshold)
  : _raft0(std::move(raft0))
  , _members(mt)
  , _connections(connections)
  , _partition_manager(partition_manager)
  , _raft_manager(raft_manager)
  , _as(as)
  , _drain_manager(drain_manager)
  , _local_monitor(
      std::move(storage_min_bytes_threshold),
      std::move(storage_min_percent_threshold),
      storage_api) {
    _tick_timer.set_callback([this] { tick(); });
    _tick_timer.arm(tick_interval());
    _leadership_notification_handle
      = _raft_manager.local().register_leadership_notification(
        [this](
          raft::group_id group,
          model::term_id term,
          std::optional<model::node_id> leader_id) {
            on_leadership_changed(group, term, leader_id);
        });
}

cluster::notification_id_type
health_monitor_backend::register_node_callback(health_node_cb_t cb) {
    vassert(ss::this_shard_id() == shard, "Called on wrong shard");

    auto id = _next_callback_id++;
    // call notification for all the groups
    for (const auto& report : _reports) {
        cb(report.second, {});
    }
    _node_callbacks.emplace_back(id, std::move(cb));
    return id;
}

void health_monitor_backend::unregister_node_callback(
  cluster::notification_id_type id) {
    vassert(ss::this_shard_id() == shard, "Called on wrong shard");

    _node_callbacks.erase(
      std::remove_if(
        _node_callbacks.begin(),
        _node_callbacks.end(),
        [id](
          const std::pair<cluster::notification_id_type, health_node_cb_t>& n) {
            return n.first == id;
        }),
      _node_callbacks.end());
}

ss::future<> health_monitor_backend::stop() {
    _raft_manager.local().unregister_leadership_notification(
      _leadership_notification_handle);

    auto f = _gate.close();
    abort_current_refresh();
    _tick_timer.cancel();

    if (_refresh_request) {
        _refresh_request.release();
    }
    co_await std::move(f);
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

    report.local_state = it->second.local_state;
    report.local_state.logical_version
      = feature_table::get_latest_logical_version();

    if (f.include_partitions) {
        report.topics = filter_topic_status(it->second.topics, f.ntp_filters);
    }

    report.drain_status = it->second.drain_status;

    return report;
}

void health_monitor_backend::abortable_refresh_request::abort() {
    if (finished) {
        return;
    }
    finished = true;
    done.set_value(errc::leadership_changed);
}

health_monitor_backend::abortable_refresh_request::abortable_refresh_request(
  model::node_id leader_id, ss::gate::holder holder, ss::semaphore_units<> u)
  : leader_id(leader_id)
  , holder(std::move(holder))
  , units(std::move(u)) {}

ss::future<std::error_code>
health_monitor_backend::abortable_refresh_request::abortable_await(
  ss::future<std::error_code> f) {
    ssx::background = std::move(f).then_wrapped(
      [self = shared_from_this()](ss::future<std::error_code> f) {
          if (self->finished) {
              return;
          }
          self->finished = true;
          if (f.failed()) {
              self->done.set_exception(f.get_exception());
          } else {
              self->done.set_value(f.get());
          }
      });
    return done.get_future().finally([self = shared_from_this()] {
        /**
         * return units but keep the gate holder until we finish
         * background request
         */
        self->units.return_all();
    });
}

ss::future<std::error_code>
health_monitor_backend::refresh_cluster_health_cache(force_refresh force) {
    auto holder = _gate.hold();
    auto leader_id = _raft0->get_leader_id();

    // if we are a leader, do nothing
    if (leader_id == _raft0->self().id()) {
        co_return errc::success;
    }

    // leadership change, abort old refresh request
    if (_refresh_request && leader_id != _refresh_request->leader_id) {
        abort_current_refresh();
    }

    // no leader controller
    if (!leader_id) {
        vlog(
          clusterlog.debug,
          "unable to refresh health metadata, no leader controller");
        // TODO: maybe we should try to ping other members in this case ?
        co_return errc::no_leader_controller;
    }

    auto units = co_await _refresh_mutex.get_units();
    // refresh leader_id after acquiring mutex
    leader_id = _raft0->get_leader_id();

    if (leader_id == _raft0->self().id()) {
        co_return errc::success;
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

    _refresh_request = ss::make_lw_shared<abortable_refresh_request>(
      *leader_id, std::move(holder), std ::move(units));

    co_return co_await _refresh_request->abortable_await(
      dispatch_refresh_cluster_health_request(*leader_id));
}

ss::future<std::error_code>
health_monitor_backend::dispatch_refresh_cluster_health_request(
  model::node_id node_id) {
    const auto timeout = model::timeout_clock::now() + max_metadata_age();
    auto reply = co_await _connections.local()
                   .with_node_client<controller_client_protocol>(
                     _raft0->self().id(),
                     ss::this_shard_id(),
                     node_id,
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
          node_id,
          reply.error().message());
        co_return reply.error();
    }

    if (!reply.value().report) {
        vlog(
          clusterlog.warn,
          "unable to get cluster health metadata from {} - {}",
          node_id,
          reply.value().error);
        co_return make_error_code(reply.value().error);
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
    co_return make_error_code(errc::success);
}

void health_monitor_backend::abort_current_refresh() {
    if (_refresh_request) {
        vlog(
          clusterlog.debug,
          "aborting current refresh request to {}",
          _refresh_request->leader_id);
        _refresh_request->abort();
    }
}

void health_monitor_backend::on_leadership_changed(
  raft::group_id group, model::term_id, std::optional<model::node_id>) {
    // we are only interested in raft0 leadership notifications
    if (_raft0->group() != group) {
        return;
    }
    // controller leadership changed, abort refresh request to current leader,
    // as it may be not available,  and allow subsequent calls to be dispatched
    // to new leader
    abort_current_refresh();
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
    auto ec = co_await maybe_refresh_cluster_health(refresh, deadline);
    if (ec) {
        co_return ec;
    }

    co_return build_cluster_report(filter);
}

ss::future<std::error_code>
health_monitor_backend::maybe_refresh_cluster_health(
  force_refresh refresh, model::timeout_clock::time_point deadline) {
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
    co_return errc::success;
}

cluster_health_report
health_monitor_backend::get_current_cluster_health_snapshot(
  const cluster_report_filter& f) {
    return build_cluster_report(f);
}

void health_monitor_backend::tick() {
    ssx::spawn_with_gate(_gate, [this]() -> ss::future<> {
        co_await _local_monitor.update_state();
        co_await tick_cluster_health();
    });
}

ss::future<> health_monitor_backend::tick_cluster_health() {
    if (!_raft0->is_leader()) {
        vlog(clusterlog.trace, "skipping tick, not leader");
        _tick_timer.arm(tick_interval());
        co_return;
    }

    // make sure that ticks will have fixed interval
    auto next_tick = ss::lowres_clock::now() + tick_interval();
    co_await collect_cluster_health().finally([this, next_tick] {
        if (!_as.local().abort_requested()) {
            _tick_timer.arm(next_tick);
        }
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

    auto old_reports = std::exchange(_reports, {});

    // update nodes reports
    for (auto& r : reports) {
        if (r) {
            const auto id = r.value().id;
            vlog(
              clusterlog.debug,
              "collected node {} health report: {}",
              id,
              r.value());

            std::optional<std::reference_wrapper<const node_health_report>>
              old_report;
            if (auto old_i = old_reports.find(id); old_i != old_reports.end()) {
                vlog(
                  clusterlog.debug,
                  "(previous node report from {}: {})",
                  id,
                  old_i->second);
                old_report = old_i->second;
            } else {
                vlog(clusterlog.debug, "(initial node report from {})", id);
            }

            for (auto& cb : _node_callbacks) {
                cb.second(r.value(), old_report);
            }

            _reports.emplace(id, std::move(r.value()));
        }
    }
}

ss::future<result<node_health_report>>
health_monitor_backend::collect_current_node_health(node_report_filter filter) {
    vlog(clusterlog.debug, "collecting health report with filter: {}", filter);
    node_health_report ret;
    ret.id = _raft0->self().id();

    co_await _local_monitor.update_state();
    ret.local_state = _local_monitor.get_state_cached();
    ret.local_state.logical_version
      = feature_table::get_latest_logical_version();
    ret.drain_status = co_await _drain_manager.local().status();

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
    model::revision_id revision_id;
};

partition_status to_partition_leader(const ntp_leader& ntpl) {
    return partition_status{
      .id = ntpl.ntp.tp.partition,
      .term = ntpl.term,
      .leader_id = ntpl.leader_id,
      .revision_id = ntpl.revision_id,
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
                .revision_id = p.second->get_revision_id(),
              };
          });
    } else {
        for (const auto& [ntp, partition] : pm.partitions()) {
            if (filters.matches(ntp)) {
                leaders.push_back(ntp_leader{
                  .ntp = ntp,
                  .term = partition->term(),
                  .leader_id = partition->get_leader_id(),
                  .revision_id = partition->get_revision_id(),
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

std::chrono::milliseconds health_monitor_backend::tick_interval() {
    return config::shard_local_cfg().health_monitor_tick_interval();
}

std::chrono::milliseconds health_monitor_backend::max_metadata_age() {
    return config::shard_local_cfg().health_monitor_max_metadata_age();
}

ss::future<result<std::optional<cluster::drain_manager::drain_status>>>
health_monitor_backend::get_node_drain_status(
  model::node_id node_id, model::timeout_clock::time_point deadline) {
    auto ec = co_await maybe_refresh_cluster_health(
      force_refresh::no, deadline);
    if (ec) {
        co_return ec;
    }

    auto it = _reports.find(node_id);
    if (it == _reports.end()) {
        co_return errc::node_does_not_exists;
    }

    co_return it->second.drain_status;
}

} // namespace cluster
