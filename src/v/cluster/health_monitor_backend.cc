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

#include "cluster/cloud_storage_size_reducer.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/node/local_monitor.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_probe.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fwd.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"
#include "storage/types.h"
#include "version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_set.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <iterator>

namespace cluster {

health_monitor_backend::health_monitor_backend(
  ss::lw_shared_ptr<raft::consensus> raft0,
  ss::sharded<members_table>& mt,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_manager>& partition_manager,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<node::local_monitor>& local_monitor,
  ss::sharded<drain_manager>& drain_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<partition_leaders_table>& partition_leaders_table,
  ss::sharded<topic_table>& topic_table)
  : _raft0(std::move(raft0))
  , _members(mt)
  , _connections(connections)
  , _partition_manager(partition_manager)
  , _raft_manager(raft_manager)
  , _as(as)
  , _drain_manager(drain_manager)
  , _feature_table(feature_table)
  , _partition_leaders_table(partition_leaders_table)
  , _topic_table(topic_table)
  , _local_monitor(local_monitor) {
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
    vlog(clusterlog.info, "Stopping Health Monitor Backend...");
    _raft_manager.local().unregister_leadership_notification(
      _leadership_notification_handle);

    auto f = _gate.close();
    _refresh_mutex.broken();
    abort_current_refresh();

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
    if (_raft0->is_elected_leader()) {
        refresh_nodes_status();
    }

    auto nodes = filter.nodes.empty() ? _members.local().node_ids()
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
      .node_reports = std::move(reports),
      .bytes_in_cloud_storage = _bytes_in_cloud_storage};
}

void health_monitor_backend::refresh_nodes_status() {
    // remove all nodes not longer present in members collection
    absl::erase_if(
      _status, [this](auto& e) { return !_members.local().contains(e.first); });

    for (auto& [id, nm] : _members.local().nodes()) {
        node_state status;
        status.id = id;
        status.membership_state = nm.state.get_membership_state();

        // current node is always alive
        if (id == _raft0->self().id()) {
            status.is_alive = alive::yes;
        }
        auto res = _raft0->get_follower_metrics(id);
        if (res) {
            status.is_alive = alive(res.value().is_live);
        }
        _status.insert_or_assign(id, status);
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
      = features::feature_table::get_latest_logical_version();

    if (f.include_partitions) {
        report.topics = filter_topic_status(it->second.topics, f.ntp_filters);
    }

    report.drain_status = it->second.drain_status;
    report.include_drain_status = _feature_table.local().is_active(
      features::feature::maintenance_mode);

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
  model::node_id leader_id, ss::gate::holder holder, ssx::semaphore_units u)
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

    // leadership change, abort old refresh request
    if (_refresh_request && leader_id != _refresh_request->leader_id) {
        abort_current_refresh();
    }

    auto units = co_await _refresh_mutex.get_units();
    // refresh leader_id after acquiring mutex
    leader_id = _raft0->get_leader_id();

    // recheck if the leader exists, since this might have changed
    // while we were waiting
    if (!leader_id) {
        vlog(
          clusterlog.info,
          "unable to refresh health metadata, no leader controller");
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

    vlog(
      clusterlog.debug,
      "refreshing health cache, leader id: {} self: {}",
      leader_id,
      _raft0->self().id());

    _refresh_request = ss::make_lw_shared<abortable_refresh_request>(
      *leader_id, std::move(holder), std ::move(units));

    // we either collect the cluster health reports while on raft0 leader or ask
    // current leader for cluster health
    auto f = leader_id == _raft0->self().id()
               ? collect_cluster_health()
               : dispatch_refresh_cluster_health_request(*leader_id);

    co_return co_await _refresh_request->abortable_await(std::move(f));
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
    vlog(clusterlog.trace, "Status cache updated from the leader: {}", _status);

    storage::disk_space_alert cluster_disk_health
      = storage::disk_space_alert::ok;
    _reports.clear();
    for (auto& n_report : reply.value().report->node_reports) {
        const auto id = n_report.id;

        // Recompute alert state, in case it was deserialized from an old
        // node that didn't include alert state in the serialized storage::disk.
        node::local_monitor::update_alert(n_report.local_state.data_disk);

        // Update cached cluster-level disk health: non-raft0-leader nodes
        cluster_disk_health = storage::max_severity(
          cluster_disk_health, n_report.local_state.data_disk.alert);

        _reports.emplace(id, std::move(n_report));
    }

    _bytes_in_cloud_storage = reply.value().report->bytes_in_cloud_storage;
    _reports_disk_health = cluster_disk_health;
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
      "requesting cluster state report with filter: {}, force refresh: {}",
      filter,
      refresh);
    auto ec = co_await maybe_refresh_cluster_health(refresh, deadline);
    if (ec) {
        co_return ec;
    }

    co_return build_cluster_report(filter);
}

ss::future<storage::disk_space_alert>
health_monitor_backend::get_cluster_disk_health(
  force_refresh refresh, model::timeout_clock::time_point deadline) {
    auto ec = co_await maybe_refresh_cluster_health(refresh, deadline);
    if (ec) {
        vlog(clusterlog.warn, "Failed to refresh cluster health.");
        // No obvious choice what to return here; really, health data should be
        // requirement for staying in the cluster quorum.
        // We return OK in the remote possibility that health monitor is failing
        // to update while an operator is addressing a out-of-space condition.
        // In this case, we'd want to err on the side of allowing the cluster to
        // operate, I guess.
        co_return storage::disk_space_alert::ok;
    }
    co_return _reports_disk_health;
}

ss::future<std::error_code>
health_monitor_backend::maybe_refresh_cluster_health(
  force_refresh refresh, model::timeout_clock::time_point deadline) {
    auto const need_refresh = refresh
                              || _last_refresh + max_metadata_age()
                                   < ss::lowres_clock::now();

    // if current node is not the controller leader and we need a refresh we
    // refresh metadata cache
    if (need_refresh) {
        vlog(clusterlog.trace, "refreshing cluster health");
        try {
            auto f = refresh_cluster_health_cache(refresh);
            auto err = co_await ss::with_timeout(deadline, std::move(f));
            if (err) {
                // Many callers may try to do this: when the leader controller
                // is unavailable, we want to avoid an excess of log messages.
                static constexpr auto rate_limit = std::chrono::seconds(1);
                thread_local static ss::logger::rate_limit rate(rate_limit);
                clusterlog.log(
                  ss::log_level::info,
                  rate,
                  "error refreshing cluster health state - {}",
                  err.message());
                co_return err;
            }
        } catch (const ss::broken_semaphore&) {
            // Refresh was waiting on _refresh_mutex during shutdown
            co_return errc::shutting_down;
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

    // TODO serialize storage_space_alert, instead of recomputing here.
    auto& s = res.value().local_state;
    node::local_monitor::update_alert(s.data_disk);

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

ss::future<> health_monitor_backend::maybe_refresh_cloud_health_stats() {
    auto holder = _gate.hold();
    auto units = co_await _refresh_mutex.get_units();
    auto leader_id = _raft0->get_leader_id();
    if (!leader_id || leader_id != _raft0->self().id()) {
        co_return;
    }
    vlog(clusterlog.debug, "collecting cloud health statistics");

    cluster::cloud_storage_size_reducer reducer(
      _topic_table,
      _members,
      _partition_leaders_table,
      _connections,
      topic_table_partition_generator::default_batch_size,
      cloud_storage_size_reducer::default_retries_allowed);

    _bytes_in_cloud_storage = co_await reducer.reduce();
}

ss::future<std::error_code> health_monitor_backend::collect_cluster_health() {
    /**
     * We are collecting cluster health on raft 0 leader only
     */
    vlog(clusterlog.debug, "collecting cluster health statistics");
    // collect all reports
    auto ids = _members.local().node_ids();
    auto reports = co_await ssx::async_transform(
      ids.begin(), ids.end(), [this](model::node_id id) {
          if (id == _raft0->self().id()) {
              return collect_current_node_health(node_report_filter{});
          }
          return collect_remote_node_health(id);
      });

    auto old_reports = std::exchange(_reports, {});

    // update nodes reports and cache cluster-level disk health
    storage::disk_space_alert cluster_disk_health
      = storage::disk_space_alert::ok;
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
            cluster_disk_health = storage::max_severity(
              r.value().local_state.get_disk_alert(), cluster_disk_health);

            _reports.emplace(id, std::move(r.value()));
        }
    }
    _reports_disk_health = cluster_disk_health;
    _last_refresh = ss::lowres_clock::now();
    co_return errc::success;
}

ss::future<result<node_health_report>>
health_monitor_backend::collect_current_node_health(node_report_filter filter) {
    vlog(clusterlog.debug, "collecting health report with filter: {}", filter);
    node_health_report ret;
    ret.id = _raft0->self().id();

    ret.local_state = _local_monitor.local().get_state_cached();
    ret.local_state.logical_version
      = features::feature_table::get_latest_logical_version();

    ret.drain_status = co_await _drain_manager.local().status();
    ret.include_drain_status = _feature_table.local().is_active(
      features::feature::maintenance_mode);

    if (filter.include_partitions) {
        ret.topics = co_await collect_topic_status(
          std::move(filter.ntp_filters));
    }

    co_return ret;
}
namespace {

struct ntp_leader {
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
};

struct ntp_report {
    model::ntp ntp;
    ntp_leader leader;
    size_t size_bytes;
    std::optional<uint8_t> under_replicated_replicas;
};

partition_status to_partition_status(const ntp_report& ntpr) {
    return partition_status{
      .id = ntpr.ntp.tp.partition,
      .term = ntpr.leader.term,
      .leader_id = ntpr.leader.leader_id,
      .revision_id = ntpr.leader.revision_id,
      .size_bytes = ntpr.size_bytes,
      .under_replicated_replicas = ntpr.under_replicated_replicas};
}

std::vector<ntp_report> collect_shard_local_reports(
  partition_manager& pm, const partitions_filter& filters) {
    std::vector<ntp_report> reports;
    // empty filter, collect all
    if (filters.namespaces.empty()) {
        reports.reserve(pm.partitions().size());
        std::transform(
          pm.partitions().begin(),
          pm.partitions().end(),
          std::back_inserter(reports),
          [](auto& p) {
              return ntp_report{
                .ntp = p.first,
                .leader = ntp_leader{
                  .term = p.second->term(),
                  .leader_id = p.second->get_leader_id(),
                  .revision_id = p.second->get_revision_id(),
                },
                .size_bytes = p.second->size_bytes() + p.second->non_log_disk_size_bytes(),
                .under_replicated_replicas = p.second->get_under_replicated(),
              };
          });
    } else {
        for (const auto& [ntp, partition] : pm.partitions()) {
            if (filters.matches(ntp)) {
                reports.push_back(ntp_report{
                .ntp = ntp,
                .leader = ntp_leader{
                  .term = partition->term(),
                  .leader_id = partition->get_leader_id(),
                  .revision_id = partition->get_revision_id(),
                },
                .size_bytes = partition->size_bytes(),
                .under_replicated_replicas = partition->get_under_replicated(),
                });
            }
        }
    }

    return reports;
}

using reports_acc_t
  = absl::node_hash_map<model::topic_namespace, std::vector<partition_status>>;

reports_acc_t
reduce_reports_map(reports_acc_t acc, std::vector<ntp_report> current_reports) {
    for (auto& ntpr : current_reports) {
        model::topic_namespace tp_ns(
          std::move(ntpr.ntp.ns), std::move(ntpr.ntp.tp.topic));

        acc[tp_ns].push_back(to_partition_status(ntpr));
    }
    return acc;
}
} // namespace
ss::future<std::vector<topic_status>>
health_monitor_backend::collect_topic_status(partitions_filter filters) {
    auto reports_map = co_await _partition_manager.map_reduce0(
      [&filters](partition_manager& pm) {
          return collect_shard_local_reports(pm, filters);
      },
      reports_acc_t{},
      &reduce_reports_map);

    std::vector<topic_status> topics;
    topics.reserve(reports_map.size());
    for (auto& [tp_ns, partitions] : reports_map) {
        topics.push_back(
          topic_status{.tp_ns = tp_ns, .partitions = std::move(partitions)});
    }

    co_return topics;
}

std::chrono::milliseconds health_monitor_backend::max_metadata_age() {
    return config::shard_local_cfg().health_monitor_max_metadata_age();
}

ss::future<result<std::optional<cluster::drain_manager::drain_status>>>
health_monitor_backend::get_node_drain_status(
  model::node_id node_id, model::timeout_clock::time_point deadline) {
    if (node_id == _raft0->self().id()) {
        // Fast path: if we are asked for our own drain status, give fresh
        // data instead of spending time reloading health status which might
        // be outdated.
        co_return co_await _drain_manager.local().status();
    }

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

ss::future<cluster_health_overview>
health_monitor_backend::get_cluster_health_overview(
  model::timeout_clock::time_point deadline) {
    auto ec = co_await maybe_refresh_cluster_health(
      force_refresh::no, deadline);

    cluster_health_overview ret;
    const auto& brokers = _members.local().nodes();
    ret.all_nodes.reserve(brokers.size());

    for (auto& [id, _] : brokers) {
        ret.all_nodes.push_back(id);
        if (id == _raft0->self().id()) {
            continue;
        }
        auto it = _status.find(id);
        if (it == _status.end() || !it->second.is_alive) {
            ret.nodes_down.push_back(id);
        }
    }

    // The size of the health status must be bounded: if all partitions
    // on a system with 50k partitions are under-replicated, it is not helpful
    // to try and cram all 50k NTPs into a vector here.
    size_t max_partitions_report = 128;

    absl::node_hash_set<model::ntp> leaderless;
    absl::node_hash_set<model::ntp> under_replicated;

    for (const auto& [_, report] : _reports) {
        for (const auto& [tp_ns, partitions] : report.topics) {
            for (const auto& partition : partitions) {
                if (
                  !partition.leader_id.has_value()
                  && leaderless.size() < max_partitions_report) {
                    leaderless.emplace(tp_ns.ns, tp_ns.tp, partition.id);
                }
                if (
                  partition.under_replicated_replicas.value_or(0) > 0
                  && under_replicated.size() < max_partitions_report) {
                    under_replicated.emplace(tp_ns.ns, tp_ns.tp, partition.id);
                }
            }
        }
    }
    ret.leaderless_partitions.reserve(leaderless.size());
    std::move(
      leaderless.begin(),
      leaderless.end(),
      std::back_inserter(ret.leaderless_partitions));

    ret.under_replicated_partitions.reserve(under_replicated.size());
    std::move(
      under_replicated.begin(),
      under_replicated.end(),
      std::back_inserter(ret.under_replicated_partitions));

    ret.controller_id = _raft0->get_leader_id();

    ret.is_healthy = ret.nodes_down.empty() && ret.leaderless_partitions.empty()
                     && ret.under_replicated_partitions.empty()
                     && ret.controller_id && !ec;

    ret.bytes_in_cloud_storage = _bytes_in_cloud_storage;

    co_return ret;
}

bool health_monitor_backend::does_raft0_have_leader() {
    return _raft0->get_leader_id().has_value();
}

} // namespace cluster
