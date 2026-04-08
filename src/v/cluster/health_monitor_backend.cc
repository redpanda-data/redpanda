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

#include "absl/container/node_hash_map.h"
#include "absl/container/node_hash_set.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cluster/cloud_storage_size_reducer.h"
#include "cluster/controller_service.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/node/local_monitor.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_probe.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fwd.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "ssx/async_algorithm.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <optional>
#include <ranges>
#include <utility>

using namespace cluster::health_monitor_backend_details;

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
  ss::sharded<topic_table>& topic_table,
  ss::sharded<node_status_table>& node_status_table)
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
  , _node_status_table(node_status_table)
  , _reports{ss::make_lw_shared<report_cache_t>()}
  , _local_monitor(local_monitor)
  , _self(_raft0->self().id()) {}

cluster::notification_id_type
health_monitor_backend::register_node_callback(health_node_cb_t cb) {
    vassert(ss::this_shard_id() == shard, "Called on wrong shard");

    auto id = _next_callback_id++;
    // call notification for all the groups
    for (const auto& report : reports()) {
        cb(*report.second, {});
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
    std::vector<node_health_report_ptr> reports;
    std::vector<node_state> statuses;

    auto nodes = filter.nodes.empty() ? _members.local().node_ids()
                                      : filter.nodes;
    reports.reserve(nodes.size());
    statuses.reserve(nodes.size());
    for (const auto& node_id : nodes) {
        auto node_metadata = _members.local().get_node_metadata_ref(node_id);
        if (!node_metadata) {
            continue;
        }

        auto r = build_node_report(node_id, filter.node_report_filter);
        if (r) {
            reports.push_back(std::move(r.value()));
        }

        auto it = _status.find(node_id);
        if (it != _status.end()) {
            statuses.emplace_back(
              node_id,
              node_metadata->get().state.get_membership_state(),
              it->second.is_alive);
        }
    }

    return cluster_health_report{
      .raft0_leader = _raft0->get_leader_id(),
      .node_states = std::move(statuses),
      .node_reports = std::move(reports),
      .bytes_in_cloud_storage = _bytes_in_cloud_storage};
}

node_health_report::topics_t filter_topic_status(
  const node_health_report::topics_t& topics, const partitions_filter& filter) {
    // empty filter matches all
    if (filter.namespaces.empty()) {
        node_health_report::topics_t ret;
        ret.reserve(topics.bucket_count());
        for (const auto& [tp_ns, partitions] : topics) {
            ret.emplace(tp_ns, copy_partition_statuses(partitions));
        }
        return ret;
    }

    node_health_report::topics_t filtered;

    for (auto& [tp_ns, partitions] : topics) {
        partition_statuses_map_t filtered_partitions;
        for (auto& [p_id, status] : partitions) {
            if (filter.matches(tp_ns, p_id)) {
                filtered_partitions.emplace(p_id, status);
            }
        }
        if (!filtered_partitions.empty()) {
            filtered.emplace(tp_ns, std::move(filtered_partitions));
        }
    }

    return filtered;
}

std::optional<node_health_report_ptr> health_monitor_backend::build_node_report(
  model::node_id id, const node_report_filter& f) {
    auto it = reports().find(id);
    if (it == reports().cend()) {
        return std::nullopt;
    }
    if (f.include_partitions && f.ntp_filters.namespaces.empty()) {
        return ss::make_foreign(it->second);
    }

    node_health_report ret{
      it->second->id,
      it->second->local_state,
      {},
      it->second->drain_status,
      it->second->node_liveness_report};
    ret.local_state.logical_version
      = features::feature_table::get_latest_logical_version();
    ret.topics = filter_topic_status(it->second->topics, f.ntp_filters);

    return ss::make_foreign(
      ss::make_lw_shared<const node_health_report>(std::move(ret)));
}

void health_monitor_backend::abortable_refresh_request::abort() {
    if (finished) {
        return;
    }
    finished = true;
    done.set_value(errc::leadership_changed);
}

health_monitor_backend::abortable_refresh_request::abortable_refresh_request(
  ss::gate::holder holder, ssx::semaphore_units u)
  : holder(std::move(holder))
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

    auto units = co_await _refresh_mutex.get_units();

    // just skip refresh request since current state is 'fresh enough' i.e.
    // not older than max metadata age
    auto now = model::timeout_clock::now();
    if (!force && now - _last_refresh < max_metadata_age()) {
        vlog(
          clusterlog.trace,
          "skipping metadata refresh request current metadata age: {} ms",
          (now - _last_refresh) / 1ms);
        co_return errc::success;
    }

    vlog(clusterlog.debug, "refreshing health cache");

    _refresh_request = ss::make_lw_shared<abortable_refresh_request>(
      std::move(holder), std ::move(units));

    co_return co_await _refresh_request->abortable_await(
      collect_cluster_health());
}

void health_monitor_backend::abort_current_refresh() {
    if (_refresh_request) {
        vlog(clusterlog.debug, "aborting current refresh request");
        _refresh_request->abort();
    }
}

namespace {
struct partition_risk {
    using underlying = uint32_t;

private:
    underlying _value;

public:
    struct c;
    constexpr explicit partition_risk()
      : _value(0) {};
    constexpr explicit partition_risk(uint32_t v)
      : _value(v) {}

    constexpr underlying operator()() const { return _value; };

    friend constexpr bool
    operator==(const partition_risk&, const partition_risk&) = default;

    friend constexpr partition_risk
    operator&(const partition_risk x, const partition_risk y) {
        return partition_risk{x() & y()};
    }

    friend constexpr partition_risk
    operator|(const partition_risk x, const partition_risk y) {
        return partition_risk{x() | y()};
    }

    friend partition_risk&
    operator|=(partition_risk& x, const partition_risk y) {
        x = x | y;
        return x;
    }
    constexpr explicit operator bool() const;
    friend std::ostream& operator<<(std::ostream&, const partition_risk&);
};

struct partition_risk::c {
    static constexpr partition_risk no_risk{0}, rf1_offline{1},
      full_acks_produce_unavailable{2}, unavailable{4}, acks1_data_loss{8};
};

constexpr partition_risk::operator bool() const {
    return *this != partition_risk::c::no_risk;
}

std::ostream& operator<<(std::ostream& o, const partition_risk& r) {
    std::vector<std::string_view> parts;
    if (r & cluster::partition_risk::c::rf1_offline) {
        parts.emplace_back("rf1_offline");
    }
    if (r & cluster::partition_risk::c::full_acks_produce_unavailable) {
        parts.emplace_back("full_acks_produce_unavailable");
    }
    if (r & cluster::partition_risk::c::unavailable) {
        parts.emplace_back("unavailable");
    }
    if (r & cluster::partition_risk::c::acks1_data_loss) {
        parts.emplace_back("acks1_data_loss");
    }

    fmt::print(o, "{{{}}}", fmt::join(parts, ", "));
    return o;
}

void record_risks_in_report(
  restart_risk_report& report,
  model::node_id self,
  const followers_stats& fs,
  const model::topic_namespace& nt,
  model::partition_id pid) {
    // 0 or 1 each
    uint16_t self_out_of_sync = std::ranges::count(fs.out_of_sync, self);
    uint16_t self_down = std::ranges::count(fs.down, self);
    uint16_t self_in_sync = 1 - self_out_of_sync - self_down;
    if (self_in_sync != 0 && self_in_sync != 1) {
        vlog(clusterlog.error, "unexpected follower stats: {}", fs);
    }

    uint16_t n_in_sync = fs.in_sync;
    uint16_t n_out_of_sync = fs.out_of_sync.size();
    uint16_t n_down = fs.down.size();
    uint16_t n_voters = n_in_sync + n_out_of_sync + n_down;

    partition_risk restart_risk{partition_risk::c::no_risk};
    if (n_voters == 1) {
        restart_risk |= partition_risk::c::rf1_offline;
    } else {
        uint16_t majority = n_voters / 2 + 1;

        // imagine current nodes goes down
        n_in_sync -= self_in_sync;
        n_out_of_sync -= self_out_of_sync;
        n_down -= self_down;

        if (n_in_sync + n_out_of_sync < majority) {
            restart_risk |= partition_risk::c::unavailable;
        }
        if (n_in_sync < majority) {
            restart_risk |= partition_risk::c::full_acks_produce_unavailable;
        }
        if (n_in_sync == 0) {
            restart_risk |= partition_risk::c::acks1_data_loss;
        }
    }

    vlog(
      clusterlog.trace,
      "record_risk_in_report ntp={}/{}/{}, risks={}",
      nt.ns,
      nt.tp,
      pid,
      restart_risk);

    switch (restart_risk()) {
    case partition_risk::c::no_risk():
        return;
    case partition_risk::c::rf1_offline():
        report.push(&restart_risk_report::rf1_offline, nt, pid);
        return;
    case partition_risk::c::full_acks_produce_unavailable():
        report.push(
          &restart_risk_report::full_acks_produce_unavailable, nt, pid);
        return;
    case (
      partition_risk::c::unavailable
      | partition_risk::c::full_acks_produce_unavailable)():
        report.push(&restart_risk_report::unavailable, nt, pid);
        return;
    case (
      partition_risk::c::acks1_data_loss
      | partition_risk::c::full_acks_produce_unavailable)():
        report.push(&restart_risk_report::acks1_data_loss, nt, pid);
        return;
    case (
      partition_risk::c::acks1_data_loss
      | partition_risk::c::full_acks_produce_unavailable
      | partition_risk::c::unavailable)():
        report.push(&restart_risk_report::acks1_data_loss, nt, pid);
        report.push(&restart_risk_report::unavailable, nt, pid);
        return;
    default:
        vassert(
          false,
          "Unexpected partition restart risk combination {} for ntp {}",
          restart_risk,
          model::ntp{nt.ns, nt.tp, pid});
    }
}
} // namespace

ss::future<errc> health_monitor_backend::walk_local_and_remote_reports(
  partition_leader_status_handler auto local_leader_handler,
  partition_leader_status_handler auto remote_leader_handler,
  partition_handler auto unclaimed_partition_handler) {
    ssx::async_counter counter;
    chunked_hash_map<
      model::topic_namespace,
      chunked_hash_set<model::partition_id>>
      unclaimed_partitions;
    // allow `collect_cluster_health` replace it concurrently
    auto reports = hold_reports();

    // local report
    auto local_report_it = reports->find(_self);
    if (local_report_it == reports->end()) {
        vlog(clusterlog.debug, "current node is not part of the cluster");
        co_return errc::node_does_not_exists;
    };
    for (const auto& [nt, partitions] : local_report_it->second->topics) {
        co_await ssx::async_for_each_counter(
          counter,
          partitions,
          [&unclaimed_partitions, nt, &local_leader_handler](
            const auto& partition_status_pair) {
              const auto& partition_status = partition_status_pair.second;
              if (const auto& fs = partition_status.followers_stats) {
                  vlog(
                    clusterlog.trace,
                    "leader found locally ntp={}/{}/{} follower_state={}",
                    nt.ns,
                    nt.tp,
                    partition_status.id,
                    *fs);
                  local_leader_handler(*fs, nt, partition_status.id);
              } else {
                  unclaimed_partitions[nt].insert(partition_status.id);
              }
          });
    }

    // remote reports
    auto other_nodes = *reports | std::views::keys
                       | std::ranges::to<std::vector>();
    // Iterate over saved keys, as new entries may be added into `*reports`
    // concurrently by `get_current_node_health`, which potentially invalidates
    // iterators.
    for (const auto node_id : other_nodes) {
        if (node_id == _self) {
            continue;
        }
        const auto& node_report = *reports->at(node_id);
        for (const auto& [nt, partitions] : node_report.topics) {
            co_await ssx::async_for_each_counter(
              counter,
              partitions,
              [&unclaimed_partitions, nt, node_id, &remote_leader_handler](
                const auto& partition_status_pair) {
                  const auto& partition_status = partition_status_pair.second;
                  auto nt_it = unclaimed_partitions.find(nt);
                  if (nt_it == unclaimed_partitions.end()) {
                      return;
                  }
                  auto p_it = nt_it->second.find(partition_status.id);
                  if (p_it == nt_it->second.end()) {
                      return;
                  }
                  // replica exists locally, followers_stats not processed yet
                  if (const auto& fs = partition_status.followers_stats) {
                      vlog(
                        clusterlog.trace,
                        "leader found on node_id={} ntp={}/{}/{} fs={}",
                        node_id,
                        nt.ns,
                        nt.tp,
                        partition_status.id,
                        *fs);
                      remote_leader_handler(*fs, nt, partition_status.id);

                      // to ignore other leaders, should there be more than one
                      nt_it->second.erase(p_it);
                      if (nt_it->second.empty()) {
                          unclaimed_partitions.erase(nt_it);
                      }
                  }
              });
        }
    }

    // Partitions eventually unclaimed, no node says it hosts their leaders.
    for (const auto& [nt, partitions] : unclaimed_partitions) {
        co_await ssx::async_for_each_counter(
          counter,
          partitions,
          [&nt, &unclaimed_partition_handler](const model::partition_id pid) {
              unclaimed_partition_handler(nt, pid);
          });
    }
    co_return errc::success;
}

const health_monitor_backend::report_cache_t&
health_monitor_backend::reports() const {
    return std::as_const(*_reports);
}

const ss::lw_shared_ptr<const health_monitor_backend::report_cache_t>
health_monitor_backend::hold_reports() const {
    return _reports;
}

ss::future<result<restart_risk_report>>
health_monitor_backend::get_current_node_restart_risks(
  size_t limit, model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    if (
      auto ec = co_await maybe_refresh_cluster_health(
        force_refresh::no, deadline)) {
        co_return ec;
    }
    if (!_restart_risks_collected) {
        // technically it may be already enabled at the moment,
        // but was disabled on last collection
        co_return make_error_code(errc::feature_disabled);
    }

    restart_risk_report report{.limit = limit};
    auto report_handler = [&report, this](
                            const followers_stats& fs,
                            const model::topic_namespace& nt,
                            model::partition_id pid) {
        record_risks_in_report(report, _self, fs, nt, pid);
    };
    auto ec = co_await walk_local_and_remote_reports(
      report_handler,
      report_handler,
      [&report,
       this](const model::topic_namespace& nt, model::partition_id pid) {
          auto rf = _topic_table.local().get_topic_replication_factor(nt);
          if (rf == replication_factor{1}) {
              report.push(&restart_risk_report::rf1_offline, nt, pid);
          } else {
              report.push(&restart_risk_report::acks1_data_loss, nt, pid);
              report.push(&restart_risk_report::unavailable, nt, pid);
          }
      });
    if (ec == errc::success) {
        co_return report;
    } else {
        co_return ec;
    }
}

ss::future<result<double>>
health_monitor_backend::get_current_node_in_sync_replicas_share(
  model::timeout_clock::time_point deadline) {
    auto holder = _gate.hold();
    if (
      auto ec = co_await maybe_refresh_cluster_health(
        force_refresh::no, deadline)) {
        co_return ec;
    }

    int in_sync_replicas = 0;
    int out_of_sync_replicas = 0;
    auto ec = co_await walk_local_and_remote_reports(
      [&in_sync_replicas](
        const followers_stats&,
        const model::topic_namespace&,
        model::partition_id) {
          // leader replica is always in sync
          ++in_sync_replicas;
      },
      [this, &out_of_sync_replicas, &in_sync_replicas](
        const followers_stats& fs,
        const model::topic_namespace&,
        model::partition_id) {
          if (std::ranges::count(fs.out_of_sync, _self)) {
              ++out_of_sync_replicas;
          } else {
              ++in_sync_replicas;
          }
      },
      [&out_of_sync_replicas](
        const model::topic_namespace&, model::partition_id) {
          // unclaimed partition is deemed leaderless and thus out of sync
          ++out_of_sync_replicas;
      });

    if (ec == errc::success) {
        double res;
        if (in_sync_replicas + out_of_sync_replicas == 0) {
            res = 1.;
        } else {
            res = double(in_sync_replicas)
                  / (in_sync_replicas + out_of_sync_replicas);
        }
        vlog(clusterlog.debug, "in_sync_replicas_share={}", res);
        co_return res;
    } else {
        co_return ec;
    }
}

bool health_monitor_backend::contains_node_health_report(
  model::node_id id) const {
    return reports().contains(id);
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
health_monitor_backend::get_cluster_data_disk_health(
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
    co_return _reports_data_disk_health;
}

ss::future<std::error_code>
health_monitor_backend::maybe_refresh_cluster_health(
  force_refresh refresh, model::timeout_clock::time_point deadline) {
    const auto need_refresh = refresh
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
        _self,
        ss::this_shard_id(),
        id,
        max_metadata_age(),
        [timeout, id](controller_client_protocol client) mutable {
            return client.collect_node_health_report(
              get_node_health_request(id), rpc::client_opts(timeout));
        })
      .then(&rpc::get_ctx_data<get_node_health_reply>)
      .then([this, id](result<get_node_health_reply> reply) {
          return process_node_reply(id, std::move(reply));
      });
}

result<node_health_report> map_reply_result(
  model::node_id target_node_id, result<get_node_health_reply> reply) {
    if (!reply) {
        return {reply.error()};
    }
    if (!reply.value().report.has_value()) {
        return {reply.value().error};
    }
    if (reply.value().report->id != target_node_id) {
        return {errc::invalid_target_node_id};
    }
    return {std::move(*reply.value().report).to_in_memory()};
}

result<node_health_report> health_monitor_backend::process_node_reply(
  model::node_id id, result<get_node_health_reply> reply) {
    auto res = map_reply_result(id, std::move(reply));
    auto [status_it, _] = _status.try_emplace(id);
    if (!res) {
        vlog(
          clusterlog.trace,
          "unable to get node health report from {} - {}",
          id,
          res.error().message());
        /**
         * log only once node state transition from alive to down
         */
        if (status_it->second.is_alive) {
            vlog(
              clusterlog.warn,
              "unable to get node health report from {} - {}, marking node as "
              "down",
              id,
              res.error().message());
            status_it->second.is_alive = alive::no;
        }
        return res.error();
    }

    // TODO serialize storage_space_alert, instead of recomputing here.
    auto& s = res.value().local_state;
    node::local_monitor::update_alert(s.data_disk);
    if (
      !status_it->second.is_alive
      && clusterlog.is_enabled(ss::log_level::info)) {
        vlog(
          clusterlog.info,
          "received node {} health report, marking node as up",
          id);
    }
    status_it->second.last_reply_timestamp = ss::lowres_clock::now();
    status_it->second.is_alive = alive::yes;

    return res;
}

ss::future<std::error_code> health_monitor_backend::collect_cluster_health() {
    bool node_restart_risks_available = _feature_table.local().is_active(
      features::feature::node_restart_risk_assessment);

    vlog(clusterlog.debug, "collecting cluster health statistics");
    // collect all reports
    auto ids = _members.local().node_ids();
    auto collected_reports
      = co_await ssx::async_transform<std::vector<result<node_health_report>>>(
        ids.begin(), ids.end(), [this](model::node_id id) {
            if (id == _self) {
                return _report_collection_mutex.with(
                  [this] { return collect_current_node_health(); });
            }
            return collect_remote_node_health(id);
        });
    auto new_reports = ss::make_lw_shared<report_cache_t>();

    // update nodes reports and cache cluster-level data disk health
    storage::disk_space_alert cluster_data_disk_health
      = storage::disk_space_alert::ok;
    for (auto& r : collected_reports) {
        if (r) {
            const auto id = r.value().id;
            vlog(clusterlog.debug, "collected node {} health report", id);

            std::optional<nhr_ptr> old_report;
            if (auto old_i = reports().find(id); old_i != reports().end()) {
                vlog(
                  clusterlog.debug,
                  "(cache contains previous node report from {})",
                  id);
                old_report = old_i->second;
            } else {
                vlog(clusterlog.debug, "(initial node report from {})", id);
            }

            for (auto& cb : _node_callbacks) {
                cb.second(r.value(), old_report);
            }
            cluster_data_disk_health = storage::max_severity(
              r.value().local_state.data_disk.alert, cluster_data_disk_health);

            new_reports->emplace(
              id,
              ss::make_lw_shared<const node_health_report>(
                std::move(r.value())));
        }
    }
    _reports_data_disk_health = cluster_data_disk_health;

    if (config::shard_local_cfg().enable_usage()) {
        vlog(clusterlog.info, "collecting cloud health statistics");

        cluster::cloud_storage_size_reducer reducer(
          _topic_table,
          _members,
          _partition_leaders_table,
          _connections,
          topic_table_partition_generator::default_batch_size,
          cloud_storage_size_reducer::default_retries_allowed);

        try {
            /// TODO: https://github.com/redpanda-data/redpanda/issues/12515
            /// Eventually move the cloud storage size metrics into the node
            /// health report which will reduce the number of redundent RPCs
            /// needed to be made
            _bytes_in_cloud_storage = co_await reducer.reduce();
        } catch (const std::exception& ex) {
            // All exceptions are already logged by this class, in this case
        }
    }

    auto not_in_members_table = [this](const auto& value) {
        return !_members.local().contains(value.first);
    };
    /**
     * Remove reports from nodes that were removed
     */
    absl::erase_if(*new_reports, not_in_members_table);
    absl::erase_if(_status, not_in_members_table);

    _reports = std::move(new_reports);
    _restart_risks_collected = node_restart_risks_available;
    _last_refresh = ss::lowres_clock::now();
    co_return errc::success;
}

ss::future<result<node_health_report>>
health_monitor_backend::collect_current_node_health() {
    vlog(clusterlog.debug, "collecting health report");
    model::node_id id = _self;

    auto local_state = _local_monitor.local().get_state_cached();
    local_state.logical_version
      = features::feature_table::get_latest_logical_version();

    auto drain_status = co_await _drain_manager.local().status();
    auto topics = co_await collect_topic_status();
    auto node_liveness_report = collect_node_liveness_report();

    auto [it, _] = _status.try_emplace(id);
    it->second.is_alive = alive::yes;
    it->second.last_reply_timestamp = ss::lowres_clock::now();

    co_return node_health_report{
      id,
      std::move(local_state),
      std::move(topics),
      std::move(drain_status),
      std::move(node_liveness_report)};
}
ss::future<result<node_health_report_ptr>>
health_monitor_backend::get_current_node_health() {
    vlog(clusterlog.debug, "getting current node health");

    auto it = reports().find(_self);
    if (it != reports().end()) {
        co_return it->second;
    }

    auto u = _report_collection_mutex.try_get_units();
    /**
     * If units are not available it indicates that the other fiber is
     * collecting node health. We wait for the report to be available
     */
    if (!u) {
        vlog(
          clusterlog.debug,
          "report collection in progress, waiting for report to be available");
        u.emplace(co_await _report_collection_mutex.get_units());
        auto it = reports().find(_self);
        if (it != reports().end()) {
            co_return it->second;
        }
    }
    /**
     * Current fiber will collect and cache the report
     */
    auto r = co_await collect_current_node_health();
    if (r.has_error()) {
        co_return r.error();
    }

    it = _reports
           ->emplace(
             _self,
             ss::make_lw_shared<node_health_report>(std::move(r.value())))
           .first;
    co_return it->second;
}

namespace {

partition_status build_partition_status(const partition& p) {
    partition_status status;
    status.id = p.ntp().tp.partition;
    status.term = p.term();
    status.leader_id = p.get_leader_id();
    status.revision_id = p.get_revision_id();
    status.size_bytes = p.size_bytes() + p.non_log_disk_size_bytes();
    status.reclaimable_size_bytes = p.reclaimable_size_bytes();
    auto ctp_stm = p.raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    if (ctp_stm) {
        status.cloud_topic_max_gc_eligible_epoch
          = ctp_stm->estimate_inactive_epoch();
    }
    status.shard = ss::this_shard_id();

    if (p.ntp().ns == model::kafka_namespace && p.started()) {
        // HWM cannot be reliably retrieved until raft has started
        status.high_watermark = model::offset_cast(
          p.log()->from_log_offset(p.high_watermark()));
    }

    if (p.raft()->is_elected_leader()) {
        const auto fms = p.raft()->get_follower_metrics();

        status.followers_stats.emplace();
        status.under_replicated_replicas = 0;
        for (const auto& fm : fms) {
            if (fm.is_learner) {
                continue;
            }
            if (fm.is_live) {
                if (fm.under_replicated) {
                    status.followers_stats->out_of_sync.push_back(fm.id);
                    ++*status.under_replicated_replicas;
                } else {
                    ++status.followers_stats->in_sync;
                }
            } else {
                status.followers_stats->down.push_back(fm.id);
                if (fm.under_replicated) {
                    ++*status.under_replicated_replicas;
                }
            }
        }
    }
    return status;
}

struct shard_report {
    chunked_hash_map<
      model::topic_namespace,
      chunked_vector<partition_status>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>
      topics;
};

shard_report collect_shard_local_reports(partition_manager& pm) {
    auto partitions = pm.partitions() | std::views::values;

    shard_report report;

    for (const auto& p : partitions) {
        const auto& ntp = p->ntp();
        auto it = report.topics.find(model::topic_namespace_view{ntp});
        if (it == report.topics.end()) {
            it = report.topics
                   .emplace(
                     model::topic_namespace{ntp.ns, ntp.tp.topic},
                     chunked_vector<partition_status>{})
                   .first;
        }
        it->second.push_back(build_partition_status(*p));
    }
    return report;
}

using reports_acc_t
  = chunked_hash_map<model::topic_namespace, partition_statuses_t>;

reports_acc_t reduce_reports_map(reports_acc_t acc, shard_report shard_report) {
    for (auto& [tp_ns, statuses] : shard_report.topics) {
        auto& reduced_topic = acc[tp_ns];
        reduced_topic.reserve(reduced_topic.size() + statuses.size());
        std::ranges::move(statuses, std::back_inserter(reduced_topic));
    }
    return acc;
}
} // namespace

ss::future<chunked_vector<topic_status>>
health_monitor_backend::collect_topic_status() {
    auto reports_map = co_await _partition_manager.map_reduce0(
      [](partition_manager& pm) { return collect_shard_local_reports(pm); },
      reports_acc_t{},
      &reduce_reports_map);

    chunked_vector<topic_status> topics;
    topics.reserve(reports_map.size());
    for (auto& [tp_ns, partitions] : reports_map) {
        topics.emplace_back(tp_ns, std::move(partitions));
    }

    co_return topics;
}

node_liveness_report health_monitor_backend::collect_node_liveness_report() {
    const auto now = rpc::clock_type::now();
    absl::flat_hash_map<model::node_id, rpc::clock_type::duration>
      node_to_last_seen{};
    auto node_status_range
      = _members.local().node_ids()
        | std::ranges::views::transform([this](model::node_id node_id) {
              return _node_status_table.local().get_node_status(node_id);
          })
        | std::ranges::views::filter(
          [](auto maybe_node_status) { return maybe_node_status.has_value(); })
        | std::ranges::views::transform(
          [](auto maybe_node_status) { return *maybe_node_status; });

    std::ranges::for_each(
      std::move(node_status_range),
      [&node_to_last_seen, &now](node_status node_status) {
          node_to_last_seen.emplace(
            node_status.node_id, now - node_status.last_seen);
      });
    return node_liveness_report{
      {.node_id_to_last_seen = std::move(node_to_last_seen)}};
}

std::chrono::milliseconds health_monitor_backend::max_metadata_age() {
    return config::shard_local_cfg().health_monitor_max_metadata_age();
}

ss::future<result<std::optional<cluster::drain_manager::drain_status>>>
health_monitor_backend::get_node_drain_status(
  model::node_id node_id, model::timeout_clock::time_point deadline) {
    if (node_id == _self) {
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

    auto it = reports().find(node_id);
    if (it == reports().end()) {
        co_return errc::node_does_not_exists;
    }

    co_return it->second->drain_status;
}
namespace {
bool is_partition_offline(
  const cluster::partition_assignment& p_as,
  const std::vector<model::node_id>& offline_nodes) {
    return std::ranges::all_of(
      p_as.replicas, [&offline_nodes](const model::broker_shard bs) {
          return std::ranges::find(offline_nodes, bs.node_id)
                 != offline_nodes.end();
      });
}
} // namespace

ss::future<> health_monitor_backend::fill_aggregate_with_offline_partitions(
  const std::vector<model::node_id>& offline_nodes,
  aggregated_report& aggr_report) {
    uint8_t retries_left = 5;

    ssx::async_counter counter;
    while (retries_left > 0) {
        try {
            for (auto it = _topic_table.local().topics_iterator_begin();
                 it != _topic_table.local().topics_iterator_end();
                 ++it) {
                const auto& topic = it->first;
                const auto& assignment_set = it->second.get_assignments();
                auto inner_it = assignment_set.begin();
                auto inner_end = assignment_set.end();
                co_await ssx::async_while_counter(
                  counter,
                  [&it, &inner_it, &inner_end] {
                      it.check();
                      return inner_it != inner_end;
                  },
                  [&inner_it, &offline_nodes, &aggr_report, &topic] {
                      const auto& p_as = *inner_it;
                      ++inner_it;
                      if (!is_partition_offline(p_as.second, offline_nodes)) {
                          return;
                      }
                      aggr_report.leaderless_count++;
                      if (
                        aggr_report.leaderless_count
                        > aggregated_report::max_partitions_report) {
                          return;
                      }
                      aggr_report.leaderless.emplace(
                        model::ntp(topic.ns, topic.tp, p_as.first));
                  });
            }
            co_return;
        } catch (const iterator_stability_violation&) {
            --retries_left;
        }
    }
}

health_monitor_backend::aggregated_report
health_monitor_backend::aggregate_reports(const report_cache_t& reports) {
    struct collector {
        chunked_hash_set<model::ntp> to_ntp_set() const {
            chunked_hash_set<model::ntp> ret;
            for (const auto& [topic, parts] : t_to_p) {
                for (auto part : parts) {
                    ret.emplace(topic.ns, topic.tp, part);
                    if (
                      ret.size() == aggregated_report::max_partitions_report) {
                        return ret;
                    }
                }
            }
            return ret;
        }

        size_t count() const {
            size_t sum = 0;
            for (const auto& [_, parts] : t_to_p) {
                sum += parts.size();
            }
            return sum;
        }

        chunked_hash_map<
          model::topic_namespace,
          chunked_hash_set<model::partition_id>>
          t_to_p;
    };

    collector leaderless, urp;

    for (const auto& [_, report] : reports) {
        for (const auto& [tp_ns, partitions] : report->topics) {
            auto& leaderless_this_topic = leaderless.t_to_p[tp_ns];
            auto& urp_this_topic = urp.t_to_p[tp_ns];

            for (const auto& [partition_id, status] : partitions) {
                if (!status.leader_id.has_value()) {
                    leaderless_this_topic.emplace(partition_id);
                }
                if (status.under_replicated_replicas.value_or(0) > 0) {
                    urp_this_topic.emplace(partition_id);
                }
            }
        }
    }

    return {
      .leaderless = leaderless.to_ntp_set(),
      .under_replicated = urp.to_ntp_set(),
      .leaderless_count = leaderless.count(),
      .under_replicated_count = urp.count()};
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
        if (id != _self) {
            auto it = _status.find(id);
            if (it == _status.end() || !it->second.is_alive) {
                ret.nodes_down.push_back(id);
            }
        }
        auto report_it = reports().find(id);
        if (report_it == reports().end()) {
            continue;
        }
        const auto& report = report_it->second;
        if (report->local_state.recovery_mode_enabled) {
            ret.nodes_in_recovery_mode.push_back(id);
        }
        auto disk_usage_alert = report->local_state.get_disk_alert();
        switch (disk_usage_alert) {
        case storage::disk_space_alert::ok:
            break;
        case storage::disk_space_alert::low_space:
        case storage::disk_space_alert::degraded:
            ret.high_disk_usage_nodes.push_back(id);
            break;
        }
    }

    std::ranges::sort(ret.all_nodes);
    std::ranges::sort(ret.nodes_down);
    std::ranges::sort(ret.nodes_in_recovery_mode);
    std::ranges::sort(ret.high_disk_usage_nodes);

    auto aggr_report = aggregate_reports(reports());
    co_await fill_aggregate_with_offline_partitions(
      ret.nodes_down, aggr_report);

    auto move_into = [](auto& dest, auto& src) {
        dest.reserve(src.size());
        std::move(src.begin(), src.end(), std::back_inserter(dest));
    };

    move_into(ret.leaderless_partitions, aggr_report.leaderless);
    move_into(ret.under_replicated_partitions, aggr_report.under_replicated);

    ret.leaderless_count = aggr_report.leaderless_count;
    ret.under_replicated_count = aggr_report.under_replicated_count;

    ret.controller_id = _raft0->get_leader_id();

    // cluster is not healthy if some nodes are down
    if (!ret.nodes_down.empty()) {
        ret.unhealthy_reasons.emplace_back("nodes_down");
    }

    // disk usage on a subset of nodes exceeds configured storage
    // alert thresholds
    if (!ret.high_disk_usage_nodes.empty()) {
        ret.unhealthy_reasons.emplace_back("high_disk_usage_nodes");
    }

    // cluster is not healthy if some partitions do not have leaders
    if (!ret.leaderless_partitions.empty()) {
        ret.unhealthy_reasons.emplace_back("leaderless_partitions");
    }

    // cluster is not healthy if some partitions have fewer replicas than
    // their configured amount
    if (!ret.under_replicated_partitions.empty()) {
        ret.unhealthy_reasons.emplace_back("under_replicated_partitions");
    }

    // cluster is not healthy if no controller is elected
    if (!ret.controller_id) {
        ret.unhealthy_reasons.emplace_back("no_elected_controller");
    }

    // cluster is not healthy if the health report can't be obtained
    if (ec) {
        ret.unhealthy_reasons.emplace_back("no_health_report");
    }

    ret.bytes_in_cloud_storage = _bytes_in_cloud_storage;

    co_return ret;
}

bool health_monitor_backend::does_raft0_have_leader() {
    return _raft0->get_leader_id().has_value();
}

ss::future<result<std::optional<kafka::offset>>>
health_monitor_backend::get_partition_high_watermark(
  model::topic_namespace_view tp_ns, model::partition_id p_id) {
    auto ec = co_await maybe_refresh_cluster_health(
      force_refresh::no,
      model::timeout_clock::now()
        + config::shard_local_cfg().health_monitor_max_metadata_age());
    if (ec) {
        co_return ec;
    }
    kafka::offset high_watermark;
    model::revision_id current_revision;
    for (const auto& [node_id, report] : *_reports) {
        auto t_it = report->topics.find(tp_ns);
        if (t_it == report->topics.end()) {
            continue;
        }
        auto partition_it = t_it->second.find(p_id);
        if (partition_it == t_it->second.end()) {
            continue;
        }
        const auto& partition_status = partition_it->second;
        if (partition_status.revision_id > current_revision) {
            current_revision = partition_status.revision_id;
            // reset offset, we are only interested in current revision
            high_watermark = kafka::offset{};
        }
        high_watermark = std::max(
          high_watermark, partition_status.high_watermark);
    }

    co_return high_watermark == kafka::offset{}
      ? std::nullopt
      : std::make_optional(high_watermark);
}

} // namespace cluster
