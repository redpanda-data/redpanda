// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_service.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "cluster/cluster_utils.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_rpc_service.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/unresolved_address.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "utils/retry.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/flat_hash_set.h>

#include <chrono>
#include <exception>
#include <optional>

namespace cluster {
metadata_dissemination_service::metadata_dissemination_service(
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<members_table>& members,
  ss::sharded<topic_table>& topics,
  ss::sharded<rpc::connection_cache>& clients,
  ss::sharded<health_monitor_frontend>& health_monitor,
  ss::sharded<features::feature_table>& feature_table)
  : _raft_manager(raft_manager)
  , _partition_manager(partition_manager)
  , _leaders(leaders)
  , _members_table(members)
  , _topics(topics)
  , _clients(clients)
  , _health_monitor(health_monitor)
  , _feature_table(feature_table)
  , _self(make_self_broker(config::node()))
  , _dissemination_interval(
      config::shard_local_cfg().metadata_dissemination_interval_ms)
  , _rpc_tls_config(config::node().rpc_server_tls()) {
    _dispatch_timer.set_callback([this] {
        ssx::spawn_with_gate(
          _bg, [this] { return dispatch_disseminate_leadership(); });
    });
}

void metadata_dissemination_service::disseminate_leadership(
  model::ntp ntp,
  model::revision_id revision,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    vlog(
      clusterlog.trace,
      "Dissemination request for {}, revision {}, term {}, leader {}",
      ntp,
      revision,
      term,
      leader_id.value());

    _requests.emplace_back(std::move(ntp), term, leader_id, revision);
}

ss::future<> metadata_dissemination_service::start() {
    _notification_handle
      = _raft_manager.local().register_leadership_notification(
        [this](
          raft::group_id group,
          model::term_id term,
          std::optional<model::node_id> leader_id) {
            auto c = _partition_manager.local().consensus_for(group);
            if (!c) {
                return;
            }
            auto ntp = c->ntp();
            handle_leadership_notification(
              std::move(ntp),
              c->config().revision_id(),
              term,
              std::move(leader_id));
        });

    if (ss::this_shard_id() != 0) {
        return ss::make_ready_future<>();
    }
    _dispatch_timer.arm(_dissemination_interval);

    return ss::make_ready_future<>();
}

void metadata_dissemination_service::handle_leadership_notification(
  model::ntp ntp,
  model::revision_id revision,
  model::term_id term,
  std::optional<model::node_id> lid) {
    ssx::spawn_with_gate(
      _bg, [this, ntp = std::move(ntp), lid, revision, term]() mutable {
          // the lock sequences the updates from raft
          return _lock.with(
            [this, ntp = std::move(ntp), lid, revision, term]() mutable {
                return container().invoke_on(
                  0,
                  [ntp = std::move(ntp), lid, revision, term](
                    metadata_dissemination_service& s) mutable {
                      return s.apply_leadership_notification(
                        std::move(ntp), revision, term, lid);
                  });
            });
      });
}

ss::future<> metadata_dissemination_service::apply_leadership_notification(
  model::ntp ntp,
  model::revision_id revision,
  model::term_id term,
  std::optional<model::node_id> lid) {
    // the gate also needs to be taken on the destination core.
    return ss::with_gate(
      _bg, [this, ntp = std::move(ntp), lid, revision, term]() mutable {
          // update partition leaders
          vlog(clusterlog.trace, "updating {} leadership locally", ntp);
          auto f = _leaders.invoke_on_all(
            [ntp, lid, revision, term](partition_leaders_table& leaders) {
                leaders.update_partition_leader(ntp, revision, term, lid);
            });
          if (lid == _self.id()) {
              // only disseminate from current leader
              f = f.then(
                [this, ntp = std::move(ntp), term, lid, revision]() mutable {
                    return disseminate_leadership(
                      std::move(ntp), revision, term, lid);
                });
          }
          return f;
      });
}

void metadata_dissemination_service::collect_pending_updates() {
    auto brokers = _members_table.local().node_ids();
    for (auto& ntp_leader : _requests) {
        auto assignment = _topics.local().get_partition_assignment(
          ntp_leader.ntp);

        if (!assignment) {
            // Partition was removed, skip dissemination
            continue;
        }

        for (auto& id : brokers) {
            if (id == _self.id()) {
                continue;
            }
            if (!_pending_updates.contains(id)) {
                _pending_updates.emplace(
                  id,
                  update_retry_meta{ss::chunked_fifo<ntp_leader_revision>{}});
            }
            vlog(
              clusterlog.trace,
              "new metadata update {} for {}",
              ntp_leader,
              id);
            _pending_updates[id].updates.push_back(ntp_leader);
        }
    }
    _requests.clear();
}

void metadata_dissemination_service::cleanup_finished_updates() {
    std::vector<model::node_id> _to_remove;
    _to_remove.reserve(_pending_updates.size());
    auto brokers = _members_table.local().node_ids();
    for (auto& [node_id, meta] : _pending_updates) {
        auto it = std::find(brokers.begin(), brokers.end(), node_id);
        if (meta.finished) {
            vlog(clusterlog.trace, "node {} update finished", node_id);
            _to_remove.push_back(node_id);
        } else if (it == brokers.end()) {
            vlog(clusterlog.trace, "node {} isn't found", node_id);
            _to_remove.push_back(node_id);
        }
    }
    for (auto id : _to_remove) {
        _pending_updates.erase(id);
    }
}

ss::future<> metadata_dissemination_service::dispatch_disseminate_leadership() {
    /**
     * Use currently available health report to update leadership
     * information. If report would contain stale data they will be ignored by
     * term check in partition leaders table
     */
    vlog(clusterlog.trace, "disseminating leadership info");
    return _health_monitor.local()
      .get_cluster_health(
        cluster_report_filter{},
        force_refresh::no,
        _dissemination_interval + model::timeout_clock::now())
      .then([this](result<cluster_health_report> report) {
          if (report.has_error()) {
              vlog(
                clusterlog.info,
                "unable to retrieve cluster health report - {}",
                report.error().message());
              return ss::now();
          }

          return update_leaders_with_health_report(std::move(report.value()));
      })
      .then([this] {
          collect_pending_updates();
          return ss::parallel_for_each(
            _pending_updates.begin(),
            _pending_updates.end(),
            [this](broker_updates_t::value_type& br_update) {
                return dispatch_one_update(br_update.first, br_update.second);
            });
      })
      .then([this] { cleanup_finished_updates(); })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.warn, "failed to disseminate leadership: {}", e);
      })
      .finally([this] { _dispatch_timer.arm(_dissemination_interval); });
}

ss::future<> metadata_dissemination_service::update_leaders_with_health_report(
  cluster_health_report report) {
    vlog(clusterlog.trace, "updating leadership from health report");
    for (const auto& report : report.node_reports) {
        co_await _leaders.invoke_on_all(
          [&report](partition_leaders_table& leaders) {
              return leaders.update_with_node_report(report);
          });
    }
}

ss::future<> metadata_dissemination_service::dispatch_one_update(
  model::node_id target_id, update_retry_meta& meta) {
    // copy updates to make retries possible
    fragmented_vector<ntp_leader_revision> updates;
    updates.reserve(meta.updates.size());
    std::copy(
      meta.updates.begin(), meta.updates.end(), std::back_inserter(updates));

    return _clients.local()
      .with_node_client<metadata_dissemination_rpc_client_protocol>(
        _self.id(),
        ss::this_shard_id(),
        target_id,
        _dissemination_interval,
        [this, updates = std::move(updates), target_id](
          metadata_dissemination_rpc_client_protocol proto) mutable {
            vlog(
              clusterlog.trace,
              "Sending {} metadata updates to {}",
              updates,
              target_id);
            return proto
              .update_leadership_v2(
                update_leadership_request_v2(std::move(updates)),
                rpc::client_opts(
                  _dissemination_interval + rpc::clock_type::now()))
              .then(&rpc::get_ctx_data<update_leadership_reply>);
        })
      .then([target_id, &meta](result<update_leadership_reply> r) {
          if (r) {
              vlog(
                clusterlog.trace,
                "Got ack to metadata update from {}",
                target_id);
              meta.finished = true;
              return;
          }
          vlog(
            clusterlog.warn,
            "Error sending metadata update {} to {}",
            r.error().message(),
            target_id);
      })
      .handle_exception([target_id](std::exception_ptr e) {
          vlog(
            clusterlog.warn,
            "Error when sending metadata update {} to node {}",
            e,
            target_id);
      });
}

ss::future<> metadata_dissemination_service::stop() {
    _raft_manager.local().unregister_leadership_notification(
      _notification_handle);
    _as.request_abort();
    _dispatch_timer.cancel();
    return _bg.close();
}

} // namespace cluster
