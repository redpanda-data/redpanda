// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_service.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_rpc_service.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/metadata_dissemination_utils.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"
#include "utils/retry.h"
#include "utils/unresolved_address.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
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
  ss::sharded<rpc::connection_cache>& clients)
  : _raft_manager(raft_manager)
  , _partition_manager(partition_manager)
  , _leaders(leaders)
  , _members_table(members)
  , _topics(topics)
  , _clients(clients)
  , _self(make_self_broker(config::shard_local_cfg()))
  , _dissemination_interval(
      config::shard_local_cfg().metadata_dissemination_interval_ms)
  , _rpc_tls_config(config::shard_local_cfg().rpc_server_tls()) {
    _dispatch_timer.set_callback([this] {
        (void)ss::with_gate(
          _bg, [this] { return dispatch_disseminate_leadership(); });
    });
    _dispatch_timer.arm_periodic(_dissemination_interval);

    for (auto& seed : config::shard_local_cfg().seed_servers()) {
        _seed_servers.push_back(seed.addr);
    }
}

void metadata_dissemination_service::disseminate_leadership(
  model::ntp ntp,
  model::term_id term,
  std::optional<model::node_id> leader_id) {
    vlog(
      clusterlog.trace,
      "Dissemination request for {}, leader {}",
      ntp,
      leader_id.value());

    _requests.push_back(ntp_leader{std::move(ntp), term, leader_id});
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
              std::move(ntp), term, std::move(leader_id));
        });

    if (ss::this_shard_id() != 0) {
        return ss::make_ready_future<>();
    }
    // poll either seed servers or configuration
    auto all_brokers = _members_table.local().all_brokers();
    // use hash set to deduplicate ids
    absl::flat_hash_set<unresolved_address> all_broker_addresses;
    all_broker_addresses.reserve(all_brokers.size() + _seed_servers.size());
    // collect ids
    for (auto& b : all_brokers) {
        all_broker_addresses.emplace(b->rpc_address());
    }
    for (auto& id : _seed_servers) {
        all_broker_addresses.emplace(id);
    }

    // We do not want to send requst to self
    all_broker_addresses.erase(_self.rpc_address());

    // Do nothing, single node cluster
    if (all_broker_addresses.empty()) {
        return ss::make_ready_future<>();
    }
    std::vector<unresolved_address> addresses;
    addresses.reserve(all_broker_addresses.size());
    addresses.insert(
      addresses.begin(),
      all_broker_addresses.begin(),
      all_broker_addresses.end());

    (void)ss::with_gate(
      _bg, [this, addresses = std::move(addresses)]() mutable {
          return update_metadata_with_retries(std::move(addresses));
      });

    return ss::make_ready_future<>();
}

void metadata_dissemination_service::handle_leadership_notification(
  model::ntp ntp, model::term_id term, std::optional<model::node_id> lid) {
    (void)ss::with_gate(_bg, [this, ntp = std::move(ntp), lid, term]() mutable {
        return container().invoke_on(
          0,
          [ntp = std::move(ntp), lid, term](
            metadata_dissemination_service& s) mutable {
              return s.apply_leadership_notification(std::move(ntp), term, lid);
          });
    });
}

ss::future<> metadata_dissemination_service::apply_leadership_notification(
  model::ntp ntp, model::term_id term, std::optional<model::node_id> lid) {
    // the gate also needs to be taken on the destination core.
    return ss::with_gate(
      _bg, [this, ntp = std::move(ntp), lid, term]() mutable {
          // the lock sequences the updates from raft
          return _lock.with([this, ntp = std::move(ntp), lid, term]() mutable {
              // update partition leaders
              auto f = _leaders.invoke_on_all(
                [ntp, lid, term](partition_leaders_table& leaders) {
                    leaders.update_partition_leader(ntp, term, lid);
                });
              if (lid == _self.id()) {
                  // only disseminate from current leader
                  f = f.then([this, ntp = std::move(ntp), term, lid]() mutable {
                      return disseminate_leadership(std::move(ntp), term, lid);
                  });
              }
              return f;
          });
      });
}

static inline ss::future<>
wait_for_next_retry(std::chrono::seconds sleep_for, ss::abort_source& as) {
    return ss::sleep_abortable(sleep_for, as)
      .handle_exception_type([](const ss::sleep_aborted&) {
          vlog(clusterlog.debug, "Getting metadata cancelled");
      });
}

ss::future<> metadata_dissemination_service::update_metadata_with_retries(
  std::vector<unresolved_address> addresses) {
    return ss::do_with(
      request_retry_meta{.addresses = std::move(addresses)},
      [this](request_retry_meta& meta) {
          meta.next = std::cbegin(meta.addresses);
          return ss::do_until(
            [this, &meta] { return meta.success || _bg.is_closed(); },
            [this, &meta]() mutable {
                return do_request_metadata_update(meta);
            });
      });
}

ss::future<> metadata_dissemination_service::do_request_metadata_update(
  request_retry_meta& meta) {
    return dispatch_get_metadata_update(*meta.next)
      .then([this, &meta](result<get_leadership_reply> r) {
          return process_get_update_reply(std::move(r), meta);
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(clusterlog.debug, "Metadata update error: {}", e);
      })
      .then([&meta, this] {
          // Success case
          if (meta.success) {
              return ss::make_ready_future<>();
          }
          // Dispatch next retry
          ++meta.next;
          if (meta.next != meta.addresses.end()) {
              return ss::make_ready_future<>();
          }
          // start from the beggining, after backoff elapsed
          meta.next = std::cbegin(meta.addresses);
          return wait_for_next_retry(
            std::chrono::seconds(meta.backoff_policy.next_backoff()), _as);
      });
}

ss::future<> metadata_dissemination_service::process_get_update_reply(
  result<get_leadership_reply> reply_result, request_retry_meta& meta) {
    if (!reply_result) {
        vlog(
          clusterlog.debug,
          "Unable to initialize metadata using node {}",
          *meta.next);
        return ss::make_ready_future<>();
    }
    // Update all NTP leaders
    return _leaders
      .invoke_on_all([reply = std::move(reply_result.value())](
                       partition_leaders_table& leaders) mutable {
          for (auto& l : reply.leaders) {
              leaders.update_partition_leader(l.ntp, l.term, l.leader_id);
          }
      })
      .then([&meta] { meta.success = true; });
}

ss::future<result<get_leadership_reply>>
metadata_dissemination_service::dispatch_get_metadata_update(
  unresolved_address address) {
    vlog(clusterlog.debug, "Requesting metadata update from node {}", address);
    return do_with_client_one_shot<metadata_dissemination_rpc_client_protocol>(
      address,
      _rpc_tls_config,
      _dissemination_interval,
      [this](metadata_dissemination_rpc_client_protocol c) {
          return c
            .get_leadership(
              get_leadership_request{},
              rpc::client_opts(
                rpc::clock_type::now() + _dissemination_interval))
            .then(&rpc::get_ctx_data<get_leadership_reply>);
      });
}

void metadata_dissemination_service::collect_pending_updates() {
    auto brokers = _members_table.local().all_broker_ids();
    for (auto& ntp_leader : _requests) {
        auto tp_md = _topics.local().get_topic_metadata(
          model::topic_namespace_view(ntp_leader.ntp));

        if (!tp_md) {
            // Topic metadata is not there anymore, partition was removed
            continue;
        }
        auto non_overlapping = calculate_non_overlapping_nodes(
          get_partition_members(ntp_leader.ntp.tp.partition, *tp_md), brokers);
        for (auto& id : non_overlapping) {
            if (!_pending_updates.contains(id)) {
                _pending_updates.emplace(id, update_retry_meta{ntp_leaders{}});
            }
            _pending_updates[id].updates.push_back(ntp_leader);
        }
    }
    _requests.clear();
}

void metadata_dissemination_service::cleanup_finished_updates() {
    std::vector<model::node_id> _to_remove;
    _to_remove.reserve(_pending_updates.size());
    for (auto& [node_id, meta] : _pending_updates) {
        if (meta.finished) {
            _to_remove.push_back(node_id);
        }
    }
    for (auto id : _to_remove) {
        _pending_updates.erase(id);
    }
}

ss::future<> metadata_dissemination_service::dispatch_disseminate_leadership() {
    collect_pending_updates();
    return ss::parallel_for_each(
             _pending_updates.begin(),
             _pending_updates.end(),
             [this](broker_updates_t::value_type& br_update) {
                 return dispatch_one_update(br_update.first, br_update.second);
             })
      .then([this] { cleanup_finished_updates(); });
}

ss::future<> metadata_dissemination_service::dispatch_one_update(
  model::node_id target_id, update_retry_meta& meta) {
    return _clients.local()
      .with_node_client<metadata_dissemination_rpc_client_protocol>(
        _self.id(),
        ss::this_shard_id(),
        target_id,
        _dissemination_interval,
        [this, &meta, target_id](
          metadata_dissemination_rpc_client_protocol proto) mutable {
            vlog(
              clusterlog.trace,
              "Sending {} metadata updates to {}",
              meta.updates.size(),
              target_id);
            return proto
              .update_leadership(
                update_leadership_request{meta.updates},
                rpc::client_opts(
                  _dissemination_interval + rpc::clock_type::now()))
              .then(&rpc::get_ctx_data<update_leadership_reply>);
        })
      .then([target_id, &meta](result<update_leadership_reply> r) {
          if (r) {
              meta.finished = true;
              return;
          }
          vlog(
            clusterlog.warn,
            "Error sending metadata update {} to {}",
            r.error().message(),
            target_id);
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(clusterlog.warn, "Error when sending metadata update {}", e);
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
