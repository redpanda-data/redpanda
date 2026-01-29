// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/group_manager.h"

#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cluster_utils.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/offsets_snapshot.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_delete.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/server/group.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_probe.h"
#include "kafka/server/group_recovery_consumer.h"
#include "kafka/server/group_tx_tracker_stm.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"
#include "model/limits.h"
#include "model/namespace.h"
#include "model/record.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <fmt/ranges.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <ranges>
#include <system_error>

using cluster::group_offsets;
using cluster::group_offsets_snapshot;

namespace kafka {

group_manager::attached_partition::attached_partition(
  ss::lw_shared_ptr<cluster::partition> p)
  : loading(true)
  , partition(std::move(p)) {
    catchup_lock = ss::make_lw_shared<ss::rwlock>();
}

group_manager::attached_partition::~attached_partition() noexcept = default;

group_manager::group_manager(
  model::topic_namespace tp_ns,
  ss::sharded<raft::group_manager>& gm,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& topic_table,
  ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cluster::health_monitor_frontend>& hm_frontend)
  : _tp_ns(std::move(tp_ns))
  , _gm(gm)
  , _pm(pm)
  , _topic_table(topic_table)
  , _tx_frontend(tx_frontend)
  , _feature_table(feature_table)
  , _hm_frontend(hm_frontend)
  , _conf(config::shard_local_cfg())
  , _self(cluster::make_self_broker(config::node()))
  , _offset_retention_check(_conf.group_offset_retention_check_ms.bind())
  , _enabled_metrics(_conf.enable_consumer_group_metrics.bind())
  , _lag_collection_interval(
      _conf.consumer_group_lag_collection_interval.bind()) {}

ss::future<> group_manager::start() {
    /*
     * receive notifications when group-metadata partitions come under
     * management on this core. note that the notify callback will be
     * synchronously invoked for all existing partitions that match the query.
     */
    _manage_notify_handle = _pm.local().register_manage_notification(
      _tp_ns.ns, _tp_ns.tp, [this](ss::lw_shared_ptr<cluster::partition> p) {
          attach_partition(std::move(p));
      });

    _unmanage_notify_handle = _pm.local().register_unmanage_notification(
      _tp_ns.ns, _tp_ns.tp, [this](model::topic_partition_view tp_p) {
          detach_partition(model::ntp(_tp_ns.ns, _tp_ns.tp, tp_p.partition));
      });

    /*
     * receive notifications for partition leadership changes. when we become a
     * leader we recovery. when we become a follower (or the partition is
     * mapped to another node/core) the in-memory cache may be cleared.
     */
    _leader_notify_handle = _gm.local().register_leadership_notification(
      [this](
        raft::group_id group,
        model::term_id term,
        std::optional<model::node_id> leader_id) {
          auto p = _pm.local().partition_for(group);
          if (p) {
              handle_leader_change(term, p, leader_id);
          }
      });

    /*
     * subscribe to topic modification events. In particular, when a topic is
     * deleted, consumer group metadata associated with the affected partitions
     * are cleaned-up.
     */
    _topic_table_notify_handle
      = _topic_table.local().register_ntp_delta_notification(
        [this](cluster::topic_table::ntp_delta_range_t deltas) {
            handle_topic_delta(deltas);
        });

    /*
     * periodically remove expired group offsets.
     */
    _expired_group_offset_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return handle_offset_expiration().finally([this] {
                if (!_gate.is_closed()) {
                    _expired_group_offset_timer.arm(_offset_retention_check());
                }
            });
        });
    });
    _expired_group_offset_timer.arm(_offset_retention_check());

    /*
     * reschedule periodic collection of expired offsets when the configured
     * frequency changes. useful if it were accidentally configured to be much
     * longer than desired / reasonable, and then fixed (e.g. 1 year vs 1 day).
     */
    _offset_retention_check.watch([this] {
        if (_expired_group_offset_timer.armed()) {
            _expired_group_offset_timer.cancel();
            _expired_group_offset_timer.arm(_offset_retention_check());
        }
    });

    if (ss::this_shard_id() == cluster::health_monitor_backend_shard) {
        constexpr auto set_lag_timer = [](group_manager* me) {
            auto has_lag_metric = enabled_metrics::from_vector(
                                    me->_enabled_metrics())
                                    .consumer_lag;
            me->_lag_metrics_timer.cancel();
            if (!me->_gate.is_closed() && has_lag_metric) {
                me->_lag_metrics_timer.arm(me->_lag_collection_interval());
            }
        };

        _lag_metrics_timer.set_callback([this, set_lag_timer]() {
            ssx::spawn_with_gate(_gate, [this, set_lag_timer] {
                return collect_consumer_lag_metrics().finally(
                  [this, set_lag_timer] { set_lag_timer(this); });
            });
        });

        _enabled_metrics.watch(
          [this, set_lag_timer]() { set_lag_timer(this); });
        _lag_collection_interval.watch(
          [this, set_lag_timer]() { set_lag_timer(this); });
        set_lag_timer(this);
    }

    return ss::make_ready_future<>();
}
/*
 * Compute if retention is enabled.
 *
 * legacy? | retention_ms | legacy_enabled |>> enabled
 * ===================================================
 * no        nullopt        false (n/a)        no
 * no        nullopt        true  (n/a)        no
 * no        val (default)  false (n/a)        yes
 * no        val (default)  true  (n/a)        yes
 * yes       nullopt        false              no
 * yes       nullopt        true               no
 * yes       val (default)  false              no
 * yes       val (default)  true               yes
 *
 * legacy: system is pre-v23.1 (or indeterminate in early bootup)
 */
std::optional<std::chrono::seconds> group_manager::offset_retention_enabled() {
    const auto enabled = [this] {
        /*
         * check if retention is disabled in all scenarios. this corresponds to
         * the setting `group_offset_retention_sec = null`.
         */
        if (!config::shard_local_cfg()
               .group_offset_retention_sec()
               .has_value()) {
            return false;
        }

        /*
         * in non-legacy clusters (i.e. original version >= v23.1) offset
         * retention is on by default (group_offset_retention_sec defaults to 7
         * days).
         */
        if (
          _feature_table.local().get_original_version()
          >= cluster::cluster_version(9)) {
            return true;
        }

        /*
         * this is a legacy / pre-v23 cluster. wait until all of the nodes have
         * been upgraded before making a final decision to enable offset
         * retention in order to avoid anomalies since each node independently
         * applies offset retention policy.
         */
        if (!_feature_table.local().is_active(
              features::feature::group_offset_retention)) {
            return false;
        }

        /*
         * this is a legacy / pre-v23.1 cluster. retention will only be enabled
         * if explicitly requested for legacy systems in order to retain the
         * effective behavior of infinite retention.
         *
         * this case also handles the early boot-up ambiguity in which the
         * original version is indeterminate. when we are here because the
         * original cluster version is unknown then because legacy support is
         * disabled by default the decision is conservative. if it is enabled
         * then it was explicitly requested and the orig version doesn't matter.
         */
        return config::shard_local_cfg()
          .legacy_group_offset_retention_enabled();
    }();

    /*
     * log change to effective value of offset_retention_enabled flag since its
     * value cannot easily be determiend by examining the current configuration.
     */
    if (_prev_offset_retention_enabled != enabled) {
        vlog(
          cg_klog.info,
          "Group offset retention is now {} (prev {}). Legacy enabled {} "
          "retention_sec {} original version {}.",
          enabled ? "enabled" : "disabled",
          _prev_offset_retention_enabled,
          config::shard_local_cfg().legacy_group_offset_retention_enabled(),
          config::shard_local_cfg().group_offset_retention_sec(),
          _feature_table.local().get_original_version());
        _prev_offset_retention_enabled = enabled;
    }

    if (!enabled) {
        return std::nullopt;
    }

    return config::shard_local_cfg().group_offset_retention_sec().value();
}

ss::future<> group_manager::handle_offset_expiration() {
    constexpr int max_concurrent_expirations = 10;

    const auto retention_period = offset_retention_enabled();
    if (!retention_period.has_value()) {
        co_return;
    }

    /*
     * build a light-weight snapshot of the groups to process. the snapshot
     * allows us to avoid concurrent modifications to _groups container.
     */
    chunked_vector<std::pair<group_ptr, size_t>> groups;
    for (auto& group : _groups) {
        groups.emplace_back(group.second, 0);
    }

    co_await ss::max_concurrent_for_each(
      groups,
      max_concurrent_expirations,
      [this, retention_period = retention_period.value()](auto& group_count) {
          return delete_expired_offsets(group_count.first, retention_period)
            .then(
              [&group_count](auto removed) { group_count.second = removed; });
      });

    auto groups_with_expired_offsets
      = groups | std::ranges::views::filter([](auto& group_count) {
            return group_count.second > 0;
        })
        | std::ranges::views::transform([](auto& group_count) {
              return std::pair<std::string_view, size_t>(
                group_count.first->id()(), group_count.second);
          });

    if (!groups_with_expired_offsets.empty()) {
        vlog(
          cg_klog.info,
          "Removed (group, offsets) {} due to offset retention",
          groups_with_expired_offsets);
    }
}

ss::future<size_t> group_manager::delete_expired_offsets(
  group_ptr group, std::chrono::seconds retention_period) {
    /*
     * delete expired offsets from the group
     */
    auto offsets = group->delete_expired_offsets(retention_period);
    co_return co_await delete_offsets(group, offsets);
}

ss::future<size_t> group_manager::delete_offsets(
  group_ptr group, const chunked_vector<model::topic_partition>& offsets) {
    /*
     * build tombstones to persistent offset deletions. the group itself may
     * also be set to dead state in which case we may be able to delete the
     * group as well.
     *
     * the group may be set to dead state even if no offsets are returned from
     * `group::delete_expired_offsets` so avoid an early return above if no
     * offsets are returned.
     */
    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    for (auto& offset : offsets) {
        vlog(
          cg_klog.trace,
          "Preparing tombstone for expired group offset {}:{}",
          group,
          offset);
        group->add_offset_tombstone_record(group->id(), offset, builder);
    }

    if (group->in_state(group_state::dead)) {
        auto it = _groups.find(group->id());
        if (it != _groups.end() && it->second == group) {
            co_await it->second->shutdown();
            _groups.erase(it);
            if (group->generation() > 0) {
                vlog(
                  cg_klog.trace,
                  "Preparing tombstone for dead group following offset "
                  "expiration {}",
                  group);
                group->add_group_tombstone_record(group->id(), builder);
            }
        }
    }

    if (builder.empty()) {
        co_return 0;
    }

    /*
     * replicate tombstone records to the group's partition. It is fine to use
     * quorum_ack as even though this operation is not critical it is
     * interleaved with quorum ack replicate calls.
     */
    auto batch = std::move(builder).build();

    try {
        auto result = co_await group->partition()->raft()->replicate(
          std::move(batch),
          raft::replicate_options(
            raft::consistency_level::quorum_ack, group->term()));

        if (result) {
            vlog(
              cg_klog.debug,
              "Wrote {} tombstone records for group {} expired offsets",
              offsets.size(),
              group);
            co_return offsets.size();

        } else if (result.error() == raft::errc::shutting_down) {
            vlog(
              cg_klog.debug,
              "Cannot replicate tombstone records for group {}: shutting down",
              group);

        } else if (result.error() == raft::errc::not_leader) {
            vlog(
              cg_klog.debug,
              "Cannot replicate tombstone records for group {}: not leader",
              group);

        } else {
            vlog(
              cg_klog.error,
              "Cannot replicate tombstone records for group {}: {} {}",
              group,
              result.error().message(),
              result.error());
        }

    } catch (...) {
        vlog(
          cg_klog.error,
          "Exception occurred replicating tombstones for group {}: {}",
          group,
          std::current_exception());
    }

    co_return 0;
}

ss::future<> group_manager::stop() {
    /**
     * This is not ususal as stop() method should only be called once. For the
     * purpose of migration we must stop all pending operations & notifications
     * in previous group manager implementation. This check allow us to call
     * stop more than once and makes it idemtpotent.
     *
     * Stop may be first called during migration and then for the second time
     * during application shutdown
     */
    if (_gate.is_closed()) {
        return ss::now();
    }
    _pm.local().unregister_manage_notification(_manage_notify_handle);
    _pm.local().unregister_unmanage_notification(_unmanage_notify_handle);
    _gm.local().unregister_leadership_notification(_leader_notify_handle);
    _topic_table.local().unregister_ntp_delta_notification(
      _topic_table_notify_handle);

    for (auto& e : _partitions) {
        e.second->as.request_abort();
    }

    _expired_group_offset_timer.cancel();
    _lag_metrics_timer.cancel();

    return _gate.close().then([this]() {
        /**
         * cancel all pending group opeartions
         */
        return ss::do_for_each(
                 _groups, [](auto& p) { return p.second->shutdown(); })
          .then([this] { _partitions.clear(); });
    });
}

void group_manager::detach_partition(const model::ntp& ntp) {
    vlog(cg_klog.debug, "detaching group metadata partition {}", ntp);
    ssx::spawn_with_gate(_gate, [this, _ntp{ntp}]() mutable {
        return do_detach_partition(std::move(_ntp));
    });
}

ss::future<> group_manager::do_detach_partition(model::ntp ntp) {
    auto it = _partitions.find(ntp);
    if (it == _partitions.end()) {
        co_return;
    }
    auto p = it->second;
    auto units = co_await p->catchup_lock->hold_write_lock();

    // Becasue shutdown group is async operation we should run it after
    // rehash for groups map
    std::vector<group_ptr> groups_for_shutdown;
    for (auto g_it = _groups.begin(); g_it != _groups.end();) {
        if (g_it->second->partition()->ntp() == p->partition->ntp()) {
            groups_for_shutdown.push_back(g_it->second);
            g_it->second->pre_shutdown();
            _groups.erase(g_it++);
            continue;
        }
        ++g_it;
    }
    // if p has background work that won't complete without an abort being
    // requested, then do that now because once the partition is removed from
    // the _partitions container it won't be available in group_manager::stop.
    if (!p->as.abort_requested()) {
        p->as.request_abort();
    }
    _partitions.erase(ntp);
    _partitions.rehash(0);

    co_await shutdown_groups(std::move(groups_for_shutdown));
}

void group_manager::attach_partition(ss::lw_shared_ptr<cluster::partition> p) {
    vlog(cg_klog.debug, "attaching group metadata partition {}", p->ntp());
    auto attached = ss::make_lw_shared<attached_partition>(p);
    auto res = _partitions.try_emplace(p->ntp(), attached);
    // TODO: this is not a forever assertion. this should just generally never
    // happen _now_ because we don't support partition migration / removal.
    // however, group manager is also not prepared for such scenarios.
    vassert(
      res.second, "double registration of ntp in group manager {}", p->ntp());
    _partitions.rehash(0);
}

ss::future<> group_manager::cleanup_removed_topic_partitions(
  const chunked_vector<model::topic_partition>& tps) {
    // operate on a light-weight copy of group pointers to avoid iterating over
    // the main index which is subject to concurrent modification.
    chunked_vector<group_ptr> groups;
    groups.reserve(_groups.size());
    for (auto& group : _groups) {
        groups.push_back(group.second);
    }

    return ss::do_with(
      std::move(groups), [this, &tps](chunked_vector<group_ptr>& groups) {
          return ss::do_for_each(groups, [this, &tps](group_ptr& group) {
              return group->remove_topic_partitions(tps).then(
                [this, g = group] {
                    if (!g->in_state(group_state::dead)) {
                        return ss::now();
                    }
                    auto it = _groups.find(g->id());
                    if (it == _groups.end()) {
                        return ss::now();
                    }
                    // ensure the group didn't change
                    if (it->second != g) {
                        return ss::now();
                    }
                    vlog(cg_klog.trace, "Removed group {}", g);
                    it->second->pre_shutdown();
                    _groups.erase(it);
                    _groups.rehash(0);
                    return ss::now();
                });
          });
      });
}

void group_manager::handle_topic_delta(
  cluster::topic_table::ntp_delta_range_t deltas) {
    // topic-partition deletions in the kafka namespace are the only deltas that
    // are relevant to the group manager
    chunked_vector<model::topic_partition> tps;
    for (const auto& delta : deltas) {
        if (
          delta.type == cluster::topic_table_ntp_delta_type::removed
          && delta.ntp.ns == model::kafka_namespace) {
            tps.emplace_back(delta.ntp.tp);
        }
    }

    if (tps.empty()) {
        return;
    }

    ssx::background
      = ssx::spawn_with_gate_then(
          _gate,
          [this, tps = std::move(tps)]() mutable {
              return ss::do_with(
                std::move(tps),
                [this](const chunked_vector<model::topic_partition>& tps) {
                    return cleanup_removed_topic_partitions(tps);
                });
          })
          .handle_exception([](std::exception_ptr e) {
              vlog(cg_klog.warn, "Topic clean-up encountered error: {}", e);
          });
}

void group_manager::handle_leader_change(
  model::term_id term,
  ss::lw_shared_ptr<cluster::partition> part,
  std::optional<model::node_id> leader) {
    ssx::spawn_with_gate(_gate, [this, term, part = std::move(part), leader] {
        if (auto it = _partitions.find(part->ntp()); it != _partitions.end()) {
            /*
             * In principle a race could occur by which a validate_group_status
             * might return true even when the underlying partition hasn't
             * recovered. a request would need to sneak in after leadership is
             * set and clients were able to route to the new coordinator, but
             * before we set loading=true to block operations while recover
             * happens.
             *
             * since upcall happens synchrnously from rafter on leadership
             * change, we early set this flag to block out any requests in case
             * we would end up waiting on recovery and cause the situation
             * above.
             */
            if (leader == _self.id()) {
                it->second->loading = true;
            }
            return ss::with_semaphore(
                     it->second->sem,
                     1,
                     [this, term, p = it->second, leader] {
                         return handle_partition_leader_change(term, p, leader);
                     })
              .finally([p = it->second] {});
        }
        return ss::make_ready_future<>();
    });
}

ss::future<std::error_code> group_manager::inject_noop(
  ss::lw_shared_ptr<cluster::partition> p,
  ss::lowres_clock::time_point timeout) {
    auto dirty_offset = p->dirty_offset();
    auto barrier_offset = co_await p->linearizable_barrier();
    if (barrier_offset.has_error()) {
        co_return barrier_offset.error();
    }
    // synchronization provided by raft after future resolves is sufficient to
    // get an up-to-date commit offset as an upperbound for our reader.
    while (barrier_offset.has_value()
           && barrier_offset.value() < dirty_offset) {
        barrier_offset = co_await p->linearizable_barrier();
        if (barrier_offset.has_error()) {
            co_return barrier_offset.error();
        }
        if (ss::lowres_clock::now() > timeout) {
            co_return raft::errc::timeout;
        }
    }
    co_return raft::errc::success;
}

ss::future<>
group_manager::gc_partition_state(ss::lw_shared_ptr<attached_partition> p) {
    vlog(cg_klog.trace, "Removing groups of {}", p->partition->ntp());

    /**
     * since this operation is destructive for partitions group we hold a
     * catchup write lock
     */
    auto units = co_await p->catchup_lock->hold_write_lock();

    // Becasue shutdown group is async operation we should run it after rehash
    // for groups map
    std::vector<group_ptr> groups_for_shutdown;
    for (auto it = _groups.begin(); it != _groups.end();) {
        if (it->second->partition()->ntp() == p->partition->ntp()) {
            groups_for_shutdown.push_back(it->second);
            vlog(cg_klog.trace, "Removed group {}", it->second);
            it->second->pre_shutdown();
            _groups.erase(it++);
            continue;
        }
        ++it;
    }
    _groups.rehash(0);

    co_await shutdown_groups(std::move(groups_for_shutdown));
}

ss::future<> group_manager::reload_groups() {
    std::vector<ss::future<>> futures;
    for (auto& [ntp, attached] : _partitions) {
        auto leader = attached->partition->get_leader_id();
        if (leader == _self.id()) {
            attached->loading = true;
        }
        auto term = attached->partition->term();
        auto f = ss::with_semaphore(
                   attached->sem,
                   1,
                   [this, p = attached, leader, term]() mutable {
                       return handle_partition_leader_change(term, p, leader);
                   })
                   .finally([p = attached->partition] {});
        futures.push_back(std::move(f));
    }
    co_await ss::when_all_succeed(futures.begin(), futures.end());
}

ss::future<result<model::offset>> group_manager::set_blocked_for_groups(
  const model::ntp& co_ntp,
  const chunked_vector<kafka::group_id>& group_ids,
  bool to_block) {
    if (!_feature_table.local().is_active(
          features::feature::consumer_groups_migrations)) {
        vlog(
          cg_klog.warn,
          "set blocked request for {} failed - consumer groups migrations "
          "feature is not active",
          co_ntp);
        co_return cluster::errc::feature_disabled;
    }
    auto p = get_attached_partition(co_ntp);
    if (!p) {
        vlog(
          cg_klog.warn,
          "set blocked request for {} failed - attached partition not found",
          co_ntp);
        co_return cluster::errc::partition_not_exists;
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        vlog(
          cluster::txlog.trace,
          "can't set blocked: coordinator_load_in_progress");
        co_return cluster::tx::errc::coordinator_load_in_progress;
    }

    const auto affected_gids
      = group_ids
        | std::views::filter([&p, to_block](const kafka::group_id& group_id) {
              return to_block != p->blocked_groups.contains(group_id);
          })
        | std::ranges::to<chunked_vector<kafka::group_id>>();

    // in case we return early with an exception or another error
    auto revert_blocking = ss::defer([p, &affected_gids, to_block] {
        if (to_block) {
            for (const auto& gid : affected_gids) {
                p->blocked_groups.erase(gid);
            }
        }
    });
    if (to_block) {
        // block early to avoid racing with new transactions
        p->blocked_groups.insert(affected_gids.begin(), affected_gids.end());

        auto last_error = cluster::tx::errc::none;
        co_await ss::max_concurrent_for_each(
          group_ids, 5, [this, &last_error](auto& group_id) {
              if (auto group = get_group(group_id)) {
                  return group->abort_txes(false).then([&last_error](auto ec) {
                      if (ec != cluster::tx::errc::none) {
                          last_error = ec;
                      }
                  });
              }
              return ss::make_ready_future<>();
          });
        if (last_error != cluster::tx::errc::none) {
            vlog(
              cg_klog.warn,
              "Failed to abort transactions for blocked groups: {}",
              last_error);
            co_return last_error;
        }
    }

    storage::record_batch_builder builder(
      model::record_batch_type::group_block, model::offset{0});
    for (const auto& group_id : group_ids) {
        group_block{group_id, to_block}.add_to_batch_builder(builder);
    }

    auto result = co_await p->partition->raft()->replicate(
      std::move(builder).build(),
      raft::replicate_options(raft::consistency_level::quorum_ack, p->term));

    if (!result) {
        co_return result.error();
    }

    // unblock only when replicated
    if (!to_block) {
        for (const auto& gid : affected_gids) {
            p->blocked_groups.erase(gid);
        }
    }

    revert_blocking.cancel();
    co_return result.value().last_offset;
}

ss::future<group_manager::group_offsets_snapshot_result>
group_manager::snapshot_groups_for_upload(
  const model::ntp& ntp, size_t max_num_groups_per_snap) {
    auto res = co_await do_snapshot_groups(
      ntp, max_num_groups_per_snap, std::nullopt);
    if (res.has_value()) {
        co_return std::move(res.assume_value());
    }
    switch (res.error()) {
    case cluster::errc::partition_not_exists:
        co_return cluster::cloud_metadata::error_outcome::ntp_not_found;
    case cluster::errc::update_in_progress:
    case cluster::errc::not_leader:
        co_return cluster::cloud_metadata::error_outcome::not_ready;
    default:
        vlog(
          cg_klog.error,
          "Unexpected error while snapshotting groups: {}",
          res.error());
        co_return cluster::cloud_metadata::error_outcome::not_ready;
    }
}

ss::future<cluster::get_group_offsets_reply>
group_manager::get_group_offsets(cluster::get_group_offsets_request req) {
    model::ntp ntp{
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      req.co_partition};
    auto snap = co_await do_snapshot_groups(
      ntp, std::numeric_limits<size_t>::max(), std::move(req.groups));
    if (snap.has_value()) {
        vassert(snap.assume_value().size() == 1, "Expected single snapshot");
        co_return cluster::get_group_offsets_reply{
          cluster::errc::success, std::move(snap.assume_value()[0].groups)};
    }
    co_return cluster::get_group_offsets_reply{snap.error(), {}};
}

ss::future<result<std::vector<group_offsets_snapshot>, cluster::errc>>
group_manager::do_snapshot_groups(
  const model::ntp& ntp,
  size_t max_num_groups_per_snap,
  std::optional<chunked_vector<group_id>> group_filter) {
    vlog(
      cg_klog.debug,
      "Snapshotting groups from ntp={}, filter={}",
      ntp,
      group_filter);
    auto it = _partitions.find(ntp);
    if (it == _partitions.end()) {
        co_return cluster::errc::partition_not_exists;
    }
    auto attached_partition = it->second;
    // Avoid overlapping with concurrent reloads of the partition.
    auto units = co_await ss::get_units(
      attached_partition->sem, 1, attached_partition->as);
    auto& catchup = attached_partition->catchup_lock;
    auto read_lock = co_await catchup->hold_read_lock();
    if (!attached_partition->partition->is_leader()) {
        co_return cluster::errc::not_leader;
    }
    if (attached_partition->loading) {
        co_return cluster::errc::update_in_progress;
    }
    // Make a copy of the group lw ptrs that we're about to snapshot, to avoid
    // racing with removals during iteration.
    chunked_vector<std::pair<group_id, group_ptr>> groups;
    auto predicate = [&ntp](const auto& g_pair) {
        const auto& [group_id, group] = g_pair;
        return group->partition()->ntp().tp.partition == ntp.tp.partition;
    };
    if (group_filter) {
        groups.reserve(group_filter->size());
        for (const auto& gid : *group_filter) {
            auto it = _groups.find(gid);
            if (it != _groups.end() && predicate(*it)) {
                groups.push_back(*it);
            } else {
                vlog(cg_klog.warn, "Cannot find group={} in ntp={}", gid, ntp);
            }
        }
    } else {
        std::ranges::copy(
          _groups | std::views::filter(predicate), std::back_inserter(groups));
    }

    std::vector<group_offsets_snapshot> snapshots;
    snapshots.emplace_back();
    auto* cur_snap = &snapshots.back();
    cur_snap->offsets_topic_pid = ntp.tp.partition;
    vlog(cg_klog.debug, "Snapshotting {} groups from {}", groups.size(), ntp);
    for (const auto& [group_id, group] : groups) {
        group_offsets go;
        go.group_id = group_id();
        absl::btree_map<
          model::topic,
          chunked_vector<group_offsets::partition_offset>>
          offsets;
        for (const auto& [tp, o] : group->offsets()) {
            offsets[tp.topic].emplace_back(
              tp.partition, model::offset_cast(o->metadata.offset));
        }
        for (auto& [t, ps] : offsets) {
            go.offsets.emplace_back(t, std::move(ps));
        }
        vlog(
          cg_klog.debug,
          "Snapshotting offsets for {} topics from group {}",
          go.offsets.size(),
          go.group_id);
        if (cur_snap->groups.size() >= max_num_groups_per_snap) {
            // Current snapshot object is too large; roll to a new object.
            snapshots.emplace_back();
            cur_snap = &snapshots.back();
            cur_snap->offsets_topic_pid = ntp.tp.partition;
        }
        cur_snap->groups.emplace_back(std::move(go));
        co_await ss::maybe_yield();
    }
    co_return std::move(snapshots);
}

ss::future<kafka::error_code>
group_manager::recover_offsets(group_offsets_snapshot snap) {
    return do_bulk_write_offsets(std::move(snap), false);
}

ss::future<cluster::set_group_offsets_reply>
group_manager::set_group_offsets(cluster::set_group_offsets_request req) {
    return do_bulk_write_offsets(std::move(req.group_offsets), true)
      .then([](kafka::error_code ec) -> cluster::set_group_offsets_reply {
          switch (ec) {
          case kafka::error_code::none:
              return {.ec = cluster::errc::success};
          case kafka::error_code::coordinator_load_in_progress:
              return {.ec = cluster::errc::update_in_progress};
          case kafka::error_code::coordinator_not_available:
          case kafka::error_code::fenced_instance_id:
          case kafka::error_code::illegal_generation:
          case kafka::error_code::not_coordinator:
          case kafka::error_code::rebalance_in_progress:
          case kafka::error_code::unknown_member_id:
              return {.ec = cluster::errc::concurrent_modification_error};
          case kafka::error_code::not_leader_for_partition:
              return {.ec = cluster::errc::not_leader};
          case kafka::error_code::request_timed_out:
              return {.ec = cluster::errc::timeout};
          case kafka::error_code::unknown_server_error:
          default:
              return {.ec = cluster::errc::unknown_update_interruption_error};
          }
      });
}

ss::future<kafka::error_code>
group_manager::do_bulk_write_offsets(group_offsets_snapshot snap, bool merge) {
    vassert(!snap.groups.empty(), "Group data must not be empty");
    auto offsets_ntp = model::ntp{
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      snap.offsets_topic_pid,
    };
    vlog(
      cg_klog.info,
      "Received request to restore {} groups from snapshot on partition {}",
      snap.groups.size(),
      offsets_ntp);
    auto it = _partitions.find(offsets_ntp);
    if (it == _partitions.end()) {
        co_return kafka::error_code::not_leader_for_partition;
    }
    auto attached_partition = it->second;
    auto units = co_await ss::get_units(
      attached_partition->sem, 1, attached_partition->as);
    if (!attached_partition->partition->is_leader()) {
        co_return kafka::error_code::not_leader_for_partition;
    }
    if (attached_partition->loading) {
        co_return kafka::error_code::coordinator_load_in_progress;
    }
    auto lock = co_await attached_partition->catchup_lock->hold_write_lock();

    vlog(
      cg_klog.info,
      "Proceeding to restore {} groups on {}",
      snap.groups.size(),
      offsets_ntp);
    for (auto& g : snap.groups) {
        offset_commit_request kafka_r;
        kafka_r.ntp = offsets_ntp;
        auto& kafka_data = kafka_r.data;
        kafka_data.group_id = kafka::group_id(g.group_id);
        auto group = get_group(kafka_data.group_id);
        if (group) {
            // The group already exists. Assume that means someone has already
            // begun using ths group, and we can't overwrite the commits since
            // this is a destructive operation.
            vlog(
              cg_klog.info,
              "Restoring group {} from snapshot on {}, group already exists in "
              "state {}, {}",
              kafka_r.data.group_id,
              offsets_ntp,
              group->state(),
              merge ? "committing offsets nevertheless" : "skipping");
            if (!merge) {
                continue;
            }
        }

        auto& kafka_topics = kafka_data.topics;
        for (auto& t : g.offsets) {
            offset_commit_request_topic kafka_t;
            kafka_t.name = model::topic(t.topic);
            for (auto& p : t.partitions) {
                offset_commit_request_partition kafka_p;
                kafka_p.partition_index = p.partition;
                kafka_p.committed_offset = kafka::offset_cast(p.offset);
                kafka_t.partitions.emplace_back(std::move(kafka_p));
            }
            kafka_topics.emplace_back(std::move(kafka_t));
            co_await ss::maybe_yield();
        }
        vlog(
          cg_klog.info,
          "Restoring group {} from snapshot on {}",
          kafka_r.data.group_id,
          offsets_ntp);
        auto stages = offset_commit(std::move(kafka_r));
        co_await std::move(stages.dispatched);
        auto kafka_res = co_await std::move(stages.result);
        error_code first_error = error_code::none;
        for (const auto& kafka_t : kafka_res.data.topics) {
            for (const auto& kafka_p : kafka_t.partitions) {
                if (kafka_p.error_code != kafka::error_code::none) {
                    vlog(
                      cg_klog.warn,
                      "Error on {}/{} while restoring group {} on {}: {}",
                      kafka_t.name,
                      kafka_p.partition_index,
                      kafka_r.data.group_id,
                      offsets_ntp,
                      kafka_p.error_code);
                    if (first_error != error_code::none) {
                        first_error = kafka_p.error_code;
                    }
                }
            }
        }
        if (first_error != error_code::none) {
            co_return first_error;
        }
    }
    co_return error_code::none;
}

ss::future<> group_manager::handle_partition_leader_change(
  model::term_id term,
  ss::lw_shared_ptr<attached_partition> p,
  std::optional<model::node_id> leader_id) {
    if (leader_id != _self.id()) {
        p->loading = false;
        return gc_partition_state(p);
    }

    vlog(cg_klog.trace, "Recovering groups of {}", p->partition->ntp());

    p->loading = true;
    auto timeout
      = ss::lowres_clock::now()
        + config::shard_local_cfg().kafka_group_recovery_timeout_ms();
    /*
     * we just became leader. make sure the log is up-to-date. see
     * struct old::group_log_record_key{} for more details. _catchup_lock
     * is rarely contended we take a writer lock only when leadership
     * changes (infrequent event)
     */
    return p->catchup_lock->hold_write_lock().then(
      [this, term, timeout, p](ss::rwlock::holder unit) {
          return inject_noop(p->partition, timeout)
            .then([this, term, timeout, p](std::error_code error) {
                if (error) {
                    vlog(
                      cg_klog.warn,
                      "error injecting partition {} linearizable barrier - {}",
                      p->partition->ntp(),
                      error.message());
                    return p->partition->raft()->step_down(
                      "unable to recover group");
                }

                /*
                 * the full log is read and deduplicated. the dedupe
                 * processing is based on the record keys, so this code
                 * should be ready to transparently take advantage of
                 * key-based compaction in the future.
                 */
                storage::local_log_reader_config reader_config(
                  p->partition->raft_start_offset(),
                  model::model_limits<model::offset>::max(),
                  std::numeric_limits<size_t>::max(),
                  std::nullopt,
                  std::nullopt,
                  std::nullopt);
                auto expected_to_read = model::prev_offset(
                  p->partition->high_watermark());
                vlog(
                  cg_klog.info,
                  "Recovering group state from {}, offset expected to read {}, "
                  "log offsets: {}, raft protocol state: {}",
                  p->partition->ntp(),
                  expected_to_read,
                  p->partition->log()->offsets(),
                  p->partition->raft()->meta());
                return p->partition->make_local_reader(std::move(reader_config))
                  .then([this, term, p, timeout, expected_to_read](
                          model::record_batch_reader reader) {
                      return std::move(reader)
                        .consume(group_recovery_consumer(p->as), timeout)
                        .then([this, term, p, expected_to_read](
                                group_recovery_consumer_state state) {
                            if (state.last_read_offset < expected_to_read) {
                                vlog(
                                  cg_klog.error,
                                  "error recovering group state from {}. "
                                  "Expected to read up to {} but last offset "
                                  "consumed is equal to {}",
                                  p->partition->ntp(),
                                  expected_to_read,
                                  state.last_read_offset);
                                // force step down to allow other node to
                                // recover group
                                return p->partition->raft()->step_down(
                                  "unable to recover group, short read");
                            }
                            // avoid trying to recover if we stopped the
                            // reader because an abort was requested
                            if (p->as.abort_requested()) {
                                return ss::make_ready_future<>();
                            }
                            return recover_partition(term, p, std::move(state))
                              .then([p] { p->loading = false; });
                        });
                  });
            })
            .finally([unit = std::move(unit)] {});
      });
}

/*
 * TODO: this routine can be improved from a copy vs move perspective, but is
 * rather complicated at the moment to start having to also analyze all the data
 * dependencies that would support optimizing for moves.
 */
ss::future<> group_manager::recover_partition(
  model::term_id term,
  ss::lw_shared_ptr<attached_partition> p,
  group_recovery_consumer_state ctx) {
    /*
     * write the offset retention feature fence. this is done in the background
     * because we need to await the offset retention feature. however, if that
     * is done  inline here then we'll prevent consumer group partition from
     * recovering and operating during a mix-version rolling upgrade.
     */
    if (!ctx.has_offset_retention_feature_fence) {
        vlog(
          cg_klog.info,
          "Scheduling write of offset retention feature fence for partition {}",
          p->partition);
        ssx::spawn_with_gate(
          _gate, [this, term, p] { return write_version_fence(term, p); });
    }

    static constexpr size_t group_batch_size = 64;
    for (auto& [_, group] : _groups) {
        if (group->partition()->ntp() == p->partition->ntp()) {
            group->reset_tx_state(term);
        }
    }
    p->term = term;
    co_await ss::max_concurrent_for_each(
      ctx.groups, group_batch_size, [this, term, p](auto& pair) {
          return do_recover_group(
            term, p, std::move(pair.first), std::move(pair.second));
      });
    p->blocked_groups = std::move(ctx.blocked_groups);
}

ss::future<> group_manager::do_recover_group(
  model::term_id term,
  ss::lw_shared_ptr<attached_partition> p,
  group_id group_id,
  group_stm group_stm) {
    if (group_stm.has_data()) {
        auto group = get_group(group_id);
        vlog(
          cg_klog.info,
          "Recovering {} - {}",
          group_id,
          group_stm.get_metadata());
        for (const auto& member : group_stm.get_metadata().members) {
            vlog(
              cg_klog.debug, "Recovering group {} member {}", group_id, member);
        }

        if (!group) {
            group = ss::make_lw_shared<kafka::group>(
              group_id,
              group_stm.get_metadata(),
              _conf,
              p->catchup_lock,
              p->partition,
              term,
              _tx_frontend,
              _feature_table);
            _groups.emplace(group_id, group);
            group->reschedule_all_member_heartbeats();
        }

        for (auto& [tp, meta] : group_stm.offsets()) {
            const auto expiry_timestamp
              = meta.metadata.expiry_timestamp == model::timestamp(-1)
                  ? std::optional<model::timestamp>(std::nullopt)
                  : meta.metadata.expiry_timestamp;
            group->try_upsert_offset(
              tp,
              group::offset_metadata{
                .log_offset = meta.log_offset,
                .offset = meta.metadata.offset,
                .metadata = meta.metadata.metadata,
                .committed_leader_epoch = meta.metadata.leader_epoch,
                .commit_timestamp = meta.metadata.commit_timestamp,
                .expiry_timestamp = expiry_timestamp,
                .non_reclaimable = meta.metadata.non_reclaimable,
              });
        }
        for (auto& [id, session] : group_stm.producers()) {
            group->try_set_fence(id, session.epoch);
            if (session.tx) {
                auto& tx = *session.tx;
                group::ongoing_transaction group_tx(
                  tx.tx_seq, tx.tm_partition, tx.timeout, tx.begin_offset);
                for (auto& [tp, o_md] : tx.offsets) {
                    group_tx.offsets[tp] = group::pending_tx_offset{
                  .offset_metadata = group_tx::partition_offset{
                    .tp = tp,
                    .offset = o_md.offset,
                    .leader_epoch = o_md.committed_leader_epoch,
                    .metadata = o_md.metadata,
                  },
                  .log_offset = o_md.log_offset};
                }

                group->insert_ongoing_tx(
                  model::producer_identity(id, session.epoch),
                  std::move(group_tx));
            }
        }

        if (group_stm.is_removed()) {
            if (group_stm.offsets().size() > 0) {
                vlog(
                  cg_klog.warn,
                  "Unexpected active group unload {} while loading {}",
                  group_id,
                  p->partition->ntp());
            }
        }
    }
    co_return;
}

ss::future<> group_manager::write_version_fence(
  model::term_id term, ss::lw_shared_ptr<attached_partition> p) {
    // how long to delay retrying a fence write if an error occurs
    constexpr auto fence_write_retry_delay = 10s;

    co_await _feature_table.local().await_feature(
      features::feature::group_offset_retention, p->as);

    while (true) {
        if (p->as.abort_requested() || _gate.is_closed()) {
            break;
        }

        // cluster v9 is where offset retention is enabled
        auto batch = _feature_table.local().encode_version_fence(
          to_cluster_version(features::release_version::v23_1_1));

        try {
            auto result = co_await p->partition->raft()->replicate(
              std::move(batch),
              raft::replicate_options(
                raft::consistency_level::quorum_ack, term));

            if (result) {
                vlog(
                  cg_klog.info,
                  "Prepared partition {} for consumer offset retention feature "
                  "during upgrade",
                  p->partition->ntp());
                co_return;

            } else if (result.error() == raft::errc::shutting_down) {
                vlog(
                  cg_klog.debug,
                  "Cannot write offset retention version fence for partition "
                  "{}: shutting down",
                  p->partition->ntp());
                co_return;

            } else if (result.error() == raft::errc::not_leader) {
                vlog(
                  cg_klog.debug,
                  "Cannot write offset retention version fence for partition "
                  "{}: not leader",
                  p->partition->ntp());
                co_return;

            } else {
                vlog(
                  cg_klog.warn,
                  "Could not write offset retention feature fence for "
                  "partition {}: {} {}",
                  p->partition->ntp(),
                  result.error().message(),
                  result.error());
            }
        } catch (const ss::gate_closed_exception&) {
            vlog(
              cg_klog.debug,
              "Cannot write offset retention version fence for partition {}: "
              "partition shutting down",
              p->partition->ntp());
            co_return;

        } catch (const ss::abort_requested_exception&) {
            vlog(
              cg_klog.debug,
              "Cannot write offset retention version fence for partition {}: "
              "partition abort requested",
              p->partition->ntp());
            co_return;

        } catch (...) {
            vlog(
              cg_klog.error,
              "Exception occurred writing offset retention feature fence for "
              "partition {}: {}",
              p->partition,
              std::current_exception());
        }

        co_await ss::sleep_abortable(fence_write_retry_delay, p->as);
    }
}

group::join_group_stages group_manager::join_group(join_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, join_group_api::key, false);
    if (error != error_code::none) {
        return group::join_group_stages(
          make_join_error(r.data.member_id, error));
    }

    if (
      r.data.session_timeout_ms < _conf.group_min_session_timeout_ms()
      || r.data.session_timeout_ms > _conf.group_max_session_timeout_ms()) {
        vlog(
          cg_klog.trace,
          "Join group {} rejected for invalid session timeout {} valid range "
          "[{},{}]. Request {}",
          r.data.group_id,
          _conf.group_min_session_timeout_ms(),
          r.data.session_timeout_ms,
          _conf.group_max_session_timeout_ms(),
          r);

        return group::join_group_stages(make_join_error(
          r.data.member_id, error_code::invalid_session_timeout));
    }

    bool is_new_group = false;
    auto group = get_group(r.data.group_id);
    if (!group) {
        // <kafka>only try to create the group if the group is UNKNOWN AND
        // the member id is UNKNOWN, if member is specified but group does
        // not exist we should reject the request.</kafka>
        if (r.data.member_id != unknown_member_id) {
            vlog(
              cg_klog.trace,
              "Join group {} rejected for known member {} joining unknown "
              "group. Request {}",
              r.data.group_id,
              r.data.member_id,
              r);

            return group::join_group_stages(
              make_join_error(r.data.member_id, error_code::unknown_member_id));
        }
        auto it = _partitions.find(r.ntp);
        if (it == _partitions.end()) {
            // the ntp's partition was available because we had to route the
            // request to the correct core, but when we looked again it was
            // gone. this is generally not going to be a scenario that can
            // happen until we have rebalancing / partition deletion feature.
            vlog(
              cg_klog.trace,
              "Join group {} rejected for unavailable ntp {}",
              r.data.group_id,
              r.ntp);
            return group::join_group_stages(
              make_join_error(r.data.member_id, error_code::not_coordinator));
        }
        auto p = it->second->partition;
        group = ss::make_lw_shared<kafka::group>(
          r.data.group_id,
          group_state::empty,
          _conf,
          it->second->catchup_lock,
          p,
          it->second->term,
          _tx_frontend,
          _feature_table);
        _groups.emplace(r.data.group_id, group);
        _groups.rehash(0);
        is_new_group = true;
        vlog(
          cg_klog.trace, "Created new group {} while joining", r.data.group_id);
    }

    auto ret = group->handle_join_group(std::move(r), is_new_group);
    return group::join_group_stages(
      ret.dispatched.finally([group] {}), ret.result.finally([group] {}));
}

group::sync_group_stages group_manager::sync_group(sync_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, sync_group_api::key, false);
    if (error != error_code::none) {
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>The coordinator is loading, which means we've lost the
            // state of the active rebalance and the group will need to start
            // over at JoinGroup. By returning rebalance in progress, the
            // consumer will attempt to rejoin without needing to rediscover the
            // coordinator. Note that we cannot return
            // COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect
            // the error.</kafka>
            return group::sync_group_stages(
              sync_group_response(error_code::rebalance_in_progress));
        }
        return group::sync_group_stages(sync_group_response(error));
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        auto stages = group->handle_sync_group(std::move(r));
        return group::sync_group_stages(
          stages.dispatched.finally([group] {}),
          stages.result.finally([group] {}));
    } else {
        vlog(
          cg_klog.trace,
          "Cannot handle sync group request for unknown group {}",
          r.data.group_id);
        return group::sync_group_stages(
          sync_group_response(error_code::unknown_member_id));
    }
}

ss::future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, heartbeat_api::key, false);
    if (error != error_code::none) {
        if (error == error_code::coordinator_load_in_progress) {
            // <kafka>the group is still loading, so respond just
            // blindly</kafka>
            return make_heartbeat_error(error_code::none);
        }
        return make_heartbeat_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_heartbeat(std::move(r)).finally([group] {});
    }

    vlog(
      cg_klog.trace,
      "Cannot handle heartbeat request for unknown group {}",
      r.data.group_id);

    return make_heartbeat_error(error_code::unknown_member_id);
}

ss::future<leave_group_response>
group_manager::leave_group(leave_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, leave_group_api::key, false);
    if (error != error_code::none) {
        return make_leave_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_leave_group(std::move(r)).finally([group] {});
    } else {
        vlog(
          cg_klog.trace,
          "Cannot handle leave group request for unknown group {}",
          r.data.group_id);
        if (r.version < api_version(3)) {
            return make_leave_error(error_code::unknown_member_id);
        }
        // since version 3 we need to fill each member error code
        leave_group_response response;
        response.data.members.reserve(r.data.members.size());
        std::transform(
          r.data.members.begin(),
          r.data.members.end(),
          std::back_inserter(response.data.members),
          [](member_identity& mid) {
              return member_response{
                .member_id = std::move(mid.member_id),
                .group_instance_id = std::move(mid.group_instance_id),
                .error_code = error_code::unknown_member_id,
              };
          });
        return ss::make_ready_future<leave_group_response>(std::move(response));
    }
}

ss::future<txn_offset_commit_response>
group_manager::txn_offset_commit(txn_offset_commit_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(r, error_code::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(
            r, error_code::coordinator_load_in_progress));
    }
    // TODO: use correct key instead of offset_commit_api::key
    // check other txn places
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(r, error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        // <kafka>the group is not relying on Kafka for group
        // management, so allow the commit</kafka>

        group = ss::make_lw_shared<kafka::group>(
          r.data.group_id,
          group_state::empty,
          _conf,
          p->catchup_lock,
          p->partition,
          p->term,
          _tx_frontend,
          _feature_table);
        _groups.emplace(r.data.group_id, group);
        _groups.rehash(0);
    }

    return group->handle_txn_offset_commit(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<cluster::commit_group_tx_reply>
group_manager::commit_tx(cluster::commit_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(cluster::tx::errc::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(
            cluster::tx::errc::coordinator_load_in_progress));
    }
    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        if (error == error_code::not_coordinator) {
            return ss::make_ready_future<cluster::commit_group_tx_reply>(
              make_commit_tx_reply(cluster::tx::errc::not_coordinator));
        } else {
            return ss::make_ready_future<cluster::commit_group_tx_reply>(
              make_commit_tx_reply(cluster::tx::errc::timeout));
        }
    }

    auto group = get_group(r.group_id);
    if (!group) {
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(cluster::tx::errc::timeout));
    }

    return group->handle_commit_tx(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<cluster::begin_group_tx_reply>
group_manager::begin_tx(cluster::begin_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(cluster::tx::errc::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(cluster::tx::errc::coordinator_load_in_progress));
    }

    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        auto ec = error == error_code::not_coordinator
                    ? cluster::tx::errc::not_coordinator
                    : cluster::tx::errc::timeout;
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(ec));
    }

    auto group = get_group(r.group_id);
    if (!group) {
        group = ss::make_lw_shared<kafka::group>(
          r.group_id,
          group_state::empty,
          _conf,
          p->catchup_lock,
          p->partition,
          p->term,
          _tx_frontend,
          _feature_table);
        _groups.emplace(r.group_id, group);
        _groups.rehash(0);
    }

    return group->handle_begin_tx(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

ss::future<cluster::abort_group_tx_reply>
group_manager::abort_tx(cluster::abort_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->partition->is_leader()) {
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx::errc::not_coordinator));
    }
    auto maybe_holder = p->catchup_lock->try_hold_read_lock();
    if (!maybe_holder) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx::errc::coordinator_load_in_progress));
    }

    auto error = validate_group_status(
      r.ntp, r.group_id, offset_commit_api::key, true);
    if (error != error_code::none) {
        auto ec = error == error_code::not_coordinator
                    ? cluster::tx::errc::not_coordinator
                    : cluster::tx::errc::timeout;
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(ec));
    }

    auto group = get_group(r.group_id);
    if (!group) {
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx::errc::timeout));
    }

    return group->handle_abort_tx(std::move(r))
      .finally([unit = std::move(*maybe_holder), group] {});
}

group::offset_commit_stages
group_manager::offset_commit(offset_commit_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_commit_api::key, false);
    if (error != error_code::none) {
        return group::offset_commit_stages(offset_commit_response(r, error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        if (r.data.generation_id < 0) {
            // <kafka>the group is not relying on Kafka for group management, so
            // allow the commit</kafka>
            auto p = _partitions.find(r.ntp)->second;
            group = ss::make_lw_shared<kafka::group>(
              r.data.group_id,
              group_state::empty,
              _conf,
              p->catchup_lock,
              p->partition,
              p->term,
              _tx_frontend,
              _feature_table);
            _groups.emplace(r.data.group_id, group);
            _groups.rehash(0);
        } else {
            // <kafka>or this is a request coming from an older generation.
            // either way, reject the commit</kafka>
            return group::offset_commit_stages(
              offset_commit_response(r, error_code::illegal_generation));
        }
    }

    auto stages = group->handle_offset_commit(std::move(r));
    stages.result = stages.result.finally([group] {});
    return stages;
}

ss::future<offset_fetch_response>
group_manager::offset_fetch(offset_fetch_request r) {
    const auto require_stable = r.data.require_stable;
    offset_fetch_response response;

    for (auto& g_req : r.data.groups) {
        auto& g_res = response.data.groups.emplace_back();
        auto error = validate_group_status(
          r.ntp, g_req.group_id, offset_fetch_api::key, true);
        if (error != error_code::none) {
            g_res.group_id = std::move(g_req.group_id);
            g_res.error_code = error;
            continue;
        }

        auto group = get_group(g_req.group_id);
        if (!group) {
            g_res = offset_fetch_response::make_group(std::move(g_req));
        } else {
            g_res = co_await group->handle_offset_fetch(
              std::move(g_req), require_stable);
        }
    }
    co_return response;
}

ss::future<offset_delete_response>
group_manager::offset_delete(offset_delete_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_delete_api::key, false);
    if (error != error_code::none) {
        co_return offset_delete_response(error);
    }

    auto group = get_group(r.data.group_id);
    if (!group || group->in_state(group_state::dead)) {
        co_return offset_delete_response(error_code::group_id_not_found);
    }

    if (!group->in_state(group_state::empty) && !group->is_consumer_group()) {
        co_return offset_delete_response(error_code::non_empty_group);
    }

    chunked_vector<model::topic_partition> requested_deletions;
    for (const auto& topic : r.data.topics) {
        for (const auto& partition : topic.partitions) {
            requested_deletions.emplace_back(
              topic.name, partition.partition_index);
        }
    }

    auto deleted_offsets = group->delete_offsets(requested_deletions);
    co_await delete_offsets(group, deleted_offsets);

    chunked_hash_set<model::topic_partition> deleted_offsets_set;
    for (auto& tp : deleted_offsets) {
        deleted_offsets_set.insert(std::move(tp));
    }

    absl::flat_hash_map<
      model::topic,
      chunked_vector<offset_delete_response_partition>>
      response_data;
    for (const auto& tp : requested_deletions) {
        auto error = kafka::error_code::none;
        if (!deleted_offsets_set.contains(tp)) {
            error = kafka::error_code::group_subscribed_to_topic;
        }
        response_data[tp.topic].emplace_back(
          offset_delete_response_partition{
            .partition_index = tp.partition, .error_code = error});
    }

    offset_delete_response response(kafka::error_code::none);
    for (auto& [t, ps] : response_data) {
        response.data.topics.emplace_back(
          offset_delete_response_topic{.name = t, .partitions = std::move(ps)});
    }
    co_return response;
}

std::pair<error_code, chunked_vector<listed_group>>
group_manager::list_groups(const list_groups_filter_data& filter_data) const {
    auto loading = std::any_of(
      _partitions.cbegin(),
      _partitions.cend(),
      [](
        const std::
          pair<const model::ntp, ss::lw_shared_ptr<attached_partition>>& p) {
          return p.second->loading;
      });

    chunked_vector<listed_group> groups;
    for (const auto& it : _groups) {
        const auto& g = it.second;

        auto no_filter_specified = filter_data.states_filter.empty();
        auto matches_filter = filter_data.states_filter.contains(g->state());

        if (no_filter_specified || matches_filter) {
            groups.push_back(
              {g->id(),
               g->protocol_type().value_or(protocol_type()),
               group_state_to_kafka_name(g->state())});
        }
    }

    auto error = loading ? error_code::coordinator_load_in_progress
                         : error_code::none;

    return std::make_pair(error, std::move(groups));
}

described_group
group_manager::describe_group(const model::ntp& ntp, const kafka::group_id& g) {
    auto error = validate_group_status(ntp, g, describe_groups_api::key, true);
    if (error != error_code::none) {
        return describe_groups_response::make_empty_described_group(g, error);
    }

    auto group = get_group(g);
    if (!group) {
        return describe_groups_response::make_dead_described_group(g);
    }

    return group->describe();
}

group_manager::partition_producers
group_manager::describe_partition_producers(const model::ntp& ntp) {
    vlog(cg_klog.debug, "describe producers: {}", ntp);
    partition_producers response;
    response.partition_index = ntp.tp.partition;
    auto it = _partitions.find(ntp);
    if (it == _partitions.end() || !it->second->partition->is_leader()) {
        response.error_code = error_code::not_leader_for_partition;
        return response;
    }
    response.error_code = kafka::error_code::none;
    // snapshot the list of groups attached to this partition
    chunked_vector<std::pair<group_id, group_ptr>> groups;
    std::copy_if(
      _groups.begin(),
      _groups.end(),
      std::back_inserter(groups),
      [&ntp](const auto& g_pair) {
          const auto& [group_id, group] = g_pair;
          return group->partition()->ntp() == ntp;
      });
    for (auto& [gid, group] : groups) {
        if (group->in_state(group_state::dead)) {
            continue;
        }
        auto partition = group->partition();
        if (!partition) {
            // unlikely, conservative check
            continue;
        }
        for (const auto& [id, state] : group->producers()) {
            auto& tx = state.transaction;
            int64_t start_offset = -1;
            if (tx && tx->begin_offset >= model::offset{0}) {
                start_offset = partition->get_offset_translator_state()
                                 ->from_log_offset(tx->begin_offset);
            }
            int64_t last_timetamp = -1;
            if (tx) {
                auto time_since_last_update = model::timeout_clock::now()
                                              - tx->last_update;
                auto last_update_ts
                  = (model::timestamp_clock::now() - time_since_last_update);
                last_timetamp = last_update_ts.time_since_epoch() / 1ms;
            }
            response.active_producers.push_back({
              .producer_id = id,
              .producer_epoch = state.epoch,
              .last_sequence = tx ? tx->tx_seq : -1,
              .last_timestamp = last_timetamp,
              .coordinator_epoch = -1,
              .current_txn_start_offset = start_offset,
            });
        }
    }
    return response;
}

ss::future<std::error_code> group_manager::empty_and_delete_groups(
  const model::ntp& ntp, const chunked_vector<group_id>& groups) {
    co_await ssx::async_for_each(groups, [this, &ntp](const group_id& group) {
        auto g = get_group(group);
        if (!g) {
            vlog(cg_klog.warn, "Group {} not found on ntp {}", group, ntp);
            return;
        }
        g->remove_full_members();
    });

    chunked_vector<std::pair<model::ntp, group_id>> groups_with_ntps{
      std::from_range,
      groups | std::views::transform([&ntp](const group_id& group_id) {
          return std::make_pair(ntp, group_id);
      })};
    auto delete_results = co_await delete_groups(std::move(groups_with_ntps));
    auto codes = std::views::transform(
      delete_results, &deletable_group_result::error_code);
    auto first_bad_code = std::ranges::find_if(codes, [](const auto& ec) {
        return ec != kafka::error_code::none
               && ec != kafka::error_code::group_id_not_found;
    });
    if (first_bad_code == codes.end()) {
        co_return std::error_code{};
    }
    co_return std::error_code{*first_bad_code};
}

ss::future<chunked_vector<deletable_group_result>> group_manager::delete_groups(
  chunked_vector<std::pair<model::ntp, group_id>> groups) {
    chunked_vector<deletable_group_result> results;

    for (auto& group_info : groups) {
        auto error = validate_group_status(
          group_info.first, group_info.second, delete_groups_api::key, false);
        if (error != error_code::none) {
            results.push_back(
              deletable_group_result{
                .group_id = std::move(group_info.second),
                .error_code = error_code::not_coordinator,
              });
            continue;
        }

        auto group = get_group(group_info.second);
        if (!group) {
            results.push_back(
              deletable_group_result{
                .group_id = std::move(group_info.second),
                .error_code = error_code::group_id_not_found,
              });
            continue;
        }

        // TODO: future optimizations
        // - handle group deletions in parallel
        // - batch tombstones same backing partition
        error = co_await group->remove();
        if (error == error_code::none) {
            group->pre_shutdown();
            _groups.erase(group_info.second);
        }
        results.push_back(
          deletable_group_result{
            .group_id = std::move(group_info.second),
            .error_code = error,
          });
    }

    _groups.rehash(0);

    co_return std::move(results);
}

bool group_manager::valid_group_id(const group_id& group, api_key api) {
    switch (api) {
    case describe_groups_api::key:
    case offset_commit_api::key:
    case delete_groups_api::key:
        [[fallthrough]];
    case offset_fetch_api::key:
        // <kafka> For backwards compatibility, we support the offset commit
        // APIs for the empty groupId, and also in DescribeGroups and
        // DeleteGroups so that users can view and delete state of all
        // groups.</kafka>
        return true;

    // join-group etc... require non-empty group ids
    default:
        return !group().empty();
    }
}

/*
 * TODO
 * - check for group being shutdown
 */
error_code group_manager::validate_group_status(
  const model::ntp& ntp,
  const group_id& group,
  api_key api,
  bool allow_blocked) const {
    if (!valid_group_id(group, api)) {
        vlog(
          cg_klog.debug,
          "Group name {} is invalid for operation {}",
          group,
          api);
        return error_code::invalid_group_id;
    }

    if (const auto it = _partitions.find(ntp); it != _partitions.end()) {
        auto& p = it->second;
        if (unlikely(!allow_blocked && p->blocked_groups.contains(group))) {
            return error_code::invalid_group_id;
        }

        if (!p->partition->is_leader()) {
            vlog(
              cg_klog.debug,
              "Group {} operation {} sent to non-leader coordinator {}",
              group,
              api,
              ntp);
            return error_code::not_coordinator;
        }
        /**
         * Check if term changed, this can happen if a node that stepped down
         * became a leader again.
         */
        if (p->partition->term() != p->term()) {
            vlog(
              cg_klog.info,
              "Group {} operation {} for partition {} processed while term "
              "changed. "
              "Group term: {} current term: {}",
              group,
              api,
              ntp,
              p->term(),
              p->partition->term());
            return error_code::not_coordinator;
        }

        if (p->loading) {
            vlog(
              cg_klog.debug,
              "Group {} operation {} sent to loading coordinator {}",
              group,
              api,
              ntp);
            return error_code::not_coordinator;
            /*
             * returning `load in progress` is the correct error code for this
             * condition, and is what kafka brokers return. it should cause a
             * client to retry with backoff. however, it seems to be a rare
             * error condition in kafka (not in redpanda) and the sarama client
             * does not check for it (java and python both check properly).
             * sarama checks for `not coordinator` which does a metadata refresh
             * and a retry. it causes a bit more work in the client, but
             * achieves the same end result.
             *
             * See https://github.com/Shopify/sarama/issues/1715
             */
            // return error_code::coordinator_load_in_progress;
        }

        return error_code::none;
    }

    vlog(
      cg_klog.debug,
      "Group {} operation {} misdirected to non-coordinator {}",
      group,
      api,
      ntp);
    return error_code::not_coordinator;
}

ss::future<cluster::get_producers_reply>
group_manager::get_group_producers_locally(
  cluster::get_producers_request request) {
    const auto& ntp = request.ntp;
    cluster::get_producers_reply reply;
    auto it = _partitions.find(ntp);
    if (it == _partitions.end() || !it->second->partition->is_leader()) {
        reply.error_code = cluster::tx::errc::not_coordinator;
        co_return reply;
    }
    auto attached_partition = *it;
    reply.error_code = cluster::tx::errc::none;
    // snapshot the list of groups attached to this partition
    chunked_hash_map<group_id, group_ptr> groups;
    std::copy_if(
      _groups.begin(),
      _groups.end(),
      std::inserter(groups, groups.end()),
      [&ntp](auto g_pair) {
          const auto& [group_id, group] = g_pair;
          return group->partition()->ntp() == ntp;
      });
    reply.producer_count = std::accumulate(
      groups.begin(),
      groups.end(),
      size_t(0),
      [](size_t acc, const auto& entry) {
          return acc + entry.second->producers().size();
      });
    for (auto& [gid, group] : groups) {
        if (reply.producers.size() >= request.max_producers_to_include) {
            break;
        }
        if (group->in_state(group_state::dead)) {
            continue;
        }
        auto partition = group->partition();
        if (!partition) {
            // unlikely, conservative check
            continue;
        }
        for (const auto& [id, state] : group->producers()) {
            if (reply.producers.size() >= request.max_producers_to_include) {
                break;
            }
            cluster::producer_state_info producer_info;
            producer_info.pid = {id, state.epoch};
            producer_info.group_id = group->id()();
            auto& tx = state.transaction;
            if (tx) {
                producer_info.tx_begin_offset = tx->begin_offset;
                producer_info.tx_seq = tx->tx_seq;
                producer_info.tx_timeout = tx->timeout;
                auto time_since_last_update = model::timeout_clock::now()
                                              - tx->last_update;
                auto last_update_ts = model::timestamp_clock::now()
                                      - time_since_last_update;
                producer_info.last_update = model::timestamp{
                  last_update_ts.time_since_epoch() / 1ms};
                producer_info.coordinator_partition = tx->coordinator_partition;
            }
            reply.producers.push_back(std::move(producer_info));
        }
    }

    // check if there any any additional (stale) groups being tracked by
    // the stm, the list should be empty in most cases unless there is
    // a divergence in state.
    auto partition = attached_partition.second->partition;
    auto stm
      = partition->raft()->stm_manager()->get<kafka::group_tx_tracker_stm>();
    if (!stm) {
        co_return reply;
    }
    const auto& stm_txes = stm->inflight_transactions();
    for (const auto& [gid, state] : stm_txes) {
        if (groups.contains(gid)) {
            continue;
        }
        // we don't enforce size limits here because this list is expected to be
        // small. stale group found, report it to the dbug output.
        for (const auto& [pid, state] : state.producer_states) {
            reply.producers.push_back({
              .pid = pid,
              .tx_begin_offset = state.begin_offset,
              .group_id = gid() + "-stale",
            });
        }
    }
    co_return reply;
}

ss::future<> group_manager::collect_consumer_lag_metrics() {
    vlog(cg_klog.trace, "group_manager::collect_consumer_lag_metrics");
    vassert(
      ss::this_shard_id() == cluster::health_monitor_backend_shard,
      "collect_consumer_lag_metrics must run on shard {}",
      cluster::health_monitor_backend_shard);

    using lag = size_t;
    using topic_map_t = cluster::partitions_filter::topic_map_t;

    constexpr auto collect_ntps = [](const auto& gm) {
        topic_map_t topic_map;
        for (const auto& group : gm._groups | std::views::values) {
            for (const auto& tp : group->offsets() | std::views::keys) {
                topic_map[tp.topic].insert(tp.partition);
            }
        }
        return topic_map;
    };

    constexpr auto reduce_ntps = [](topic_map_t acc, topic_map_t val) {
        for (auto& [topic, parts] : val) {
            acc[topic].insert(parts.begin(), parts.end());
        }
        return acc;
    };

    auto ntps = co_await container().map_reduce0(
      collect_ntps, topic_map_t{}, reduce_ntps);

    if (ntps.empty()) {
        co_return;
    }

    auto report_r = co_await _hm_frontend.local().get_cluster_health(
      {.node_report_filter{.ntp_filters{
        .namespaces = {{model::kafka_namespace, std::move(ntps)}}}}},
      cluster::force_refresh::no,
      model::timeout_clock::now() + _lag_collection_interval());
    if (!report_r) {
        vlog(
          klog.warn,
          "group_manager::collect_consumer_lag_metrics: "
          "failed to get cluster health report: {}",
          report_r.error());
        co_return;
    }

    static constexpr auto find_partition_hwm =
      [](
        const cluster::cluster_health_report& response,
        const model::topic_partition& tp) -> std::optional<kafka::offset> {
        std::optional<kafka::offset> max_hwm;
        for (const auto& report : response.node_reports) {
            const model::topic_namespace_view tn{
              model::kafka_namespace, tp.topic};
            auto topic_it = report->topics.find(tn);
            if (topic_it == report->topics.end()) {
                continue;
            }
            auto partition_it = topic_it->second.find(tp.partition);
            if (partition_it == topic_it->second.end()) {
                continue;
            }
            auto hwm = partition_it->second.high_watermark;
            if (!max_hwm || hwm > *max_hwm) {
                max_hwm = hwm;
            }
        }
        return max_hwm;
    };

    const auto set_metrics = [&report_r](const group_manager& gm) {
        for (const auto& group : gm._groups | std::views::values) {
            consumer_lag_metrics lag_metrics{};
            for (const auto& [tp, group_topic_offsets] : group->offsets()) {
                if (auto hwm = find_partition_hwm(report_r.value(), tp); hwm) {
                    auto committed_offset = offset_cast(
                      group_topic_offsets->metadata.offset);
                    lag part_lag{static_cast<lag>(
                      std::max(*hwm - committed_offset, offset{0}))};
                    lag_metrics.sum += part_lag;
                    lag_metrics.max = std::max(lag_metrics.max, part_lag);
                }
            }
            group->set_lag_metrics(lag_metrics);
        }
    };

    co_await container().invoke_on_all(set_metrics);
}

} // namespace kafka
