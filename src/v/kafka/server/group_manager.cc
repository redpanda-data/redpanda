// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/group_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/partition_manager.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "kafka/protocol/delete_groups.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_delete.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_recovery_consumer.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "raft/types.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

namespace kafka {

group_manager::group_manager(
  model::topic_namespace tp_ns,
  ss::sharded<raft::group_manager>& gm,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& topic_table,
  ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
  ss::sharded<features::feature_table>& feature_table,
  group_metadata_serializer_factory serializer_factory,
  enable_group_metrics enable_metrics)
  : _tp_ns(std::move(tp_ns))
  , _gm(gm)
  , _pm(pm)
  , _topic_table(topic_table)
  , _tx_frontend(tx_frontend)
  , _feature_table(feature_table)
  , _serializer_factory(std::move(serializer_factory))
  , _conf(config::shard_local_cfg())
  , _self(cluster::make_self_broker(config::node()))
  , _enable_group_metrics(enable_metrics)
  , _offset_retention_check(_conf.group_offset_retention_check_ms.bind()) {}

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
      _tp_ns.ns, _tp_ns.tp, [this](model::partition_id p_id) {
          detach_partition(model::ntp(_tp_ns.ns, _tp_ns.tp, p_id));
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
      = _topic_table.local().register_delta_notification(
        [this](cluster::topic_table::delta_range_t deltas) {
            handle_topic_delta(deltas);
        });

    /*
     * periodically remove expired group offsets.
     */
    _timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return handle_offset_expiration().finally([this] {
                if (!_gate.is_closed()) {
                    _timer.arm(_offset_retention_check());
                }
            });
        });
    });
    _timer.arm(_offset_retention_check());

    /*
     * reschedule periodic collection of expired offsets when the configured
     * frequency changes. useful if it were accidentally configured to be much
     * longer than desired / reasonable, and then fixed (e.g. 1 year vs 1 day).
     */
    _offset_retention_check.watch([this] {
        if (_timer.armed()) {
            _timer.cancel();
            _timer.arm(_offset_retention_check());
        }
    });

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
          klog.info,
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
    fragmented_vector<group_ptr> groups;
    for (auto& group : _groups) {
        groups.push_back(group.second);
    }

    size_t total = 0;
    co_await ss::max_concurrent_for_each(
      groups,
      max_concurrent_expirations,
      [this, &total, retention_period = retention_period.value()](auto group) {
          return delete_expired_offsets(group, retention_period)
            .then([&total](auto removed) { total += removed; });
      });

    if (total) {
        vlog(
          klog.info, "Removed {} offsets from {} groups", total, groups.size());
    }
}

ss::future<size_t> group_manager::delete_expired_offsets(
  group_ptr group, std::chrono::seconds retention_period) {
    /*
     * delete expired offsets from the group
     */
    auto offsets = group->delete_expired_offsets(retention_period);
    return delete_offsets(group, std::move(offsets));
}

ss::future<size_t> group_manager::delete_offsets(
  group_ptr group, std::vector<model::topic_partition> offsets) {
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
          klog.trace,
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
                  klog.trace,
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
     * replicate tombstone records to the group's partition. avoid acks=all
     * because the process is largely best-effort and the in-memory state was
     * already cleaned up.
     */
    auto batch = std::move(builder).build();
    auto reader = model::make_memory_record_batch_reader(std::move(batch));

    try {
        auto result = co_await group->partition()->raft()->replicate(
          group->term(),
          std::move(reader),
          raft::replicate_options(raft::consistency_level::leader_ack));

        if (result) {
            vlog(
              klog.debug,
              "Wrote {} tombstone records for group {} expired offsets",
              offsets.size(),
              group);
            co_return offsets.size();

        } else if (result.error() == raft::errc::shutting_down) {
            vlog(
              klog.debug,
              "Cannot replicate tombstone records for group {}: shutting down",
              group);

        } else if (result.error() == raft::errc::not_leader) {
            vlog(
              klog.debug,
              "Cannot replicate tombstone records for group {}: not leader",
              group);

        } else {
            vlog(
              klog.error,
              "Cannot replicate tombstone records for group {}: {} {}",
              group,
              result.error().message(),
              result.error());
        }

    } catch (...) {
        vlog(
          klog.error,
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
    _topic_table.local().unregister_delta_notification(
      _topic_table_notify_handle);

    for (auto& e : _partitions) {
        e.second->as.request_abort();
    }

    _timer.cancel();

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
    klog.debug("detaching group metadata partition {}", ntp);
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
    auto units = co_await p->catchup_lock.hold_write_lock();

    // Becasue shutdown group is async operation we should run it after
    // rehash for groups map
    std::vector<group_ptr> groups_for_shutdown;
    for (auto g_it = _groups.begin(); g_it != _groups.end();) {
        if (g_it->second->partition()->ntp() == p->partition->ntp()) {
            groups_for_shutdown.push_back(g_it->second);
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
    klog.debug("attaching group metadata partition {}", p->ntp());
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
  const std::vector<model::topic_partition>& tps) {
    // operate on a light-weight copy of group pointers to avoid iterating over
    // the main index which is subject to concurrent modification.
    std::vector<group_ptr> groups;
    groups.reserve(_groups.size());
    for (auto& group : _groups) {
        groups.push_back(group.second);
    }

    return ss::do_with(
      std::move(groups), [this, &tps](std::vector<group_ptr>& groups) {
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
                    vlog(klog.trace, "Removed group {}", g);
                    _groups.erase(it);
                    _groups.rehash(0);
                    return ss::now();
                });
          });
      });
}

void group_manager::handle_topic_delta(
  cluster::topic_table::delta_range_t deltas) {
    // topic-partition deletions in the kafka namespace are the only deltas that
    // are relevant to the group manager
    std::vector<model::topic_partition> tps;
    for (const auto& delta : deltas) {
        if (
          delta.type == cluster::topic_table_delta::op_type::del
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
                [this](const std::vector<model::topic_partition>& tps) {
                    return cleanup_removed_topic_partitions(tps);
                });
          })
          .handle_exception([](std::exception_ptr e) {
              vlog(klog.warn, "Topic clean-up encountered error: {}", e);
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

ss::future<> group_manager::inject_noop(
  ss::lw_shared_ptr<cluster::partition> p,
  [[maybe_unused]] ss::lowres_clock::time_point timeout) {
    auto dirty_offset = p->dirty_offset();
    auto barrier_offset = co_await p->linearizable_barrier();
    // synchronization provided by raft after future resolves is sufficient to
    // get an up-to-date commit offset as an upperbound for our reader.
    while (barrier_offset.has_value()
           && barrier_offset.value() < dirty_offset) {
        barrier_offset = co_await p->linearizable_barrier();
    }
}

ss::future<>
group_manager::gc_partition_state(ss::lw_shared_ptr<attached_partition> p) {
    /**
     * since this operation is destructive for partitions group we hold a
     * catchup write lock
     */

    auto units = co_await p->catchup_lock.hold_write_lock();

    // Becasue shutdown group is async operation we should run it after rehash
    // for groups map
    std::vector<group_ptr> groups_for_shutdown;
    for (auto it = _groups.begin(); it != _groups.end();) {
        if (it->second->partition()->ntp() == p->partition->ntp()) {
            groups_for_shutdown.push_back(it->second);
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

ss::future<> group_manager::handle_partition_leader_change(
  model::term_id term,
  ss::lw_shared_ptr<attached_partition> p,
  std::optional<model::node_id> leader_id) {
    if (leader_id != _self.id()) {
        p->loading = false;
        return gc_partition_state(p);
    }

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
    return p->catchup_lock.hold_write_lock()
      .then([this, term, timeout, p](ss::basic_rwlock<>::holder unit) {
          return inject_noop(p->partition, timeout)
            .then([this, term, timeout, p] {
                /*
                 * the full log is read and deduplicated. the dedupe
                 * processing is based on the record keys, so this code
                 * should be ready to transparently take advantage of
                 * key-based compaction in the future.
                 */
                storage::log_reader_config reader_config(
                  p->partition->start_offset(),
                  model::model_limits<model::offset>::max(),
                  0,
                  std::numeric_limits<size_t>::max(),
                  kafka_read_priority(),
                  std::nullopt,
                  std::nullopt,
                  std::nullopt);

                return p->partition->make_reader(reader_config)
                  .then([this, term, p, timeout](
                          model::record_batch_reader reader) {
                      return std::move(reader)
                        .consume(
                          group_recovery_consumer(_serializer_factory(), p->as),
                          timeout)
                        .then([this, term, p](
                                group_recovery_consumer_state state) {
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
      })
      .finally([p] {});
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
          klog.info,
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
}

ss::future<> group_manager::do_recover_group(
  model::term_id term,
  ss::lw_shared_ptr<attached_partition> p,
  group_id group_id,
  group_stm group_stm) {
    if (group_stm.has_data()) {
        auto group = get_group(group_id);
        vlog(
          klog.info, "Recovering {} - {}", group_id, group_stm.get_metadata());
        for (const auto& member : group_stm.get_metadata().members) {
            vlog(klog.debug, "Recovering group {} member {}", group_id, member);
        }

        if (!group) {
            group = ss::make_lw_shared<kafka::group>(
              group_id,
              group_stm.get_metadata(),
              _conf,
              p->partition,
              _tx_frontend,
              _feature_table,
              _serializer_factory(),
              _enable_group_metrics);
            group->reset_tx_state(term);
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
                .commit_timestamp = meta.metadata.commit_timestamp,
                .expiry_timestamp = expiry_timestamp,
                .non_reclaimable = meta.metadata.non_reclaimable,
              });
        }

        for (const auto& [_, tx] : group_stm.prepared_txs()) {
            group->insert_prepared(tx);
        }
        for (auto& [id, epoch] : group_stm.fences()) {
            group->try_set_fence(id, epoch);
        }
        for (auto& [id, tx_data] : group_stm.tx_data()) {
            group->try_set_tx_data(id, tx_data.tx_seq, tx_data.tm_partition);
        }
        for (auto& [id, timeout] : group_stm.timeouts()) {
            group->try_set_timeout(id, timeout);
        }

        if (group_stm.is_removed()) {
            if (group_stm.offsets().size() > 0) {
                klog.warn(
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
          cluster::cluster_version{9});
        auto reader = model::make_memory_record_batch_reader(std::move(batch));

        try {
            auto result = co_await p->partition->raft()->replicate(
              term,
              std::move(reader),
              raft::replicate_options(raft::consistency_level::quorum_ack));

            if (result) {
                vlog(
                  klog.info,
                  "Prepared partition {} for consumer offset retention feature "
                  "during upgrade",
                  p->partition->ntp());
                co_return;

            } else if (result.error() == raft::errc::shutting_down) {
                vlog(
                  klog.debug,
                  "Cannot write offset retention version fence for partition "
                  "{}: shutting down",
                  p->partition->ntp());
                co_return;

            } else if (result.error() == raft::errc::not_leader) {
                vlog(
                  klog.debug,
                  "Cannot write offset retention version fence for partition "
                  "{}: not leader",
                  p->partition->ntp());
                co_return;

            } else {
                vlog(
                  klog.warn,
                  "Could not write offset retention feature fence for "
                  "partition {}: {} {}",
                  p->partition->ntp(),
                  result.error().message(),
                  result.error());
            }
        } catch (const ss::gate_closed_exception&) {
            vlog(
              klog.debug,
              "Cannot write offset retention version fence for partition {}: "
              "partition shutting down",
              p->partition->ntp());
            co_return;

        } catch (const ss::abort_requested_exception&) {
            vlog(
              klog.debug,
              "Cannot write offset retention version fence for partition {}: "
              "partition abort requested",
              p->partition->ntp());
            co_return;

        } catch (...) {
            vlog(
              klog.error,
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
      r.ntp, r.data.group_id, join_group_api::key);
    if (error != error_code::none) {
        return group::join_group_stages(
          make_join_error(r.data.member_id, error));
    }

    if (
      r.data.session_timeout_ms < _conf.group_min_session_timeout_ms()
      || r.data.session_timeout_ms > _conf.group_max_session_timeout_ms()) {
        vlog(
          klog.trace,
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
              klog.trace,
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
              klog.trace,
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
          p,
          _tx_frontend,
          _feature_table,
          _serializer_factory(),
          _enable_group_metrics);
        group->reset_tx_state(it->second->term);
        _groups.emplace(r.data.group_id, group);
        _groups.rehash(0);
        is_new_group = true;
        vlog(klog.trace, "Created new group {} while joining", r.data.group_id);
    }

    auto ret = group->handle_join_group(std::move(r), is_new_group);
    return group::join_group_stages(
      ret.dispatched.finally([group] {}), ret.result.finally([group] {}));
}

group::sync_group_stages group_manager::sync_group(sync_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, sync_group_api::key);
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
          klog.trace,
          "Cannot handle sync group request for unknown group {}",
          r.data.group_id);
        return group::sync_group_stages(
          sync_group_response(error_code::unknown_member_id));
    }
}

ss::future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, heartbeat_api::key);
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
      klog.trace,
      "Cannot handle heartbeat request for unknown group {}",
      r.data.group_id);

    return make_heartbeat_error(error_code::unknown_member_id);
}

ss::future<leave_group_response>
group_manager::leave_group(leave_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, leave_group_api::key);
    if (error != error_code::none) {
        return make_leave_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_leave_group(std::move(r)).finally([group] {});
    } else {
        vlog(
          klog.trace,
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
    if (!p || !p->catchup_lock.try_read_lock()) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<txn_offset_commit_response>(
          txn_offset_commit_response(
            r, error_code::coordinator_load_in_progress));
    }
    p->catchup_lock.read_unlock();

    return p->catchup_lock.hold_read_lock().then(
      [this, p, r = std::move(r)](ss::basic_rwlock<>::holder unit) mutable {
          // TODO: use correct key instead of offset_commit_api::key
          // check other txn places
          auto error = validate_group_status(
            r.ntp, r.data.group_id, offset_commit_api::key);
          if (error != error_code::none) {
              return ss::make_ready_future<txn_offset_commit_response>(
                txn_offset_commit_response(r, error));
          }

          auto group = get_group(r.data.group_id);
          if (!group) {
              // <kafka>the group is not relying on Kafka for group management,
              // so allow the commit</kafka>

              group = ss::make_lw_shared<kafka::group>(
                r.data.group_id,
                group_state::empty,
                _conf,
                p->partition,
                _tx_frontend,
                _feature_table,
                _serializer_factory(),
                _enable_group_metrics);
              group->reset_tx_state(p->term);
              _groups.emplace(r.data.group_id, group);
              _groups.rehash(0);
          }

          return group->handle_txn_offset_commit(std::move(r))
            .finally([unit = std::move(unit), group] {});
      });
}

ss::future<cluster::commit_group_tx_reply>
group_manager::commit_tx(cluster::commit_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->catchup_lock.try_read_lock()) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::commit_group_tx_reply>(
          make_commit_tx_reply(cluster::tx_errc::coordinator_load_in_progress));
    }
    p->catchup_lock.read_unlock();

    return p->catchup_lock.hold_read_lock().then(
      [this, r = std::move(r)](ss::basic_rwlock<>::holder unit) mutable {
          auto error = validate_group_status(
            r.ntp, r.group_id, offset_commit_api::key);
          if (error != error_code::none) {
              if (error == error_code::not_coordinator) {
                  return ss::make_ready_future<cluster::commit_group_tx_reply>(
                    make_commit_tx_reply(cluster::tx_errc::not_coordinator));
              } else {
                  return ss::make_ready_future<cluster::commit_group_tx_reply>(
                    make_commit_tx_reply(cluster::tx_errc::timeout));
              }
          }

          auto group = get_group(r.group_id);
          if (!group) {
              return ss::make_ready_future<cluster::commit_group_tx_reply>(
                make_commit_tx_reply(cluster::tx_errc::timeout));
          }

          return group->handle_commit_tx(std::move(r))
            .finally([unit = std::move(unit), group] {});
      });
}

ss::future<cluster::begin_group_tx_reply>
group_manager::begin_tx(cluster::begin_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->catchup_lock.try_read_lock()) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::begin_group_tx_reply>(
          make_begin_tx_reply(cluster::tx_errc::coordinator_load_in_progress));
    }
    p->catchup_lock.read_unlock();

    return p->catchup_lock.hold_read_lock().then(
      [this, p, r = std::move(r)](ss::basic_rwlock<>::holder unit) mutable {
          auto error = validate_group_status(
            r.ntp, r.group_id, offset_commit_api::key);
          if (error != error_code::none) {
              auto ec = error == error_code::not_coordinator
                          ? cluster::tx_errc::not_coordinator
                          : cluster::tx_errc::timeout;
              return ss::make_ready_future<cluster::begin_group_tx_reply>(
                make_begin_tx_reply(ec));
          }

          auto group = get_group(r.group_id);
          if (!group) {
              group = ss::make_lw_shared<kafka::group>(
                r.group_id,
                group_state::empty,
                _conf,
                p->partition,
                _tx_frontend,
                _feature_table,
                _serializer_factory(),
                _enable_group_metrics);
              group->reset_tx_state(p->term);
              _groups.emplace(r.group_id, group);
              _groups.rehash(0);
          }

          return group->handle_begin_tx(std::move(r))
            .finally([unit = std::move(unit), group] {});
      });
}

ss::future<cluster::prepare_group_tx_reply>
group_manager::prepare_tx(cluster::prepare_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->catchup_lock.try_read_lock()) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::prepare_group_tx_reply>(
          make_prepare_tx_reply(
            cluster::tx_errc::coordinator_load_in_progress));
    }
    p->catchup_lock.read_unlock();

    return p->catchup_lock.hold_read_lock().then(
      [this, r = std::move(r)](ss::basic_rwlock<>::holder unit) mutable {
          auto error = validate_group_status(
            r.ntp, r.group_id, offset_commit_api::key);
          if (error != error_code::none) {
              auto ec = error == error_code::not_coordinator
                          ? cluster::tx_errc::not_coordinator
                          : cluster::tx_errc::timeout;
              return ss::make_ready_future<cluster::prepare_group_tx_reply>(
                make_prepare_tx_reply(ec));
          }

          auto group = get_group(r.group_id);
          if (!group) {
              return ss::make_ready_future<cluster::prepare_group_tx_reply>(
                make_prepare_tx_reply(cluster::tx_errc::timeout));
          }

          return group->handle_prepare_tx(std::move(r))
            .finally([unit = std::move(unit), group] {});
      });
}

ss::future<cluster::abort_group_tx_reply>
group_manager::abort_tx(cluster::abort_group_tx_request&& r) {
    auto p = get_attached_partition(r.ntp);
    if (!p || !p->catchup_lock.try_read_lock()) {
        // transaction operations can't run in parallel with loading
        // state from the log (happens once per term change)
        vlog(
          cluster::txlog.trace,
          "can't process a tx: coordinator_load_in_progress");
        return ss::make_ready_future<cluster::abort_group_tx_reply>(
          make_abort_tx_reply(cluster::tx_errc::coordinator_load_in_progress));
    }
    p->catchup_lock.read_unlock();

    return p->catchup_lock.hold_read_lock().then(
      [this, r = std::move(r)](ss::basic_rwlock<>::holder unit) mutable {
          auto error = validate_group_status(
            r.ntp, r.group_id, offset_commit_api::key);
          if (error != error_code::none) {
              auto ec = error == error_code::not_coordinator
                          ? cluster::tx_errc::not_coordinator
                          : cluster::tx_errc::timeout;
              return ss::make_ready_future<cluster::abort_group_tx_reply>(
                make_abort_tx_reply(ec));
          }

          auto group = get_group(r.group_id);
          if (!group) {
              return ss::make_ready_future<cluster::abort_group_tx_reply>(
                make_abort_tx_reply(cluster::tx_errc::timeout));
          }

          return group->handle_abort_tx(std::move(r))
            .finally([unit = std::move(unit), group] {});
      });
}

group::offset_commit_stages
group_manager::offset_commit(offset_commit_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_commit_api::key);
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
              p->partition,
              _tx_frontend,
              _feature_table,
              _serializer_factory(),
              _enable_group_metrics);
            group->reset_tx_state(p->term);
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
group_manager::offset_fetch(offset_fetch_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_fetch_api::key);
    if (error != error_code::none) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(error));
    }

    auto group = get_group(r.data.group_id);
    if (!group) {
        return ss::make_ready_future<offset_fetch_response>(
          offset_fetch_response(r.data.topics));
    }

    return group->handle_offset_fetch(std::move(r)).finally([group] {});
}

ss::future<offset_delete_response>
group_manager::offset_delete(offset_delete_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, offset_delete_api::key);
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

    std::vector<model::topic_partition> requested_deletions;
    for (const auto& topic : r.data.topics) {
        for (const auto& partition : topic.partitions) {
            requested_deletions.emplace_back(
              topic.name, partition.partition_index);
        }
    }

    auto deleted_offsets = group->delete_offsets(requested_deletions);
    co_await delete_offsets(group, deleted_offsets);

    absl::flat_hash_set<model::topic_partition> deleted_offsets_set;
    for (auto& tp : deleted_offsets) {
        deleted_offsets_set.insert(std::move(tp));
    }

    absl::
      flat_hash_map<model::topic, std::vector<offset_delete_response_partition>>
        response_data;
    for (const auto& tp : requested_deletions) {
        auto error = kafka::error_code::none;
        if (!deleted_offsets_set.contains(tp)) {
            error = kafka::error_code::group_subscribed_to_topic;
        }
        response_data[tp.topic].emplace_back(offset_delete_response_partition{
          .partition_index = tp.partition, .error_code = error});
    }

    offset_delete_response response(kafka::error_code::none);
    for (auto& [t, ps] : response_data) {
        response.data.topics.emplace_back(
          offset_delete_response_topic{.name = t, .partitions = std::move(ps)});
    }
    co_return response;
}

std::pair<error_code, std::vector<listed_group>>
group_manager::list_groups() const {
    auto loading = std::any_of(
      _partitions.cbegin(),
      _partitions.cend(),
      [](const std::
           pair<const model::ntp, ss::lw_shared_ptr<attached_partition>>& p) {
          return p.second->loading;
      });

    std::vector<listed_group> groups;
    for (const auto& it : _groups) {
        const auto& g = it.second;
        groups.push_back(
          {g->id(), g->protocol_type().value_or(protocol_type())});
    }

    auto error = loading ? error_code::coordinator_load_in_progress
                         : error_code::none;

    return std::make_pair(error, groups);
}

described_group
group_manager::describe_group(const model::ntp& ntp, const kafka::group_id& g) {
    auto error = validate_group_status(ntp, g, describe_groups_api::key);
    if (error != error_code::none) {
        return describe_groups_response::make_empty_described_group(g, error);
    }

    auto group = get_group(g);
    if (!group) {
        return describe_groups_response::make_dead_described_group(g);
    }

    return group->describe();
}

ss::future<std::vector<deletable_group_result>> group_manager::delete_groups(
  std::vector<std::pair<model::ntp, group_id>> groups) {
    std::vector<deletable_group_result> results;

    for (auto& group_info : groups) {
        auto error = validate_group_status(
          group_info.first, group_info.second, delete_groups_api::key);
        if (error != error_code::none) {
            results.push_back(deletable_group_result{
              .group_id = std::move(group_info.second),
              .error_code = error_code::not_coordinator,
            });
            continue;
        }

        auto group = get_group(group_info.second);
        if (!group) {
            results.push_back(deletable_group_result{
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
            _groups.erase(group_info.second);
        }
        results.push_back(deletable_group_result{
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
  const model::ntp& ntp, const group_id& group, api_key api) {
    if (!valid_group_id(group, api)) {
        vlog(
          klog.debug, "Group name {} is invalid for operation {}", group, api);
        return error_code::invalid_group_id;
    }

    if (const auto it = _partitions.find(ntp); it != _partitions.end()) {
        if (!it->second->partition->is_leader()) {
            vlog(
              klog.debug,
              "Group {} operation {} sent to non-leader coordinator {}",
              group,
              api,
              ntp);
            return error_code::not_coordinator;
        }

        if (it->second->loading) {
            vlog(
              klog.debug,
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
      klog.debug,
      "Group {} operation {} misdirected to non-coordinator {}",
      group,
      api,
      ntp);
    return error_code::not_coordinator;
}

} // namespace kafka
