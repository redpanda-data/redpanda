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
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_recovery_consumer.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/coroutine.hh>

namespace kafka {

group_manager::group_manager(
  model::topic_namespace tp_ns,
  ss::sharded<raft::group_manager>& gm,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& topic_table,
  group_metadata_serializer_factory serializer_factory,
  config::configuration& conf)
  : _tp_ns(std::move(tp_ns))
  , _gm(gm)
  , _pm(pm)
  , _topic_table(topic_table)
  , _serializer_factory(std::move(serializer_factory))
  , _conf(conf)
  , _self(cluster::make_self_broker(config::node())) {}

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
        [this](const std::vector<cluster::topic_table::delta>& deltas) {
            handle_topic_delta(deltas);
        });

    return ss::make_ready_future<>();
}

ss::future<> group_manager::stop() {
    _pm.local().unregister_manage_notification(_manage_notify_handle);
    _pm.local().unregister_unmanage_notification(_unmanage_notify_handle);
    _gm.local().unregister_leadership_notification(_leader_notify_handle);
    _topic_table.local().unregister_delta_notification(
      _topic_table_notify_handle);

    for (auto& e : _partitions) {
        e.second->as.request_abort();
    }

    return _gate.close().then([this] {
        /**
         * cancel all pending group opeartions
         */
        for (auto& [_, group] : _groups) {
            group->shutdown();
        }
    });
}

void group_manager::detach_partition(const model::ntp& ntp) {
    klog.debug("detaching group metadata partition {}", ntp);
    std::vector<group_ptr> groups;
    groups.reserve(_groups.size());
    for (auto& group : _groups) {
        groups.push_back(group.second);
    }

    for (auto& gr : groups) {
        // skip if group is not managed by current NTP
        if (gr->partition()->ntp() != ntp) {
            continue;
        }
        auto it = _groups.find(gr->id());
        if (it == _groups.end()) {
            continue;
        }

        vlog(klog.trace, "Removed group {}", gr);
        _groups.erase(it);
        _groups.rehash(0);
    }
    _partitions.erase(ntp);
    _partitions.rehash(0);
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
  const std::vector<cluster::topic_table_delta>& deltas) {
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

    for (auto it = _groups.begin(); it != _groups.end();) {
        if (it->second->partition()->ntp() == p->partition->ntp()) {
            it->second->shutdown();
            _groups.erase(it++);
            continue;
        }
        ++it;
    }
    _groups.rehash(0);
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
    for (auto& [_, group] : _groups) {
        if (group->partition()->ntp() == p->partition->ntp()) {
            group->reset_tx_state(term);
        }
    }
    p->term = term;

    for (auto& [group_id, group_stm] : ctx.groups) {
        if (group_stm.has_data()) {
            auto group = get_group(group_id);
            vlog(
              klog.info,
              "Recovering {} - {}",
              group_id,
              group_stm.get_metadata());
            if (!group) {
                group = ss::make_lw_shared<kafka::group>(
                  group_id,
                  group_stm.get_metadata(),
                  _conf,
                  p->partition,
                  _serializer_factory());
                group->reset_tx_state(term);
                _groups.emplace(group_id, group);
                group->reschedule_all_member_heartbeats();
            }

            for (auto& [tp, meta] : group_stm.offsets()) {
                group->try_upsert_offset(
                  tp,
                  group::offset_metadata{
                    meta.log_offset,
                    meta.metadata.offset,
                    meta.metadata.metadata,
                  });
            }

            for (auto& [id, epoch] : group_stm.fences()) {
                group->try_set_fence(id, epoch);
            }
        }
    }

    for (auto& [group_id, group_stm] : ctx.groups) {
        if (group_stm.prepared_txs().size() == 0) {
            continue;
        }
        auto group = get_group(group_id);
        if (!group) {
            group = ss::make_lw_shared<kafka::group>(
              group_id,
              group_state::empty,
              _conf,
              p->partition,
              _serializer_factory());
            group->reset_tx_state(term);
            _groups.emplace(group_id, group);
        }
        for (const auto& [_, tx] : group_stm.prepared_txs()) {
            group->insert_prepared(tx);
        }
        for (auto& [id, epoch] : group_stm.fences()) {
            group->try_set_fence(id, epoch);
        }
    }

    /*
     * <kafka>if the cache already contains a group which should be removed,
     * raise an error. Note that it is possible (however unlikely) for a
     * consumer group to be removed, and then to be used only for offset storage
     * (i.e. by "simple" consumers)</kafka>
     */
    for (auto& [group_id, group_stm] : ctx.groups) {
        if (group_stm.is_removed()) {
            if (_groups.contains(group_id) && group_stm.offsets().size() > 0) {
                klog.warn(
                  "Unexpected active group unload {} loading {}",
                  group_id,
                  p->partition->ntp());
            }
        }
    }

    _groups.rehash(0);

    return ss::make_ready_future<>();
}

ss::future<join_group_response>
group_manager::join_group(join_group_request&& r) {
    auto error = validate_group_status(
      r.ntp, r.data.group_id, join_group_api::key);
    if (error != error_code::none) {
        return make_join_error(r.data.member_id, error);
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
        return make_join_error(
          r.data.member_id, error_code::invalid_session_timeout);
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
            return make_join_error(
              r.data.member_id, error_code::unknown_member_id);
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
            return make_join_error(
              r.data.member_id, error_code::not_coordinator);
        }
        auto p = it->second->partition;
        group = ss::make_lw_shared<kafka::group>(
          r.data.group_id, group_state::empty, _conf, p, _serializer_factory());
        group->reset_tx_state(it->second->term);
        _groups.emplace(r.data.group_id, group);
        _groups.rehash(0);
        is_new_group = true;
        vlog(klog.trace, "Created new group {} while joining", r.data.group_id);
    }

    return group->handle_join_group(std::move(r), is_new_group)
      .finally([group] {});
}

ss::future<sync_group_response>
group_manager::sync_group(sync_group_request&& r) {
    if (r.data.group_instance_id) {
        vlog(klog.trace, "Static group membership is not supported");
        return make_sync_error(error_code::unsupported_version);
    }

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
            return make_sync_error(error_code::rebalance_in_progress);
        }
        return make_sync_error(error);
    }

    auto group = get_group(r.data.group_id);
    if (group) {
        return group->handle_sync_group(std::move(r)).finally([group] {});
    } else {
        vlog(
          klog.trace,
          "Cannot handle sync group request for unknown group {}",
          r.data.group_id);
        return make_sync_error(error_code::unknown_member_id);
    }
}

ss::future<heartbeat_response> group_manager::heartbeat(heartbeat_request&& r) {
    if (r.data.group_instance_id) {
        vlog(klog.trace, "Static group membership is not supported");
        return make_heartbeat_error(error_code::unsupported_version);
    }

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
        return make_leave_error(error_code::unknown_member_id);
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
                _serializer_factory());
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
                _serializer_factory());
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
              _serializer_factory());
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
    stages.committed = stages.committed.finally([group] {});
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
          klog.trace, "Group name {} is invalid for operation {}", group, api);
        return error_code::invalid_group_id;
    }

    if (const auto it = _partitions.find(ntp); it != _partitions.end()) {
        if (!it->second->partition->is_leader()) {
            vlog(
              klog.trace,
              "Group {} operation {} sent to non-leader coordinator {}",
              group,
              api,
              ntp);
            return error_code::not_coordinator;
        }

        if (it->second->loading) {
            vlog(
              klog.trace,
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
      klog.trace,
      "Group {} operation {} misdirected to non-coordinator {}",
      group,
      api,
      ntp);
    return error_code::not_coordinator;
}

} // namespace kafka
