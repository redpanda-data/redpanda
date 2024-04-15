/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/shard_placement_table.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/topic_table.h"
#include "ssx/async_algorithm.h"

#include <seastar/util/defer.hh>

namespace cluster {

std::ostream& operator<<(
  std::ostream& o, const shard_placement_table::shard_local_assignment& as) {
    fmt::print(
      o,
      "{{group: {}, log_revision: {}, shard_revision: {}}}",
      as.group,
      as.log_revision,
      as.shard_revision);
    return o;
}

std::ostream&
operator<<(std::ostream& o, shard_placement_table::hosted_status s) {
    switch (s) {
    case shard_placement_table::hosted_status::receiving:
        return o << "receiving";
    case shard_placement_table::hosted_status::hosted:
        return o << "hosted";
    case shard_placement_table::hosted_status::obsolete:
        return o << "obsolete";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(
  std::ostream& o, const shard_placement_table::shard_local_state& ls) {
    fmt::print(
      o,
      "{{group: {}, log_revision: {}, status: {}, shard_revision: {}}}",
      ls.group,
      ls.log_revision,
      ls.status,
      ls.shard_revision);
    return o;
}

shard_placement_table::reconciliation_action
shard_placement_table::placement_state::get_reconciliation_action(
  std::optional<model::revision_id> expected_log_revision) const {
    if (!expected_log_revision) {
        if (assigned) {
            return reconciliation_action::wait_for_target_update;
        }
        return reconciliation_action::remove;
    }
    if (current && current->log_revision < expected_log_revision) {
        return reconciliation_action::remove;
    }
    if (_is_initial_for && _is_initial_for < expected_log_revision) {
        return reconciliation_action::remove;
    }
    if (assigned) {
        if (assigned->log_revision != expected_log_revision) {
            return reconciliation_action::wait_for_target_update;
        }
        if (_next) {
            return reconciliation_action::transfer;
        }
        return reconciliation_action::create;
    } else {
        return reconciliation_action::transfer;
    }
}

std::ostream&
operator<<(std::ostream& o, const shard_placement_table::placement_state& ps) {
    fmt::print(
      o,
      "{{current: {}, assigned: {}, is_initial_for: {}, next: {}}}",
      ps.current,
      ps.assigned,
      ps._is_initial_for,
      ps._next);
    return o;
}

namespace {

static constexpr auto kvstore_key_space
  = storage::kvstore::key_space::shard_placement;

// enum type is irrelevant, serde will serialize to 32 bit anyway
enum class kvstore_key_type {
    assignment = 1,
    current_state = 2,
};

struct assignment_marker
  : serde::
      envelope<assignment_marker, serde::version<0>, serde::compat_version<0>> {
    model::revision_id log_revision;
    model::shard_revision_id shard_revision;

    auto serde_fields() { return std::tie(log_revision, shard_revision); }
};

bytes assignment_kvstore_key(const raft::group_id group) {
    iobuf buf;
    serde::write(buf, kvstore_key_type::assignment);
    serde::write(buf, group);
    return iobuf_to_bytes(buf);
}

struct current_state_marker
  : serde::envelope<
      current_state_marker,
      serde::version<0>,
      serde::compat_version<0>> {
    // NOTE: we need ntp in this marker because we want to be able to find and
    // clean garbage kvstore state for old groups that have already been deleted
    // from topic_table. Some of the partition kvstore state items use keys
    // based on group id and some - based on ntp, so we need both.
    model::ntp ntp;
    model::revision_id log_revision;
    model::shard_revision_id shard_revision;
    bool is_complete = false;

    auto serde_fields() {
        return std::tie(ntp, log_revision, shard_revision, is_complete);
    }
};

bytes current_state_kvstore_key(const raft::group_id group) {
    iobuf buf;
    serde::write(buf, kvstore_key_type::current_state);
    serde::write(buf, group);
    return iobuf_to_bytes(buf);
}

} // namespace

shard_placement_table::shard_placement_table(storage::kvstore& kvstore)
  : _kvstore(kvstore) {}

ss::future<> shard_placement_table::initialize(
  const topic_table& topics, model::node_id self) {
    // We expect topic_table to remain unchanged throughout the loop because the
    // method is supposed to be called after local controller replay is finished
    // but before we start getting new controller updates from the leader.
    auto tt_version = topics.topics_map_revision();
    ssx::async_counter counter;
    for (const auto& [ns_tp, md_item] : topics.all_topics_metadata()) {
        vassert(
          tt_version == topics.topics_map_revision(),
          "topic_table unexpectedly changed");

        co_await ssx::async_for_each_counter(
          counter,
          md_item.get_assignments().begin(),
          md_item.get_assignments().end(),
          [&](const partition_assignment& p_as) {
              vassert(
                tt_version == topics.topics_map_revision(),
                "topic_table unexpectedly changed");

              model::ntp ntp{ns_tp.ns, ns_tp.tp, p_as.id};
              auto replicas_view = topics.get_replicas_view(ntp, md_item, p_as);
              auto target = placement_target_on_node(replicas_view, self);
              if (!target) {
                  return;
              }

              if (ss::this_shard_id() == assignment_shard_id) {
                  auto it = _ntp2entry.emplace(ntp, std::make_unique<entry_t>())
                              .first;
                  it->second->target = target.value();
              }

              // We add an initial hosted marker for the partition on the shard
              // from the original replica set (even in the case of cross-shard
              // move). The reason for this is that if there is an ongoing
              // cross-shard move, we can't be sure if it was done before the
              // previous shutdown or not, so during reconciliation we'll first
              // look for kvstore state on the original shard, and, if there is
              // none (meaning that the update was finished previously), use the
              // state on the destination shard.

              auto orig_shard = find_shard_on_node(
                replicas_view.orig_replicas(), self);

              if (ss::this_shard_id() == target->shard) {
                  vlog(
                    clusterlog.info,
                    "expecting partition {} with log revision {} on this shard "
                    "(original shard {})",
                    ntp,
                    target->log_revision,
                    orig_shard);
              }

              auto placement = placement_state();
              auto assigned = shard_local_assignment{
                .group = target->group,
                .log_revision = target->log_revision,
                .shard_revision = _cur_shard_revision};

              if (orig_shard && target->shard != orig_shard) {
                  // cross-shard transfer, orig_shard gets the hosted marker
                  if (ss::this_shard_id() == orig_shard) {
                      placement.current = shard_local_state(
                        assigned, hosted_status::hosted);
                      _states.emplace(ntp, placement);
                  } else if (ss::this_shard_id() == target->shard) {
                      placement.assigned = assigned;
                      _states.emplace(ntp, placement);
                  }
              } else if (ss::this_shard_id() == target->shard) {
                  // in other cases target shard gets the hosted marker
                  placement.current = shard_local_state(
                    assigned, hosted_status::hosted);
                  placement.assigned = assigned;
                  _states.emplace(ntp, placement);
              }
          });
    }

    if (!_ntp2entry.empty()) {
        _cur_shard_revision += 1;
    }
}

ss::future<> shard_placement_table::set_target(
  const model::ntp& ntp,
  std::optional<shard_placement_target> target,
  shard_callback_t shard_callback) {
    vassert(
      ss::this_shard_id() == assignment_shard_id,
      "method can only be invoked on shard {}",
      assignment_shard_id);

    if (!target && !_ntp2entry.contains(ntp)) {
        co_return;
    }

    auto entry_it = _ntp2entry.try_emplace(ntp).first;
    if (!entry_it->second) {
        entry_it->second = std::make_unique<entry_t>();
    }
    entry_t& entry = *entry_it->second;

    auto release_units = ss::defer(
      [&, units = co_await entry.mtx.get_units()]() mutable {
          bool had_waiters = entry.mtx.waiters() > 0;
          units.return_all();
          if (!had_waiters && !entry.target) {
              _ntp2entry.erase(ntp);
          }
      });

    const auto prev_target = entry.target;
    if (prev_target == target) {
        vlog(
          clusterlog.trace,
          "[{}] modify target no-op, current: {}",
          ntp,
          prev_target);
        co_return;
    }

    const model::shard_revision_id shard_rev = _cur_shard_revision;
    _cur_shard_revision += 1;

    vlog(
      clusterlog.trace,
      "[{}] modify target: {} -> {}, shard_rev: {}",
      ntp,
      prev_target,
      target,
      shard_rev);

    // 1. Persist the new target in kvstore

    if (target) {
        co_await container().invoke_on(
          target->shard,
          [&target, shard_rev, &ntp](shard_placement_table& other) {
              auto marker_buf = serde::to_iobuf(assignment_marker{
                .log_revision = target->log_revision,
                .shard_revision = shard_rev,
              });
              vlog(
                clusterlog.trace,
                "[{}] put assigned marker, lr: {} sr: {}",
                ntp,
                target->log_revision,
                shard_rev);
              return other._kvstore.put(
                kvstore_key_space,
                assignment_kvstore_key(target->group),
                std::move(marker_buf));
          });
    } else {
        co_await container().invoke_on(
          prev_target.value().shard,
          [group = prev_target->group, &ntp](shard_placement_table& other) {
              vlog(clusterlog.trace, "[{}] remove assigned marker", ntp);
              return other._kvstore.remove(
                kvstore_key_space, assignment_kvstore_key(group));
          });
    }

    // 2. At this point we've successfully committed the new target to
    // persistent storage. Update in-memory state.

    entry.target = target;

    if (prev_target && (!target || target->shard != prev_target->shard)) {
        co_await container().invoke_on(
          prev_target->shard,
          [&ntp, shard_callback](shard_placement_table& other) {
              auto it = other._states.find(ntp);
              if (it == other._states.end() || !it->second.assigned) {
                  return;
              }

              vlog(
                clusterlog.trace,
                "[{}] removing assigned on this shard (was: {})",
                ntp,
                it->second.assigned);

              it->second.assigned = std::nullopt;
              if (it->second.is_empty()) {
                  // We are on a shard that was previously a target, but didn't
                  // get to starting the transfer.
                  other._states.erase(it);
              }

              // Notify the caller that something has changed on this shard.
              shard_callback(ntp);
          });
    }

    if (target) {
        const bool is_initial
          = (!prev_target || prev_target->log_revision != target->log_revision);
        shard_local_assignment as{
          .group = target->group,
          .log_revision = target->log_revision,
          .shard_revision = shard_rev,
        };
        co_await container().invoke_on(
          target->shard,
          [&ntp, &as, is_initial, shard_callback](shard_placement_table& spt) {
              auto& state = spt._states.try_emplace(ntp).first->second;

              vlog(
                clusterlog.trace,
                "[{}] setting assigned on this shard to: {} (was: {}), "
                "is_initial: {}",
                ntp,
                as,
                state.assigned,
                is_initial);

              state.assigned = as;
              if (is_initial) {
                  state._is_initial_for = as.log_revision;
              }

              // Notify the caller that something has changed on this shard.
              shard_callback(ntp);
          });
    }

    // 3. Lastly, remove obsolete kvstore marker

    if (prev_target && (!target || target->shard != prev_target->shard)) {
        co_await container().invoke_on(
          prev_target->shard,
          [group = prev_target->group, &ntp](shard_placement_table& other) {
              vlog(
                clusterlog.trace, "[{}] remove obsolete assigned marker", ntp);
              return other._kvstore
                .remove(kvstore_key_space, assignment_kvstore_key(group))
                .handle_exception([group](std::exception_ptr ex) {
                    // Ignore the exception because the update has already been
                    // committed. Obsolete marker will be deleted after the next
                    // restart.
                    vlog(
                      clusterlog.debug,
                      "failed to remove assignment marker for group {}: {}",
                      group,
                      ex);
                });
          });
    }
}

std::optional<shard_placement_table::placement_state>
shard_placement_table::state_on_this_shard(const model::ntp& ntp) const {
    auto it = _states.find(ntp);
    if (it != _states.end()) {
        return it->second;
    }
    return std::nullopt;
}

ss::future<std::error_code> shard_placement_table::prepare_create(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto state_it = _states.find(ntp);
    vassert(state_it != _states.end(), "[{}] expected state", ntp);
    auto& state = state_it->second;
    vassert(
      state.assigned && state.assigned->log_revision == expected_log_rev,
      "[{}] unexpected assigned: {} (expected log revision: {})",
      ntp,
      state.assigned,
      expected_log_rev);

    if (state.current && state.current->log_revision != expected_log_rev) {
        // wait until partition with obsolete log revision is removed
        co_return errc::waiting_for_reconfiguration_finish;
    }

    // copy assigned as it may change while we are updating kvstore
    auto assigned = *state.assigned;

    if (!state.current) {
        if (state._is_initial_for == expected_log_rev) {
            auto marker_buf = serde::to_iobuf(current_state_marker{
              .ntp = ntp,
              .log_revision = expected_log_rev,
              .shard_revision = assigned.shard_revision,
              .is_complete = true,
            });
            vlog(
              clusterlog.trace,
              "[{}] put initial cur state marker, lr: {} sr: {}",
              ntp,
              expected_log_rev,
              assigned.shard_revision);
            co_await _kvstore.put(
              kvstore_key_space,
              current_state_kvstore_key(assigned.group),
              std::move(marker_buf));

            state.current = shard_local_state(assigned, hosted_status::hosted);
            if (state._is_initial_for == expected_log_rev) {
                // could have changed while we were updating kvstore.
                state._is_initial_for = std::nullopt;
            }
        } else {
            // x-shard transfer hasn't started yet, wait for it.
            co_return errc::waiting_for_partition_shutdown;
        }
    }

    if (state.current->status != hosted_status::hosted) {
        // x-shard transfer is in progress, wait for it to end.
        co_return errc::waiting_for_partition_shutdown;
    }

    // ready to create
    co_return errc::success;
}

ss::future<result<ss::shard_id>> shard_placement_table::prepare_transfer(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto state_it = _states.find(ntp);
    vassert(state_it != _states.end(), "[{}] expected state", ntp);
    auto& state = state_it->second;

    if (state.current) {
        vassert(
          state.current->log_revision == expected_log_rev,
          "[{}] unexpected current: {} (expected log revision: {})",
          ntp,
          state.current,
          expected_log_rev);

        if (state.current->status == hosted_status::receiving) {
            // This shard needs to transfer partition state somewhere else, but
            // haven't yet received it itself. Wait for it.
            co_return errc::waiting_for_partition_shutdown;
        }

        if (state.current->status == hosted_status::obsolete) {
            // Previous finish_transfer_on_source() failed? Retry it.
            co_await do_delete(ntp, state);
            co_return errc::success;
        }
    } else {
        vassert(
          state._is_initial_for == expected_log_rev,
          "[{}] unexpected is_initial_for: {} (expected log revision: {})",
          ntp,
          state._is_initial_for,
          expected_log_rev);
    }

    if (!state._next) {
        vassert(
          !state.assigned,
          "[{}] unexpected assigned: {} (expected log revision: {})",
          ntp,
          state.assigned,
          expected_log_rev);

        auto maybe_dest = co_await container().invoke_on(
          assignment_shard_id,
          [&ntp, expected_log_rev](shard_placement_table& spt) {
              auto it = spt._ntp2entry.find(ntp);
              if (it == spt._ntp2entry.end()) {
                  return std::optional<ss::shard_id>{};
              }
              const auto& target = it->second->target;
              if (target && target->log_revision == expected_log_rev) {
                  return std::optional{target->shard};
              }
              return std::optional<ss::shard_id>{};
          });
        if (!maybe_dest || maybe_dest == ss::this_shard_id()) {
            // Inconsistent state, likely because we are in the middle of
            // shard_placement_table update, wait for it to finish.
            co_return errc::waiting_for_shard_placement_update;
        }
        ss::shard_id destination = maybe_dest.value();

        // check if destination is ready
        auto ec = co_await container().invoke_on(
          destination, [&ntp, expected_log_rev](shard_placement_table& dest) {
              auto dest_it = dest._states.find(ntp);
              if (
                dest_it == dest._states.end() || !dest_it->second.assigned
                || dest_it->second.assigned->log_revision != expected_log_rev) {
                  // We are in the middle of shard_placement_table update, and
                  // the destination shard doesn't yet know that it is the
                  // destination. Wait for the update to finish.
                  return ss::make_ready_future<errc>(
                    errc::waiting_for_shard_placement_update);
              }
              auto& dest_state = dest_it->second;

              if (dest_state._next) {
                  // probably still finishing a previous transfer to this
                  // shard and we are already trying to transfer it back.
                  return ss::make_ready_future<errc>(
                    errc::waiting_for_partition_shutdown);
              } else if (dest_state.current) {
                  if (dest_state.current->log_revision != expected_log_rev) {
                      // someone has to delete obsolete log revision first
                      return ss::make_ready_future<errc>(
                        errc::waiting_for_partition_shutdown);
                  }
                  // probably still finishing a previous transfer to this
                  // shard and we are already trying to transfer it back.
                  return ss::make_ready_future<errc>(
                    errc::waiting_for_partition_shutdown);
              }

              // at this point we commit to the transfer on the
              // destination shard
              dest_state.current = shard_local_state(
                dest_state.assigned.value(), hosted_status::receiving);
              if (dest_state._is_initial_for <= expected_log_rev) {
                  dest_state._is_initial_for = std::nullopt;
              }

              // TODO: immediate hosted or _is_initial_for if source is empty.

              auto marker_buf = serde::to_iobuf(current_state_marker{
                .ntp = ntp,
                .log_revision = expected_log_rev,
                .shard_revision = dest_state.current.value().shard_revision,
                .is_complete = false,
              });
              vlog(
                clusterlog.trace,
                "[{}] put receiving cur state marker, lr: {} sr: {}",
                ntp,
                expected_log_rev,
                dest_state.current->shard_revision);
              return dest._kvstore
                .put(
                  kvstore_key_space,
                  current_state_kvstore_key(dest_state.current->group),
                  std::move(marker_buf))
                .then([] { return errc::success; });
          });

        if (ec != errc::success) {
            co_return ec;
        }

        // at this point we commit to the transfer on the source shard
        state._next = destination;
    }

    // TODO: check that _next is still waiting for our transfer
    co_return state._next.value();
}

ss::future<> shard_placement_table::finish_transfer_on_destination(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto it = _states.find(ntp);
    if (it == _states.end()) {
        co_return;
    }
    auto& state = it->second;
    if (state.current && state.current->log_revision == expected_log_rev) {
        vassert(
          state.current->status == hosted_status::receiving,
          "[{}] unexpected local status, current: {}",
          ntp,
          it->second.current);

        auto marker_buf = serde::to_iobuf(current_state_marker{
          .ntp = ntp,
          .log_revision = expected_log_rev,
          .shard_revision = state.current->shard_revision,
          .is_complete = true,
        });
        vlog(
          clusterlog.trace,
          "[{}] put transferred cur state marker, lr: {} sr: {}",
          ntp,
          expected_log_rev,
          state.current->shard_revision);
        co_await _kvstore.put(
          kvstore_key_space,
          current_state_kvstore_key(state.current->group),
          std::move(marker_buf));

        state.current->status = hosted_status::hosted;
    }
    vlog(
      clusterlog.trace,
      "[{}] finished transfer on destination, placement: {}",
      ntp,
      state);
}

ss::future<> shard_placement_table::finish_transfer_on_source(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;

    if (state.current) {
        vassert(
          state.current->log_revision == expected_log_rev,
          "[{}] unexpected current: {} (expected log revision: {})",
          ntp,
          state.current,
          expected_log_rev);
    } else if (state._is_initial_for == expected_log_rev) {
        state._is_initial_for = std::nullopt;
    }

    co_await do_delete(ntp, state);
}

ss::future<std::error_code> shard_placement_table::prepare_delete(
  const model::ntp& ntp, model::revision_id cmd_revision) {
    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;

    if (state._is_initial_for && state._is_initial_for < cmd_revision) {
        state._is_initial_for = std::nullopt;
        if (state.is_empty()) {
            _states.erase(it);
            co_return errc::success;
        }
    }

    if (state.current) {
        vassert(
          state.current->log_revision < cmd_revision,
          "[{}] unexpected current: {} (cmd revision: {})",
          ntp,
          state.current,
          cmd_revision);

        if (state.current->status == hosted_status::receiving) {
            // If transfer to this shard is still in progress, we'll wait for
            // the source shard to finish or cancel it before deleting.
            co_return errc::waiting_for_partition_shutdown;
        }

        state.current->status = hosted_status::obsolete;
    }

    co_return errc::success;
}

ss::future<> shard_placement_table::finish_delete(
  const model::ntp& ntp, model::revision_id expected_log_rev) {
    auto it = _states.find(ntp);
    vassert(it != _states.end(), "[{}] expected state", ntp);
    auto& state = it->second;
    vassert(
      state.current && state.current->log_revision == expected_log_rev,
      "[{}] unexpected current: {} (expected log revision: {})",
      ntp,
      state.current,
      expected_log_rev);

    if (state._next) {
        // notify destination shard that the transfer won't finish
        co_await container().invoke_on(
          state._next.value(),
          [&ntp, expected_log_rev](shard_placement_table& dest) {
              auto it = dest._states.find(ntp);
              if (
                it != dest._states.end() && it->second.current
                && it->second.current->log_revision == expected_log_rev
                && it->second.current->status == hosted_status::receiving) {
                  it->second.current->status = hosted_status::obsolete;
              }

              // TODO: notify reconciliation fiber
          });
    }

    co_await do_delete(ntp, state);
}

ss::future<> shard_placement_table::do_delete(
  const model::ntp& ntp, placement_state& state) {
    state._next = std::nullopt;

    if (state.current) {
        vlog(clusterlog.trace, "[{}] remove cur state marker", ntp);
        co_await _kvstore.remove(
          kvstore_key_space, current_state_kvstore_key(state.current->group));
        state.current = std::nullopt;
    }

    if (state.is_empty()) {
        _states.erase(ntp);
    }
    co_return;
}

} // namespace cluster
