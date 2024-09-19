// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_updates_dispatcher.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/logger.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <absl/container/node_hash_map.h>
#include <fmt/ranges.h>

#include <iterator>
#include <system_error>
#include <vector>

namespace cluster {

topic_updates_dispatcher::topic_updates_dispatcher(
  ss::sharded<partition_allocator>& pal,
  ss::sharded<topic_table>& table,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<partition_balancer_state>& pb_state)
  : _partition_allocator(pal)
  , _topic_table(table)
  , _partition_leaders_table(leaders)
  , _partition_balancer_state(pb_state) {}

ss::future<std::error_code> topic_updates_dispatcher::do_topic_delete(
  topic_lifecycle_transition transition, model::offset base_offset) {
    in_progress_map in_progress;
    std::optional<assignments_set> topic_assignments;

    bool local_delete = transition.mode
                          == topic_lifecycle_transition_mode::oneshot_delete
                        || transition.mode
                             == topic_lifecycle_transition_mode::pending_gc;

    // If the command includes local deletion of the topic, we will do some
    // extra work as well as applying the command to the topic table.
    if (local_delete) {
        topic_assignments = _topic_table.local().get_topic_assignments(
          transition.topic.nt);
        if (topic_assignments) {
            in_progress = collect_in_progress(
              transition.topic.nt, *topic_assignments);
        }
    }

    return dispatch_updates_to_cores(transition, base_offset)
      .then([this,
             tp_ns = std::move(transition.topic.nt),
             topic_assignments = std::move(topic_assignments),
             in_progress = std::move(in_progress),
             local_delete](std::error_code ec) {
          vlog(
            clusterlog.info,
            "dispatched to cores: {} (local delete {})",
            ec,
            local_delete);
          if (ec == errc::success && local_delete) {
              vassert(
                topic_assignments.has_value(),
                "Topic had to exist before successful delete");
              vlog(
                clusterlog.trace,
                "Deallocating ntp: {}, in_progress ops: {}",
                tp_ns,
                in_progress);
              deallocate_topic(tp_ns, *topic_assignments, in_progress);

              for (const auto& [_, p_as] : *topic_assignments) {
                  _partition_balancer_state.local()
                    .handle_ntp_move_begin_or_cancel(
                      tp_ns.ns, tp_ns.tp, p_as.id, p_as.replicas, {});
              }
          }

          return ec;
      });
}

ss::future<std::error_code>
topic_updates_dispatcher::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto cmd = co_await deserialize(std::move(b), commands);

    co_return co_await std::visit(
      [this, offset](auto cmd) { return apply(std::move(cmd), offset); },
      std::move(cmd));
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  create_topic_cmd command, model::offset offset) {
    auto tp_ns = command.key;
    ss::chunked_fifo<partition_assignment> assignments;

    std::copy(
      command.value.assignments.begin(),
      command.value.assignments.end(),
      std::back_inserter(assignments));

    auto ec = co_await dispatch_updates_to_cores(std::move(command), offset);

    if (ec == errc::success) {
        add_allocations_for_new_partitions(assignments);
        for (const auto& p_as : assignments) {
            _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
              tp_ns.ns, tp_ns.tp, p_as.id, {}, p_as.replicas);
            co_await ss::coroutine::maybe_yield();
        }

        co_return errc::success;
    }

    co_return ec;
}

ss::future<std::error_code>
topic_updates_dispatcher::apply(delete_topic_cmd cmd, model::offset offset) {
    // Legacy delete commands never create tombstones, so revision
    // ID doesn't matter: use a synthetic '0' version.
    topic_lifecycle_transition transition{
      .topic
      = nt_revision{.nt = cmd.key, .initial_revision_id = model::initial_revision_id{0}},
      .mode = topic_lifecycle_transition_mode::oneshot_delete,
    };

    return do_topic_delete(transition, offset);
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  topic_lifecycle_transition_cmd cmd, model::offset offset) {
    return do_topic_delete(cmd.value, offset);
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  move_partition_replicas_cmd cmd, model::offset offset) {
    auto p_as = _topic_table.local().get_partition_assignment(cmd.key);
    auto ec = co_await dispatch_updates_to_cores(cmd, offset);
    if (!ec) {
        const auto& ntp = cmd.key;
        vassert(
          p_as.has_value(),
          "Partition {} have to exist before successful "
          "partition reallocation",
          ntp);

        update_allocations_for_reconfiguration(p_as->replicas, cmd.value);

        _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
          ntp.ns, ntp.tp.topic, ntp.tp.partition, p_as->replicas, cmd.value);
    }
    co_return ec;
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  update_partition_replicas_cmd cmd, model::offset offset) {
    const auto& ntp = cmd.value.ntp;
    auto p_as = _topic_table.local().get_partition_assignment(ntp);
    auto ec = co_await dispatch_updates_to_cores(cmd, offset);
    if (!ec) {
        vassert(
          p_as.has_value(),
          "Partition {} have to exist before successful "
          "partition reallocation",
          ntp);

        update_allocations_for_reconfiguration(
          p_as->replicas, cmd.value.replicas);

        _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
          ntp.ns,
          ntp.tp.topic,
          ntp.tp.partition,
          p_as->replicas,
          cmd.value.replicas);
    }
    co_return ec;
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  cancel_moving_partition_replicas_cmd cmd, model::offset offset) {
    auto current_assignment = _topic_table.local().get_partition_assignment(
      cmd.key);
    auto new_target_replicas = _topic_table.local().get_previous_replica_set(
      cmd.key);
    auto ntp = cmd.key;
    return dispatch_updates_to_cores(std::move(cmd), offset)
      .then([this,
             ntp = std::move(ntp),
             current_assignment = std::move(current_assignment),
             new_target_replicas = std::move(new_target_replicas)](
              std::error_code ec) {
          if (ec) {
              return ec;
          }
          vassert(
            current_assignment.has_value() && new_target_replicas.has_value(),
            "Previous replicas for NTP {} must exists as finish "
            "update can only be applied to partition that is "
            "currently being updated",
            ntp);

          auto to_add = subtract(
            *new_target_replicas, current_assignment->replicas);
          _partition_allocator.local().add_final_counts(to_add);

          auto to_remove = subtract(
            current_assignment->replicas, *new_target_replicas);
          _partition_allocator.local().remove_final_counts(to_remove);

          _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
            ntp.ns,
            ntp.tp.topic,
            ntp.tp.partition,
            current_assignment->replicas,
            *new_target_replicas);
          return ec;
      });
}
ss::future<std::error_code> topic_updates_dispatcher::apply(
  finish_moving_partition_replicas_cmd cmd, model::offset offset) {
    // initial replica set of the move command (not changed when
    // operation is cancelled)
    auto previous_replicas = _topic_table.local().get_previous_replica_set(
      cmd.key);
    // requested/target replica set of original move command(not
    // changed when move is cancelled)
    auto target_replicas = _topic_table.local().get_target_replica_set(cmd.key);
    /**
     * Finish moving command may be related either with cancellation
     * or with finish of an original move
     *
     * For original move the direction of data transfer is:
     *
     *  previous_replica -> target_replicas
     *
     * for cancelled move it is:
     *
     *  target_replicas -> previous_replicas
     *
     * Finish command contains the final replica set it will be
     * target_replicas for finished move and previous_replicas for
     * finished cancellation
     */
    return dispatch_updates_to_cores(cmd, offset)
      .then([this,
             ntp = std::move(cmd.key),
             previous_replicas = std::move(previous_replicas),
             target_replicas = std::move(target_replicas),
             command_replicas = std::move(cmd.value)](std::error_code ec) {
          if (ec) {
              return ec;
          }
          vassert(
            previous_replicas.has_value(),
            "Previous replicas for NTP {} must exists as finish "
            "update can only be applied to partition that is "
            "currently being updated",
            ntp);
          vassert(
            target_replicas.has_value(),
            "Target replicas for NTP {} must exists as finish "
            "update can only be applied to partition that is "
            "currently being updated",
            ntp);
          std::vector<model::broker_shard> to_delete;
          // move was successful, not cancelled
          if (target_replicas == command_replicas) {
              to_delete = subtract(*previous_replicas, command_replicas);
          } else {
              vassert(
                previous_replicas == command_replicas,
                "When finishing cancelled move of partition {} the "
                "finish command replica set {} must be equal to "
                "previous_replicas {} from topic table in progress "
                "update tracker",
                ntp,
                command_replicas,
                previous_replicas);
              to_delete = subtract(*target_replicas, command_replicas);
          }
          _partition_allocator.local().remove_allocations(to_delete);

          return ec;
      });
}
ss::future<std::error_code> topic_updates_dispatcher::apply(
  update_topic_properties_cmd cmd, model::offset offset) {
    return dispatch_updates_to_cores(std::move(cmd), offset);
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  create_partition_cmd cmd, model::offset offset) {
    auto tp_ns = cmd.key;
    ss::chunked_fifo<partition_assignment> assignments;

    std::copy(
      cmd.value.assignments.begin(),
      cmd.value.assignments.end(),
      std::back_inserter(assignments));
    auto ec = co_await dispatch_updates_to_cores(std::move(cmd), offset);

    if (ec == errc::success) {
        add_allocations_for_new_partitions(assignments);

        for (const auto& p_as : assignments) {
            _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
              tp_ns.ns, tp_ns.tp, p_as.id, {}, p_as.replicas);
        }
    }
    co_return ec;
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  move_topic_replicas_cmd cmd, model::offset offset) {
    auto assignments = _topic_table.local().get_topic_assignments(cmd.key);
    auto ec = co_await dispatch_updates_to_cores(cmd, offset);
    if (!assignments.has_value()) {
        co_return std::error_code(errc::topic_not_exists);
    }
    if (ec == errc::success) {
        for (const auto& [partition_id, replicas] : cmd.value) {
            auto assignment_it = assignments.value().find(partition_id);
            auto ntp = model::ntp(cmd.key.ns, cmd.key.tp, partition_id);
            if (assignment_it == assignments.value().end()) {
                co_return std::error_code(errc::partition_not_exists);
            }

            update_allocations_for_reconfiguration(
              assignment_it->second.replicas, replicas);

            _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
              ntp.ns,
              ntp.tp.topic,
              ntp.tp.partition,
              assignment_it->second.replicas,
              replicas);
        }
    }
    co_return ec;
}
ss::future<std::error_code> topic_updates_dispatcher::apply(
  revert_cancel_partition_move_cmd cmd, model::offset offset) {
    /**
     * In this case partition underlying raft group reconfiguration
     * already finished when it is attempted to be cancelled.
     *
     * In the case where original move was scheduled
     * to happen from replica set A to B:
     *
     *      A->B
     *
     * Cancellation would result in reconfiguration:
     *
     *      B->A
     * But since the move A->B finished we update the topic table
     * back to the state from before the cancellation
     *
     */

    // Replica set that the original move was requested from (A from
    // an example above)
    auto previous_replicas = _topic_table.local().get_previous_replica_set(
      cmd.value.ntp);
    auto target_replicas = _topic_table.local().get_target_replica_set(
      cmd.value.ntp);
    return dispatch_updates_to_cores(cmd, offset)
      .then([this,
             ntp = std::move(cmd.value.ntp),
             previous_replicas = std::move(previous_replicas),
             target_replicas = std::move(target_replicas)](std::error_code ec) {
          if (ec) {
              return ec;
          }

          vassert(
            previous_replicas.has_value(),
            "Previous replicas for NTP {} must exists as revert "
            "update can only be applied to partition that move is "
            "currently being cancelled",
            ntp);
          vassert(
            target_replicas.has_value(),
            "Target replicas for NTP {} must exists as revert "
            "update can only be applied to partition that move is "
            "currently being cancelled",
            ntp);

          auto to_add = subtract(*target_replicas, *previous_replicas);
          _partition_allocator.local().add_final_counts(to_add);

          auto to_delete = subtract(*previous_replicas, *target_replicas);
          _partition_allocator.local().remove_allocations(to_delete);
          _partition_allocator.local().remove_final_counts(to_delete);

          _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
            ntp.ns,
            ntp.tp.topic,
            ntp.tp.partition,
            *previous_replicas,
            *target_replicas);
          return ec;
      });
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  force_partition_reconfiguration_cmd cmd, model::offset base_offset) {
    auto p_as = _topic_table.local().get_partition_assignment(cmd.key);
    auto ec = co_await dispatch_updates_to_cores(cmd, base_offset);
    if (ec) {
        co_return ec;
    }

    const auto& ntp = cmd.key;
    vassert(
      p_as.has_value(),
      "Partition {} have to exist before successful force-reconfiguration",
      ntp);

    update_allocations_for_reconfiguration(p_as->replicas, cmd.value.replicas);

    _partition_balancer_state.local().handle_ntp_move_begin_or_cancel(
      ntp.ns,
      ntp.tp.topic,
      ntp.tp.partition,
      p_as->replicas,
      cmd.value.replicas);

    co_return ec;
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  set_topic_partitions_disabled_cmd cmd, model::offset base_offset) {
    co_return co_await dispatch_updates_to_cores(cmd, base_offset);
}

ss::future<std::error_code> topic_updates_dispatcher::apply(
  bulk_force_reconfiguration_cmd cmd, model::offset base_offset) {
    co_return co_await dispatch_updates_to_cores(std::move(cmd), base_offset);
}

topic_updates_dispatcher::in_progress_map
topic_updates_dispatcher::collect_in_progress(
  const model::topic_namespace& tp_ns,
  const assignments_set& current_assignments) {
    in_progress_map in_progress;
    in_progress.reserve(current_assignments.size());
    // collect in progress assignments
    for (auto& [_, p] : current_assignments) {
        model::ntp ntp(tp_ns.ns, tp_ns.tp, p.id);
        const auto& in_progress_updates
          = _topic_table.local().updates_in_progress();
        auto it = in_progress_updates.find(ntp);
        if (it == in_progress_updates.end()) {
            continue;
        }
        const auto state = it->second.get_state();
        if (is_cancelled_state(state)) {
            in_progress[p.id] = it->second.get_target_replicas();
        } else {
            in_progress[p.id] = it->second.get_previous_replicas();
        }
    }
    return in_progress;
}

template<typename Cmd>
ss::future<std::error_code> do_apply(
  ss::shard_id shard,
  Cmd cmd,
  ss::sharded<topic_table>& table,
  model::offset o) {
    return table.invoke_on(
      shard, [cmd = std::move(cmd), o](topic_table& local_table) mutable {
          return local_table.apply(std::move(cmd), o);
      });
}

template<typename Cmd>
ss::future<std::error_code>
topic_updates_dispatcher::dispatch_updates_to_cores(Cmd cmd, model::offset o) {
    auto results = co_await ssx::parallel_transform(
      boost::irange<ss::shard_id>(0, ss::smp::count),
      [this, cmd = std::move(cmd), o](ss::shard_id shard) mutable {
          return do_apply(shard, cmd, _topic_table, o);
      });

    vassert(
      std::equal(std::next(results.begin()), results.end(), results.begin()),
      "State inconsistency across shards detected results: {}",
      results);

    co_return results.front();
}

void topic_updates_dispatcher::deallocate_topic(
  const model::topic_namespace& tp_ns,
  const assignments_set& topic_assignments,
  const in_progress_map& in_progress) {
    for (auto& [_, p_as] : topic_assignments) {
        // we must remove the allocation that would normally
        // be removed with update_finished request
        auto it = in_progress.find(p_as.id);
        auto to_delete = it == in_progress.end()
                           ? p_as.replicas
                           : union_vectors(it->second, p_as.replicas);
        _partition_allocator.local().remove_allocations(to_delete);
        _partition_allocator.local().remove_final_counts(p_as.replicas);
        if (unlikely(clusterlog.is_enabled(ss::log_level::trace))) {
            model::ntp ntp(tp_ns.ns, tp_ns.tp, p_as.id);
            vlog(
              clusterlog.trace,
              "Deallocated ntp: {}, current assignment: {}, "
              "to_delete: {}",
              ntp,
              fmt::join(p_as.replicas, ","),
              fmt::join(to_delete, ","));
        }
    }
}
template<typename T>
void topic_updates_dispatcher::add_allocations_for_new_partitions(
  const T& assignments) {
    auto& allocator = _partition_allocator.local();
    for (const auto& pas : assignments) {
        allocator.add_allocations_for_new_partition(pas.replicas, pas.group);
    }
}

void topic_updates_dispatcher::update_allocations_for_reconfiguration(
  const std::vector<model::broker_shard>& previous,
  const std::vector<model::broker_shard>& target) {
    auto to_add = subtract(target, previous);
    _partition_allocator.local().add_allocations(to_add);
    _partition_allocator.local().add_final_counts(to_add);

    auto to_remove = subtract(previous, target);
    _partition_allocator.local().remove_final_counts(to_remove);
}

ss::future<>
topic_updates_dispatcher::fill_snapshot(controller_snapshot& snap) const {
    co_await _topic_table.local().fill_snapshot(snap);
    snap.topics.highest_group_id
      = _partition_allocator.local().state().last_group_id();
}

ss::future<> topic_updates_dispatcher::apply_snapshot(
  model::offset offset, const controller_snapshot& snap) {
    co_await _topic_table.invoke_on_all([&snap, offset](topic_table& topics) {
        return topics.apply_snapshot(offset, snap);
    });

    co_await _partition_allocator.local().apply_snapshot(snap);

    co_await _partition_balancer_state.local().apply_snapshot(snap);
}

} // namespace cluster
