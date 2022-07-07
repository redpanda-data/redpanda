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
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <absl/container/node_hash_map.h>

#include <iterator>
#include <system_error>
#include <vector>

namespace cluster {

topic_updates_dispatcher::topic_updates_dispatcher(
  ss::sharded<partition_allocator>& pal,
  ss::sharded<topic_table>& table,
  ss::sharded<partition_leaders_table>& leaders)
  : _partition_allocator(pal)
  , _topic_table(table)
  , _partition_leaders_table(leaders) {}

ss::future<std::error_code>
topic_updates_dispatcher::apply_update(model::record_batch b) {
    auto base_offset = b.base_offset();
    return deserialize(std::move(b), commands)
      .then([this, base_offset](auto cmd) {
          return ss::visit(
            std::move(cmd),
            [this, base_offset](delete_topic_cmd del_cmd) {
                auto tp_ns = del_cmd.key;
                auto topic_assignments
                  = _topic_table.local().get_topic_assignments(del_cmd.value);
                in_progress_map in_progress;

                if (topic_assignments) {
                    in_progress = collect_in_progress(
                      del_cmd.key, *topic_assignments);
                }
                return dispatch_updates_to_cores(del_cmd, base_offset)
                  .then(
                    [this,
                     tp_ns = std::move(tp_ns),
                     topic_assignments = std::move(topic_assignments),
                     in_progress = std::move(in_progress)](std::error_code ec) {
                        if (ec == errc::success) {
                            vassert(
                              topic_assignments.has_value(),
                              "Topic had to exist before successful delete");
                        }

                        for (auto& p_as : *topic_assignments) {
                            _partition_allocator.local().deallocate(
                              p_as.replicas);
                            auto it = in_progress.find(
                              model::ntp(tp_ns.ns, tp_ns.tp, p_as.id));

                            // we must remove the allocation that would normally
                            // be removed with update_finished request
                            if (it != in_progress.end()) {
                                auto to_delete = subtract_replica_sets(
                                  it->second, p_as.replicas);
                                _partition_allocator.local().remove_allocations(
                                  to_delete);
                            }
                        }

                        return ec;
                    });
            },
            [this, base_offset](create_topic_cmd create_cmd) {
                return dispatch_updates_to_cores(create_cmd, base_offset)
                  .then([this, create_cmd](std::error_code ec) {
                      if (ec == errc::success) {
                          update_allocations(create_cmd.value.assignments);
                      }
                      return ec;
                  })

                  .then([this, create_cmd](std::error_code ec) {
                      if (ec == errc::success) {
                          std::vector<ntp_leader> leaders;
                          const auto& tp_ns = create_cmd.value.cfg.tp_ns;
                          for (auto& p_as : create_cmd.value.assignments) {
                              leaders.emplace_back(
                                model::ntp(tp_ns.ns, tp_ns.tp, p_as.id),
                                p_as.replicas.begin()->node_id);
                          }
                          return update_leaders_with_estimates(leaders).then(
                            [ec]() {
                                return ss::make_ready_future<std::error_code>(
                                  ec);
                            });
                      }
                      return ss::make_ready_future<std::error_code>(ec);
                  });
            },
            [this, base_offset](move_partition_replicas_cmd cmd) {
                auto p_as = _topic_table.local().get_partition_assignment(
                  cmd.key);
                return dispatch_updates_to_cores(cmd, base_offset)
                  .then([this, p_as = std::move(p_as), cmd = std::move(cmd)](
                          std::error_code ec) {
                      if (!ec) {
                          vassert(
                            p_as.has_value(),
                            "Partition {} have to exist before successful "
                            "partition reallocation",
                            cmd.key);
                          auto to_add = subtract_replica_sets(
                            cmd.value, p_as->replicas);
                          _partition_allocator.local().add_allocations(to_add);
                      }
                      return ec;
                  });
            },
            [this, base_offset](cancel_moving_partition_replicas_cmd cmd) {
                auto current_assignment
                  = _topic_table.local().get_partition_assignment(cmd.key);
                auto new_target_replicas
                  = _topic_table.local().get_previous_replica_set(cmd.key);
                auto ntp = cmd.key;
                return dispatch_updates_to_cores(std::move(cmd), base_offset)
                  .then([this,
                         ntp = std::move(ntp),
                         current_assignment = std::move(current_assignment),
                         new_target_replicas = std::move(new_target_replicas)](
                          std::error_code ec) {
                      if (ec) {
                          return ec;
                      }
                      vassert(
                        current_assignment.has_value()
                          && new_target_replicas.has_value(),
                        "Previous replicas for NTP {} must exists as finish "
                        "update can only be applied to partition that is "
                        "currently being updated",
                        ntp);

                      auto to_delete = subtract_replica_sets(
                        current_assignment->replicas, *new_target_replicas);
                      _partition_allocator.local().remove_allocations(
                        to_delete);
                      return ec;
                  });
            },
            [this, base_offset](finish_moving_partition_replicas_cmd cmd) {
                auto previous_replicas
                  = _topic_table.local().get_previous_replica_set(cmd.key);
                return dispatch_updates_to_cores(cmd, base_offset)
                  .then([this,
                         ntp = std::move(cmd.key),
                         previous_replicas = previous_replicas,
                         current_replicas = std::move(cmd.value)](
                          std::error_code ec) {
                      if (ec) {
                          return ec;
                      }
                      vassert(
                        previous_replicas.has_value(),
                        "Previous replicas for NTP {} must exists as finish "
                        "update can only be applied to partition that is "
                        "currently being updated",
                        ntp);

                      auto to_delete = subtract_replica_sets(
                        *previous_replicas, current_replicas);
                      _partition_allocator.local().remove_allocations(
                        to_delete);
                      return ec;
                  });
            },
            [this, base_offset](update_topic_properties_cmd cmd) {
                return dispatch_updates_to_cores(std::move(cmd), base_offset);
            },
            [this, base_offset](create_partition_cmd cmd) {
                return dispatch_updates_to_cores(cmd, base_offset)
                  .then([this, cmd](std::error_code ec) {
                      if (ec == errc::success) {
                          update_allocations(cmd.value.assignments);
                      }
                      return ec;
                  });
            },
            [this, base_offset](create_non_replicable_topic_cmd cmd) {
                auto assignments = _topic_table.local().get_topic_assignments(
                  cmd.key.source);
                return dispatch_updates_to_cores(cmd, base_offset)
                  .then([this, assignments = std::move(assignments)](
                          std::error_code ec) {
                      if (ec == errc::success) {
                          vassert(
                            assignments.has_value(), "null topic_metadata");
                          std::vector<partition_assignment> p_as;
                          p_as.reserve(assignments->size());
                          std::move(
                            assignments->begin(),
                            assignments->end(),
                            std::back_inserter(p_as));
                          update_allocations(std::move(p_as));
                      }
                      return ec;
                  });
            });
      });
}
topic_updates_dispatcher::in_progress_map
topic_updates_dispatcher::collect_in_progress(
  const model::topic_namespace& tp_ns,
  const assignments_set& current_assignments) {
    absl::node_hash_map<model::ntp, std::vector<model::broker_shard>>
      in_progress;
    in_progress.reserve(current_assignments.size());
    // collect in progress assignments
    for (auto& p : current_assignments) {
        model::ntp ntp(tp_ns.ns, tp_ns.tp, p.id);
        auto previous = _topic_table.local().get_previous_replica_set(ntp);
        if (previous) {
            in_progress.emplace(std::move(ntp), std::move(previous.value()));
        }
    }
    return in_progress;
}

ss::future<> topic_updates_dispatcher::update_leaders_with_estimates(
  std::vector<ntp_leader> leaders) {
    for (const auto& i : leaders) {
        vlog(
          clusterlog.debug,
          "update_leaders_with_estimates: new NTP {} leader {}",
          i.first,
          i.second);
    }
    return ss::do_with(
      std::move(leaders), [this](std::vector<ntp_leader>& leaders) {
          return ss::parallel_for_each(leaders, [this](ntp_leader& leader) {
              return _partition_leaders_table.invoke_on_all(
                [leader](partition_leaders_table& l) {
                    return l.update_partition_leader(
                      leader.first, model::term_id(1), leader.second);
                });
          });
      });
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
    using ret_t = std::vector<std::error_code>;
    return ss::do_with(
      ret_t{}, [this, cmd = std::move(cmd), o](ret_t& ret) mutable {
          ret.reserve(ss::smp::count);
          return ss::parallel_for_each(
                   boost::irange(0, (int)ss::smp::count),
                   [this, &ret, cmd = std::move(cmd), o](int shard) mutable {
                       return do_apply(shard, cmd, _topic_table, o)
                         .then([&ret](std::error_code r) { ret.push_back(r); });
                   })
            .then([&ret] { return std::move(ret); })
            .then([](std::vector<std::error_code> results) mutable {
                auto ret = results.front();
                for (auto& r : results) {
                    vassert(
                      ret == r,
                      "State inconsistency across shards detected, expected "
                      "result: {}, have: {}",
                      ret,
                      r);
                }
                return ret;
            });
      });
}

void topic_updates_dispatcher::deallocate_topic(
  const model::topic_namespace& tp_ns,
  const assignments_set& topic_assignments,
  const in_progress_map& in_progress) {
    for (auto& p_as : topic_assignments) {
        _partition_allocator.local().deallocate(p_as.replicas);
        auto it = in_progress.find(model::ntp(tp_ns.ns, tp_ns.tp, p_as.id));

        // we must remove the allocation that would normally
        // be removed with update_finished request
        if (it != in_progress.end()) {
            auto to_delete = subtract_replica_sets(it->second, p_as.replicas);
            _partition_allocator.local().remove_allocations(to_delete);
        }
    }
}

void topic_updates_dispatcher::update_allocations(
  std::vector<partition_assignment> assignments) {
    // for create topics we update allocation state
    std::vector<model::broker_shard> shards;
    raft::group_id max_group_id = raft::group_id(0);
    for (auto& pas : assignments) {
        max_group_id = std::max(max_group_id, pas.group);
        std::move(
          pas.replicas.begin(), pas.replicas.end(), std::back_inserter(shards));
    }

    _partition_allocator.local().update_allocation_state(shards, max_group_id);
}

} // namespace cluster
