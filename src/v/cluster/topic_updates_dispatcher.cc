// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_updates_dispatcher.h"

#include "cluster/commands.h"
#include "cluster/partition_leaders_table.h"
#include "model/metadata.h"
#include "raft/types.h"

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
                // delete case - we need state copy to
                auto tp_md = _topic_table.local().get_topic_metadata(
                  del_cmd.value);
                return dispatch_updates_to_cores(del_cmd, base_offset)
                  .then([this, tp_md = std::move(tp_md)](std::error_code ec) {
                      if (ec == errc::success) {
                          vassert(
                            tp_md.has_value(),
                            "Topic had to exist before successful delete");
                          deallocate_topic(*tp_md);
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
                  .then(
                    [this, p_as = std::move(p_as), cmd](std::error_code ec) {
                        if (!ec) {
                            vassert(
                              p_as.has_value(),
                              "Partition {} have to exist before successful "
                              "partition "
                              "reallocation",
                              cmd.key);

                            reallocate_partition(p_as->replicas, cmd.value);
                        }
                        return ec;
                    });
            },
            [this, base_offset](finish_moving_partition_replicas_cmd cmd) {
                return dispatch_updates_to_cores(std::move(cmd), base_offset);
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

void topic_updates_dispatcher::deallocate_topic(const topic_metadata& tp_md) {
    // we have to deallocate topics
    for (auto& p : tp_md.get_assignments()) {
        _partition_allocator.local().deallocate(p.replicas);
    }
}

void topic_updates_dispatcher::reallocate_partition(
  const std::vector<model::broker_shard>& previous,
  const std::vector<model::broker_shard>& current) {
    // we do not want to update group id in here as we are changing partition
    // that already exists, hence group id doesn't have to be updated.
    _partition_allocator.local().update_allocation_state(current, previous);
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
