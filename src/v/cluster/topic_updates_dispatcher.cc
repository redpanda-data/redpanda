// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topic_updates_dispatcher.h"

#include "cluster/commands.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <iterator>
#include <system_error>
#include <vector>

namespace cluster {

topic_updates_dispatcher::topic_updates_dispatcher(
  ss::sharded<partition_allocator>& pal, ss::sharded<topic_table>& table)
  : _partition_allocator(pal)
  , _topic_table(table) {}

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
                  .then([this, tp_md](std::error_code ec) {
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
                          update_allocations(create_cmd);
                      }
                      return ec;
                  });
            },
            [this, base_offset](move_partition_replicas_cmd cmd) {
                auto tp_md = _topic_table.local().get_topic_metadata(
                  model::topic_namespace_view(cmd.key));
                return dispatch_updates_to_cores(cmd, base_offset)
                  .then([this, tp_md, cmd](std::error_code ec) {
                      if (!ec) {
                          vassert(
                            tp_md.has_value(),
                            "Topic had to exist before successful partition "
                            "reallocation");
                          auto it = std::find_if(
                            std::cbegin(tp_md->partitions),
                            std::cend(tp_md->partitions),
                            [p_id = cmd.key.tp.partition](
                              const model::partition_metadata& pmd) {
                                return pmd.id == p_id;
                            });
                          vassert(
                            it != tp_md->partitions.cend(),
                            "Reassigned partition must exist");

                          reallocate_partition(it->replicas, cmd.value);
                      }
                      return ec;
                  });
            },
            [this, base_offset](finish_moving_partition_replicas_cmd cmd) {
                return dispatch_updates_to_cores(std::move(cmd), base_offset);
            },
            [this, base_offset](update_topic_properties_cmd cmd) {
                return dispatch_updates_to_cores(std::move(cmd), base_offset);
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
  const model::topic_metadata& tp_md) {
    // we have to deallocate topics
    for (auto& p : tp_md.partitions) {
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

void topic_updates_dispatcher::update_allocations(const create_topic_cmd& cmd) {
    // for create topics we update allocation state
    std::vector<model::broker_shard> shards;
    raft::group_id max_group_id = raft::group_id(0);
    for (auto& pas : cmd.value.assignments) {
        max_group_id = std::max(max_group_id, pas.group);
        std::move(
          pas.replicas.begin(), pas.replicas.end(), std::back_inserter(shards));
    }

    _partition_allocator.local().update_allocation_state(
      std::move(shards), max_group_id);
}

} // namespace cluster
