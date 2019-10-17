#pragma once
#include "seastarx.h"
#include "redpanda/kafka/groups/types.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

namespace kafka::groups {

// clang-format off
CONCEPT(
template<typename T>
concept GroupShardMapper = requires(T m, const kafka::group_id& group_id) {
    { m.shard_for(group_id) } -> seastar::shard_id;
};
)
// clang-format on

/// \brief Forward group operations to the owning core.
template<typename GroupMgr, typename Shards>
CONCEPT(requires GroupShardMapper<Shards>)
class group_router final {
public:
    /// \brief Create an instance of the group router.
    ///
    /// The constructor takes a reference to sharded<Shards> but stores a
    /// reference to the local core's Shards instance. When instantiating
    /// the group router via sharded<group_router>::start, the constructor will
    /// run on each core so sharded<>::local() is valid.
    group_router(
      scheduling_group sched_group,
      smp_service_group smp_group,
      sharded<GroupMgr>& group_manager,
      sharded<Shards>& shards)
      : _sg(sched_group)
      , _ssg(smp_group)
      , _group_manager(group_manager)
      , _shards(shards.local()) {
    }

private:
    scheduling_group _sg;
    smp_service_group _ssg;
    sharded<GroupMgr>& _group_manager;
    Shards& _shards;
};

} // namespace kafka::groups
