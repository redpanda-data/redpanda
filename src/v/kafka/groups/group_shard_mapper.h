#pragma once
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/reactor.hh>

namespace kafka {

// clang-format off
CONCEPT(
template<typename T>
concept PartitionShardMapper = requires(T m, model::ntp ntp) {
    { m.shard_for(ntp) } -> ss::shard_id;
};
)
// clang-format on

/// \brief Mapping from Kafka group to owning core.
template<typename Shards>
CONCEPT(requires PartitionShardMapper<Shards>)
class group_shard_mapper {
public:
    explicit group_shard_mapper(Shards& shards)
      : _shards(shards) {}

    std::optional<ss::shard_id> shard_for(const kafka::group_id& group) {
        incremental_xxhash64 inc;
        inc.update(group);
        auto p = static_cast<model::partition_id::type>(
          jump_consistent_hash(inc.digest(), _num_partitions));
        auto tp = model::topic_partition{_topic, model::partition_id{p}};
        auto ntp = model::ntp{_ns, tp};
        return _shards.shard_for(ntp);
    }

    void configure() {
        /// TODO
        ///  - This needs to be hooked up to the controller some how in order to
        ///  get an initial configuration (and any updates) of the group
        ///  metadata topic.
    }

private:
    model::ns _ns;
    model::topic _topic;
    model::partition_id::type _num_partitions{};
    Shards& _shards;
};

} // namespace kafka
