#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "rpc/models.h"

namespace cluster {

struct partition_assignment {
    /// \brief this is the same as a seastar::shard_id
    /// however, seastar uses unsized-ints (unsigned)
    /// and for predictability we need fixed-sized ints
    uint32_t shard;
    raft::group_id group;
    model::ntp ntp;
    model::broker broker;
};

} // namespace cluster

namespace rpc {
template<>
inline future<cluster::partition_assignment> deserialize(source& in) {
    struct _simple {
        uint32_t shard;
        raft::group_id group;
        model::ntp ntp;
    };
    return deserialize<_simple>(in).then([&in](_simple s) {
        return deserialize<model::broker>(in).then(
          [s = std::move(s)](model::broker b) {
              return cluster::partition_assignment{.shard = s.shard,
                                                   .group = s.group,
                                                   .ntp = s.ntp,
                                                   .broker = std::move(b)};
          });
    });
}
} // namespace rpc
