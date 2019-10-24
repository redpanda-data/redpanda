#include "cluster/types.h"
#include "rpc/models.h"
#include "rpc/serialize.h"

template<>
future<cluster::partition_assignment> deserialize(source& in) {
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
