#include "cluster/types.h"
#include "rpc/models.h"
#include "rpc/serialize.h"

namespace rpc {
template<>
void serialize(bytes_ostream& out, cluster::topic_configuration&& t) {
    rpc::serialize(
      out,
      sstring(t.ns),
      sstring(t.topic),
      t.partition_count,
      t.replication_factor,
      t.compression,
      t.compaction,
      t.retention_bytes,
      t.retention);
}

template<>
future<cluster::partition_assignment> deserialize(source& in) {
    struct _simple {
        raft::group_id group;
        model::ntp ntp;
    };
    return deserialize<_simple>(in).then([&in](_simple s) {
        return deserialize<std::vector<cluster::broker_shard>>(in).then(
          [s = std::move(s)](std::vector<cluster::broker_shard> replicas) {
              return cluster::partition_assignment{
                .group = s.group,
                .ntp = std::move(s.ntp),
                .replicas = std::move(replicas)};
          });
    });
}
} // namespace rpc
