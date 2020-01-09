#include "cluster/types.h"
#include "rpc/models.h"
#include "rpc/serialize.h"

namespace rpc {
template<>
void serialize(iobuf& out, cluster::topic_configuration&& t) {
    rpc::serialize(
      out,
      ss::sstring(t.ns),
      ss::sstring(t.topic),
      t.partition_count,
      t.replication_factor,
      t.compression,
      t.compaction,
      t.retention_bytes,
      t.retention);
}

template<>
ss::future<cluster::topic_configuration> deserialize(rpc::source& in) {
    struct _simple {
        ss::sstring ns;
        ss::sstring topic;
        int32_t partition_count;
        int16_t replication_factor;
        model::compression compression;
        model::topic_partition::compaction compaction;
        uint64_t retention_bytes;
        model::timeout_clock::duration retention;
    };

    return deserialize<_simple>(in).then([](_simple s) {
        cluster::topic_configuration tp_cfg(
          model::ns(std::move(s.ns)),
          model::topic(std::move(s.topic)),
          s.partition_count,
          s.replication_factor);
        tp_cfg.compression = s.compression;
        tp_cfg.compaction = s.compaction;
        tp_cfg.retention_bytes = s.retention_bytes;
        tp_cfg.retention = s.retention;
        return std::move(tp_cfg);
    });
}

template<>
void serialize(iobuf& out, cluster::join_request&& r) {
    rpc::serialize(out, std::move(r.node));
}

template<>
ss::future<cluster::join_request> deserialize(rpc::source& in) {
    return deserialize<model::broker>(in).then(
      [](model::broker b) { return cluster::join_request(std::move(b)); });
}

} // namespace rpc
