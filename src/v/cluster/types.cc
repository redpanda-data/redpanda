#include "cluster/types.h"

namespace reflection {
void adl<cluster::topic_configuration>::to(
  iobuf& out, cluster::topic_configuration&& t) {
    reflection::serialize(
      out,
      ss::sstring(std::move(t.ns)),
      ss::sstring(std::move(t.topic)),
      t.partition_count,
      t.replication_factor,
      t.compression,
      t.compaction,
      t.retention_bytes,
      t.retention);
}

cluster::topic_configuration
adl<cluster::topic_configuration>::from(iobuf_parser& in) {
    auto ns = model::ns(adl<ss::sstring>{}.from(in));
    auto topic = model::topic(adl<ss::sstring>{}.from(in));
    auto count = adl<int32_t>{}.from(in);
    auto rf = adl<int16_t>{}.from(in);
    auto tp_cfg = cluster::topic_configuration(
      std::move(ns), std::move(topic), count, rf);

    tp_cfg.compression = adl<model::compression>{}.from(in);
    tp_cfg.compaction = adl<model::topic_partition::compaction>{}.from(in);
    tp_cfg.retention_bytes = adl<uint64_t>{}.from(in);
    tp_cfg.retention = adl<model::timeout_clock::duration>{}.from(in);
    return tp_cfg;
}

void adl<cluster::join_request>::to(iobuf& out, cluster::join_request&& r) {
    adl<model::broker>().to(out, std::move(r.node));
}

cluster::join_request adl<cluster::join_request>::from(iobuf io) {
    return reflection::from_iobuf<cluster::join_request>(std::move(io));
}

cluster::join_request adl<cluster::join_request>::from(iobuf_parser& in) {
    return cluster::join_request(adl<model::broker>().from(in));
}
} // namespace reflection