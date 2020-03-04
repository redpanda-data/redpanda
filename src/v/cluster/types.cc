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

void adl<cluster::topic_result>::to(iobuf& out, cluster::topic_result&& t) {
    reflection::serialize(out, std::move(t.topic), t.ec);
}

cluster::topic_result adl<cluster::topic_result>::from(iobuf_parser& in) {
    auto topic = model::topic(adl<ss::sstring>{}.from(in));
    auto ec = adl<cluster::errc>{}.from(in);
    return cluster::topic_result(std::move(topic), ec);
}

void adl<cluster::create_topics_request>::to(
  iobuf& out, cluster::create_topics_request&& r) {
    reflection::serialize(out, std::move(r.topics), r.timeout.count());
}

cluster::create_topics_request
adl<cluster::create_topics_request>::from(iobuf io) {
    return reflection::from_iobuf<cluster::create_topics_request>(
      std::move(io));
}

cluster::create_topics_request
adl<cluster::create_topics_request>::from(iobuf_parser& in) {
    using underlying_t = std::vector<cluster::topic_configuration>;
    auto configs = adl<underlying_t>().from(in);
    auto timeout = model::timeout_clock::duration(adl<uint64_t>().from(in));
    return cluster::create_topics_request{std::move(configs), timeout};
}

void adl<cluster::create_topics_reply>::to(
  iobuf& out, cluster::create_topics_reply&& r) {
    reflection::serialize(out, std::move(r.results), std::move(r.metadata));
}

cluster::create_topics_reply adl<cluster::create_topics_reply>::from(iobuf io) {
    return reflection::from_iobuf<cluster::create_topics_reply>(std::move(io));
}

cluster::create_topics_reply
adl<cluster::create_topics_reply>::from(iobuf_parser& in) {
    auto results = adl<std::vector<cluster::topic_result>>().from(in);
    auto md = adl<std::vector<model::topic_metadata>>().from(in);
    return cluster::create_topics_reply{std::move(results), std::move(md)};
}
} // namespace reflection