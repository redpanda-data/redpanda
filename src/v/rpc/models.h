#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"

namespace rpc {
template<>
inline void serialize(bytes_ostream& out, model::ntp&& ntp) {
    rpc::serialize(
      out, sstring(ntp.ns), sstring(ntp.tp.topic), ntp.tp.partition);
}
template<>
inline future<model::ntp> deserialize(source& in) {
    struct _ntp {
        sstring ns;
        sstring topic;
        model::partition_id::type p;
    };
    return deserialize<_ntp>(in).then([](_ntp n) {
        return model::ntp{model::ns(n.ns),
                          model::topic_partition{model::topic(n.topic),
                                                 model::partition_id(n.p)}};
    });
}

template<>
inline void serialize(bytes_ostream& out, model::broker&& r) {
    rpc::serialize(out, r.id()(), sstring(r.host()), r.port());
}

// from wire
template<>
inline future<model::offset> deserialize(source& in) {
    using type = model::offset::type;
    return deserialize<type>(in).then(
      [](type v) { return model::offset{std::move(v)}; });
}
template<>
inline future<model::broker> deserialize(source& in) {
    struct broker_contents {
        model::node_id id;
        sstring host;
        int32_t port;
        std::optional<sstring> rack;
    };
    return deserialize<broker_contents>(in).then([](broker_contents res) {
        return model::broker(
          std::move(res.id),
          std::move(res.host),
          res.port,
          std::move(res.rack));
    });
}
template<>
inline void serialize(bytes_ostream& ref, model::record&& record) {
    rpc::serialize(
      ref,
      record.size_bytes(),
      record.attributes().value(),
      record.timestamp_delta(),
      record.offset_delta(),
      record.release_key(),
      record.release_packed_value_and_headers());
}

template<>
inline future<model::record> deserialize(source& in) {
    struct simple_record {
        uint32_t size_bytes;
        model::record_attributes attributes;
        int32_t timestamp_delta;
        int32_t offset_delta;
        fragbuf key;
        fragbuf value_and_headers;
    };
    return rpc::deserialize<simple_record>(in).then([](simple_record r) {
        return model::record(
          r.size_bytes,
          r.attributes,
          r.timestamp_delta,
          r.offset_delta,
          std::move(r.key),
          std::move(r.value_and_headers));
    });
}

} // namespace rpc
