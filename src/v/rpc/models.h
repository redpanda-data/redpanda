#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

namespace rpc {
template<>
inline void serialize(iobuf& out, model::ntp&& ntp) {
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
inline void serialize(iobuf& out, net::inet_address&& addr) {
    using family_t = seastar::net::inet_address::family;
    switch (addr.in_family()) {
    case family_t::INET:
        rpc::serialize(out, addr.in_family(), addr.as_ipv4_address().ip);
        break;
    case family_t::INET6:
        rpc::serialize(out, addr.in_family(), addr.as_ipv6_address().ip);
        break;
    }
}

template<>
inline future<net::inet_address> deserialize(source& in) {
    using addr_t = seastar::net::inet_address;
    return rpc::deserialize<addr_t::family>(in).then([&in](addr_t::family f) {
        switch (f) {
        case addr_t::family::INET:
            using ip_t = uint32_t;
            return rpc::deserialize<ip_t>(in).then([](uint32_t ip) {
                return net::inet_address(net::ipv4_address(ip));
            });
        case addr_t::family::INET6:
            using ip6_t = net::ipv6_address::ipv6_bytes;
            return rpc::deserialize<ip6_t>(in).then([](ip6_t ip) {
                return net::inet_address(net::ipv6_address(ip));
            });
        }
        __builtin_unreachable();
    });
}

template<>
inline void serialize(iobuf& out, socket_address&& addr) {
    rpc::serialize(out, std::move(addr.addr()), addr.port());
}

template<>
inline future<socket_address> deserialize(source& in) {
    return rpc::deserialize<net::inet_address>(in).then(
      [&in](net::inet_address addr) mutable {
          return rpc::deserialize<uint16_t>(in).then(
            [addr = std::move(addr)](uint16_t port) mutable {
                return socket_address(std::move(addr), port);
            });
      });
}

// FIXME: Change to generic unordered map serdes when RPC will work with ADL
template<>
inline void serialize(iobuf& out, std::unordered_map<sstring, sstring>&& map) {
    using type = std::vector<std::pair<sstring, sstring>>;
    type vec;
    vec.reserve(map.size());
    std::move(std::begin(map), std::end(map), std::back_inserter(vec));
    serialize(out, std::move(vec));
}

template<>
inline future<std::unordered_map<sstring, sstring>> deserialize(source& in) {
    using type = std::vector<std::pair<sstring, sstring>>;
    return deserialize<type>(in).then([](type pairs) {
        std::unordered_map<sstring, sstring> map;
        for (auto& p : pairs) {
            map.insert(std::move(p));
        }
        return map;
    });
}

template<>
inline void serialize(iobuf& out, model::broker_properties&& p) {
    serialize(
      out,
      p.cores,
      p.available_memory,
      p.available_disk,
      std::move(p.mount_paths),
      std::move(p.etc_props));
}

template<>
inline future<model::broker_properties> deserialize(source& in) {
    struct simple {
        uint32_t cores;
        uint32_t available_memory;
        uint32_t available_disk;
    };
    return deserialize<simple>(in).then([&in](simple s) mutable {
        return deserialize<std::vector<sstring>>(in).then(
          [&in, s = std::move(s)](std::vector<sstring> m_points) mutable {
              return deserialize<std::unordered_map<sstring, sstring>>(in).then(
                [s = std::move(s), m_points = std::move(m_points)](
                  std::unordered_map<sstring, sstring> props) mutable {
                    return model::broker_properties{
                      .cores = s.cores,
                      .available_memory = s.available_memory,
                      .available_disk = s.available_disk,
                      .mount_paths = std::move(m_points),
                      .etc_props = std::move(props)};
                });
          });
    });
}

template<>
inline void serialize(iobuf& out, unresolved_address&& a) {
    rpc::serialize(out, sstring(a.host()), a.port());
}

template<>
inline future<unresolved_address> deserialize(source& in) {
    struct simple {
        sstring host;
        uint16_t port;
    };
    return deserialize<simple>(in).then(
      [](simple s) { return unresolved_address(std::move(s.host), s.port); });
}

template<>
inline void serialize(iobuf& out, model::broker&& r) {
    rpc::serialize(
      out,
      r.id(),
      unresolved_address(r.kafka_api_address()),
      unresolved_address(r.rpc_address()),
      std::optional(r.rack()),
      model::broker_properties(r.properties()));
}

template<>
inline future<model::broker> deserialize(source& in) {
    struct broker_contents {
        model::node_id id;
        unresolved_address kafka_api_addr;
        unresolved_address rpc_address;
        std::optional<sstring> rack;
    };
    return deserialize<broker_contents>(in).then([&in](broker_contents res) {
        return deserialize<model::broker_properties>(in).then(
          [&in, res = std::move(res)](model::broker_properties props) {
              return model::broker(
                std::move(res.id),
                std::move(res.kafka_api_addr),
                std::move(res.rpc_address),
                std::move(res.rack),
                props);
          });
    });
}

struct simple_record {
    uint32_t size_bytes;
    model::record_attributes attributes;
    int32_t timestamp_delta;
    int32_t offset_delta;
    iobuf key;
    iobuf value_and_headers;
};

template<>
inline void serialize(iobuf& ref, model::record&& record) {
    rpc::serialize(
      ref,
      simple_record{
        .size_bytes = record.size_bytes(),
        .attributes = record.attributes(),
        .timestamp_delta = record.timestamp_delta(),
        .offset_delta = record.offset_delta(),
        .key = record.share_key(),
        .value_and_headers = record.share_packed_value_and_headers()});
}

template<>
inline future<model::record> deserialize(source& in) {
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

struct batch_header {
    model::record_batch_header bhdr;
    uint32_t batch_size;
    int8_t is_compressed;
};

template<>
inline void serialize(iobuf& out, model::record_batch&& batch) {
    batch_header hdr{
      .bhdr = batch.release_header(),
      .batch_size = batch.size(),
      .is_compressed = static_cast<int8_t>(batch.compressed() ? 1 : 0)};
    rpc::serialize(out, std::move(hdr));
    if (!batch.compressed()) {
        for (model::record& r : batch) {
            rpc::serialize(out, std::move(r));
        }
    } else {
        rpc::serialize(out, std::move(batch).release().release());
    }
}

template<>
inline future<model::record_batch> deserialize(source& in) {
    return rpc::deserialize<batch_header>(in).then([&in](batch_header b_hdr) {
        if (b_hdr.is_compressed == 1) {
            return rpc::deserialize<iobuf>(in).then(
              [hdr = std::move(b_hdr)](iobuf f) mutable {
                  return model::record_batch(
                    std::move(hdr.bhdr),
                    model::record_batch::compressed_records(
                      hdr.batch_size, std::move(f)));
              });
        }
        // not compressed
        return do_with(
                 boost::irange<uint32_t>(0, b_hdr.batch_size),
                 [&in](boost::integer_range<uint32_t>& r) {
                     return copy_range<std::vector<model::record>>(
                       r,
                       [&in](int) { return deserialize<model::record>(in); });
                 })
          .then([hdr = std::move(b_hdr)](std::vector<model::record> recs) {
              return model::record_batch(std::move(hdr.bhdr), std::move(recs));
          });
    });
}
} // namespace rpc
