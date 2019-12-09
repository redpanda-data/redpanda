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

template<>
inline void serialize(iobuf& out, model::broker&& r) {
    rpc::serialize(
      out, r.id(), sstring(r.host()), r.port(), std::optional(r.rack()));
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
inline void serialize(iobuf& ref, model::record&& record) {
    rpc::serialize(
      ref,
      record.size_bytes(),
      record.attributes().value(),
      record.timestamp_delta(),
      record.offset_delta(),
      record.share_key(),
      record.share_packed_value_and_headers());
}

template<>
inline future<model::record> deserialize(source& in) {
    struct simple_record {
        uint32_t size_bytes;
        model::record_attributes attributes;
        int32_t timestamp_delta;
        int32_t offset_delta;
        iobuf key;
        iobuf value_and_headers;
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
