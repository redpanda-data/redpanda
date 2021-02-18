// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/types.h"

#include "hashing/crc32c.h"
#include "reflection/for_each_field.h"

#include <seastar/core/byteorder.hh>

#include <boost/crc.hpp>
#include <fmt/format.h>

#include <ostream>

namespace rpc {
template<typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
void crc_one(crc32& crc, T t) {
    T args_le = ss::cpu_to_le(t);
    crc.extend(args_le);
}

uint32_t checksum_header_only(const header& h) {
    auto crc = crc32();
    crc_one(
      crc,
      static_cast<std::underlying_type_t<compression_type>>(h.compression));
    crc_one(crc, h.payload_size);
    crc_one(crc, h.meta);
    crc_one(crc, h.correlation_id);
    crc_one(crc, h.payload_checksum);
    return crc.value();
}

std::ostream& operator<<(std::ostream& o, const header& h) {
    // NOTE: if we use the int8_t types, ostream doesn't print 0's
    // artificially ast version and compression as ints
    return o << "{version:" << int(h.version)
             << ", header_checksum:" << h.header_checksum
             << ", compression:" << static_cast<int>(h.compression)
             << ", payload_size:" << h.payload_size << ", meta:" << h.meta
             << ", correlation_id:" << h.correlation_id
             << ", payload_checksum:" << h.payload_checksum << "}";
}

std::ostream& operator<<(std::ostream& o, const server_configuration& c) {
    o << "{";
    for (auto& a : c.addrs) {
        o << a;
    }
    o << ", max_service_memory_per_core: " << c.max_service_memory_per_core
      << ", has_tls_credentials: " << (c.credentials ? "yes" : "no")
      << ", metrics_enabled:" << !c.disable_metrics;
    return o << "}";
}

std::ostream& operator<<(std::ostream& os, const server_endpoint& ep) {
    fmt::print(os, "{{{}:{}}}", ep.name, ep.addr);
    return os;
}

std::ostream& operator<<(std::ostream& o, const status& s) {
    switch (s) {
    case status::success:
        return o << "rpc::status::success";
    case status::method_not_found:
        return o << "rpc::status::method_not_found";
    case status::request_timeout:
        return o << "rpc::status::request_timeout";
    case status::server_error:
        return o << "rpc::status::server_error";
    default:
        return o << "rpc::status::unknown";
    }
}

} // namespace rpc
