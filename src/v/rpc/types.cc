#include "rpc/types.h"

#include "reflection/for_each_field.h"

#include <seastar/core/byteorder.hh>

#include <boost/crc.hpp>
#include <fmt/format.h>

namespace rpc {
template<typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
void crc_16_one(boost::crc_ccitt_type& crc, T t) {
    T args_le = ss::cpu_to_le(t);
    crc.process_bytes(
      // NOLINTNEXTLINE
      reinterpret_cast<const char*>(&args_le),
      sizeof(t));
}

uint16_t checksum_header_only(const header& h) {
    boost::crc_ccitt_type crc;
    crc_16_one(
      crc,
      static_cast<std::underlying_type_t<compression_type>>(h.compression));
    crc_16_one(crc, h.payload_size);
    crc_16_one(crc, h.meta);
    crc_16_one(crc, h.correlation_id);
    crc_16_one(crc, h.payload_checksum);
    return crc.checksum();
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

} // namespace rpc
