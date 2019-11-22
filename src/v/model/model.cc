#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "utils/string_switch.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <fmt/ostream.h>

namespace model {

std::ostream& operator<<(std::ostream& os, const compression& c) {
    os << "{compression: ";
    switch (c) {
    case compression::none:
        os << "none";
        break;
    case compression::gzip:
        os << "gzip";
        break;
    case compression::snappy:
        os << "snappy";
        break;
    case compression::lz4:
        os << "lz4";
        break;
    case compression::zstd:
        os << "zstd";
        break;
    }
    return os << "}";
}

std::ostream& operator<<(std::ostream& os, timestamp ts) {
    if (ts != timestamp::missing()) {
        return fmt_print(os, "{{timestamp: {}}}", ts.value());
    }
    return os << "{timestamp: missing}";
}

std::ostream& operator<<(std::ostream& os, const topic_partition& tp) {
    return fmt_print(os, "{{topic_partition: {}:{}}}", tp.topic, tp.partition);
}

std::ostream& operator<<(std::ostream& os, const ntp& n) {
    return fmt_print(os, "{{ntp: {}:{}}}", n.ns, n.tp);
}

std::ostream& operator<<(std::ostream& os, timestamp_type ts) {
    switch (ts) {
    case timestamp_type::append_time:
        return os << "{append_time}";
    case timestamp_type::create_time:
        return os << "{create_time}";
    }
    throw std::runtime_error("Unknown timestamp type");
}

std::ostream& operator<<(std::ostream& os, const record& record) {
    return fmt_print(
      os,
      "{{record: size_bytes={}, timestamp_delta={}, "
      "offset_delta={}, key={} bytes, value_and_headers={} bytes}}",
      record._size_bytes,
      record._timestamp_delta,
      record._offset_delta,
      record._key.size_bytes(),
      record._value_and_headers.size_bytes());
}

std::ostream&
operator<<(std::ostream& os, const record_batch_attributes& attrs) {
    return fmt_print(os, "{}:{}", attrs.compression(), attrs.timestamp_type());
}

std::ostream& operator<<(std::ostream& os, const record_batch_header& header) {
    return fmt_print(
      os,
      "{{header: size_bytes={}, base_offset={}, crc={}, attrs={}, "
      "last_offset_delta={}, first_timestamp={}, max_timestamp={}}}",
      header.size_bytes,
      header.base_offset,
      header.crc,
      header.attrs,
      header.last_offset_delta,
      header.first_timestamp,
      header.max_timestamp);
}

std::ostream&
operator<<(std::ostream& os, const record_batch::compressed_records& records) {
    return fmt_print(
      os, "{{compressed_records: size_bytes={}}}", records.size_bytes());
}

std::ostream& operator<<(std::ostream& os, const record_batch& batch) {
    fmt::print(
      os, "{{record_batch: {}, count={},records=", batch._header, batch.size());
    if (batch.compressed()) {
        os << batch.get_compressed_records();
    } else {
        os << "{";
        for (auto& r : batch) {
            os << r;
        }
        os << "}";
    }
    os << "}";
    return os;
}

sstring ntp::path() const {
    return fmt::format("{}/{}/{}", ns(), tp.topic(), tp.partition());
}

std::istream& operator>>(std::istream& i, compression& c) {
    sstring s;
    i >> s;
    c = string_switch<compression>(s)
          .match_all("none", "uncompressed", compression::none)
          .match("gzip", compression::gzip)
          .match("snappy", compression::snappy)
          .match("lz4", compression::lz4)
          .match("zstd", compression::zstd);
    return i;
}

std::ostream& operator<<(std::ostream& o, const model::broker_properties& b) {
    return fmt_print(
      o,
      "{cores {}, mem_available {}, disk_available {}}",
      b.cores,
      b.available_memory,
      b.available_disk,
      b.mount_paths,
      b.etc_props);
}

std::ostream& operator<<(std::ostream& o, const model::broker& b) {
    return o;
    return fmt_print(
      o,
      "id: {} kafka_api_address: {} rpc_address: {} rack: {} "
      "properties: {}",
      b.id(),
      b.kafka_api_address(),
      b.rpc_address(),
      b.rack(),
      b.properties());
}
} // namespace model
