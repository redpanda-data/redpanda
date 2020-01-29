#include "storage/log_segment_appender_utils.h"

#include "model/record.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "storage/constants.h"
#include "storage/logger.h"
#include "utils/vint.h"
#include "vassert.h"

#include <seastar/core/byteorder.hh>

#include <bits/stdint-uintn.h>

#include <memory>
#include <type_traits>
namespace storage {

iobuf disk_header_to_iobuf(
  const model::record_batch_header& h, uint32_t batch_size) {
    constexpr size_t hdr_size = packed_header_size + sizeof(batch_size);
    iobuf b;
    reflection::serialize(
      b,
      h.size_bytes,
      h.base_offset,
      h.type,
      h.crc,
      h.attrs.value(),
      h.last_offset_delta,
      h.first_timestamp.value(),
      h.max_timestamp.value(),
      batch_size);
    vassert(
      b.size_bytes() == hdr_size,
      "disk headers must be of static size:{}, but got{}",
      hdr_size,
      b.size_bytes());
    return b;
}

template<typename T>
typename std::enable_if_t<std::is_integral<T>::value, ss::future<>>
write(log_segment_appender& out, T i) {
    auto p = reinterpret_cast<const char*>(&i);
    return out.append(p, sizeof(T));
}

ss::future<> write_vint(log_segment_appender& out, vint::value_type v) {
    auto encoding_buffer = std::unique_ptr<bytes::value_type[]>(
      new bytes::value_type[vint::max_length]);
    auto p = encoding_buffer.get();
    const auto size = vint::serialize(v, p);
    return out.append(reinterpret_cast<const char*>(p), size)
      .finally([e = std::move(encoding_buffer)] {});
}

ss::future<> write(log_segment_appender& out, const iobuf& buf) {
    return out.append(buf);
}

ss::future<> write(log_segment_appender& out, const model::record& record) {
    return write_vint(out, record.size_bytes())
      .then([&] { return write(out, record.attributes().value()); })
      .then([&] { return write_vint(out, record.timestamp_delta()); })
      .then([&] { return write_vint(out, record.offset_delta()); })
      .then([&] { return write_vint(out, record.key().size_bytes()); })
      .then([&] { return out.append(record.key()); })
      .then([&] { return out.append(record.packed_value_and_headers()); });
}

ss::future<>
write(log_segment_appender& appender, const model::record_batch& batch) {
    auto hdrbuf = disk_header_to_iobuf(batch.header(), batch.size());
    // control_share is *very* cheap
    return write(appender, hdrbuf.control_share())
      .then([&appender, &batch, cpy = hdrbuf.control_share()] {
          if (batch.compressed()) {
              return write(appender, batch.get_compressed_records().records());
          }
          return ss::do_for_each(
            batch, [&appender](const model::record& record) {
                return write(appender, record);
            });
      });
}

} // namespace storage
