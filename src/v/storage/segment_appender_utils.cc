#include "storage/segment_appender_utils.h"

#include "model/record.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "storage/logger.h"
#include "utils/vint.h"
#include "vassert.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>

#include <bits/stdint-uintn.h>

#include <memory>
#include <type_traits>
namespace storage {

iobuf disk_header_to_iobuf(const model::record_batch_header& h) {
    iobuf b;
    reflection::serialize(
      b,
      h.size_bytes,
      h.base_offset(),
      h.type(),
      h.crc,
      h.attrs.value(),
      h.last_offset_delta,
      h.first_timestamp.value(),
      h.max_timestamp.value(),
      h.producer_id,
      h.producer_epoch,
      h.base_sequence,
      h.record_count);
    vassert(
      b.size_bytes() == model::packed_record_batch_header_size,
      "disk headers must be of static size:{}, but got{}",
      model::packed_record_batch_header_size,
      b.size_bytes());
    return b;
}

template<typename T>
typename std::enable_if_t<std::is_integral<T>::value, ss::future<>>
write(segment_appender& out, T i) {
    // NOLINTNEXTLINE
    auto p = reinterpret_cast<const char*>(&i);
    return out.append(p, sizeof(T));
}

ss::future<> write_vint(segment_appender& out, vint::value_type v) {
    auto encoding_buffer
      = std::make_unique<std::array<bytes::value_type, vint::max_length>>();
    auto p = encoding_buffer->data();
    const auto size = vint::serialize(v, p);
    // NOLINTNEXTLINE
    return out.append(reinterpret_cast<const char*>(p), size)
      .finally([e = std::move(encoding_buffer)] {});
}

ss::future<> write(segment_appender& out, const iobuf& buf) {
    return out.append(buf);
}

ss::future<>
write(segment_appender& out, const std::vector<model::record_header>& headers) {
    return write_vint(out, headers.size()).then([&] {
        if (!headers.empty()) {
            return ss::do_for_each(headers, [&](const model::record_header& h) {
                return write_vint(out, h.key_size())
                  .then([&] {
                      if (h.key_size() > 0) {
                          return out.append(h.key());
                      }
                      return ss::make_ready_future<>();
                  })
                  .then([&] { return write_vint(out, h.value_size()); })
                  .then([&] {
                      if (h.value_size() > 0) {
                          return out.append(h.value());
                      }
                      return ss::make_ready_future<>();
                  });
            });
        }
        return ss::make_ready_future<>();
    });

} // namespace storage

ss::future<> write(segment_appender& out, const model::record& record) {
    return write_vint(out, record.size_bytes())
      .then([&] { return write(out, record.attributes().value()); })
      .then([&] { return write_vint(out, record.timestamp_delta()); })
      .then([&] { return write_vint(out, record.offset_delta()); })
      .then([&] { return write_vint(out, record.key_size()); })
      .then([&] {
          if (record.key_size() > 0) {
              return out.append(record.key());
          }
          return ss::make_ready_future<>();
      })
      .then([&] { return write_vint(out, record.value_size()); })
      .then([&] {
          if (record.value_size() > 0) {
              return out.append(record.value());
          }
          return ss::make_ready_future<>();
      })
      .then([&] { return write(out, record.headers()); });
}

ss::future<>
write(segment_appender& appender, const model::record_batch& batch) {
    auto hdrbuf = disk_header_to_iobuf(batch.header());
    // control_share is *very* cheap
    return write(appender, hdrbuf.control_share())
      .then([&appender, &batch, cpy = hdrbuf.control_share()] {
          if (batch.compressed()) {
              return write(appender, batch.get_compressed_records());
          }
          return ss::do_for_each(
            batch, [&appender](const model::record& record) {
                return write(appender, record);
            });
      });
}

} // namespace storage
