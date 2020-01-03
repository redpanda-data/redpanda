#include "storage2/segment_writer.h"

#include "model/record.h"
#include "storage2/segment_appender.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/unaligned.hh>

namespace storage {

template<typename T>
typename std::enable_if_t<std::is_integral<T>::value, seastar::future<>>
write(segment_appender& out, T i) {
    auto* nr = reinterpret_cast<const unaligned<T>*>(&i);
    i = cpu_to_be(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    return out.append(p, sizeof(T));
}

future<> write_vint(segment_appender& out, vint::value_type v) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    const auto size = vint::serialize(v, encoding_buffer.begin());
    return out.append(
      reinterpret_cast<const char*>(encoding_buffer.data()), size);
}

future<> write(segment_appender& out, const iobuf& buf) {
    return out.append(buf);
}

// todo: having a single future for every written int is an overkill
// todo: coalesce those into one buffer write.
future<> write(segment_appender& out, const model::record& record) {
    return write_vint(out, record.size_bytes())
      .then([&] { return write(out, record.attributes().value()); })
      .then([&] { return write_vint(out, record.timestamp_delta()); })
      .then([&] { return write_vint(out, record.offset_delta()); })
      .then([&] { return write_vint(out, record.key().size_bytes()); })
      .then([&] { return write(out, record.key()); })
      .then([&] { return write(out, record.packed_value_and_headers()); });
}

future<> write(segment_appender& appender, const model::record_batch& batch) {
    return write_vint(appender, batch.size_bytes())
      .then(
        [&appender, &batch] { return write(appender, batch.base_offset()()); })
      .then([&appender, &batch] { return write(appender, batch.type()()); })
      .then([&appender, &batch] { return write(appender, batch.crc()); })
      .then([&appender, &batch] {
          return write(appender, batch.attributes().value());
      })
      .then([&appender, &batch] {
          return write(appender, batch.last_offset_delta());
      })
      .then([&appender, &batch] {
          return write(appender, batch.first_timestamp().value());
      })
      .then([&appender, &batch] {
          return write(appender, batch.max_timestamp().value());
      })
      .then([&appender, &batch] {
          // Note that we don't append the unused Kafka fields, but we do
          // take them into account when calculating the batch checksum.
          return write(appender, batch.size());
      })
      .then([&appender, &batch] {
          if (batch.compressed()) {
              return write(appender, batch.get_compressed_records().records());
          }
          return do_for_each(batch, [&appender](const model::record& record) {
              return write(appender, record);
          });
      });
}

future<> write_to(segment_appender& appender, model::record_batch&& batch) {
    return do_with(std::move(batch), [&appender](model::record_batch& batch) {
        return write(appender, batch);
    });
}

} // namespace storage