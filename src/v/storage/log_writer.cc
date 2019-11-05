#include "storage/log_writer.h"

#include "storage/log.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/unaligned.hh>

#include <type_traits>

namespace storage {

default_log_writer::default_log_writer(log& log) noexcept
  : _log(log)
  , _last_offset(log.max_offset()) {
}

template<typename T>
typename std::enable_if_t<std::is_integral<T>::value, seastar::future<>>
write(log_segment_appender& out, T i) {
    auto* nr = reinterpret_cast<const unaligned<T>*>(&i);
    i = cpu_to_be(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    return out.append(p, sizeof(T));
}

future<> write_vint(log_segment_appender& out, vint::value_type v) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    const auto size = vint::serialize(v, encoding_buffer.begin());
    return out.append(
      reinterpret_cast<const char*>(encoding_buffer.data()), size);
}

future<> write(log_segment_appender& out, const fragbuf& buf) {
    return out.append(buf);
}

future<> write(log_segment_appender& out, const model::record& record) {
    return write_vint(out, record.size_bytes())
      .then([&] { return write(out, record.attributes().value()); })
      .then([&] { return write_vint(out, record.timestamp_delta()); })
      .then([&] { return write_vint(out, record.offset_delta()); })
      .then([&] { return write_vint(out, record.key().size_bytes()); })
      .then([&] { return write(out, record.key()); })
      .then([&] { return write(out, record.packed_value_and_headers()); });
}

future<>
write(log_segment_appender& appender, const model::record_batch& batch) {
    return write_vint(appender, batch.size_bytes())
      .then([&appender, &batch] {
          return write(appender, batch.base_offset()());
      })
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

future<stop_iteration> default_log_writer::
operator()(model::record_batch&& batch) {
    return do_with(std::move(batch), [this](model::record_batch& batch) {
        if (_last_offset > batch.base_offset()) {
            auto e = std::make_exception_ptr(std::runtime_error(fmt::format(
              "Attempted to write batch at offset:{}, which violates our "
              "monotonic offset of: {}",
              batch.base_offset(),
              _last_offset)));
            _log.get_probe().batch_write_error(e);
            return make_exception_future<stop_iteration>(std::move(e));
        }
        auto offset_before = _log.appender().file_byte_offset();
        return write(_log.appender(), batch)
          .then([this, &batch, offset_before] {
              _last_offset = batch.last_offset();
              _log.get_probe().add_bytes_written(
                _log.appender().file_byte_offset() - offset_before);
              return _log.maybe_roll().then([] { return stop_iteration::no; });
          })
          .handle_exception([this](std::exception_ptr e) {
              _log.get_probe().batch_write_error(e);
              return make_exception_future<stop_iteration>(e);
          });
    });
}

model::offset default_log_writer::end_of_stream() {
    return _last_offset;
}

} // namespace storage
