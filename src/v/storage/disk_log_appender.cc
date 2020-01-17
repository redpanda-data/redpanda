#include "storage/disk_log_appender.h"

#include "storage/disk_log_impl.h"
#include "storage/log_segment_appender.h"
#include "utils/vint.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/unaligned.hh>

#include <type_traits>

namespace storage {

disk_log_appender::disk_log_appender(
  disk_log_impl& log,
  log_append_config config,
  log_clock::time_point append_time,
  model::offset offset) noexcept
  : _log(log)
  , _config(config)
  , _append_time(append_time)
  , _base_offset(offset)
  , _last_offset(log.max_offset()) {}

template<typename T>
typename std::enable_if_t<std::is_integral<T>::value, ss::future<>>
write(log_segment_appender& out, T i) {
    auto* nr = reinterpret_cast<const ss::unaligned<T>*>(&i);
    i = cpu_to_be(*nr);
    auto p = reinterpret_cast<const char*>(&i);
    return out.append(p, sizeof(T));
}

ss::future<> write_vint(log_segment_appender& out, vint::value_type v) {
    std::array<bytes::value_type, vint::max_length> encoding_buffer;
    const auto size = vint::serialize(v, encoding_buffer.begin());
    return out.append(
      reinterpret_cast<const char*>(encoding_buffer.data()), size);
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
      .then([&] { return write(out, record.key()); })
      .then([&] { return write(out, record.packed_value_and_headers()); });
}

ss::future<>
write(log_segment_appender& appender, const model::record_batch& batch) {
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
          return ss::do_for_each(
            batch, [&appender](const model::record& record) {
                return write(appender, record);
            });
      });
}

ss::future<> disk_log_appender::initialize() {
    return _log._failure_probes.append().then([this] {
        auto f = ss::make_ready_future<>();
        if (__builtin_expect(!_log._segs.back().reader, false)) {
            // FIXME: We need to persist the last offset somewhere.
            _log._term = _config.term;
            auto offset = _log._segs.size() > 0
                            ? _log._segs.back().reader->max_offset()
                                + model::offset(1)
                            : model::offset(0);
            f = _log.new_segment(offset, _log._term, _config.io_priority);
        }
        if (_log._term != _config.term) {
            _log._term = _config.term;
            f = f.then([this] { return _log.do_roll(); });
        }
        return f;
    });
}

ss::future<ss::stop_iteration>
disk_log_appender::operator()(model::record_batch&& batch) {
    batch.set_base_offset(_base_offset);
    _base_offset = batch.last_offset() + model::offset(1);
    return ss::do_with(std::move(batch), [this](model::record_batch& batch) {
        if (_last_offset > batch.base_offset()) {
            auto e = std::make_exception_ptr(std::runtime_error(fmt::format(
              "Attempted to write batch at offset:{}, which violates our "
              "monotonic offset of: {}",
              batch.base_offset(),
              _last_offset)));
            _log.get_probe().batch_write_error(e);
            return ss::make_exception_future<ss::stop_iteration>(std::move(e));
        }
        auto offset_before = _log._segs.back().appender->file_byte_offset();
        stlog.trace(
          "Wrting batch of {} records offsets [{},{}], compressed: {}",
          batch.size(),
          batch.base_offset(),
          batch.last_offset(),
          batch.compressed());
        return write(*_log._segs.back().appender, batch)
          .then([this, &batch, offset_before] {
              // we use inclusive upper bound
              _last_offset = batch.last_offset();
              _log.get_probe().add_bytes_written(
                _log._segs.back().appender->file_byte_offset() - offset_before);
              return _log.maybe_roll(_last_offset).then([] {
                  return ss::stop_iteration::no;
              });
          })
          .handle_exception([this](std::exception_ptr e) {
              _log.get_probe().batch_write_error(e);
              return ss::make_exception_future<ss::stop_iteration>(e);
          });
    });
}

ss::future<append_result> disk_log_appender::end_of_stream() {
    _log._tracker.update_dirty_offset(_last_offset);
    _log._segs.back().reader->set_last_written_offset(_last_offset);
    auto f = ss::make_ready_future<>();
    /// fsync, means we fsync _every_ record_batch
    /// most API's will want to batch the fsync, at least
    /// to the record_batch_reader level
    if (_config.should_fsync) {
        f = _log.flush();
    }
    return f.then([this] {
        return append_result{_append_time, _base_offset, _last_offset};
    });
}

} // namespace storage
