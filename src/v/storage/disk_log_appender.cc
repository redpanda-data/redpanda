#include "storage/disk_log_appender.h"

#include "likely.h"
#include "storage/disk_log_impl.h"
#include "storage/log_segment_appender.h"

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
  , _idx(offset)
  , _base_offset(offset)
  , _last_offset(log.max_offset()) {}

ss::future<ss::stop_iteration>
disk_log_appender::operator()(model::record_batch&& batch) {
    batch.header().base_offset = _idx;
    _idx = batch.last_offset() + model::offset(1);
    _last_term = batch.term();
    auto next_offset = batch.base_offset();
    return _log.maybe_roll(_last_term, next_offset, _config.io_priority)
      .then([this, batch = std::move(batch)]() mutable {
          return _log._segs.back()
            ->append(std::move(batch))
            .then([this](append_result r) {
                _byte_size += r.byte_size;
                // do not track base_offset, only the last one
                _last_offset = r.last_offset;
                _log.get_probe().add_bytes_written(r.byte_size);
                return ss::stop_iteration::no;
            });
      })
      .handle_exception([this](std::exception_ptr e) {
          _log.get_probe().batch_write_error(e);
          return ss::make_exception_future<ss::stop_iteration>(e);
      });
}

ss::future<append_result> disk_log_appender::end_of_stream() {
    auto f = ss::make_ready_future<>();
    /// fsync, means we fsync _every_ record_batch
    /// most API's will want to batch the fsync, at least
    /// to the record_batch_reader level
    if (_config.should_fsync) {
        f = _log.flush();
    }
    return f.then([this] {
        return append_result{.append_time = _append_time,
                             .base_offset = _base_offset,
                             .last_offset = _last_offset,
                             .byte_size = _byte_size,
                             .last_term = _last_term};
    });
}

} // namespace storage
