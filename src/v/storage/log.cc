#include "storage/log.h"

#include "storage/log_manager.h"
#include "storage/log_writer.h"
#include "storage/version.h"

#include <fmt/format.h>

namespace storage {

log::log(
  model::ntp ntp,
  log_manager& manager,
  log_set segs) noexcept
  : _ntp(std::move(ntp))
  , _manager(manager)
  , _segs(std::move(segs)) {
    if (_segs.size()) {
        _tracker.update_committed_offset(_segs.last()->max_offset());
        _tracker.update_dirty_offset(_segs.last()->max_offset());
        _term = _segs.last()->term();
    } else {
        _term = 0;
    }
}

sstring log::base_directory() const {
    return fmt::format("{}/{}", _manager.config().base_dir, _ntp.path());
}

future<> log::close() {
    auto active = make_ready_future<>();
    if (_active_segment) {
        active = _active_segment->flush();
    }
    return active.then([this] {
        return parallel_for_each(
          _segs, [](log_segment_ptr& seg) { return seg->close(); });
    });
}

future<> log::new_segment(
  model::offset o, model::term_id term, const io_priority_class& pc) {
    return _manager.make_log_segment(_ntp, o, term())
      .then([this, pc](log_segment_ptr seg) {
          _active_segment = std::move(seg);
          _segs.add(_active_segment);
          _appender.emplace(_active_segment->data_appender(pc));
      });
}

future<log::append_result>
log::append(model::record_batch_reader&& reader, log_append_config config) {
    auto f = make_ready_future<>();
    if (__builtin_expect(!_active_segment, false)) {
        // FIXME: We need to persist the last offset somewhere.
        auto offset = _segs.size() > 0 ? _segs.last()->max_offset()
                                       : model::offset(0);
        f = new_segment(offset, _term(), config.io_priority);
    }
    return f.then(
      [this, reader = std::move(reader), config = std::move(config)]() mutable {
          return do_with(
            std::move(reader),
            [this,
             config = std::move(config)](model::record_batch_reader& reader) {
                auto now = log_clock::now();
                auto base = _active_segment->max_offset();
                auto writer = log_writer(
                  std::make_unique<default_log_writer>(*this));
                return reader.consume(std::move(writer), config.timeout)
                  .then([this, config = std::move(config), now, base](
                          model::offset last_offset) {
                      _tracker.update_dirty_offset(last_offset);
                      _active_segment->set_last_written_offset(last_offset);
                      auto f = make_ready_future<>();
                      if (config.should_fsync) {
                          f = _appender->flush();
                      }
                      return f.then([this, now, base, last_offset] {
                          return append_result{now, base, last_offset};
                      });
                  });
            });
      });
}
future<> log::roll(model::offset o, model::term_id t) {
    _term = std::move(t);
    if (!_active_segment) {
        return make_ready_future<>();
    }
    return do_roll(o);
}

future<> log::do_roll(model::offset current_offset) {
    _active_segment->set_last_written_offset(current_offset);
    return _appender->flush().then([this, current_offset] {
        return new_segment(
          current_offset + 1, _term(), _appender->priority_class());
    });
}
future<> log::maybe_roll(model::offset current_offset) {
    if (_appender->offset() < _manager.max_segment_size()) {
        return make_ready_future<>();
    }
    return do_roll(current_offset);
}

log_segment_appender& log::appender() {
    return *_appender;
}

model::record_batch_reader log::make_reader(log_reader_config config) {
    return model::make_record_batch_reader<log_reader>(
      _segs, _tracker, std::move(config));
}

} // namespace storage
