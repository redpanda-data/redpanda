#include "storage/disk_log_impl.h"

#include "storage/disk_log_appender.h"
#include "storage/log_manager.h"
#include "storage/log_set.h"
#include "storage/offset_assignment.h"
#include "storage/version.h"

#include <seastar/core/reactor.hh>

#include <fmt/format.h>

namespace storage {

disk_log_impl::disk_log_impl(
  model::ntp ntp, ss::sstring workdir, log_manager& manager, log_set segs)
  : log::impl(std::move(ntp), std::move(workdir))
  , _manager(manager)
  , _segs(std::move(segs)) {
    _probe.setup_metrics(this->ntp());
    if (_segs.size()) {
        _tracker.update_committed_offset(_segs.last()->max_offset());
        _tracker.update_dirty_offset(_segs.last()->max_offset());
        _term = _segs.last()->term();
    } else {
        _term = model::term_id(0);
    }
}

ss::future<> disk_log_impl::close() {
    auto active = ss::make_ready_future<>();
    if (_appender) {
        // flush + truncate + close
        active = _appender->close();
    }
    return active.then([this] {
        return ss::parallel_for_each(
          _segs, [](segment_reader_ptr& seg) { return seg->close(); });
    });
}

ss::future<> disk_log_impl::new_segment(
  model::offset o, model::term_id term, const ss::io_priority_class& pc) {
    return _manager.make_log_segment(ntp(), o, term, pc)
      .then([this, pc](log_manager::log_handles handles) {
          _active_segment = std::move(handles.reader);
          _appender = std::move(handles.appender);
          _segs.add(_active_segment);
          _probe.segment_created();
      });
}

// config timeout is for the one calling reader consumer
log_appender disk_log_impl::make_appender(log_append_config cfg) {
    auto now = log_clock::now();
    auto base = _tracker.dirty_offset() >= model::offset(0)
                  ? _tracker.dirty_offset() + model::offset(1)
                  : model::offset(0);
    return log_appender(
      std::make_unique<disk_log_appender>(*this, cfg, now, base));
}

ss::future<> disk_log_impl::flush() {
    if (!_appender) {
        return ss::make_ready_future<>();
    }
    return _appender->flush().then([this] {
        _tracker.update_committed_offset(_tracker.dirty_offset());
        _active_segment->set_last_written_offset(_tracker.committed_offset());
        _active_segment->set_last_visible_byte_offset(
          _appender->file_byte_offset());
    });
}
ss::future<> disk_log_impl::do_roll() {
    return flush().then([this] { return _appender->close(); }).then([this] {
        auto offset = _tracker.committed_offset() + model::offset(1);
        stlog.trace("Rolling log segment offset {}, term {}", offset, _term);
        return new_segment(offset, _term, _appender->priority_class());
    });
}
ss::future<> disk_log_impl::maybe_roll(model::offset current_offset) {
    if (_appender->file_byte_offset() < _manager.max_segment_size()) {
        return ss::make_ready_future<>();
    }
    _tracker.update_dirty_offset(current_offset);
    return do_roll();
}

model::record_batch_reader
disk_log_impl::make_reader(log_reader_config config) {
    return model::make_record_batch_reader<log_reader>(
      _segs, _tracker, std::move(config), _probe);
}

ss::future<> disk_log_impl::truncate_whole_segments(
  log_set::const_iterator first_to_remove) {
    std::vector<segment_reader_ptr> truncated_logs;
    truncated_logs.reserve(std::distance(first_to_remove, std::cend(_segs)));
    while (first_to_remove != _segs.end()) {
        auto truncated = _segs.last();
        stlog.debug(
          "Truncated whole log segment {}", truncated->get_filename());
        truncated_logs.push_back(truncated);
        _segs.pop_last();
    }

    return parallel_for_each(
             truncated_logs,
             [](const segment_reader_ptr& seg) {
                 return seg->close().then(
                   [seg] { return remove_file(seg->get_filename()); });
             })
      .then([d = _manager.config().base_dir] { return sync_directory(d); });
}

ss::future<> disk_log_impl::do_truncate(model::offset o) {
    stlog.trace("Truncating log {} at offset {}", ntp(), o);

    if (o < model::offset(0)) {
        // remove all
        stlog.trace("Truncating  whole log {}", ntp());
        _tracker.update_dirty_offset(model::offset{});
        _tracker.update_committed_offset(model::offset{});
        return truncate_whole_segments(_segs.begin()).then([this] {
            if (!_appender) {
                return ss::make_ready_future<>();
            }
            return flush()
              .then([this] { return _appender->close(); })
              .then([this] {
                  _appender = nullptr;
                  _active_segment = nullptr;
              });
        });
    }
    auto to_truncate_in_middle = _segs.lower_bound(o);
    if (to_truncate_in_middle == _segs.end()) {
        return ss::make_exception_future<>(std::invalid_argument(
          fmt::format("Unable to truncate as offset {} is not in the log", o)));
    }

    // first remove whole segments
    auto first_to_remove = std::next(to_truncate_in_middle);
    auto f = truncate_whole_segments(first_to_remove);
    // truncate in the middle
    _tracker.update_dirty_offset(o);
    _tracker.update_committed_offset(o);
    struct file_offset_batch_consumer {
        ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
            f_pos += (batch.size_bytes() + vint::vint_size(batch.size_bytes()));
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }
        uint64_t end_of_stream() { return f_pos; }
        uint64_t f_pos{0};
    };

    return f.then([this, o, to_truncate = *to_truncate_in_middle] {
        log_reader_config cfg{.start_offset = to_truncate->base_offset(),
                              .max_bytes = std::numeric_limits<size_t>::max(),
                              .min_bytes = 0,
                              .prio = ss::default_priority_class(),
                              .type_filter = {},
                              .max_offset = o};
        return ss::do_with(
          file_offset_batch_consumer{},
          model::make_record_batch_reader<log_segment_batch_reader>(
            to_truncate, _tracker, std::move(cfg), _probe),
          [this, o, to_truncate](
            file_offset_batch_consumer& c,
            model::record_batch_reader& reader) mutable {
              return reader.consume(c, model::no_timeout)
                .then([this, o, to_truncate](uint64_t f_pos) {
                    stlog.debug(
                      "Truncating segment {} of size {} at {}",
                      to_truncate->get_filename(),
                      to_truncate->file_size(),
                      f_pos);

                    if (_active_segment->base_offset() <= o) {
                        // we have an appender to this segment,
                        // truncate using the appender
                        _active_segment->set_last_visible_byte_offset(f_pos);
                        _active_segment->set_last_written_offset(o);
                        return _appender->truncate(f_pos);
                    }
                    // truncate old segment and roll
                    const auto flags = ss::open_flags::rw;
                    return ss::open_file_dma(to_truncate->get_filename(), flags)
                      .then([this, f_pos](ss::file f) {
                          return f.truncate(f_pos)
                            .then([f]() mutable { return f.flush(); })
                            .finally([f]() mutable { return f.close(); })
                            .finally([f] {});
                      })
                      .then([this] { return do_roll(); });
                });
          });
    });
}

std::ostream& disk_log_impl::print(std::ostream& o) const {
    return o << "{term=" << _term << ", logs=" << _segs << "}";
}

log make_disk_backed_log(model::ntp ntp, log_manager& manager, log_set segs) {
    auto workdir = fmt::format("{}/{}", manager.config().base_dir, ntp.path());
    auto ptr = ss::make_shared<disk_log_impl>(
      std::move(ntp), std::move(workdir), manager, std::move(segs));
    return log(ptr);
}

} // namespace storage
