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
    if (!_segs.empty()) {
        _tracker.update_committed_offset(_segs.back().reader()->max_offset());
        _tracker.update_dirty_offset(_segs.back().reader()->max_offset());
        _term = _segs.back().reader()->term();
    } else {
        _term = model::term_id(0);
    }
}

ss::future<> disk_log_impl::close() {
    return ss::parallel_for_each(_segs, [](segment& h) { return h.close(); });
}

ss::future<> disk_log_impl::remove_empty_segments() {
    return ss::do_until(
      [this] { return _segs.empty() || !_segs.back().empty(); },
      [this] {
          return _segs.back().close().then([this] { _segs.pop_back(); });
      });
}

ss::future<> disk_log_impl::new_segment(
  model::offset o, model::term_id term, const ss::io_priority_class& pc) {
    return _manager.make_log_segment(ntp(), o, term, pc)
      .then([this, pc](segment handles) mutable {
          return remove_empty_segments().then(
            [this, h = std::move(handles)]() mutable {
                _segs.add(std::move(h));
                _probe.segment_created();
            });
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
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    return _segs.back().flush();
}
ss::future<> disk_log_impl::do_roll() {
    return flush()
      // FIXME(agallego) - figure out how to not forward to segment
      //.then([this] { return _segs.back().appender()->close(); })
      .then([this] {
          auto offset = _tracker.committed_offset() + model::offset(1);
          stlog.trace("Rolling log segment offset {}, term {}", offset, _term);
          return new_segment(
            offset, _term, _segs.back().appender()->priority_class());
      });
}
ss::future<> disk_log_impl::maybe_roll(model::offset current_offset) {
    if (
      _segs.back().appender()->file_byte_offset()
      < _manager.max_segment_size()) {
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

// ss::future<> disk_log_impl::truncate_whole_segments(
//   log_set::const_iterator first_to_remove) {
//     std::vector<segment_reader_ptr> truncated_logs;
//     truncated_logs.reserve(std::distance(first_to_remove, std::cend(_segs)));
//     while (first_to_remove != _segs.end()) {
//         auto truncated = _segs.back().reader;
//         stlog.debug(
//           "Truncated whole log segment {}", truncated->filename());
//         truncated_logs.push_back(truncated);
//         _segs.pop_back();
//     }

//     return parallel_for_each(
//              truncated_logs,
//              [](const segment_reader_ptr& seg) {
//                  return seg->close().then(
//                    [seg] { return remove_file(seg->filename()); });
//              })
//       .then([d = _manager.config().base_dir] { return sync_directory(d); });
// }

ss::future<> disk_log_impl::do_truncate(model::offset o) {
    return ss::make_ready_future<>();
    // if (o < model::offset(0)) {
    //     // remove all
    //     stlog.trace("Truncating  whole log {}", ntp());
    //     _tracker.update_dirty_offset(model::offset{});
    //     _tracker.update_committed_offset(model::offset{});
    //     return truncate_whole_segments(_segs.begin()).then([this] {
    //         if (!_segs.back().appender) {
    //             return ss::make_ready_future<>();
    //         }
    //         return flush()
    //           .then([this] { return _segs.back().appender()->close(); })
    //           .then([this] {
    //               _segs.back().appender = nullptr;
    //               _segs.back().reader = nullptr;
    //           });
    //     });
    // }
    // auto to_truncate_in_middle = _segs.lower_bound(o)->reader;
    // if (to_truncate_in_middle == _segs.end()) {
    //     return ss::make_exception_future<>(std::invalid_argument(
    //       fmt::format("Unable to truncate as offset {} is not in the log",
    //       o)));
    // }

    // // first remove whole segments
    // auto first_to_remove = std::next(to_truncate_in_middle);
    // auto f = truncate_whole_segments(first_to_remove);
    // // truncate in the middle
    // _tracker.update_dirty_offset(o);
    // _tracker.update_committed_offset(o);
    // struct file_offset_batch_consumer {
    //     ss::future<ss::stop_iteration> operator()(model::record_batch batch)
    //     {
    //         f_pos += (batch.size_bytes() +
    //         vint::vint_size(batch.size_bytes())); return
    //         ss::make_ready_future<ss::stop_iteration>(
    //           ss::stop_iteration::no);
    //     }
    //     uint64_t end_of_stream() { return f_pos; }
    //     uint64_t f_pos{0};
    // };

    // return f.then([this, o, to_truncate = to_truncate_in_middle->reader] {
    //     log_reader_config cfg{.start_offset = to_truncate->base_offset(),
    //                           .max_bytes =
    //                           std::numeric_limits<size_t>::max(), .min_bytes
    //                           = 0, .prio = ss::default_priority_class(),
    //                           .type_filter = {},
    //                           .max_offset = o};
    //     return ss::do_with(
    //       file_offset_batch_consumer{},
    //       model::make_record_batch_reader<log_segment_batch_reader>(
    //         to_truncate, _tracker, std::move(cfg), _probe),
    //       [this, o, to_truncate](
    //         file_offset_batch_consumer& c,
    //         model::record_batch_reader& reader) mutable {
    //           return reader.consume(c, model::no_timeout)
    //             .then([this, o, to_truncate](uint64_t f_pos) {
    //                 stlog.debug(
    //                   "Truncating segment {} of size {} at {}",
    //                   to_truncate->filename(),
    //                   to_truncate->file_size(),
    //                   f_pos);

    //                 if (_segs.back().reader()->base_offset() <= o) {
    //                     // we have an appender to this segment,
    //                     // truncate using the appender
    //                     _segs.back().reader()->set_last_visible_byte_offset(
    //                       f_pos);
    //                     _segs.back().reader()->set_last_written_offset(o);
    //                     return _segs.back().appender()->truncate(f_pos);
    //                 }
    //                 // truncate old segment and roll
    //                 const auto flags = ss::open_flags::rw;
    //                 return ss::open_file_dma(to_truncate->filename(),
    //                 flags)
    //                   .then([this, f_pos](ss::file f) {
    //                       return f.truncate(f_pos)
    //                         .then([f]() mutable { return f.flush(); })
    //                         .finally([f]() mutable { return f.close(); })
    //                         .finally([f] {});
    //                   })
    //                   .then([this] { return do_roll(); });
    //             });
    //       });
    // });
}

std::ostream& disk_log_impl::print(std::ostream& o) const {
    return o << "{term=" << _term << ", logs=" << _segs << "}";
}

std::ostream& operator<<(std::ostream& o, const disk_log_impl& d) {
    return d.print(o);
}

log make_disk_backed_log(model::ntp ntp, log_manager& manager, log_set segs) {
    auto workdir = fmt::format("{}/{}", manager.config().base_dir, ntp.path());
    auto ptr = ss::make_shared<disk_log_impl>(
      std::move(ntp), std::move(workdir), manager, std::move(segs));
    return log(ptr);
}

} // namespace storage
