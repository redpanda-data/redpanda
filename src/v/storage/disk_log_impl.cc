#include "storage/disk_log_impl.h"

#include "model/timeout_clock.h"
#include "storage/disk_log_appender.h"
#include "storage/log_manager.h"
#include "storage/log_set.h"
#include "storage/logger.h"
#include "storage/offset_assignment.h"
#include "storage/offset_to_filepos_consumer.h"
#include "storage/version.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>

#include <fmt/format.h>

#include <iterator>

namespace storage {

disk_log_impl::disk_log_impl(
  model::ntp ntp, ss::sstring workdir, log_manager& manager, log_set segs)
  : log::impl(std::move(ntp), std::move(workdir))
  , _manager(manager)
  , _segs(std::move(segs)) {
    _probe.setup_metrics(this->ntp());
    if (!_segs.empty()) {
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
    model::offset base(0);
    if (auto o = max_offset(); o() >= 0) {
        // start at *next* valid and inclusive offset!
        base = o + model::offset(1);
    }
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
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    auto iopc = _segs.back().appender()->priority_class();
    return _segs.back().release_appender().then([this, iopc] {
        model::offset base = max_offset() + model::offset(1);
        stlog.trace("Rolling log segment offset {}, term {}", base, _term);
        return new_segment(base, _term, iopc);
    });
}
ss::future<> disk_log_impl::maybe_roll() {
    if (
      _segs.back().appender()->file_byte_offset()
      < _manager.max_segment_size()) {
        return ss::make_ready_future<>();
    }
    return do_roll();
}

model::record_batch_reader
disk_log_impl::make_reader(log_reader_config config) {
    return model::make_record_batch_reader<log_reader>(
      _segs, std::move(config), _probe);
}

std::optional<model::term_id> disk_log_impl::get_term(model::offset o) const {
    auto it = _segs.lower_bound(o);
    if (it != _segs.end()) {
        return it->term();
    }

    return std::nullopt;
}

static ss::future<> delete_full_segments(std::vector<segment> to_remove) {
    return ss::do_with(std::move(to_remove), [](std::vector<segment>& remove) {
        return ss::do_for_each(remove, [](segment& s) {
            return s.close()
              .handle_exception([&s](std::exception_ptr e) {
                  stlog.info("error:{} closing segment: {}", e, s);
              })
              .then([&s] { return ss::remove_file(s.reader()->filename()); })
              .then([&s] { return ss::remove_file(s.oindex()->filename()); })
              .handle_exception([&s](std::exception_ptr e) {
                  stlog.info("error:{} removing segment files: {}", e, s);
              });
        });
    });
}

ss::future<> disk_log_impl::do_truncate(model::offset o) {
    if (o > max_offset() || o < start_offset()) {
        // out of range
        stlog.info("Truncate offset: '{}' is out of range for {}", o, *this);
        return ss::make_ready_future<>();
    }
    std::vector<segment> to_remove;
    auto begin_remove = _segs.lower_bound(o); // cannot be lower_bound
    if (begin_remove != _segs.end()) {
        if (begin_remove->reader()->base_offset() < o) {
            begin_remove = std::next(begin_remove);
        }
    }
    for (size_t i = 0, max = std::distance(begin_remove, _segs.end()); i < max;
         ++i) {
        stlog.info(
          "Truncating full {}. tuncation offset request:{}", _segs.back(), o);
        to_remove.push_back(std::move(_segs.back()));
        _segs.pop_back();
    }
    using fpos_type = internal::offset_to_filepos_consumer::type;
    return delete_full_segments(std::move(to_remove))
      .then([this, o] {
          if (_segs.empty()) {
              return ss::make_ready_future<fpos_type>();
          }
          auto& last = _segs.back();
          if (o <= last.dirty_offset() && o >= last.reader()->base_offset()) {
              auto pidx = last.oindex()->lower_bound_pair(o);
              model::offset start = last.oindex()->base_offset();
              size_t initial_size = 0;
              if (pidx) {
                  start = pidx->first;
                  initial_size = pidx->second;
              }
              auto rdr = make_reader(log_reader_config{
                .start_offset = start,
                .max_bytes = std::numeric_limits<size_t>::max(),
                .min_bytes = std::numeric_limits<size_t>::max(),
                // TODO: pass a priority for truncate
                .prio = ss::default_priority_class(),
                .max_offset = o});
              return ss::do_with(
                std::move(rdr),
                [o, initial_size](model::record_batch_reader& rdr) {
                    return rdr.consume(
                      internal::offset_to_filepos_consumer(o, initial_size),
                      model::no_timeout);
                });
          }
          return ss::make_ready_future<fpos_type>();
      })
      .then([this, o](fpos_type phs) {
          if (!phs) {
              return ss::make_ready_future<>();
          }
          auto [prev_last_offset, file_position] = phs.value();
          // the last offset, is one before this batch' base_offset
          if (file_position == 0) {
              std::vector<segment> rem;
              rem.push_back(std::move(_segs.back()));
              _segs.pop_back();
              stlog.info(
                "Truncating full segment, after indexing:{}, :{}",
                prev_last_offset,
                rem.back());
              return delete_full_segments(std::move(rem));
          }
          return _segs.back().truncate(prev_last_offset, file_position);
      });
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
