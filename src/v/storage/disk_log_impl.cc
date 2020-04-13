#include "storage/disk_log_impl.h"

#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "storage/disk_log_appender.h"
#include "storage/log_manager.h"
#include "storage/logger.h"
#include "storage/offset_assignment.h"
#include "storage/offset_to_filepos_consumer.h"
#include "storage/segment_set.h"
#include "storage/version.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/fair_queue.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/time/time.h>
#include <fmt/format.h>

#include <iterator>

namespace storage {

disk_log_impl::disk_log_impl(
  ntp_config cfg, log_manager& manager, segment_set segs)
  : log::impl(std::move(cfg))
  , _manager(manager)
  , _segs(std::move(segs))
  , _lock_mngr(_segs) {
    for (auto& s : segs) {
        _probe.add_initial_segment(*s);
    }
    _probe.setup_metrics(this->config().ntp);
}
disk_log_impl::~disk_log_impl() {
    vassert(_closed, "log segment must be closed before deleting");
}
ss::future<> disk_log_impl::close() {
    _closed = true;
    return _lgate.close().then([this] {
        return ss::parallel_for_each(
          _segs, [](ss::lw_shared_ptr<segment>& h) { return h->close(); });
    });
}

ss::future<> disk_log_impl::gc(
  model::timestamp collection_upper_bound,
  std::optional<size_t> max_partition_retention_size) {
    // TODO: this a workaround until we have raft-snapshotting in the the
    // controller so that we can still evict older data. At the moment we keep
    // the full history.
    constexpr std::string_view redpanda_ignored_ns = "redpanda";
    constexpr std::string_view kafka_ignored_ns = "kafka_internal";
    if (
      config().ntp.ns() == redpanda_ignored_ns
      || config().ntp.ns() == kafka_ignored_ns) {
        return ss::make_ready_future<>();
    }
    if (max_partition_retention_size) {
        size_t max = max_partition_retention_size.value();
        while (!_segs.empty() && _probe.partition_size() > max) {
            dispatch_remove(_segs.front(), "gc[size_based_retention]");
            _segs.pop_front();
        }
    }
    // The following compaction has a Kafka behavior compatibility bug. for
    // which we defer do nothing at the moment, possibly crashing the machine
    // and running out of disk. Kafka uses the same logic below as of
    // March-6th-2020. The problem is that you can construct a case where the
    // first log segment max_timestamp is infinity and this loop will never
    // trigger a compaction since the data is coming from the clients.
    //
    // A workaround is to have production systems set both, the max
    // retention.bytes and the max retention.ms setting in the configuration
    // files so that size-based retention eventually evicts the problematic
    // segment, preventing a crash.
    //
    while (
      !_segs.empty()
      && (_segs.front()->index().max_timestamp() <= collection_upper_bound)) {
        auto f = _segs.front();
        vlog(
          stlog.debug,
          "Detected segment max_timestamp:{} older than eviction time:{}",
          absl::FromUnixMillis(f->index().max_timestamp().value()),
          absl::FromUnixMillis(collection_upper_bound.value()));
        dispatch_remove(f, "gc[time_based_retention]");
        _segs.pop_front();
    }
    return ss::make_ready_future<>();
}

ss::future<> disk_log_impl::remove_empty_segments() {
    return ss::do_until(
      [this] { return _segs.empty() || !_segs.back()->empty(); },
      [this] {
          return _segs.back()->close().then([this] { _segs.pop_back(); });
      });
}
model::offset disk_log_impl::start_offset() const {
    if (_segs.empty()) {
        return model::offset{};
    }
    return _segs.front()->reader().base_offset();
}
model::offset disk_log_impl::max_offset() const {
    if (!_segs.empty()) {
        for (int i = (int)_segs.size() - 1; i >= 0; --i) {
            auto& seg = _segs[i];
            if (!seg->empty()) {
                return seg->dirty_offset();
            }
        }
    }
    return model::offset{};
}
model::term_id disk_log_impl::term() const {
    if (_segs.empty()) {
        // does not make sense to return unitinialized term
        // if we have no term, default to the first term.
        // the next append() will truncate if greater
        return model::term_id{0};
    }
    return _segs.back()->term();
}

model::offset disk_log_impl::committed_offset() const {
    if (!_segs.empty()) {
        for (int i = (int)_segs.size() - 1; i >= 0; --i) {
            auto& seg = _segs[i];
            if (!seg->empty()) {
                return seg->committed_offset();
            }
        }
    }
    return model::offset{};
}

ss::future<> disk_log_impl::new_segment(
  model::offset o, model::term_id t, ss::io_priority_class pc) {
    vassert(
      o() >= 0 && t() >= 0, "offset:{} and term:{} must be initialized", o, t);
    return _manager.make_log_segment(config().ntp, o, t, pc)
      .then([this](ss::lw_shared_ptr<segment> handles) mutable {
          return remove_empty_segments().then(
            [this, h = std::move(handles)]() mutable {
                vassert(!_closed, "cannot add log segment to closed log");
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
    return _segs.back()->flush();
}

size_t disk_log_impl::bytes_left_before_roll() const {
    if (_segs.empty()) {
        return 0;
    }
    auto& back = _segs.back();
    if (!back->has_appender()) {
        return 0;
    }
    auto fo = back->appender().file_byte_offset();
    auto max = _manager.max_segment_size();
    if (fo >= max) {
        return 0;
    }
    return max - fo;
}

ss::future<> disk_log_impl::maybe_roll(
  model::term_id t, model::offset next_offset, ss::io_priority_class iopc) {
    vassert(t >= term(), "Term:{} must be greater than base:{}", t, term());
    if (
      t != term() || _segs.empty() || !_segs.back()->has_appender()
      || _segs.back()->appender().file_byte_offset()
           > _manager.max_segment_size()) {
        if (!_segs.empty() && _segs.back()->has_appender()) {
            auto ptr = _segs.back();
            // we must wait for all ongoing operations - which
            // might take a littlebit since we have to acquire a write_lock()
            (void)ss::with_gate(_lgate, [ptr] {
                return ptr->release_appender();
            }).handle_exception([](std::exception_ptr e) {
                vlog(stlog.info, "Error releasing appender: {}", e);
            });
        }
        return new_segment(next_offset, t, iopc);
    }
    return ss::make_ready_future<>();
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(log_reader_config config) {
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), cfg, _probe);
      });
}

std::optional<model::term_id> disk_log_impl::get_term(model::offset o) const {
    auto it = _segs.lower_bound(o);
    if (it != _segs.end()) {
        return (*it)->term();
    }

    return std::nullopt;
}
ss::future<std::optional<timequery_result>>
disk_log_impl::timequery(timequery_config cfg) {
    if (_segs.empty()) {
        return ss::make_ready_future<std::optional<timequery_result>>();
    }
    return make_reader(log_reader_config(
                         _segs.front()->reader().base_offset(),
                         cfg.max_offset,
                         0,
                         2048, // We just need one record batch
                         cfg.prio,
                         std::nullopt,
                         cfg.time))
      .then([cfg](model::record_batch_reader reader) {
          return model::consume_reader_to_memory(
                   std::move(reader), model::no_timeout)
            .then([cfg](model::record_batch_reader::storage_t batches) {
                using ret_t = std::optional<timequery_result>;
                if (
                  !batches.empty()
                  && batches.front().header().first_timestamp >= cfg.time) {
                    return ret_t(timequery_result(
                      batches.front().base_offset(),
                      batches.front().header().first_timestamp));
                }
                return ret_t();
            });
      });
}

void disk_log_impl::dispatch_remove(
  ss::lw_shared_ptr<segment> s, std::string_view ctx) {
    vlog(stlog.info, "{} - tombstone & delete segment: {}", ctx, s);
    // stats accounting must happen synchronously
    _probe.delete_segment(*s);
    // background close
    s->tombstone();
    (void)ss::with_gate(_lgate, [s] {
        return s->close().finally([s] {});
    }).handle_exception([](std::exception_ptr e) {
        vlog(stlog.info, "Ignoring exception on shutdown: {}", e);
    });
}
ss::future<> disk_log_impl::do_truncate(model::offset o) {
    if (o > max_offset() || o < start_offset()) {
        vlog(
          stlog.error,
          "log::truncate out of range: {}, {}-{}",
          o,
          start_offset(),
          max_offset());
        return ss::make_ready_future<>();
    }
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    for (int i = (int)_segs.size() - 1; i >= 0; --i) {
        ss::lw_shared_ptr<segment> ptr = _segs[i];
        if (ptr->reader().base_offset() >= o) {
            dispatch_remove(ptr, "truncate[back-iterator]");
            _segs.pop_back();
        } else {
            break;
        }
    }
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    auto& last = *_segs.back();
    if (o > last.dirty_offset()) {
        return ss::make_ready_future<>();
    }
    auto pidx = last.index().find_nearest(o);
    model::offset start = last.index().base_offset();
    size_t initial_size = 0;
    if (pidx) {
        start = pidx->offset;
        initial_size = pidx->filepos;
    }
    // TODO: pass a priority for truncate
    return make_reader(
             log_reader_config(start, o, ss::default_priority_class()))
      .then([o, initial_size](model::record_batch_reader reader) {
          return std::move(reader).consume(
            internal::offset_to_filepos_consumer(o, initial_size),
            model::no_timeout);
      })
      .then([this](std::optional<std::pair<model::offset, size_t>> phs) {
          if (!phs) {
              return ss::make_ready_future<>();
          }
          auto [prev_last_offset, file_position] = phs.value();
          auto last = _segs.back();
          if (file_position == 0) {
              dispatch_remove(last, "truncate[post-translation]");
              _segs.pop_back();
              return ss::make_ready_future<>();
          }
          _probe.remove_partition_bytes(
            last->reader().file_size() - file_position);
          return last->truncate(prev_last_offset, file_position);
      });
} // namespace storage

std::ostream& disk_log_impl::print(std::ostream& o) const {
    return o << "{term=" << term() << ", logs=" << _segs << "}";
}

std::ostream& operator<<(std::ostream& o, const disk_log_impl& d) {
    return d.print(o);
}

log make_disk_backed_log(
  ntp_config cfg, log_manager& manager, segment_set segs) {
    auto ptr = ss::make_shared<disk_log_impl>(
      std::move(cfg), manager, std::move(segs));
    return log(ptr);
}

} // namespace storage
