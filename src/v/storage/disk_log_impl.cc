#include "storage/disk_log_impl.h"

#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "storage/disk_log_appender.h"
#include "storage/log_manager.h"
#include "storage/logger.h"
#include "storage/offset_assignment.h"
#include "storage/offset_to_filepos_consumer.h"
#include "storage/segment_set.h"
#include "storage/types.h"
#include "storage/version.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/fair_queue.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

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
    vassert(_closed, "log segment must be closed before deleting:{}", *this);
}

ss::future<> disk_log_impl::remove() {
    vassert(!_closed, "Invalid double closing of log - {}", *this);
    _closed = true;
    // gets all the futures started in the background
    std::vector<ss::future<>> permanent_delete;
    permanent_delete.reserve(_segs.size());
    while (!_segs.empty()) {
        auto s = _segs.back();
        _segs.pop_back();
        permanent_delete.emplace_back(
          remove_segment_permanently(s, "disk_log_impl::remove()"));
    }
    // wait for all futures
    return ss::when_all_succeed(
             permanent_delete.begin(), permanent_delete.end())
      .then([this]() {
          vlog(stlog.info, "Finished removing all segments:{}", config());
      });
}
ss::future<> disk_log_impl::close() {
    vassert(!_closed, "Invalid double closing of log - {}", *this);
    _closed = true;
    return ss::parallel_for_each(_segs, [](ss::lw_shared_ptr<segment>& h) {
        return h->close().handle_exception([h](std::exception_ptr e) {
            vlog(stlog.error, "Error closing segment:{} - {}", e, h);
        });
    });
}

ss::future<> disk_log_impl::garbage_collect_max_partition_size(
  size_t max_bytes, ss::abort_source& as) {
    return ss::do_until(
      [this, max_bytes, &as] {
          return _segs.empty() || _probe.partition_size() <= max_bytes
                 || as.abort_requested();
      },
      [this] {
          auto ptr = _segs.front();
          _segs.pop_front();
          return remove_segment_permanently(ptr, "gc[size_based_retention]");
      });
}

ss::future<> disk_log_impl::garbage_collect_oldest_segments(
  model::timestamp time, ss::abort_source& as) {
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
    return ss::do_until(
      [this, time, &as] {
          return _segs.empty() || _segs.front()->index().max_timestamp() > time
                 || as.abort_requested();
      },
      [this] {
          auto ptr = _segs.front();
          _segs.pop_front();
          return remove_segment_permanently(ptr, "gc[time_based_retention]");
      });
}

ss::future<> disk_log_impl::compact(compaction_config cfg) {
    return gc(std::move(cfg));
}

ss::future<> disk_log_impl::gc(compaction_config cfg) {
    vassert(!_closed, "gc on closed log - {}", *this);

    if (cfg.as.abort_requested()) {
        return ss::make_ready_future<>();
    }
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
    if (cfg.max_bytes) {
        size_t max = cfg.max_bytes.value();
        if (!_segs.empty() && _probe.partition_size() > max) {
            return garbage_collect_max_partition_size(max, cfg.as);
        }
    }
    return garbage_collect_oldest_segments(cfg.eviction_time, cfg.as);
}

ss::future<> disk_log_impl::remove_empty_segments() {
    return ss::do_until(
      [this] { return _segs.empty() || !_segs.back()->empty(); },
      [this] {
          return _segs.back()->close().then([this] { _segs.pop_back(); });
      });
}

model::term_id disk_log_impl::term() const {
    if (_segs.empty()) {
        // does not make sense to return unitinialized term
        // if we have no term, default to the first term.
        // the next append() will truncate if greater
        return model::term_id{0};
    }
    return _segs.back()->offsets().term;
}

offset_stats disk_log_impl::offsets() const {
    if (_segs.empty()) {
        return offset_stats{};
    }
    // NOTE: we have to do this because ss::circular_buffer<> does not provide
    // with reverse iterators, so we manually find the iterator
    segment_set::type end;
    for (int i = (int)_segs.size() - 1; i >= 0; --i) {
        auto& seg = _segs[i];
        if (!seg->empty()) {
            end = seg;
            break;
        }
    }
    if (!end) {
        return offset_stats{};
    }
    // we have valid begin and end
    const auto& bof = _segs.front()->offsets();
    const auto& eof = end->offsets();
    return storage::offset_stats{
      .start_offset = bof.base_offset,
      .start_offset_term = bof.term,

      .committed_offset = eof.committed_offset,
      .committed_offset_term = eof.term,

      .dirty_offset = eof.dirty_offset,
      .dirty_offset_term = eof.term,
    };
}

ss::future<> disk_log_impl::new_segment(
  model::offset o, model::term_id t, ss::io_priority_class pc) {
    vassert(
      o() >= 0 && t() >= 0, "offset:{} and term:{} must be initialized", o, t);
    return _manager.make_log_segment(config(), o, t, pc)
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
    vassert(!_closed, "make_appender on closed log - {}", *this);
    auto now = log_clock::now();
    auto ofs = offsets();
    return log_appender(
      std::make_unique<disk_log_appender>(*this, cfg, now, ofs.dirty_offset));
}

ss::future<> disk_log_impl::flush() {
    vassert(!_closed, "flush on closed log - {}", *this);
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
    _probe.maybe_roll();
    if (_segs.empty()) {
        return new_segment(next_offset, t, iopc);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender()) {
        return new_segment(next_offset, t, iopc);
    }
    if (
      t != term()
      || ptr->appender().file_byte_offset() > _manager.max_segment_size()) {
        return ptr->release_appender().then([this, next_offset, t, iopc] {
            return new_segment(next_offset, t, iopc);
        });
    }
    return ss::make_ready_future<>();
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(log_reader_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), cfg, _probe);
      });
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(timequery_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          log_reader_config config(
            lease->range.empty()
              ? model::offset{}
              : (*lease->range.begin())->offsets().base_offset,
            cfg.max_offset,
            0,
            2048, // We just need one record batch
            cfg.prio,
            std::nullopt,
            cfg.time);
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), config, _probe);
      });
}

std::optional<model::term_id> disk_log_impl::get_term(model::offset o) const {
    auto it = _segs.lower_bound(o);
    if (it != _segs.end()) {
        return (*it)->offsets().term;
    }

    return std::nullopt;
}
ss::future<std::optional<timequery_result>>
disk_log_impl::timequery(timequery_config cfg) {
    vassert(!_closed, "timequery on closed log - {}", *this);
    if (_segs.empty()) {
        return ss::make_ready_future<std::optional<timequery_result>>();
    }
    return make_reader(std::move(cfg))
      .then([cfg](model::record_batch_reader reader) {
          return model::consume_reader_to_memory(
                   std::move(reader), model::no_timeout)
            .then([cfg](model::record_batch_reader::storage_t st) {
                using ret_t = std::optional<timequery_result>;
                auto& batches = std::get<model::record_batch_reader::data_t>(
                  st);
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

ss::future<> disk_log_impl::remove_segment_permanently(
  ss::lw_shared_ptr<segment> s, std::string_view ctx) {
    vlog(stlog.info, "{} - tombstone & delete segment: {}", ctx, s);
    // stats accounting must happen synchronously
    _probe.delete_segment(*s);
    // background close
    s->tombstone();
    if (s->has_outstanding_locks()) {
        vlog(
          stlog.info,
          "Segment has outstanding locks. Might take a while to close:{}",
          s->reader().filename());
    }
    return s->close()
      .handle_exception([s](std::exception_ptr e) {
          vlog(stlog.error, "Cannot close segment: {} - {}", e, s);
      })
      .finally([s] {});
}

ss::future<> disk_log_impl::remove_full_segments(model::offset o) {
    return ss::do_until(
      [this, o] {
          return _segs.empty() || _segs.back()->offsets().base_offset < o;
      },
      [this] {
          auto ptr = _segs.back();
          _segs.pop_back();
          return remove_segment_permanently(ptr, "remove_full_segments");
      });
}
ss::future<>
disk_log_impl::remove_prefix_full_segments(truncate_prefix_config cfg) {
    return ss::do_until(
      [this, cfg] {
          return _segs.empty()
                 || _segs.front()->offsets().dirty_offset < cfg.max_offset;
      },
      [this] {
          auto ptr = _segs.front();
          _segs.pop_front();
          return remove_segment_permanently(ptr, "remove_prefix_full_segments");
      });
}

ss::future<> disk_log_impl::truncate_prefix(truncate_prefix_config cfg) {
    vassert(!_closed, "truncate_prefix() on closed log - {}", *this);
    return _failure_probes.truncate_prefix().then([this, cfg]() mutable {
        // dispatch the actual truncation
        return do_truncate_prefix(cfg);
    });
}

ss::future<> disk_log_impl::do_truncate_prefix(truncate_prefix_config cfg) {
    vassert(
      cfg.sloppy == truncate_prefix_config::sloppy_prefix::yes,
      "We don't support exact prefix truncation yet. Config: {} - {}",
      cfg,
      *this);
    auto stats = offsets();
    if (cfg.max_offset < stats.start_offset) {
        return ss::make_ready_future<>();
    }
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    if (_segs.front()->offsets().dirty_offset >= cfg.max_offset) {
        return remove_prefix_full_segments(cfg).then([this, cfg] {
            // recurse
            return do_truncate_prefix(cfg);
        });
    }
    auto begin = _segs.front();
    if (begin->has_appender()) {
        // we don't truncate active appenders
        return ss::make_ready_future<>();
    }
    // NOTE: at the moment we don't support mid segment truncation
    // since it requires index invalidation
    return ss::make_ready_future<>();
}

ss::future<> disk_log_impl::truncate(truncate_config cfg) {
    vassert(!_closed, "truncate() on closed log - {}", *this);
    return _failure_probes.truncate().then([this, cfg]() mutable {
        // dispatch the actual truncation
        return do_truncate(cfg);
    });
}

ss::future<> disk_log_impl::do_truncate(truncate_config cfg) {
    auto stats = offsets();
    if (cfg.base_offset > stats.dirty_offset) {
        return ss::make_ready_future<>();
    }
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    // Note different from the stats variable above because
    // we want to delete even empty segments.
    if (_segs.back()->offsets().base_offset >= cfg.base_offset) {
        return remove_full_segments(cfg.base_offset).then([this, cfg] {
            // recurse
            return do_truncate(cfg);
        });
    }
    auto& last = *_segs.back();
    if (cfg.base_offset > last.offsets().dirty_offset) {
        return ss::make_ready_future<>();
    }
    auto pidx = last.index().find_nearest(cfg.base_offset);
    model::offset start = last.index().base_offset();
    size_t initial_size = 0;
    if (pidx) {
        start = pidx->offset;
        initial_size = pidx->filepos;
    }
    return last.flush()
      .then([this, cfg, start, initial_size] {
          return make_reader(
                   log_reader_config(start, cfg.base_offset, cfg.prio))
            .then([cfg, initial_size](model::record_batch_reader reader) {
                return std::move(reader).consume(
                  internal::offset_to_filepos_consumer(
                    cfg.base_offset, initial_size),
                  model::no_timeout);
            });
      })
      .then([this, cfg](std::optional<std::pair<model::offset, size_t>> phs) {
          if (!phs) {
              return ss::make_exception_future<>(std::runtime_error(fmt::format(
                "User asked to truncate at:{}, but "
                "internal::offset_to_filepos_consumer could not translate "
                "physical offsets. Log state: {}",
                cfg,
                *this)));
          }
          auto [prev_last_offset, file_position] = phs.value();
          auto last = _segs.back();
          if (file_position == 0) {
              _segs.pop_back();
              return remove_segment_permanently(
                last, "truncate[post-translation]");
          }
          _probe.remove_partition_bytes(last->size_bytes() - file_position);
          return last->truncate(prev_last_offset, file_position)
            .handle_exception([last, phs, this](std::exception_ptr e) {
                vassert(
                  false,
                  "Could not truncate:{} logical max:{}, physical offset:{} on "
                  "segment:{} - log:{}",
                  e,
                  phs->first,
                  phs->second,
                  last,
                  *this);
            });
      });
}

std::ostream& disk_log_impl::print(std::ostream& o) const {
    return o << "{offsets:" << offsets() << ", closed:" << _closed
             << ", config:" << config() << ", logs:" << _segs << "}";
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
