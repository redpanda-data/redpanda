#include "storage/disk_log_impl.h"

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "reflection/adl.h"
#include "storage/disk_log_appender.h"
#include "storage/log_manager.h"
#include "storage/logger.h"
#include "storage/offset_assignment.h"
#include "storage/offset_to_filepos_consumer.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
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
  ntp_config cfg, log_manager& manager, segment_set segs, kvstore& kvstore)
  : log::impl(std::move(cfg))
  , _manager(manager)
  , _segs(std::move(segs))
  , _kvstore(kvstore)
  , _start_offset(read_start_offset())
  , _lock_mngr(_segs) {
    const bool is_compacted = config().is_compacted();
    for (auto& s : segs) {
        _probe.add_initial_segment(*s);
        if (is_compacted) {
            // keeps track of metadata operations perform on each segment
            // for compaction, etc.
            _segbits[s->offsets().base_offset] = segment_bitflags::none;
        }
    }
    _probe.setup_metrics(this->config().ntp());
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
      })
      .then([this] {
          return _kvstore.remove(
            kvstore::key_space::storage, start_offset_key());
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
  size_t max_bytes, ss::abort_source* as) {
    return ss::do_until(
      [this, max_bytes, as] {
          return _segs.empty() || _probe.partition_size() <= max_bytes
                 || as->abort_requested();
      },
      [this] {
          auto ptr = _segs.front();
          _segs.pop_front();
          return remove_segment_permanently(ptr, "gc[size_based_retention]");
      });
}

ss::future<> disk_log_impl::garbage_collect_oldest_segments(
  model::timestamp time, ss::abort_source* as) {
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
      [this, time, as] {
          return _segs.empty() || _segs.front()->index().max_timestamp() > time
                 || as->abort_requested();
      },
      [this] {
          auto ptr = _segs.front();
          _segs.pop_front();
          return remove_segment_permanently(ptr, "gc[time_based_retention]");
      });
}

static inline ss::lw_shared_ptr<segment> find_reverse_not_compacted(
  segment_set& s,
  const absl::flat_hash_map<model::offset, disk_log_impl::segment_bitflags>&
    bits) {
    int i = static_cast<int>(s.size()) - 1;
    while (i >= 0) {
        auto ret = s[i--];
        const auto flags = bits.find(ret->offsets().base_offset);
        if (
          flags != bits.end()
          && (flags->second & disk_log_impl::segment_bitflags::self_compacted)
               == disk_log_impl::segment_bitflags::none) {
            return ret;
        }
    }
    return nullptr;
}
ss::future<> disk_log_impl::do_compact(compaction_config cfg) {
    // Temporary work-around to disable compaction for production
    static constexpr const bool compaction_enabled = false;
    // use signed type
    auto seg = find_reverse_not_compacted(_segs, _segbits);
    if (seg) {
        if (!compaction_enabled) {
            auto& flags = _segbits[seg->offsets().base_offset];
            flags |= segment_bitflags::self_compacted;
            return ss::now();
        }
        // compact
        return storage::internal::self_compact_segment(seg, cfg, _probe)
          .then([this, seg] {
              // find it again, do not capture the iterator
              auto& flags = _segbits[seg->offsets().base_offset];
              flags |= segment_bitflags::self_compacted;
              vlog(
                stlog.debug,
                "Finished self compacting: {}",
                seg->reader().filename());
          });
    }
    // all segments are self-compacted
    // do cross segment compaction
    return ss::now();
}
ss::future<> disk_log_impl::compact(compaction_config cfg) {
    ss::future<> f = ss::now();
    if (config().is_collectable()) {
        f = gc(cfg);
    }
    if (unlikely(
          config().has_overrides()
          && config().get_overrides().cleanup_policy_bitflags
               == model::cleanup_policy_bitflags::none)) {
        // prevent *any* collection - used for snapshots
        // all the internal redpanda logs - i.e.: controller, etc should
        // have this set
        f = ss::now();
    }
    if (config().is_compacted() && !_segs.empty()) {
        f = f.then([this, cfg] { return do_compact(cfg); });
    }
    return f;
}

ss::future<> disk_log_impl::gc(compaction_config cfg) {
    vassert(!_closed, "gc on closed log - {}", *this);

    if (unlikely(cfg.asrc->abort_requested())) {
        return ss::make_ready_future<>();
    }
    // TODO: this a workaround until we have raft-snapshotting in the the
    // controller so that we can still evict older data. At the moment we keep
    // the full history.
    constexpr std::string_view redpanda_ignored_ns = "redpanda";
    constexpr std::string_view kafka_ignored_ns = "kafka_internal";
    if (
      config().ntp().ns() == redpanda_ignored_ns
      || config().ntp().ns() == kafka_ignored_ns) {
        return ss::make_ready_future<>();
    }
    if (cfg.max_bytes) {
        size_t max = cfg.max_bytes.value();
        if (!_segs.empty() && _probe.partition_size() > max) {
            return garbage_collect_max_partition_size(max, cfg.asrc);
        }
    }
    return garbage_collect_oldest_segments(cfg.eviction_time, cfg.asrc);
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
        offset_stats ret;
        ret.start_offset = _start_offset;
        if (ret.start_offset > model::offset(0)) {
            ret.dirty_offset = ret.start_offset - model::offset(1);
            ret.committed_offset = ret.dirty_offset;
        }
        return ret;
    }
    // NOTE: we have to do this because ss::circular_buffer<> does not provide
    // with reverse iterators, so we manually find the iterator
    segment_set::type end;
    segment_set::type term_start;
    for (int i = (int)_segs.size() - 1; i >= 0; --i) {
        auto& seg = _segs[i];
        if (!seg->empty()) {
            if (!end) {
                end = seg;
            }
            // find term start offset
            if (seg->offsets().term < end->offsets().term) {
                break;
            }
            term_start = seg;
        }
    }
    if (!end) {
        offset_stats ret;
        ret.start_offset = _start_offset;
        if (ret.start_offset > model::offset(0)) {
            ret.dirty_offset = ret.start_offset - model::offset(1);
            ret.committed_offset = ret.dirty_offset;
        }
        return ret;
    }
    // we have valid begin and end
    const auto& bof = _segs.front()->offsets();
    const auto& eof = end->offsets();
    // term start
    const auto term_start_offset = term_start->offsets().base_offset;

    const auto start_offset = _start_offset() >= 0 ? _start_offset
                                                   : bof.base_offset;

    return storage::offset_stats{
      .start_offset = start_offset,

      .committed_offset = eof.committed_offset,
      .committed_offset_term = eof.term,

      .dirty_offset = eof.dirty_offset,
      .dirty_offset_term = eof.term,
      .last_term_start_offset = term_start_offset,
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
                if (config().is_compacted()) {
                    _segbits[h->offsets().base_offset] = segment_bitflags::none;
                }
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
    auto next_offset = ofs.dirty_offset;
    if (next_offset() >= 0) {
        // when dirty offset >= 0 it is implicity encoding the state of a
        // non-empty log (see offsets()). for a non-empty log, the offset of the
        // next batch to be applied is one past the last batch (dirty + 1).
        next_offset++;

    } else {
        // otherwise, the log is empty. in this case the offset of the next
        // batch to be appended is the starting offset of the log, which may be
        // explicitly set via operations like prefix truncation.
        next_offset = ofs.start_offset;

        // but, in the case of a brand new log, no starting offset has been
        // explicitly set, so it is defined implicitly to be 0.
        if (next_offset() < 0) {
            next_offset = model::offset(0);
        }
    }
    return log_appender(
      std::make_unique<disk_log_appender>(*this, cfg, now, next_offset));
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
disk_log_impl::make_unchecked_reader(log_reader_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), cfg, _probe);
      });
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(log_reader_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);
    if (config.start_offset < _start_offset) {
        return ss::make_exception_future<model::record_batch_reader>(
          std::runtime_error(fmt::format(
            "Reader cannot read before start of the log {} < {}",
            config.start_offset,
            _start_offset)));
    }
    return make_unchecked_reader(config);
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(timequery_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          auto start_offset = _start_offset;
          if (!lease->range.empty()) {
              // adjust for partial visibility of segment prefix
              start_offset = std::max(
                start_offset, (*lease->range.begin())->offsets().base_offset);
          }
          log_reader_config config(
            start_offset,
            cfg.max_offset,
            0,
            2048, // We just need one record batch
            cfg.prio,
            std::nullopt,
            cfg.time,
            cfg.abort_source);
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), config, _probe);
      });
}

std::optional<model::term_id> disk_log_impl::get_term(model::offset o) const {
    auto it = _segs.lower_bound(o);
    if (it != _segs.end() && o >= _start_offset) {
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
    _segbits.erase(s->offsets().base_offset);
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
          return _segs.empty()
                 || std::max(_segs.back()->offsets().base_offset, _start_offset)
                      < o;
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
                 || _segs.front()->offsets().dirty_offset >= cfg.start_offset;
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
    if (cfg.start_offset <= _start_offset) {
        return ss::make_ready_future<>();
    }

    /*
     * Persist the desired starting offset
     */
    return _kvstore
      .put(
        kvstore::key_space::storage,
        start_offset_key(),
        reflection::to_iobuf(cfg.start_offset))
      .then([this, cfg] {
          /*
           * Then delete all segments (potentially including the active segment)
           * whose max offset falls below the new starting offset.
           */
          return remove_prefix_full_segments(cfg);
      })
      .then([this, cfg] {
          /*
           * The two salient scenarios that can result are:
           *
           * (1) The log was initially empty, or the new starting offset fell
           * beyond the end of the log's dirty offset in which case all segments
           * have been deleted, and the log is now empty.
           *
           *     In this case the log will be rolled at the next append,
           *     creating a new segment beginning at the new starting offset.
           *     see `make_appender` for how the next offset is determined.
           *
           * (2) Zero or more full segments are removed whose ending offset is
           * ordered before the new starting offset. The new offset may have
           * fallen within an existing segment.
           *
           *     Segments below the new starting offset can be garbage
           *     collected. In this case when the new starting offset has fallen
           *     within an existing segment, the enforcement of the new offset
           *     is only logical, e.g. verify reader offset ranges.  Reclaiming
           *     the space corresponding to the non-visible prefix of the
           *     segment would require either (1) file hole punching or (2)
           *     rewriting the segment.  The overhead is never more than one
           *     segment, and this optimization is left as future work.
           */
          _start_offset = cfg.start_offset;

          /*
           * We want to maintain the following relationship for consistency:
           *
           *     start offset <= committed offset <= dirty offset
           *
           * However, it might be that the new starting offset is ordered
           * between the commited and dirty offsets. When this occurs we pay a
           * small penalty to resolve the inconsistency by flushing the log and
           * advancing the committed offset to be the same as the dirty offset.
           */
          auto ofs = offsets();
          if (_start_offset >= ofs.committed_offset) {
              return flush();
          }
          return ss::now();
      });
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
    cfg.base_offset = std::max(cfg.base_offset, _start_offset);
    // Note different from the stats variable above because
    // we want to delete even empty segments.
    if (
      cfg.base_offset
      < std::max(_segs.back()->offsets().base_offset, _start_offset)) {
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
          // an unchecked reader is created which does not enforce the logical
          // starting offset. this is needed because we really do want to read
          // all the data in the segment to find the correct physical offset.
          return make_unchecked_reader(
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

bytes disk_log_impl::start_offset_key() const {
    iobuf buf;
    auto ntp = config().ntp();
    reflection::serialize(buf, kvstore_key_type::start_offset, std::move(ntp));
    return iobuf_to_bytes(buf);
}

model::offset disk_log_impl::read_start_offset() const {
    auto value = _kvstore.get(kvstore::key_space::storage, start_offset_key());
    if (value) {
        auto offset = reflection::adl<model::offset>{}.from(std::move(*value));
        return offset;
    }
    return model::offset{};
}

std::ostream& disk_log_impl::print(std::ostream& o) const {
    return o << "{offsets:" << offsets() << ", closed:" << _closed
             << ", config:" << config() << ", logs:" << _segs << "}";
}

std::ostream& operator<<(std::ostream& o, const disk_log_impl& d) {
    return d.print(o);
}

log make_disk_backed_log(
  ntp_config cfg, log_manager& manager, segment_set segs, kvstore& kvstore) {
    auto ptr = ss::make_shared<disk_log_impl>(
      std::move(cfg), manager, std::move(segs), kvstore);
    return log(ptr);
}

} // namespace storage
