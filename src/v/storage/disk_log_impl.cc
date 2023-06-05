// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/disk_log_impl.h"

#include "config/configuration.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "storage/disk_log_appender.h"
#include "storage/fwd.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/logger.h"
#include "storage/offset_assignment.h"
#include "storage/offset_to_filepos_consumer.h"
#include "storage/readers_cache.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"
#include "storage/version.h"
#include "units.h"
#include "utils/gate_guard.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fair_queue.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

#include <fmt/format.h>

#include <exception>
#include <iterator>
#include <optional>
#include <sstream>

using namespace std::literals::chrono_literals;

namespace storage {

disk_log_impl::disk_log_impl(
  ntp_config cfg, log_manager& manager, segment_set segs, kvstore& kvstore)
  : log::impl(std::move(cfg))
  , _manager(manager)
  , _segment_size_jitter(
      internal::random_jitter(_manager.config().segment_size_jitter))
  , _segs(std::move(segs))
  , _kvstore(kvstore)
  , _start_offset(read_start_offset())
  , _lock_mngr(_segs)
  , _max_segment_size(compute_max_segment_size())
  , _readers_cache(std::make_unique<readers_cache>(
      config().ntp(), _manager.config().readers_cache_eviction_timeout)) {
    const bool is_compacted = config().is_compacted();
    for (auto& s : _segs) {
        _probe.add_initial_segment(*s);
        if (is_compacted) {
            s->mark_as_compacted_segment();
        }
    }
    _probe.initial_segments_count(_segs.size());
    _probe.setup_metrics(this->config().ntp());
}
disk_log_impl::~disk_log_impl() {
    vassert(_closed, "log segment must be closed before deleting:{}", *this);
}

size_t disk_log_impl::compute_max_segment_size() {
    auto segment_size = std::min(max_segment_size(), segment_size_hard_limit);
    return segment_size * (1 + _segment_size_jitter);
}

ss::future<> disk_log_impl::remove() {
    vassert(!_closed, "Invalid double closing of log - {}", *this);
    _closed = true;
    // wait for compaction to finish
    co_await _compaction_gate.close();
    // gets all the futures started in the background
    std::vector<ss::future<>> permanent_delete;
    permanent_delete.reserve(_segs.size());
    while (!_segs.empty()) {
        auto s = _segs.back();
        _segs.pop_back();
        permanent_delete.emplace_back(
          remove_segment_permanently(s, "disk_log_impl::remove()"));
    }

    co_await _readers_cache->stop().then(
      [this, permanent_delete = std::move(permanent_delete)]() mutable {
          // wait for all futures
          return ss::when_all_succeed(
                   permanent_delete.begin(), permanent_delete.end())
            .then([this]() {
                vlog(stlog.info, "Finished removing all segments:{}", config());
            })
            .then([this] {
                return _kvstore.remove(
                  kvstore::key_space::storage,
                  internal::start_offset_key(config().ntp()));
            })
            .then([this] {
                return _kvstore.remove(
                  kvstore::key_space::storage,
                  internal::clean_segment_key(config().ntp()));
            });
      });
}

ss::future<std::optional<ss::sstring>> disk_log_impl::close() {
    vassert(!_closed, "Invalid double closing of log - {}", *this);
    vlog(stlog.debug, "closing log {}", *this);
    _closed = true;
    if (
      _eviction_monitor
      && !_eviction_monitor->promise.get_future().available()) {
        _eviction_monitor->promise.set_exception(segment_closed_exception());
    }
    // wait for compaction to finish
    vlog(stlog.trace, "waiting for {} compaction to finish", config().ntp());
    co_await _compaction_gate.close();
    vlog(stlog.trace, "stopping {} readers cache", config().ntp());

    // close() on the segments is not expected to fail, but it might
    // encounter I/O errors (e.g. ENOSPC, EIO, out of file handles)
    // when trying to flush.  If that happens, all bets are off and
    // we will not mark our latest segment as clean (even if that
    // particular segment didn't report an I/O error)
    bool errors = false;

    co_await _readers_cache->stop().then([this, &errors] {
        return ss::parallel_for_each(
          _segs, [&errors](ss::lw_shared_ptr<segment>& h) {
              return h->close().handle_exception(
                [&errors, h](std::exception_ptr e) {
                    vlog(stlog.error, "Error closing segment:{} - {}", e, h);
                    errors = true;
                });
          });
    });

    if (_segs.size() && !errors) {
        auto clean_seg = _segs.back()->filename();
        vlog(
          stlog.debug,
          "closed {}, last clean segment is {}",
          config().ntp(),
          clean_seg);
        co_return clean_seg;
    }

    // Avoid assuming there will always be a segment: it is legal to
    // open a log + close it without writing anything.
    co_return std::nullopt;
}

model::offset disk_log_impl::size_based_gc_max_offset(size_t max_size) {
    size_t reclaimed_size = 0;
    model::offset ret;
    // do nothing
    if (_probe.partition_size() <= max_size) {
        return ret;
    }

    for (const auto& segment : _segs) {
        reclaimed_size += segment->size_bytes();
        ret = segment->offsets().dirty_offset;
        if (_probe.partition_size() - reclaimed_size <= max_size) {
            break;
        }
    }
    return ret;
}

model::offset disk_log_impl::time_based_gc_max_offset(model::timestamp time) {
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

    // if the segment max timestamp is bigger than now plus threshold we
    // will report the segment max timestamp as bogus timestamp
    static constexpr auto const_threshold = 1min;
    auto bogus_threshold = model::timestamp(
      model::timestamp::now().value() + const_threshold / 1ms);

    auto it = std::find_if(
      std::cbegin(_segs),
      std::cend(_segs),
      [this, time, bogus_threshold](const ss::lw_shared_ptr<segment>& s) {
          auto max_ts = s->index().max_timestamp();
          // first that is not going to be collected
          if (max_ts > bogus_threshold) {
              vlog(
                gclog.warn,
                "[{}] found segment with bogus max timestamp: {} - {}",
                config().ntp(),
                max_ts,
                s);
          }

          return max_ts > time;
      });

    if (it == _segs.cbegin()) {
        return model::offset{};
    }

    it = std::prev(it);
    return (*it)->offsets().committed_offset;
}

ss::future<model::offset>
disk_log_impl::monitor_eviction(ss::abort_source& as) {
    if (_eviction_monitor) {
        throw std::logic_error("Eviction promise already registered. Eviction "
                               "can not be monitored twice.");
    }

    auto opt_sub = as.subscribe([this]() noexcept {
        _eviction_monitor->promise.set_exception(
          ss::abort_requested_exception());
        _eviction_monitor.reset();
    });
    // already aborted
    if (!opt_sub) {
        return ss::make_exception_future<model::offset>(
          ss::abort_requested_exception());
    }

    return _eviction_monitor
      .emplace(
        eviction_monitor{ss::promise<model::offset>{}, std::move(*opt_sub)})
      .promise.get_future();
}

void disk_log_impl::set_collectible_offset(model::offset o) {
    vlog(
      gclog.debug,
      "[{}] setting max collectible offset {}, prev offset {}",
      config().ntp(),
      o,
      _max_collectible_offset);
    _max_collectible_offset = std::max(_max_collectible_offset, o);
}

bool disk_log_impl::is_front_segment(const segment_set::type& ptr) const {
    return !_segs.empty()
           && ptr->reader().filename() == (*_segs.begin())->reader().filename();
}

ss::future<> disk_log_impl::garbage_collect_segments(
  model::offset max_offset, ss::abort_source* as, std::string_view ctx) {
    vlog(
      gclog.debug,
      "[{}] {} requested to remove segments up to {} offset",
      config().ntp(),
      ctx,
      max_offset);
    // we only notify eviction monitor if there are segments to evict
    auto have_segments_to_evict = _segs.size() > 1
                                  && _segs.front()->offsets().committed_offset
                                       <= max_offset;

    if (_eviction_monitor && have_segments_to_evict) {
        _eviction_monitor->promise.set_value(max_offset);
        _eviction_monitor.reset();
    }

    max_offset = std::min(max_offset, _max_collectible_offset);

    return ss::do_until(
      [this, as, max_offset] {
          return _segs.size() <= 1 || as->abort_requested()
                 || _segs.front()->offsets().committed_offset > max_offset;
      },
      [this, ctx] {
          auto ptr = _segs.front();
          return update_start_offset(
                   ptr->offsets().dirty_offset + model::offset(1))
            .then([this, ptr, ctx](bool /*updated*/) {
                if (!is_front_segment(ptr)) {
                    return ss::now();
                }
                _segs.pop_front();
                return remove_segment_permanently(ptr, ctx);
            });
      });
}

ss::future<> disk_log_impl::garbage_collect_max_partition_size(
  size_t max_bytes, ss::abort_source* as) {
    model::offset max_offset = size_based_gc_max_offset(max_bytes);
    return garbage_collect_segments(max_offset, as, "gc[size_based_retention]");
}

ss::future<> disk_log_impl::garbage_collect_oldest_segments(
  model::timestamp time, ss::abort_source* as) {
    vlog(
      gclog.debug,
      "[{}] time retention timestamp: {}, first segment max timestamp: {}",
      config().ntp(),
      time,
      _segs.empty() ? model::timestamp::min()
                    : _segs.front()->index().max_timestamp());
    model::offset max_offset = time_based_gc_max_offset(time);
    return garbage_collect_segments(max_offset, as, "gc[time_based_retention]");
}

ss::future<> disk_log_impl::do_compact(compaction_config cfg) {
    vlog(
      gclog.trace,
      "[{}] applying 'compaction' log cleanup policy with config: {}",
      config().ntp(),
      cfg);
    // find first not compacted segment

    // loop until we compact segment or reached end of segments set
    while (!_segs.empty()) {
        const auto end_it = std::prev(_segs.end());
        auto seg_it = std::find_if(
          _segs.begin(), end_it, [](ss::lw_shared_ptr<segment>& s) {
              return !s->has_appender() && s->is_compacted_segment()
                     && !s->finished_self_compaction();
          });
        // nothing to compact
        if (seg_it == end_it) {
            break;
        }

        auto segment = *seg_it;
        auto result = co_await storage::internal::self_compact_segment(
          segment, cfg, _probe, *_readers_cache, _manager.resources());

        vlog(
          gclog.debug,
          "[{}] segment {} compaction result: {}",
          config().ntp(),
          segment->reader().filename(),
          result);
        if (result.did_compact()) {
            _compaction_ratio.update(result.compaction_ratio());
            co_return;
        }
    }

    if (auto range = find_compaction_range(); range) {
        auto r = co_await compact_adjacent_segments(std::move(*range), cfg);
        vlog(
          stlog.debug,
          "adjejcent segments of {}, compaction result: {}",
          config().ntp(),
          r);
        _compaction_ratio.update(r.compaction_ratio());
    }
}

std::optional<std::pair<segment_set::iterator, segment_set::iterator>>
disk_log_impl::find_compaction_range() {
    /*
     * adjacent segment compaction.
     *
     * the strategy is to choose a pair of adjacent segments and first combine
     * them into a single segment that replaces the pair, and then perform
     * self-compaction on the replacement segment.
     */
    if (_segs.size() < 2) {
        return std::nullopt;
    }

    // sliding window over segments. currently restricted to two segments
    auto range = std::make_pair(_segs.begin(), std::next(_segs.begin(), 2));

    while (true) {
        // the simple compaction process in use right now builds a concatenation
        // of segments so we avoid processing a group that is too large.
        const auto total_size = std::accumulate(
          range.first,
          range.second,
          size_t(0),
          [](size_t acc, ss::lw_shared_ptr<segment>& seg) {
              return acc + seg->size_bytes();
          });

        // batches in a segment have a term that is implicitly defined by the
        // name of the file they are contained in. since we need to retain the
        // term information for reach batch we'll avoid combining segments with
        // different terms. this can be addressed in a later optimization.
        const auto same_term = std::all_of(
          range.first,
          range.second,
          [term = (*range.first)->offsets().term](
            ss::lw_shared_ptr<segment>& seg) {
              return seg->offsets().term == term;
          });

        // Clamp max compacted segment size to 1.5GiB to avoid compaction index
        // size overflow. See
        // https://github.com/redpanda-data/redpanda/issues/7647 for details.
        // This is a workaround for older versions, proper fix is
        // https://github.com/redpanda-data/redpanda/pull/7687
        size_t max_compacted_segment_size = std::min(
          _manager.config().max_compacted_segment_size(), 1536_MiB);

        // found a good range if all the tests pass
        if (same_term && total_size < max_compacted_segment_size) {
            break;
        }

        // no candidate range found, yet. advance the window if we aren't
        // already at the end of the set. one option would also be to shrink the
        // range from the lower end if the range is large enough. advanced
        // scheduling is future work.
        if (range.second == _segs.end()) {
            return std::nullopt;
        }
        ++range.first;
        ++range.second;
    }

    // the chosen segments all need to be stable
    const auto unstable = std::any_of(
      range.first, range.second, [](ss::lw_shared_ptr<segment>& seg) {
          return seg->has_appender();
      });
    if (unstable) {
        return std::nullopt;
    }

    return range;
}

ss::future<compaction_result> disk_log_impl::compact_adjacent_segments(
  std::pair<segment_set::iterator, segment_set::iterator> range,
  storage::compaction_config cfg) {
    // lightweight copy of segments in range. once a scheduling event occurs in
    // this method we can't rely on the iterators in the range remaining valid.
    // for example, a concurrent truncate may erase an element from the range.
    std::vector<ss::lw_shared_ptr<segment>> segments;
    std::copy(range.first, range.second, std::back_inserter(segments));

    if (gclog.is_enabled(ss::log_level::debug)) {
        std::stringstream segments_str;
        for (size_t i = 0; i < segments.size(); i++) {
            fmt::print(segments_str, "Segment {}: {}, ", i + 1, *segments[i]);
        }
        vlog(
          gclog.debug,
          "Compacting {} adjacent segments: [{}]",
          segments.size(),
          segments_str.str());
    }

    // the segment which will be expanded to replace
    auto target = segments.front();

    // concatenate segments from the compaction range into replacement segment
    // backed by a staging file. the process is completed while holding a read
    // lock on the range, which is then released. the remainder of the
    // compaction process operates on replacement segment, and any conflicting
    // log operations are later identified before committing changes.
    auto staging_path = std::filesystem::path(
      fmt::format("{}.compaction.staging", target->reader().filename()));
    auto [replacement, generations]
      = co_await storage::internal::make_concatenated_segment(
        staging_path, segments, cfg, _manager.resources());

    // compact the combined data in the replacement segment. the partition size
    // tracking needs to be adjusted as compaction routines assume the segment
    // size is already contained in the partition size probe
    replacement->mark_as_compacted_segment();
    _probe.add_initial_segment(*replacement.get());
    auto ret = co_await storage::internal::self_compact_segment(
      replacement, cfg, _probe, *_readers_cache, _manager.resources());
    _probe.delete_segment(*replacement.get());
    vlog(gclog.debug, "Final compacted segment {}", replacement);

    /*
     * remove index files. they will be rebuilt by the single segment compaction
     * operation, and also ensures we examine segments correctly on recovery.
     */
    if (co_await ss::file_exists(target->index().filename())) {
        co_await ss::remove_file(target->index().filename());
    }

    auto compact_index = internal::compacted_index_path(
      target->reader().filename().c_str());
    if (co_await ss::file_exists(compact_index.string())) {
        co_await ss::remove_file(compact_index.string());
    }

    // lock the range. only metadata (e.g. open/rename/delete) i/o occurs with
    // these locks held so it is a relatively short duration. all of the data
    // copying and compaction i/o occurred above with no locks held. 5 retries
    // with a max lock timeout of 1 second. if we don't get the locks there is
    // probably a reader. compaction will revisit.
    auto locks = co_await internal::write_lock_segments(segments, 1s, 5);

    // fast check if we should abandon all the expensive i/o work if we happened
    // to be racing with an operation like truncation or shutdown.
    vassert(
      generations.size() == segments.size(),
      "Each segment must have corresponding generation");
    auto gen_it = generations.begin();
    for (const auto& segment : segments) {
        // check generation id under write lock
        if (unlikely(segment->get_generation_id() != *gen_it)) {
            vlog(
              gclog.info,
              "Aborting compaction of a segment: {}. Generation id mismatch, "
              "previous generation: {}",
              *segment,
              *gen_it);
            ret.executed_compaction = false;
            ret.size_after = ret.size_before;

            // Clean up any staging files that will go unused.
            // TODO: generalize this cleanup for other compaction abort paths.
            std::vector<std::filesystem::path> rm;
            rm.reserve(3);
            rm.emplace_back(replacement->reader().filename().c_str());
            rm.emplace_back(replacement->index().filename().c_str());
            rm.emplace_back(internal::compacted_index_path(
              replacement->reader().filename().c_str()));
            vlog(
              gclog.debug, "Cleaning up files from aborted compaction: {}", rm);
            for (const auto& f : rm) {
                if (co_await ss::file_exists(ss::sstring(f))) {
                    co_await ss::remove_file(ss::sstring(f));
                }
            }
            co_return ret;
        }
        if (unlikely(segment->is_closed())) {
            throw std::runtime_error(fmt::format(
              "Aborting compaction of closed segment: {}", *segment));
        }
        ++gen_it;
    }
    // transfer segment state from replacement to target
    locks = co_await internal::transfer_segment(
      target, replacement, cfg, _probe, std::move(locks));

    // remove the now redundant segments, if they haven't already been removed.
    // this could occur if racing with functions like truncate which manipulate
    // the segment set before acquiring segment locks. this also means that the
    // input iterator range may not longer be valid so we must manually search
    // the segment set.  the current adjacent segment compaction limits
    // compaction to two segments, and we check that assumption here and use
    // simplified clean-up routine.
    locks.clear();
    vassert(segments.size() == 2, "Cannot compact more than two segments");
    auto it = std::find(_segs.begin(), _segs.end(), segments.back());
    if (it != _segs.end()) {
        _segs.erase(it, std::next(it));
        co_await remove_segment_permanently(
          segments.back(), "compact_adjacent_segments");
    }

    co_return ret;
}
compaction_config
disk_log_impl::apply_overrides(compaction_config defaults) const {
    if (!config().has_overrides()) {
        return defaults;
    }
    compaction_config ret = defaults;
    /**
     * Override retention bytes
     */
    auto retention_bytes = config().get_overrides().retention_bytes;
    if (retention_bytes.is_disabled()) {
        ret.max_bytes = std::nullopt;
    }
    if (retention_bytes.has_value()) {
        ret.max_bytes = retention_bytes.value();
    }
    /**
     * Override retention time
     */
    auto retention_time = config().get_overrides().retention_time;
    if (retention_time.is_disabled()) {
        ret.eviction_time = model::timestamp::min();
    }
    if (retention_time.has_value()) {
        ret.eviction_time = model::timestamp(
          model::timestamp::now().value() - retention_time.value().count());
    }
    return ret;
}

ss::future<> disk_log_impl::compact(compaction_config cfg) {
    return ss::try_with_gate(_compaction_gate, [this, cfg]() mutable {
        vlog(
          gclog.trace,
          "[{}] house keeping with configuration from manager: {}",
          config().ntp(),
          cfg);
        cfg = apply_overrides(cfg);
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
        return f.then(
          [this] { _probe.set_compaction_ratio(_compaction_ratio.get()); });
    });
}

ss::future<> disk_log_impl::gc(compaction_config cfg) {
    vassert(!_closed, "gc on closed log - {}", *this);
    vlog(
      gclog.trace,
      "[{}] applying 'deletion' log cleanup policy with config: {}",
      config().ntp(),
      cfg);
    if (unlikely(cfg.asrc->abort_requested())) {
        return ss::make_ready_future<>();
    }
    // TODO: this a workaround until we have raft-snapshotting in the the
    // controller so that we can still evict older data. At the moment we keep
    // the full history.
    bool is_internal_namespace = config().ntp().ns() == model::redpanda_ns
                                 || config().ntp().ns()
                                      == model::kafka_internal_namespace;
    bool is_tx_manager_ntp = config().ntp().ns
                               == model::kafka_internal_namespace
                             && config().ntp().tp.topic
                                  == model::tx_manager_topic;
    if (!is_tx_manager_ntp && is_internal_namespace) {
        vlog(
          gclog.trace,
          "[{}] skipped log deletion, internal topic",
          config().ntp());
        return ss::make_ready_future<>();
    }
    if (cfg.max_bytes) {
        size_t max = cfg.max_bytes.value();
        vlog(
          gclog.debug,
          "[{}] retention max bytes: {}, current partition size: {}",
          config().ntp(),
          max,
          _probe.partition_size());
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
    for (int i = (int)_segs.size() - 1; i >= 0; --i) {
        auto& seg = _segs[i];
        if (!seg->empty()) {
            end = seg;
            break;
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

    const auto start_offset = _start_offset() >= 0 ? _start_offset
                                                   : bof.base_offset;

    return storage::offset_stats{
      .start_offset = start_offset,

      .committed_offset = eof.committed_offset,
      .committed_offset_term = eof.term,

      .dirty_offset = eof.dirty_offset,
      .dirty_offset_term = eof.term,
    };
}

model::offset disk_log_impl::find_last_term_start_offset() const {
    if (_segs.empty()) {
        return {};
    }

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
        return {};
    }

    return term_start->offsets().base_offset;
}

model::timestamp disk_log_impl::start_timestamp() const {
    if (_segs.empty()) {
        return model::timestamp{};
    }

    auto& s = _segs.front();
    return s->index().base_timestamp();
}

ss::future<> disk_log_impl::new_segment(
  model::offset o, model::term_id t, ss::io_priority_class pc) {
    vassert(
      o() >= 0 && t() >= 0, "offset:{} and term:{} must be initialized", o, t);
    // Recomputing here means that any roll size checks after this takes into
    // account updated segment size.
    _max_segment_size = compute_max_segment_size();
    return _manager
      .make_log_segment(
        config(),
        o,
        t,
        pc,
        config::shard_local_cfg().storage_read_buffer_size(),
        config::shard_local_cfg().storage_read_readahead_count())
      .then([this](ss::lw_shared_ptr<segment> handles) mutable {
          return remove_empty_segments().then(
            [this, h = std::move(handles)]() mutable {
                vassert(!_closed, "cannot add log segment to closed log");
                if (config().is_compacted()) {
                    h->mark_as_compacted_segment();
                }
                _segs.add(std::move(h));
                _probe.segment_created();
                _stm_manager->make_snapshot_in_background();
                _stm_dirty_bytes_units.return_all();
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

size_t disk_log_impl::max_segment_size() const {
    // override for segment size
    if (config().has_overrides() && config().get_overrides().segment_size) {
        return *config().get_overrides().segment_size;
    }
    // no overrides use defaults
    return config().is_compacted() ? _manager.config().compacted_segment_size()
                                   : _manager.config().max_segment_size();
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
    auto max = _max_segment_size;
    if (fo >= max) {
        return 0;
    }
    return max - fo;
}

ss::future<> disk_log_impl::force_roll(ss::io_priority_class iopc) {
    auto t = term();
    auto next_offset = offsets().dirty_offset + model::offset(1);
    if (_segs.empty()) {
        return new_segment(next_offset, t, iopc);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender()) {
        return new_segment(next_offset, t, iopc);
    }
    return ptr->release_appender(_readers_cache.get())
      .then([this, next_offset, t, iopc] {
          return new_segment(next_offset, t, iopc);
      });
}

ss::future<> disk_log_impl::maybe_roll(
  model::term_id t, model::offset next_offset, ss::io_priority_class iopc) {
    vassert(t >= term(), "Term:{} must be greater than base:{}", t, term());
    if (_segs.empty()) {
        return new_segment(next_offset, t, iopc);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender()) {
        return new_segment(next_offset, t, iopc);
    }
    bool size_should_roll = false;

    if (ptr->appender().file_byte_offset() >= _max_segment_size) {
        size_should_roll = true;
    }
    if (t != term() || size_should_roll) {
        return ptr->release_appender(_readers_cache.get())
          .then([this, next_offset, t, iopc] {
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
disk_log_impl::make_cached_reader(log_reader_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);

    auto rdr = _readers_cache->get_reader(config);
    if (rdr) {
        return ss::make_ready_future<model::record_batch_reader>(
          std::move(*rdr));
    }
    return _lock_mngr.range_lock(config)
      .then([this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return std::make_unique<log_reader>(std::move(lease), cfg, _probe);
      })
      .then([this](auto rdr) { return _readers_cache->put(std::move(rdr)); });
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

    if (config.start_offset > config.max_offset) {
        auto lease = std::make_unique<lock_manager::lease>(segment_set({}));
        auto empty = model::make_record_batch_reader<log_reader>(
          std::move(lease), config, _probe);
        return ss::make_ready_future<model::record_batch_reader>(
          std::move(empty));
    }
    return make_cached_reader(config);
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
            cfg.type_filter,
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

std::optional<model::offset>
disk_log_impl::get_term_last_offset(model::term_id term) const {
    if (unlikely(_segs.empty())) {
        return std::nullopt;
    }

    auto it = _segs.upper_bound(term);
    if (it == _segs.begin()) {
        return std::nullopt;
    }
    it = std::prev(it);

    if ((*it)->offsets().term == term) {
        return (*it)->offsets().dirty_offset;
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
                  && batches.front().header().max_timestamp >= cfg.time) {
                    return ret_t(batch_timequery(batches.front(), cfg.time));
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

    return _readers_cache->evict_segment_readers(s)
      .then([s](readers_cache::range_lock_holder cache_lock) {
          return s->close().finally([cache_lock = std::move(cache_lock)] {});
      })
      .handle_exception([s](std::exception_ptr e) {
          vlog(stlog.error, "Cannot close segment: {} - {}", e, s);
      })
      .finally([this, s] { _probe.segment_removed(); });
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
    /*
     * Persist the desired starting offset
     */
    co_await update_start_offset(cfg.start_offset);

    /*
     * Then delete all segments (potentially including the active segment)
     * whose max offset falls below the new starting offset.
     */
    {
        auto cache_lock = co_await _readers_cache->evict_prefix_truncate(
          cfg.start_offset);
        co_await remove_prefix_full_segments(cfg);
    }

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
        co_await flush();
    }
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

    if (cfg.base_offset > stats.dirty_offset || _segs.empty()) {
        co_return;
    }

    cfg.base_offset = std::max(cfg.base_offset, _start_offset);
    // Note different from the stats variable above because
    // we want to delete even empty segments.
    if (
      cfg.base_offset
      < std::max(_segs.back()->offsets().base_offset, _start_offset)) {
        co_await remove_full_segments(cfg.base_offset);
        // recurse
        co_return co_await do_truncate(cfg);
    }
    auto last = _segs.back();
    if (cfg.base_offset > last->offsets().dirty_offset) {
        co_return;
    }

    co_await last->flush();
    /**
     * We look for the offset preceding the the requested truncation offset.
     *
     * This guarantee that the offset to file position translating consumer will
     * see at least one batch that precedes the truncation point. This will
     * allow establishing correct dirty offset after truncation.
     */

    // if no offset is found in an index we will start from the segment base
    // offset
    model::offset start = last->offsets().base_offset;

    auto pidx = last->index().find_nearest(
      std::max(start, model::prev_offset(cfg.base_offset)));
    size_t initial_size = 0;
    if (pidx) {
        start = pidx->offset;
        initial_size = pidx->filepos;
    }

    auto initial_generation_id = last->get_generation_id();

    // an unchecked reader is created which does not enforce the logical
    // starting offset. this is needed because we really do want to read
    // all the data in the segment to find the correct physical offset.
    auto reader = co_await make_unchecked_reader(
      log_reader_config(start, model::offset::max(), cfg.prio));
    auto phs = co_await std::move(reader).consume(
      internal::offset_to_filepos_consumer(
        start, cfg.base_offset, initial_size),
      model::no_timeout);

    // all segments were deleted, return
    if (_segs.empty()) {
        co_return;
    }

    auto last_ptr = _segs.back();

    if (initial_generation_id != last_ptr->get_generation_id()) {
        vlog(
          stlog.debug,
          "segment generation changed during truncation, retrying with "
          "configuration: {}, initial generation: {}, current "
          "generation: {}",
          cfg,
          initial_generation_id,
          last_ptr->get_generation_id());
        co_return co_await do_truncate(cfg);
    }

    if (!phs) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "User asked to truncate at:{}, with initial physical "
          "position of: {{offset: {}, pos: {}}}, but "
          "internal::offset_to_filepos_consumer "
          "could not translate physical offsets. Log state: {}",
          cfg,
          start,
          initial_size,
          *this));
    }
    auto [prev_last_offset, file_position] = phs.value();

    if (file_position == 0) {
        _segs.pop_back();
        co_return co_await remove_segment_permanently(
          last_ptr, "truncate[post-translation]");
    }
    _probe.remove_partition_bytes(last_ptr->size_bytes() - file_position);
    auto cache_lock = co_await _readers_cache->evict_truncate(cfg.base_offset);

    try {
        co_return co_await last_ptr->truncate(prev_last_offset, file_position);
    } catch (...) {
        vassert(
          false,
          "Could not truncate:{} logical max:{}, physical "
          "offset:{} on segment:{} - log:{}",
          std::current_exception(),
          phs->first,
          phs->second,
          last,
          *this);
    }
}

model::offset disk_log_impl::read_start_offset() const {
    auto value = _kvstore.get(
      kvstore::key_space::storage, internal::start_offset_key(config().ntp()));
    if (value) {
        auto offset = reflection::adl<model::offset>{}.from(std::move(*value));
        return offset;
    }
    return model::offset{};
}

ss::future<bool> disk_log_impl::update_start_offset(model::offset o) {
    // Critical invariant for _start_offset is that it never decreases.
    // We update it under lock to ensure this invariant - otherwise we can
    // never be sure that we are not overwriting a bigger value that
    // is concurrently written to kvstore.
    return _start_offset_lock.with([this, o] {
        if (o <= _start_offset) {
            return ss::make_ready_future<bool>(false);
        }

        return _kvstore
          .put(
            kvstore::key_space::storage,
            internal::start_offset_key(config().ntp()),
            reflection::to_iobuf(o))
          .then([this, o] {
              _start_offset = o;
              return true;
          });
    });
}

ss::future<>
disk_log_impl::update_configuration(ntp_config::default_overrides o) {
    // Note: This hook is called to update topic level configuration overrides.
    // Cluster level configuration updates are handled separately by
    // binding to shard local configuration properties of interest.

    auto was_compacted = config().is_compacted();
    mutable_config().set_overrides(o);

    /**
     * For most of the settings we always query ntp config, only cleanup_policy
     * needs special treatment.
     */
    // enable compaction
    if (!was_compacted && config().is_compacted()) {
        for (auto& s : _segs) {
            s->mark_as_compacted_segment();
        }
    }
    // disable compaction
    if (was_compacted && !config().is_compacted()) {
        for (auto& s : _segs) {
            s->unmark_as_compacted_segment();
        }
    }

    return ss::now();
}
/**
 * We express compaction backlog as the size of a data that have to be read to
 * perform full compaction.
 *
 * According to this assumption compaction backlog consist of two components
 *
 * 1) size of not yet self compacted segments
 * 2) size of all possible adjacent segments compactions
 *
 * Component 1. of a compaction backlog is simply a sum of sizes of all not self
 * compacted segments.
 *
 * Calculation of 2nd part of compaction backlog is based on the observation
 * that adjacent segment compactions can be presented as a tree
 * (each leaf represents a log segment)
 *                              ┌────┐
 *                        ┌────►│ s5 │◄┐
 *                        │     └────┘ │
 *                        │            │
 *                        │            │
 *                        │            │
 *                      ┌─┴──┐         │
 *                    ┌►│ s4 │◄┐       │
 *                    │ └────┘ │       │
 *                    │        │       │
 *                    │        │       │
 *                    │        │       │
 *                    │        │       │
 *                  ┌─┴──┐  ┌──┴─┐  ┌──┴─┐
 *                  │ s1 │  │ s2 │  │ s3 │
 *                  └────┘  └────┘  └────┘
 *
 * To create segment from upper tree level two self compacted adjacent segments
 * from level below are concatenated and then resulting segment is self
 * compacted.
 *
 * In presented example size of s4:
 *
 *          sizeof(s4) = sizeof(s1) + sizeof(s2)
 *
 * Estimation of an s4 size after it will be self compacted is based on the
 * average compaction factor - `cf`. After self compaction size of
 * s4 will be estimated as
 *
 *         sizeof(s4) = cf * s4
 *
 * This allows calculating next compaction step which would be:
 *
 *         sizeof(s5) = cf * sizeof(s4) + s3 = cf * (sizeof(s1) + sizeof(s2))
 *
 * In order to calculate the backlog we have to sum both terms.
 *
 * Continuing those operation for upper tree levels we can obtain an equation
 * describing adjacent segments compaction backlog:
 *
 * cnt - segments count
 *
 *  backlog = sum(n=1,cnt) [sum(k=0, cnt - n + 1)][cf^k * sizeof(sn)] -
 *  cf^(cnt-1) * s1
 */
int64_t disk_log_impl::compaction_backlog() const {
    if (!config().is_compacted() || _segs.empty()) {
        return 0;
    }

    std::vector<std::vector<ss::lw_shared_ptr<segment>>> segments_per_term;
    auto current_term = _segs.front()->offsets().term;
    segments_per_term.emplace_back();
    auto idx = 0;
    int64_t backlog = 0;
    for (auto& s : _segs) {
        if (!s->finished_self_compaction()) {
            backlog += static_cast<int64_t>(s->size_bytes());
        }
        // if has appender do not include into adjacent segments calculation
        if (s->has_appender()) {
            continue;
        }

        if (current_term != s->offsets().term) {
            ++idx;
            segments_per_term.emplace_back();
        }
        segments_per_term[idx].push_back(s);
    }
    auto cf = _compaction_ratio.get();

    for (const auto& segs : segments_per_term) {
        auto segment_count = segs.size();
        if (segment_count == 1) {
            continue;
        }
        for (size_t n = 1; n <= segment_count; ++n) {
            auto& s = segs[n - 1];
            auto sz = s->finished_self_compaction() ? s->size_bytes()
                                                    : s->size_bytes() * cf;
            for (size_t k = 0; k <= segment_count - n; ++k) {
                if (k == segment_count - 1) {
                    continue;
                }
                if (k == 0) {
                    backlog += static_cast<int64_t>(sz);
                } else {
                    backlog += static_cast<int64_t>(std::pow(cf, k) * sz);
                }
            }
        }
    }

    return backlog;
}

/**
 * Record appended bytes & maybe trigger STM snapshots if they have
 * exceeded a threshold.
 */
void disk_log_impl::wrote_stm_bytes(size_t byte_size) {
    auto checkpoint_hint = _manager.resources().stm_take_bytes(
      byte_size, _stm_dirty_bytes_units);
    if (checkpoint_hint) {
        _stm_manager->make_snapshot_in_background();
        _stm_dirty_bytes_units.return_all();
    }
}

storage_resources& disk_log_impl::resources() { return _manager.resources(); }

std::ostream& disk_log_impl::print(std::ostream& o) const {
    fmt::print(
      o,
      "{{offsets: {}, max_collectible_offset: {}, is_closed: {}, segments: "
      "[{}], config: {}}}",
      offsets(),
      _max_collectible_offset,
      _closed,
      _segs,
      config());
    return o;
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
