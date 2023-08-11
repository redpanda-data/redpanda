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
#include "storage/offset_to_filepos.h"
#include "storage/readers_cache.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"
#include "storage/version.h"
#include "utils/gate_guard.h"
#include "utils/human.h"
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
/*
 * Some logs must be exempt from the cleanup=delete policy such that their full
 * history is retained. This function explicitly protects against any accidental
 * configuration changes that might violate that constraint. Examples include
 * the controller and internal kafka topics, with the exception of the
 * transaction manager topic.
 *
 * Once controller snapshots are enabled this rule will relaxed accordingly.
 *
 * Interaction with space management
 * =================================
 *
 * We leave some topics exempt (for now) from trimming even when space
 * management is turned on.
 *
 *    * Controller: control space using new snapshot mechanism
 *    * Consumer groups, transactions, producer ids, etc...
 *
 * The main concern with consumer groups is performance: changes in leadership
 * of a CG partition trigger a full replay of the consumer group partition. If
 * it is allowed to be swapped to cloud tier, then performance issues will arise
 * from normal cluster operation (leadershp movement) and this cost is not
 * driven / constrained by historical reads. Similarly for transactions and
 * idempotence. Controller topic should space can be managed by snapshots.
 */
bool deletion_exempt(const model::ntp& ntp) {
    bool is_internal_namespace = ntp.ns() == model::redpanda_ns
                                 || ntp.ns() == model::kafka_internal_namespace;
    bool is_tx_manager_ntp = ntp.ns == model::kafka_internal_namespace
                             && ntp.tp.topic == model::tx_manager_topic;
    return !is_tx_manager_ntp && is_internal_namespace;
}

disk_log_impl::disk_log_impl(
  ntp_config cfg,
  log_manager& manager,
  segment_set segs,
  kvstore& kvstore,
  ss::sharded<features::feature_table>& feature_table)
  : log(std::move(cfg))
  , _manager(manager)
  , _segment_size_jitter(
      internal::random_jitter(_manager.config().segment_size_jitter))
  , _segs(std::move(segs))
  , _kvstore(kvstore)
  , _feature_table(feature_table)
  , _start_offset(read_start_offset())
  , _lock_mngr(_segs)
  , _probe(std::make_unique<storage::probe>())
  , _max_segment_size(compute_max_segment_size())
  , _readers_cache(std::make_unique<readers_cache>(
      config().ntp(), _manager.config().readers_cache_eviction_timeout)) {
    const bool is_compacted = config().is_compacted();
    for (auto& s : _segs) {
        _probe->add_initial_segment(*s);
        if (is_compacted) {
            s->mark_as_compacted_segment();
        }
    }
    _probe->initial_segments_count(_segs.size());
    _probe->setup_metrics(this->config().ntp());
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
    co_await _compaction_housekeeping_gate.close();
    // gets all the futures started in the background
    std::vector<ss::future<>> permanent_delete;
    permanent_delete.reserve(_segs.size());
    while (!_segs.empty()) {
        auto s = _segs.back();
        _segs.pop_back();
        permanent_delete.emplace_back(
          remove_segment_permanently(s, "disk_log_impl::remove()"));
    }

    co_await _readers_cache->stop()
      .then([this, permanent_delete = std::move(permanent_delete)]() mutable {
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
      })
      .finally([this] { _probe->clear_metrics(); });
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
    co_await _compaction_housekeeping_gate.close();
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

    _probe->clear_metrics();

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

std::optional<model::offset>
disk_log_impl::size_based_gc_max_offset(gc_config cfg) {
    if (!cfg.max_bytes.has_value()) {
        return std::nullopt;
    }

    const auto max_size = cfg.max_bytes.value();
    const size_t partition_size = _probe->partition_size();
    vlog(
      gclog.debug,
      "[{}] retention max bytes: {}, current partition size: {}",
      config().ntp(),
      max_size,
      partition_size);
    if (_segs.empty() || partition_size <= max_size) {
        return std::nullopt;
    }

    size_t reclaimed_size = 0;
    const size_t safe_to_reclaim = partition_size - max_size;
    std::optional<model::offset> ret;

    for (const auto& segment : _segs) {
        reclaimed_size += segment->size_bytes();
        if (reclaimed_size > safe_to_reclaim) {
            break;
        }
        ret = segment->offsets().dirty_offset;
    }
    return ret;
}

std::optional<model::offset>
disk_log_impl::time_based_gc_max_offset(gc_config cfg) {
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
    vlog(
      gclog.debug,
      "[{}] time retention timestamp: {}, first segment retention timestamp: "
      "{}",
      config().ntp(),
      cfg.eviction_time,
      _segs.empty() ? model::timestamp::min()
                    : _segs.front()->index().retention_timestamp());

    static constexpr auto const_threshold = 1min;
    auto bogus_threshold = model::timestamp(
      model::timestamp::now().value() + const_threshold / 1ms);

    auto it = std::find_if(
      std::cbegin(_segs),
      std::cend(_segs),
      [this, time = cfg.eviction_time, bogus_threshold](
        const ss::lw_shared_ptr<segment>& s) {
          auto retention_ts = s->index().retention_timestamp();

          if (retention_ts > bogus_threshold) {
              // Warn on timestamps more than the "bogus" threshold in future
              vlog(
                gclog.warn,
                "[{}] found segment with bogus retention timestamp: {} (base "
                "{}, max {}) - {}",
                config().ntp(),
                retention_ts,
                s->index().base_timestamp(),
                s->index().max_timestamp(),
                s);
          }

          // first that is not going to be collected
          return retention_ts > time;
      });

    if (it == _segs.cbegin()) {
        return std::nullopt;
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

ss::future<model::offset>
disk_log_impl::request_eviction_until_offset(model::offset max_offset) {
    vlog(
      gclog.debug,
      "[{}] requested eviction of segments up to {} offset",
      config().ntp(),
      max_offset);
    // we only notify eviction monitor if there are segments to evict
    auto have_segments_to_evict = _segs.size() > 1
                                  && _segs.front()->offsets().committed_offset
                                       <= max_offset;

    if (_eviction_monitor && have_segments_to_evict) {
        _eviction_monitor->promise.set_value(max_offset);
        _eviction_monitor.reset();

        co_return model::next_offset(max_offset);
    }

    co_return _start_offset;
}

ss::future<> disk_log_impl::do_compact(
  compaction_config cfg, std::optional<model::offset> new_start_offset) {
    vlog(
      gclog.trace,
      "[{}] applying 'compaction' log cleanup policy with config: {}",
      config().ntp(),
      cfg);

    // create a logging predicate for offsets..
    auto offsets_compactible = [&cfg, &new_start_offset, this](segment& s) {
        if (new_start_offset && s.offsets().base_offset < *new_start_offset) {
            vlog(
              gclog.debug,
              "[{}] segment {} base offs {}, new start offset {}, "
              "skipping self compaction.",
              config().ntp(),
              s.reader().filename(),
              s.offsets().base_offset,
              *new_start_offset);
            return false;
        }

        if (s.has_compactible_offsets(cfg)) {
            vlog(
              gclog.debug,
              "[{}] segment {} stable offs {}, max compactible {}, "
              "compacting.",
              config().ntp(),
              s.reader().filename(),
              s.offsets().stable_offset,
              cfg.max_collectible_offset);
            return true;
        } else {
            vlog(
              gclog.trace,
              "[{}] segment {} stable offs {} > max compactible offs {}, "
              "skipping self compaction.",
              config().ntp(),
              s.reader().filename(),
              s.offsets().stable_offset,
              cfg.max_collectible_offset);
            return false;
        }
    };

    // loop until we compact segment or reached end of segments set
    while (!_segs.empty()) {
        const auto end_it = std::prev(_segs.end());
        auto seg_it = std::find_if(
          _segs.begin(),
          end_it,
          [offsets_compactible](ss::lw_shared_ptr<segment>& s) {
              return !s->has_appender() && s->is_compacted_segment()
                     && !s->finished_self_compaction()
                     && offsets_compactible(*s);
          });
        // nothing to compact
        if (seg_it == end_it) {
            break;
        }

        auto segment = *seg_it;
        auto result = co_await storage::internal::self_compact_segment(
          segment,
          _stm_manager,
          cfg,
          *_probe,
          *_readers_cache,
          _manager.resources(),
          storage::internal::should_apply_delta_time_offset(_feature_table));

        vlog(
          gclog.debug,
          "[{}] segment {} compaction result: {}",
          config().ntp(),
          segment->reader().filename(),
          result);
        if (result.did_compact()) {
            segment->clear_cached_disk_usage();
            _compaction_ratio.update(result.compaction_ratio());
            co_return;
        }
    }

    if (auto range = find_compaction_range(cfg); range) {
        auto r = co_await compact_adjacent_segments(std::move(*range), cfg);
        vlog(
          stlog.debug,
          "Adjacent segments of {}, compaction result: {}",
          config().ntp(),
          r);
        if (r.did_compact()) {
            _compaction_ratio.update(r.compaction_ratio());
        }
    }
}

std::optional<std::pair<segment_set::iterator, segment_set::iterator>>
disk_log_impl::find_compaction_range(const compaction_config& cfg) {
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

        // found a good range if all the tests pass
        if (
          same_term
          && total_size < _manager.config().max_compacted_segment_size()) {
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

    // the chosen segments all need to be stable.
    // Each participating segment should individually pass the compactible
    // offset check for the compacted segment to be stable.
    const auto unstable = std::any_of(
      range.first, range.second, [&cfg](ss::lw_shared_ptr<segment>& seg) {
          return seg->has_appender() || !seg->has_compactible_offsets(cfg);
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

    // Important that we take this lock _before_ any segment locks.
    auto segment_modify_lock = co_await _segment_rewrite_lock.get_units();

    // the segment which will be expanded to replace
    auto target = segments.front();

    target->clear_cached_disk_usage();

    // concatenate segments from the compaction range into replacement segment
    // backed by a staging file. the process is completed while holding a read
    // lock on the range, which is then released. the remainder of the
    // compaction process operates on replacement segment, and any conflicting
    // log operations are later identified before committing changes.
    auto staging_path = target->reader().path().to_compaction_staging();
    auto [replacement, generations]
      = co_await storage::internal::make_concatenated_segment(
        staging_path, segments, cfg, _manager.resources(), _feature_table);

    // compact the combined data in the replacement segment. the partition size
    // tracking needs to be adjusted as compaction routines assume the segment
    // size is already contained in the partition size probe
    replacement->mark_as_compacted_segment();
    _probe->add_initial_segment(*replacement.get());
    auto ret = co_await storage::internal::self_compact_segment(
      replacement,
      _stm_manager,
      cfg,
      *_probe,
      *_readers_cache,
      _manager.resources(),
      storage::internal::should_apply_delta_time_offset(_feature_table));
    _probe->delete_segment(*replacement.get());
    vlog(gclog.debug, "Final compacted segment {}", replacement);

    /*
     * remove index files. they will be rebuilt by the single segment compaction
     * operation, and also ensures we examine segments correctly on recovery.
     */
    if (co_await ss::file_exists(target->index().path().string())) {
        co_await ss::remove_file(target->index().path().string());
    }

    auto compact_index = target->reader().path().to_compacted_index();
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
            rm.emplace_back(replacement->index().path().string());
            rm.emplace_back(replacement->reader().path().to_compacted_index());
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
      target, replacement, cfg, *_probe, std::move(locks));

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

bool disk_log_impl::has_local_retention_override() const {
    if (config().has_overrides()) {
        const auto& overrides = config().get_overrides();
        auto bytes = overrides.retention_local_target_bytes;
        auto time = overrides.retention_local_target_ms;
        return bytes.is_engaged() || time.is_engaged();
    }
    return false;
}

gc_config disk_log_impl::maybe_override_retention_config(gc_config cfg) const {
    // Read replica topics have a different default retention
    if (config().is_read_replica_mode_enabled()) {
        cfg.eviction_time = std::max(
          model::timestamp(
            model::timestamp::now().value()
            - ntp_config::read_replica_retention.count()),
          cfg.eviction_time);
        return cfg;
    }

    // cloud_retention is disabled, do not override
    if (!is_cloud_retention_active()) {
        return cfg;
    }

    /*
     * don't override with local retention settings--let partition data expand
     * up to standard retention settings.
     */
    if (!config::shard_local_cfg().retention_local_strict()) {
        vlog(
          gclog.trace,
          "[{}] Skipped retention override for topic with remote write "
          "enabled: {}",
          config().ntp(),
          cfg);
        return cfg;
    }

    cfg = override_retention_config(cfg);

    vlog(
      gclog.trace,
      "[{}] Overrode retention for topic with remote write enabled: {}",
      config().ntp(),
      cfg);

    return cfg;
}

gc_config disk_log_impl::override_retention_config(gc_config cfg) const {
    tristate<std::size_t> local_retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> local_retention_ms{std::nullopt};

    // Apply the topic level overrides for local retention.
    if (config().has_overrides()) {
        local_retention_bytes
          = config().get_overrides().retention_local_target_bytes;
        local_retention_ms = config().get_overrides().retention_local_target_ms;
    }

    // If local retention was not explicitly disabled or enabled, then use the
    // defaults.
    if (
      !local_retention_bytes.is_disabled()
      && !local_retention_bytes.has_optional_value()) {
        local_retention_bytes = tristate<size_t>{
          config::shard_local_cfg().retention_local_target_bytes_default()};
    }

    if (
      !local_retention_ms.is_disabled()
      && !local_retention_ms.has_optional_value()) {
        local_retention_ms = tristate<std::chrono::milliseconds>{
          config::shard_local_cfg().retention_local_target_ms_default()};
    }

    if (local_retention_bytes.has_optional_value()) {
        if (cfg.max_bytes) {
            cfg.max_bytes = std::min(
              local_retention_bytes.value(), cfg.max_bytes.value());
        } else {
            cfg.max_bytes = local_retention_bytes.value();
        }
    }

    if (local_retention_ms.has_optional_value()) {
        cfg.eviction_time = std::max(
          model::timestamp(
            model::timestamp::now().value()
            - local_retention_ms.value().count()),
          cfg.eviction_time);
    }

    return cfg;
}

bool disk_log_impl::is_cloud_retention_active() const {
    return config::shard_local_cfg().cloud_storage_enabled()
           && (config().is_archival_enabled());
}

/*
 * applies overrides for non-cloud storage settings
 */
gc_config disk_log_impl::apply_base_overrides(gc_config defaults) const {
    if (!config().has_overrides()) {
        return defaults;
    }

    auto ret = defaults;

    /**
     * Override retention bytes
     */
    auto retention_bytes = config().get_overrides().retention_bytes;
    if (retention_bytes.is_disabled()) {
        ret.max_bytes = std::nullopt;
    }
    if (retention_bytes.has_optional_value()) {
        ret.max_bytes = retention_bytes.value();
    }

    /**
     * Override retention time
     */
    auto retention_time = config().get_overrides().retention_time;
    if (retention_time.is_disabled()) {
        ret.eviction_time = model::timestamp::min();
    }
    if (retention_time.has_optional_value()) {
        ret.eviction_time = model::timestamp(
          model::timestamp::now().value() - retention_time.value().count());
    }

    return ret;
}

gc_config disk_log_impl::apply_overrides(gc_config defaults) const {
    auto ret = apply_base_overrides(defaults);
    return maybe_override_retention_config(ret);
}

ss::future<> disk_log_impl::housekeeping(housekeeping_config cfg) {
    ss::gate::holder holder{_compaction_housekeeping_gate};
    vlog(
      gclog.trace,
      "[{}] house keeping with configuration from manager: {}",
      config().ntp(),
      cfg);

    /*
     * gc/retention policy
     */
    auto new_start_offset = co_await do_gc(cfg.gc);

    /*
     * comapction. could factor out into a public interface like gc/retention if
     * there is a need to run it separately.
     */
    if (config().is_compacted() && !_segs.empty()) {
        co_await do_compact(cfg.compact, new_start_offset);
    }

    _probe->set_compaction_ratio(_compaction_ratio.get());
}

ss::future<> disk_log_impl::gc(gc_config cfg) {
    ss::gate::holder holder{_compaction_housekeeping_gate};
    co_await do_gc(cfg);
}

ss::future<std::optional<model::offset>> disk_log_impl::do_gc(gc_config cfg) {
    vassert(!_closed, "gc on closed log - {}", *this);

    cfg = apply_overrides(cfg);

    /*
     * _cloud_gc_offset is used to communicate the intent to collect
     * partition data in excess of normal retention settings (and for infinite
     * retention / no retention settings). it is expected that an external
     * process such as disk space management drives this process such that after
     * a round of gc has run the intent flag can be cleared.
     */
    if (_cloud_gc_offset.has_value()) {
        const auto offset = _cloud_gc_offset.value();
        _cloud_gc_offset.reset();

        vassert(
          is_cloud_retention_active(), "Expected remote retention active");

        vlog(
          gclog.info,
          "[{}] applying 'deletion' log cleanup with remote retention override "
          "offset {} and config {}",
          config().ntp(),
          _cloud_gc_offset,
          cfg);

        co_return co_await request_eviction_until_offset(offset);
    }

    if (!config().is_collectable()) {
        co_return std::nullopt;
    }

    vlog(
      gclog.trace,
      "[{}] applying 'deletion' log cleanup policy with config: {}",
      config().ntp(),
      cfg);

    auto max_offset = co_await maybe_adjusted_retention_offset(cfg);

    if (max_offset) {
        co_return co_await request_eviction_until_offset(*max_offset);
    }

    co_return std::nullopt;
}

ss::future<> disk_log_impl::retention_adjust_timestamps(
  std::chrono::seconds ignore_in_future) {
    auto ignore_threshold = model::timestamp(
      model::timestamp::now().value() + ignore_in_future / 1ms);

    fragmented_vector<segment_set::type> segs_all_bogus;
    for (const auto& s : _segs) {
        auto max_ts = s->index().retention_timestamp();

        // If the actual max timestamp from user records is out of bounds, clamp
        // it to something more plausible, either from other batches or from
        // filesystem metadata if no batches with valid timestamps are
        // available.
        if (max_ts >= ignore_threshold) {
            auto alternate_batch_ts = s->index().find_highest_timestamp_before(
              ignore_threshold);
            if (alternate_batch_ts.has_value()) {
                // Some batch in the segment has a timestamp within threshold,
                // use that instead of the official max ts.
                vlog(
                  gclog.warn,
                  "[{}] Timestamp in future detected, check client clocks.  "
                  "Adjusting retention timestamp from {} to max valid record "
                  "timestamp {} on {}",
                  config().ntp(),
                  max_ts,
                  alternate_batch_ts.value(),
                  s->path().string());
                s->index().set_retention_timestamp(alternate_batch_ts.value());
            } else {
                // Collect segments with all bogus segments. We'll adjust them
                // below.
                segs_all_bogus.emplace_back(s);
            }
        } else {
            // We may drop out as soon as we see a segment with a valid
            // timestamp: this is collectible by time_based_gc_max_offset
            // without us making an adjustment, and if there are any later
            // segments that require correction we will hit them after
            // this earlier segment has  been collected.
            break;
        }
    }
    // Doing this outside the main loop since it incurs a scheduling point, and
    // segs_all_bogus is guaranteed to be stable, unlike _segs.
    for (const auto& s : segs_all_bogus) {
        // No indexed batch in the segment has a usable timestamp: fall
        // back to using the mtime of the file.  This is not accurate
        // at all (the file might have been created long
        // after the data was written) but is better than nothing.
        auto max_ts = s->index().retention_timestamp();
        auto file_timestamp = co_await s->get_file_timestamp();
        vlog(
          gclog.warn,
          "[{}] Timestamp in future detected, check client clocks.  "
          "Adjusting retention timestamp from {} to file mtime {} on "
          "{}",
          config().ntp(),
          max_ts,
          file_timestamp,
          s->path().string());
        s->index().set_retention_timestamp(file_timestamp);
    }
}

ss::future<std::optional<model::offset>>
disk_log_impl::maybe_adjusted_retention_offset(gc_config cfg) {
    auto ignore_timestamps_in_future
      = config::shard_local_cfg().storage_ignore_timestamps_in_future_sec();
    if (ignore_timestamps_in_future.has_value()) {
        // Correct any timestamps too far in future, before calculating the
        // retention offset.
        co_await retention_adjust_timestamps(
          ignore_timestamps_in_future.value());
    }

    co_return retention_offset(cfg);
}

std::optional<model::offset> disk_log_impl::retention_offset(gc_config cfg) {
    if (deletion_exempt(config().ntp())) {
        vlog(
          gclog.trace,
          "[{}] skipped log deletion, exempt topic",
          config().ntp());
        return std::nullopt;
    }

    auto max_offset_by_size = size_based_gc_max_offset(cfg);
    auto max_offset_by_time = time_based_gc_max_offset(cfg);

    return std::max(max_offset_by_size, max_offset_by_time);
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

    const auto start_offset = _start_offset >= model::offset{0}
                                ? _start_offset
                                : _segs.front()->offsets().base_offset;

    auto seg = _segs.lower_bound(start_offset);
    if (seg == _segs.end()) {
        return model::timestamp{};
    }

    return (*seg)->index().base_timestamp();
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
                _probe->segment_created();
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
    size_t result;
    if (config().has_overrides() && config().get_overrides().segment_size) {
        result = *config().get_overrides().segment_size;
    } else {
        // no overrides use defaults
        result = config().is_compacted()
                   ? _manager.config().compacted_segment_size()
                   : _manager.config().max_segment_size();
    }

    // Clamp to safety limits on segment sizes, in case the
    // property was set without proper validation (e.g. on
    // an older version or before limits were set)
    auto min_limit = config::shard_local_cfg().log_segment_size_min();
    auto max_limit = config::shard_local_cfg().log_segment_size_max();
    if (min_limit) {
        result = std::max(*min_limit, result);
    }
    if (max_limit) {
        result = std::min(*max_limit, result);
    }

    return result;
}

uint64_t disk_log_impl::size_bytes_after_offset(model::offset o) const {
    uint64_t size = 0;
    for (size_t i = _segs.size() - 1; i-- > 0;) {
        auto& seg = _segs[i];
        if (seg->offsets().base_offset < o) {
            break;
        }

        size += seg->size_bytes();
    }

    return size;
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
    auto roll_lock_holder = co_await _segments_rolling_lock.get_units();
    auto t = term();
    auto next_offset = offsets().dirty_offset + model::offset(1);
    if (_segs.empty()) {
        co_return co_await new_segment(next_offset, t, iopc);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender()) {
        co_return co_await new_segment(next_offset, t, iopc);
    }
    co_return co_await ptr->release_appender(_readers_cache.get())
      .then([this, next_offset, t, iopc] {
          return new_segment(next_offset, t, iopc);
      });
}

ss::future<> disk_log_impl::maybe_roll_unlocked(
  model::term_id t, model::offset next_offset, ss::io_priority_class iopc) {
    vassert(
      !_segments_rolling_lock.ready(),
      "Must have taken _segments_rolling_lock");

    vassert(t >= term(), "Term:{} must be greater than base:{}", t, term());
    if (_segs.empty()) {
        co_return co_await new_segment(next_offset, t, iopc);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender()) {
        co_return co_await new_segment(next_offset, t, iopc);
    }
    bool size_should_roll = false;

    if (ptr->appender().file_byte_offset() >= _max_segment_size) {
        size_should_roll = true;
    }
    if (t != term() || size_should_roll) {
        co_await ptr->release_appender(_readers_cache.get());
        co_await new_segment(next_offset, t, iopc);
    }
}

ss::future<> disk_log_impl::apply_segment_ms() {
    auto gate = _compaction_housekeeping_gate.hold();
    // Holding the lock blocks writes to the last open segment.
    // This is required in order to avoid the logic in this function
    // racing with an inflight append. Contention on this lock should
    // be very light, since we wouldn't need to enforce segment.ms
    // if this partition was high throughput (segment would have rolled
    // naturally).
    auto lock = co_await _segments_rolling_lock.get_units();

    if (_segs.empty()) {
        co_return;
    }
    auto last = _segs.back();
    if (!last->has_appender()) {
        // skip, rolling is already happening
        co_return;
    }
    auto first_write_ts = last->first_write_ts();
    if (!first_write_ts) {
        // skip check, no writes yet in this segment
        co_return;
    }

    auto seg_ms = config().segment_ms();
    if (!seg_ms.has_value()) {
        // skip, disabled or no default value
        co_return;
    }

    auto& local_config = config::shard_local_cfg();
    // clamp user provided value with (hopefully sane) server min and max
    // values, this should protect against overflow UB
    if (
      first_write_ts.value()
        + std::clamp(
          seg_ms.value(),
          local_config.log_segment_ms_min(),
          local_config.log_segment_ms_max())
      > ss::lowres_clock::now()) {
        // skip, time hasn't expired
        co_return;
    }

    auto pc = last->appender()
                .get_priority_class(); // note: has_appender is true, the
                                       // bouncer condition checked this
    co_await last->release_appender(_readers_cache.get());
    auto offsets = last->offsets();
    co_await new_segment(
      offsets.committed_offset + model::offset{1}, offsets.term, pc);
}

ss::future<model::record_batch_reader>
disk_log_impl::make_unchecked_reader(log_reader_config config) {
    vassert(!_closed, "make_reader on closed log - {}", *this);
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), cfg, *_probe);
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
          return std::make_unique<log_reader>(std::move(lease), cfg, *_probe);
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
          std::move(lease), config, *_probe);
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
              const ss::lw_shared_ptr<segment>& segment = *lease->range.begin();
              std::optional<segment_index::entry> index_entry = std::nullopt;

              // The index (and hence, binary search) is used only if the
              // timestamps on the batches are monotonically increasing.
              if (segment->index().batch_timestamps_are_monotonic()) {
                  index_entry = segment->index().find_nearest(cfg.time);
                  vlog(
                    stlog.debug,
                    "Batch timestamps have monotonically increasing "
                    "timestamps; used segment index to find first batch before "
                    "timestamp {}: offset={} with ts={}",
                    cfg.time,
                    index_entry->offset,
                    index_entry->timestamp);
              }

              auto offset_within_segment = index_entry
                                             ? index_entry->offset
                                             : segment->offsets().base_offset;

              // adjust for partial visibility of segment prefix
              start_offset = std::max(start_offset, offset_within_segment);
          }

          vlog(
            stlog.debug,
            "Starting timequery lookup from offset={} for ts={} in log "
            "with start_offset={}",
            start_offset,
            cfg.time,
            _start_offset);

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
            std::move(lease), config, *_probe);
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

std::optional<model::offset>
disk_log_impl::index_lower_bound(model::offset o) const {
    if (unlikely(_segs.empty())) {
        return std::nullopt;
    }
    if (o == model::offset{}) {
        return std::nullopt;
    }
    auto it = _segs.lower_bound(o);
    if (it == _segs.end()) {
        return std::nullopt;
    }
    auto& idx = (*it)->index();
    if (idx.max_offset() == o) {
        // input already lies on a boundary
        return o;
    }
    if (auto entry = idx.find_nearest(o)) {
        return model::prev_offset(entry->offset);
    }
    return std::nullopt;
}

ss::future<std::optional<timequery_result>>
disk_log_impl::timequery(timequery_config cfg) {
    vassert(!_closed, "timequery on closed log - {}", *this);
    if (_segs.empty()) {
        return ss::make_ready_future<std::optional<timequery_result>>();
    }
    return make_reader(cfg).then([cfg](model::record_batch_reader reader) {
        return model::consume_reader_to_memory(
                 std::move(reader), model::no_timeout)
          .then([cfg](model::record_batch_reader::storage_t st) {
              using ret_t = std::optional<timequery_result>;
              auto& batches = std::get<model::record_batch_reader::data_t>(st);
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
    vlog(stlog.info, "Removing \"{}\" ({}, {})", s->filename(), ctx, s);
    // stats accounting must happen synchronously
    _probe->delete_segment(*s);
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
      .finally([this, s] { _probe->segment_removed(); });
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
        return do_truncate_prefix(cfg)
          .then([this] {
              /*
               * after truncation do a quick refresh of cached variables that
               * are computed during disk usage calculation. this is useful for
               * providing more timely updates of reclaimable space through the
               * health report.
               */
              return disk_usage_and_reclaimable_space(
                _manager.default_gc_config());
          })
          .discard_result();
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
        // Before truncation, erase any claim about a particular segment being
        // clean: this may refer to a segment we are about to delete, or it
        // may refer to a segment that we will modify the indices+data for: on
        // subsequent startup we must assume that these segments require
        // recovery.  The clean_segment_key will get written again on next
        // clean shutdown.
        return _kvstore
          .remove(
            kvstore::key_space::storage,
            internal::clean_segment_key(config().ntp()))
          .then([this, cfg] {
              // dispatch the actual truncation
              return do_truncate(cfg, std::nullopt);
          });
    });
}

ss::future<> disk_log_impl::do_truncate(
  truncate_config cfg,
  std::optional<std::pair<ssx::semaphore_units, ssx::semaphore_units>>
    lock_guards) {
    if (!lock_guards) {
        auto seg_rolling_units = co_await _segments_rolling_lock.get_units();
        ssx::semaphore_units seg_rewrite_units
          = co_await _segment_rewrite_lock.get_units();
        lock_guards = {
          std::move(seg_rewrite_units), std::move(seg_rolling_units)};
    }

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
        co_return co_await do_truncate(cfg, std::move(lock_guards));
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
    model::timestamp initial_timestamp = last->index().max_timestamp();
    if (pidx) {
        start = pidx->offset;
        initial_size = pidx->filepos;
        initial_timestamp = pidx->timestamp;
    }

    auto initial_generation_id = last->get_generation_id();

    // an unchecked reader is created which does not enforce the logical
    // starting offset. this is needed because we really do want to read
    // all the data in the segment to find the correct physical offset.
    auto reader = co_await make_unchecked_reader(
      log_reader_config(start, model::offset::max(), cfg.prio));
    auto phs = co_await std::move(reader).consume(
      internal::offset_to_filepos_consumer(
        start, cfg.base_offset, initial_size, initial_timestamp),
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
        co_return co_await do_truncate(cfg, std::move(lock_guards));
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
    auto [prev_last_offset, file_position, new_max_timestamp] = phs.value();

    if (file_position == 0) {
        _segs.pop_back();
        co_return co_await remove_segment_permanently(
          last_ptr, "truncate[post-translation]");
    }
    _probe->remove_partition_bytes(last_ptr->size_bytes() - file_position);
    auto cache_lock = co_await _readers_cache->evict_truncate(cfg.base_offset);

    try {
        co_return co_await last_ptr->truncate(
          prev_last_offset, file_position, new_max_timestamp);
    } catch (...) {
        vassert(
          false,
          "Could not truncate:{} logical max:{}, physical "
          "offset:{}, new max timestamp:{} on segment:{} - log:{}",
          std::current_exception(),
          prev_last_offset,
          file_position,
          new_max_timestamp,
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

/// Calculate the compaction backlog of the segments within a particular term
///
/// This is the inner part of compaction_backlog()
int64_t compaction_backlog_term(
  std::vector<ss::lw_shared_ptr<segment>> segs, double cf) {
    int64_t backlog = 0;

    // Only compare each segment to a limited number of other segments, to
    // avoid the loop below blowing up in runtime when there are many segments
    // in the same term.
    static constexpr size_t limit_lookahead = 8;

    auto segment_count = segs.size();
    if (segment_count <= 1) {
        return 0;
    }

    for (size_t n = 1; n <= segment_count; ++n) {
        auto& s = segs[n - 1];
        auto sz = s->finished_self_compaction() ? s->size_bytes()
                                                : s->size_bytes() * cf;
        for (size_t k = 0; k <= segment_count - n && k < limit_lookahead; ++k) {
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

    return backlog;
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

    auto current_term = _segs.front()->offsets().term;
    auto cf = _compaction_ratio.get();
    int64_t backlog = 0;
    std::vector<ss::lw_shared_ptr<segment>> segments_this_term;

    // Limit how large we will try to allocate the sgements_this_term vector:
    // this protects us against corner cases where a term has a really large
    // number of segments.  Typical compaction use cases will have many fewer
    // segments per term than this (because segments are continuously compacted
    // away).  Corner cases include non-compactible data in a compacted topic,
    // or enabling compaction on a previously non-compacted topic.
    static constexpr size_t limit_segments_this_term = 1024;

    for (auto& s : _segs) {
        if (!s->finished_self_compaction()) {
            backlog += static_cast<int64_t>(s->size_bytes());
        }
        // if has appender do not include into adjacent segments calculation
        if (s->has_appender()) {
            continue;
        }

        if (current_term != s->offsets().term) {
            // New term: consume segments from the previous term.
            backlog += compaction_backlog_term(
              std::move(segments_this_term), cf);
            segments_this_term.clear();
        }

        if (segments_this_term.size() < limit_segments_this_term) {
            segments_this_term.push_back(s);
        }
    }

    // Consume segments from last term in the log after falling out of loop
    backlog += compaction_backlog_term(std::move(segments_this_term), cf);

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
      "{{offsets: {}, is_closed: {}, segments: "
      "[{}], config: {}}}",
      offsets(),
      _closed,
      _segs,
      config());
    return o;
}

std::ostream& operator<<(std::ostream& o, const disk_log_impl& d) {
    return d.print(o);
}

ss::shared_ptr<log> make_disk_backed_log(
  ntp_config cfg,
  log_manager& manager,
  segment_set segs,
  kvstore& kvstore,
  ss::sharded<features::feature_table>& feature_table) {
    return ss::make_shared<disk_log_impl>(
      std::move(cfg), manager, std::move(segs), kvstore, feature_table);
}

/*
 * assumes that the compaction gate is held.
 */
ss::future<std::pair<usage, reclaim_size_limits>>
disk_log_impl::disk_usage_and_reclaimable_space(gc_config input_cfg) {
    std::optional<model::offset> max_offset;
    if (config().is_collectable()) {
        auto cfg = apply_overrides(input_cfg);
        max_offset = retention_offset(cfg);
    }

    /*
     * offset calculation for local retention reclaimable is different than the
     * retention above and takes into account local retention advisory flag.
     */
    auto local_retention_cfg = apply_base_overrides(input_cfg);
    local_retention_cfg = override_retention_config(local_retention_cfg);
    const auto local_retention_offset
      = co_await maybe_adjusted_retention_offset(local_retention_cfg);

    /*
     * evicting data based on the retention policy is a coordinated effort
     * between disk_log_impl housekeeping and the raft/eviction_stm. it works
     * roughly as follows:
     *
     * 1. log computes ideal target offset for reclaim: max_offset computed by
     *    the call to retention_offset(cfg).
     *
     * 2. log notifies the eviction stm which computes: min(max_offset, clamp)
     *    where clamp is any additional restrictions, such as what has been
     *    uploaded to cloud storage.
     *
     * 3. raft attempts to prefix truncate the log at the clamped offset after
     *    it has prepared and taken any necessary snapshots.
     *
     * Because this effort is split between the log, raft, and eviction_stm we
     * end up having to duplicate some of the logic here, such as handling the
     * max collectible offset from stm. A future refactoring may consider moving
     * more of the retention controls into a higher level location.
     */
    const auto max_collectible = stm_manager()->max_collectible_offset();
    const auto retention_offset = [&]() -> std::optional<model::offset> {
        if (max_offset.has_value()) {
            return std::min(max_offset.value(), max_collectible);
        }
        return std::nullopt;
    }();

    /*
     * truncate_prefix() dry run. the available segments are tabulated even when
     * retention is not enabled data may be available in cloud storage and
     * still be subject to reclaim in low disk space situations.
     */
    fragmented_vector<segment_set::type> retention_segments;
    fragmented_vector<segment_set::type> available_segments;
    fragmented_vector<segment_set::type> remaining_segments;
    fragmented_vector<segment_set::type> local_retention_segments;
    for (auto& seg : _segs) {
        if (
          retention_offset.has_value()
          && seg->offsets().dirty_offset <= retention_offset.value()) {
            retention_segments.push_back(seg);
        } else if (
          is_cloud_retention_active()
          && seg->offsets().dirty_offset <= max_collectible) {
            available_segments.push_back(seg);
        } else {
            remaining_segments.push_back(seg);
        }

        /*
         * track segments that are reclaimable and above local retention.
         * effectively identcal to the condition in get_reclaimable_offsets. it
         * is repeated here because it is convenient to roll up these stats into
         * the usage information for consumption by the health monitor. the end
         * state is that the usage reporting here and the work in
         * get_reclaimable_offsets is going to be merged together.
         */
        if (
          !config().is_read_replica_mode_enabled()
          && is_cloud_retention_active() && seg != _segs.back()
          && seg->offsets().dirty_offset <= max_collectible
          && local_retention_offset.has_value()
          && seg->offsets().dirty_offset <= local_retention_offset.value()) {
            local_retention_segments.push_back(seg);
        }
    }

    auto [retention, available, remaining, lcl] = co_await ss::when_all_succeed(
      // reduce segment subject to retention policy
      ss::map_reduce(
        retention_segments,
        [](const segment_set::type& seg) { return seg->persistent_size(); },
        usage{},
        [](usage acc, usage u) { return acc + u; }),

      // reduce segments available for reclaim
      ss::map_reduce(
        available_segments,
        [](const segment_set::type& seg) { return seg->persistent_size(); },
        usage{},
        [](usage acc, usage u) { return acc + u; }),

      // reduce segments not available for reclaim
      ss::map_reduce(
        remaining_segments,
        [](const segment_set::type& seg) { return seg->persistent_size(); },
        usage{},
        [](usage acc, usage u) { return acc + u; }),

      // reduce segments not available for reclaim
      ss::map_reduce(
        local_retention_segments,
        [](const segment_set::type& seg) { return seg->persistent_size(); },
        usage{},
        [](usage acc, usage u) { return acc + u; }));

    /*
     * usage is the persistent size of on disk segment components (e.g. data and
     * indicies) accumulated across all segments.
     */
    usage usage = retention + available + remaining;

    /*
     * reclaim report contains total amount of data reclaimable by retention
     * policy or available for reclaim due to being in cloud storage tier.
     */
    reclaim_size_limits reclaim{
      .retention = retention.total(),
      .available = retention.total() + available.total(),
      .local_retention = lcl.total(),
    };

    /*
     * cache this for access by the health
     */
    _reclaimable_local_size_bytes = reclaim.local_retention;

    co_return std::make_pair(usage, reclaim);
}

/*
 * assumes that the compaction gate is held.
 */
ss::future<usage_target>
disk_log_impl::disk_usage_target(gc_config cfg, usage usage) {
    usage_target target;

    target.min_capacity
      = config::shard_local_cfg().storage_reserve_min_segments()
        * max_segment_size();

    cfg = apply_base_overrides(cfg);

    /*
     * compacted topics are always stored whole on local storage such that local
     * retention settings do not come into play.
     */
    if (config().is_compacted()) {
        /*
         * if there is no delete policy enabled for a compacted topic then its
         * local capacity requirement is limited only by compaction process and
         * how much data is written. for this case we use a heuristic of to
         * report space wanted as `factor * current capacity` to reflect that we
         * want space for continued growth.
         */
        if (!config().is_collectable() || deletion_exempt(config().ntp())) {
            target.min_capacity_wanted = usage.total() * 2;
            co_return target;
        }

        /*
         * otherwise, we fall through and evaluate the space wanted metric using
         * any configured retention policies _without_ overriding based on cloud
         * retention settings. the assumption here is that retention and not
         * compaction is what will limit on disk space. this heuristic should be
         * updated if we decide to also make "nice to have" estimates based on
         * expected compaction ratios.
         */
    } else {
        // applies local retention overrides for cloud storage
        cfg = maybe_override_retention_config(cfg);
    }

    /*
     * estimate how much space is needed to be meet size based retention policy.
     *
     * even though retention bytes has a built-in mechanism (ie -1 / nullopt) to
     * represent infinite retention, it is concievable that a user sets a large
     * value to achieve the same goal. in this case we cannot know precisely
     * what the intention is, and so we preserve the large value.
     *
     * TODO: it is likely that we will want to clamp this value at some point,
     * either here or at a higher level.
     */
    std::optional<size_t> want_size;
    if (cfg.max_bytes.has_value()) {
        want_size = cfg.max_bytes.value();
        /*
         * since prefix truncation / garbage collection only removes whole
         * segments we can make the estimate a bit more conservative by rounding
         * up to roughly the nearest segment size.
         */
        want_size.value() += max_segment_size()
                             - (cfg.max_bytes.value() % max_segment_size());
    }

    /*
     * grab the time based retention estimate and combine. if only time or size
     * is available, use that. if neither is available, use the `factor *
     * current size` heuristic to express that we want to allow for growth.
     * when both are available the minimum is taken. the reason for this is that
     * when retention rules are eventually applied, garbage collection will
     * select the rule that results in the most data being collected.
     */
    auto want_time = co_await disk_usage_target_time_retention(cfg);

    if (!want_time.has_value() && !want_size.has_value()) {
        target.min_capacity_wanted = usage.total() * 2;
    } else if (want_time.has_value() && want_size.has_value()) {
        target.min_capacity_wanted = std::min(
          want_time.value(), want_size.value());
    } else if (want_time.has_value()) {
        target.min_capacity_wanted = want_time.value();
    } else {
        target.min_capacity_wanted = want_size.value();
    }

    co_return target;
}

ss::future<std::optional<size_t>>
disk_log_impl::disk_usage_target_time_retention(gc_config cfg) {
    /*
     * take the opportunity to maybe fixup janky weird timestamps. also done in
     * normal garbage collection path and should be idempotent.
     */
    if (auto ignore_timestamps_in_future
        = config::shard_local_cfg().storage_ignore_timestamps_in_future_sec();
        ignore_timestamps_in_future.has_value()) {
        co_await retention_adjust_timestamps(
          ignore_timestamps_in_future.value());
    }

    /*
     * we are going to use whole segments for the data we'll use to extrapolate
     * the time-based retention capacity needs to avoid complexity of index
     * lookups (for now); if there isn't an entire segment worth of data, we
     * might not have a good sample size either. first we'll find the oldest
     * segment whose starting offset is greater than the eviction time. this and
     * subsequent segments will be fully contained within the time range.
     */
    auto it = std::find_if(
      std::cbegin(_segs),
      std::cend(_segs),
      [time = cfg.eviction_time](const ss::lw_shared_ptr<segment>& s) {
          return s->index().base_timestamp() >= time;
      });

    // collect segments for reducing
    fragmented_vector<segment_set::type> segments;
    for (; it != std::cend(_segs); ++it) {
        segments.push_back(*it);
    }

    /*
     * not enough data. caller will substitute in a reasonable value.
     */
    if (segments.size() < 2) {
        vlog(
          stlog.trace,
          "time-based-usage: skipping log without too few segments ({}) ntp {}",
          segments.size(),
          config().ntp());
        co_return std::nullopt;
    }

    auto start_timestamp = segments.front()->index().base_timestamp();
    auto end_timestamp = segments.back()->index().max_timestamp();
    auto duration = end_timestamp - start_timestamp;
    auto missing = start_timestamp - cfg.eviction_time;

    /*
     * timestamps are weird or there isn't a lot of data. be careful with "not a
     * lot of data" when considering time because throughput may be large. so we
     * go with something like 10 seconds just as a sanity check.
     */
    constexpr auto min_time_needed = model::timestamp(10'000);
    if (missing <= model::timestamp(0) || duration <= min_time_needed) {
        vlog(
          stlog.trace,
          "time-based-usage: skipping with time params start {} end {} etime "
          "{} dur {} miss {} ntp {}",
          start_timestamp,
          end_timestamp,
          cfg.eviction_time,
          duration,
          missing,
          config().ntp());
        co_return std::nullopt;
    }

    // roll up the amount of disk space taken by these segments
    auto usage = co_await ss::map_reduce(
      segments,
      [](const segment_set::type& seg) { return seg->persistent_size(); },
      storage::usage{},
      [](storage::usage acc, storage::usage u) { return acc + u; });

    // extrapolate out for the missing period of time in the retention period
    auto missing_bytes = (usage.total() * missing.value()) / duration.value();
    auto total = usage.total() + missing_bytes;

    vlog(
      stlog.trace,
      "time-based-usage: reporting total {} usage {} start {} end {} etime {} "
      "dur {} miss {} ntp {}",
      total,
      usage.total(),
      start_timestamp,
      end_timestamp,
      cfg.eviction_time,
      duration,
      missing,
      config().ntp());

    co_return total;
}

ss::future<usage_report> disk_log_impl::disk_usage(gc_config cfg) {
    // protect against concurrent log removal with housekeeping loop
    auto gate = _compaction_housekeeping_gate.hold();

    /*
     * compute the amount of current disk usage as well as the amount available
     * for being reclaimed.
     */
    auto [usage, reclaim] = co_await disk_usage_and_reclaimable_space(cfg);

    /*
     * compute target capacities such as minimum required capacity as well as
     * capacities needed to meet goals such as local retention.
     */
    auto target = co_await disk_usage_target(cfg, usage);

    /*
     * the intention here is to establish a needed <= wanted relationship which
     * should generally provide a nicer set of numbers for consumers to use.
     */
    target.min_capacity_wanted = std::max(
      target.min_capacity_wanted, target.min_capacity);

    co_return usage_report(usage, reclaim, target);
}

fragmented_vector<ss::lw_shared_ptr<segment>>
disk_log_impl::cloud_gc_eligible_segments() {
    vassert(
      is_cloud_retention_active(),
      "Expected {} to have cloud retention enabled",
      config().ntp());

    constexpr size_t keep_segs = 1;

    // must-have restriction
    if (_segs.size() <= keep_segs) {
        return {};
    }

    /*
     * how much are we allowed to collect? sub-systems (e.g. transactions)
     * may signal restrictions through the max collectible offset. for cloud
     * topics max collectible will include a reflection of how much data has
     * been uploaded into the cloud.
     */
    const auto max_collectible = stm_manager()->max_collectible_offset();

    // collect eligible segments
    fragmented_vector<segment_set::type> segments;
    for (auto remaining = _segs.size() - keep_segs; auto& seg : _segs) {
        if (seg->offsets().committed_offset <= max_collectible) {
            segments.push_back(seg);
        }
        if (--remaining <= 0) {
            break;
        }
    }

    return segments;
}

void disk_log_impl::set_cloud_gc_offset(model::offset offset) {
    if (!is_cloud_retention_active()) {
        vlog(
          stlog.debug,
          "Ignoring request to set GC offset on non-cloud enabled partition "
          "{}. Configuration may have recently changed.",
          config().ntp());
        return;
    }
    if (deletion_exempt(config().ntp())) {
        vlog(
          stlog.debug,
          "Ignoring request to trim at GC offset for exempt partition {}",
          config().ntp());
        return;
    }
    _cloud_gc_offset = offset;
}

ss::future<reclaimable_offsets>
disk_log_impl::get_reclaimable_offsets(gc_config cfg) {
    // protect against concurrent log removal with housekeeping loop
    auto gate = _compaction_housekeeping_gate.hold();

    reclaimable_offsets res;

    if (!is_cloud_retention_active()) {
        vlog(
          stlog.debug,
          "Reporting no reclaimable offsets for non-cloud partition {}",
          config().ntp());
        co_return res;
    }

    /*
     * there is currently a bug with read replicas that makes the max
     * collectible offset unreliable. the read replica topics still have a
     * retention setting, but we are going to exempt them from forced reclaim
     * until this bug is fixed to avoid any complications.
     *
     * https://github.com/redpanda-data/redpanda/issues/11936
     */
    if (config().is_read_replica_mode_enabled()) {
        vlog(
          stlog.debug,
          "Reporting no reclaimable offsets for read replica partition {}",
          config().ntp());
        co_return res;
    }

    // see comment on deletion_exempt
    if (deletion_exempt(config().ntp())) {
        vlog(
          stlog.debug,
          "Reporting no reclaimable space for exempt partition {}",
          config().ntp());
        co_return res;
    }

    /*
     * calculate the effective local retention. this forces the local retention
     * override in contrast to housekeeping GC where the overrides are applied
     * only when local retention is non-advisory.
     */
    cfg = apply_base_overrides(cfg);
    cfg = override_retention_config(cfg);
    const auto local_retention_offset
      = co_await maybe_adjusted_retention_offset(cfg);

    /*
     * when local retention is based off an explicit override, then we treat it
     * as the partition having a retention hint and use it to deprioritize
     * selection of data to reclaim over data without any hints.
     */
    const auto hinted = has_local_retention_override();

    /*
     * for a cloud-backed topic the max collecible offset is the threshold below
     * which data has been uploaded and can safely be removed from local disk.
     */
    const auto max_collectible = stm_manager()->max_collectible_offset();

    /*
     * lightweight segment set copy for safe iteration
     */
    fragmented_vector<segment_set::type> segments;
    for (const auto& seg : _segs) {
        segments.push_back(seg);
    }

    /*
     * currently we use two segments as the low space size
     */
    std::optional<model::offset> low_space_offset;
    if (segments.size() >= 2) {
        low_space_offset = segments[segments.size() - 2]->offsets().base_offset;
    }

    vlog(
      stlog.debug,
      "Categorizing {} {} segments for {} with local retention {} low "
      "space {} max collectible {}",
      segments.size(),
      (hinted ? "hinted" : "non-hinted"),
      config().ntp(),
      local_retention_offset,
      low_space_offset,
      max_collectible);

    /*
     * categorize each segment.
     */
    for (const auto& seg : segments) {
        const auto usage = co_await seg->persistent_size();
        const auto seg_size = usage.total();

        /*
         * the active segment designation takes precedence because it requires
         * special consideration related to the implications of force rolling.
         */
        if (seg == segments.back()) {
            /*
             * since the active segment receives all new data at any given time
             * it may not be fully uploaded to cloud storage so it is hard to
             * say anything definitive about it. instead, we report its size as
             * a potential:
             *
             *   1. rolling the active segment will bound progress towards
             *   making its current full size reclaimable as max collectible
             *   increases to cover the entire segment.
             *
             *   2. finally, we don't report it if max collectible hasn't even
             *   made it to the active segment yet.
             */
            if (seg->offsets().base_offset <= max_collectible) {
                res.force_roll = seg_size;
                vlog(
                  stlog.trace,
                  "Reporting partially collectible {} active segment",
                  human::bytes(seg_size));
            }
            break;
        }

        // to be categorized
        const reclaimable_offsets::offset point{
          .offset = seg->offsets().dirty_offset,
          .size = seg_size,
        };

        /*
         * if the current segment is not fully collectible, then subsequent
         * segments will not be either, and don't require consideration.
         */
        if (point.offset > max_collectible) {
            vlog(
              stlog.trace,
              "Stopping collection at offset {}:{} above max collectible {}",
              point.offset,
              human::bytes(point.size),
              max_collectible);
            break;
        }

        /*
         * when local retention is non-advisory then standard garbage collection
         * housekeeping will automatically remove data down to local retention.
         *
         * however when local retention is advisory, then partition storage is
         * allowed to expand up to the consumable retention. in this case
         * housekeeping will remove data above consumable retention, and we
         * categorize this excess data here down to the local retention.
         */
        if (
          local_retention_offset.has_value()
          && point.offset <= local_retention_offset.value()) {
            res.effective_local_retention.push_back(point);
            vlog(
              stlog.trace,
              "Adding offset {}:{} as local retention reclaimable",
              point.offset,
              human::bytes(point.size));
            continue;
        }

        /*
         * the low space represents the limit of data we want to remove from
         * any partition before moving on to more extreme tactics.
         */
        if (
          low_space_offset.has_value()
          && point.offset <= low_space_offset.value()) {
            if (hinted) {
                res.low_space_hinted.push_back(point);
                vlog(
                  stlog.trace,
                  "Adding offset {}:{} as low space hinted",
                  point.offset,
                  human::bytes(point.size));
            } else {
                res.low_space_non_hinted.push_back(point);
                vlog(
                  stlog.trace,
                  "Adding offset {}:{} as low space non-hinted",
                  point.offset,
                  human::bytes(point.size));
            }
            continue;
        }

        res.active_segment.push_back(point);
        vlog(
          stlog.trace,
          "Adding offset {}:{} as active segment bounded",
          point.offset,
          human::bytes(point.size));
    }

    co_return res;
}

size_t disk_log_impl::reclaimable_local_size_bytes() const {
    /*
     * circumstances/configuration under which this log will be trimming back to
     * local retention size may change. catch these before reporting potentially
     * stale information.
     */
    if (!is_cloud_retention_active()) {
        return 0;
    }
    if (config().is_read_replica_mode_enabled()) {
        // https://github.com/redpanda-data/redpanda/issues/11936
        return 0;
    }
    if (deletion_exempt(config().ntp())) {
        return 0;
    }
    return _reclaimable_local_size_bytes;
}

} // namespace storage
