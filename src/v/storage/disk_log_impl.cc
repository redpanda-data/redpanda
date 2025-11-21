// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/disk_log_impl.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "compaction/key_offset_map.h"
#include "config/configuration.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/offset_interval.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
#include "ssx/watchdog.h"
#include "storage/api.h"
#include "storage/chunk_cache.h"
#include "storage/compacted_offset_list.h"
#include "storage/compaction_reducers.h"
#include "storage/disk_log_appender.h"
#include "storage/exceptions.h"
#include "storage/fwd.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/log_reader.h"
#include "storage/logger.h"
#include "storage/offset_assignment.h"
#include "storage/offset_to_filepos.h"
#include "storage/readers_cache.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment.h"
#include "storage/segment_deduplication_utils.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/types.h"
#include "storage/version.h"
#include "utils/human.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fair_queue.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>

#include <fmt/format.h>
#include <roaring/roaring.hh>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>

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
 *
 * Note on unsafe_enable_consumer_offsets_delete_retention: This a special
 * configuration some select users can use to enable retention on CO topic
 * because the compaction logic is ineffective and they would like to use
 * retention as a stop gap until that is fixed. This configuration will be
 * deprecated once we fix the compaction gaps.
 */
bool deletion_exempt(const model::ntp& ntp) {
    bool is_internal_namespace = ntp.ns() == model::redpanda_ns
                                 || ntp.ns() == model::kafka_internal_namespace;
    bool is_tx_manager_ntp = ntp.ns == model::kafka_internal_namespace
                             && ntp.tp.topic == model::tx_manager_topic;
    bool is_consumer_offsets_ntp = ntp.ns()
                                     == model::kafka_consumer_offsets_nt.ns()
                                   && ntp.tp.topic
                                        == model::kafka_consumer_offsets_nt.tp;
    return (!is_tx_manager_ntp && is_internal_namespace)
           || (is_consumer_offsets_ntp && !config::shard_local_cfg().unsafe_enable_consumer_offsets_delete_retention());
}

// Meant for reading batches from a single `segment`. This does not consider any
// other `log` state- meaning, the `log` could have been prefix truncated and
// batches read from the `segment` could be ahead of the new `_start_offset`.
//
// This class should only be used when it is necessary to consider the batches
// still physically stored in a `segment` on disk, e.g for file position
// calculations. For all other applications, the `log_reader` should be used.
class single_segment_reader final : public model::record_batch_reader::impl {
public:
    using storage_t = model::record_batch_reader::storage_t;
    single_segment_reader(
      ss::lw_shared_ptr<segment> seg,
      ss::rwlock::holder seg_read_lock,
      local_log_reader_config reader_cfg,
      probe& pb)
      : _seg(seg)
      , _seg_read_lock(std::move(seg_read_lock))
      , _config(reader_cfg)
      , _rdr(*_seg, _config, pb) {}

    ~single_segment_reader() final = default;

    bool is_end_of_stream() const final { return _is_end_of_stream; }

    ss::future<storage_t>
    do_load_slice(model::timeout_clock::time_point timeout) final {
        auto recs = co_await _rdr.read_some(timeout);
        if (!recs.has_value() || recs.value().empty()) {
            _is_end_of_stream = true;
            co_return storage_t{};
        }

        auto& batches = recs.value();
        co_return std::move(batches);
    }

    ss::future<> finally() noexcept final { return _rdr.close(); }

    void print(std::ostream& os) final {
        fmt::print(
          os,
          "storage::single_segment_reader for {}, config {}",
          _seg->filename(),
          _config);
    }

private:
    ss::lw_shared_ptr<segment> _seg;
    ss::rwlock::holder _seg_read_lock;
    local_log_reader_config _config;
    log_segment_batch_reader _rdr;
    bool _is_end_of_stream{false};
};

disk_log_impl::disk_log_impl(
  ntp_config cfg,
  raft::group_id group,
  log_manager& manager,
  segment_set segs,
  kvstore& kvstore,
  ss::sharded<features::feature_table>& feature_table,
  std::vector<model::record_batch_type> translator_batch_types)
  : log(std::move(cfg))
  , _manager(manager)
  , _segment_size_jitter(
      internal::random_jitter(_manager.config().segment_size_jitter))
  , _segs(std::move(segs))
  , _kvstore(kvstore)
  , _feature_table(feature_table)
  , _start_offset(read_start_offset())
  , _lock_mngr(_segs)
  , _offset_translator(
      std::move(translator_batch_types),
      group,
      config().ntp(),
      _kvstore,
      resources())
  , _probe(std::make_unique<storage::probe>())
  , _max_segment_size(compute_max_segment_size())
  , _readers_cache(
      std::make_unique<readers_cache>(
        config().ntp(),
        _manager.config().readers_cache_eviction_timeout,
        config::shard_local_cfg().readers_cache_target_max_size.bind()))
  , _compaction_enabled(config().is_locally_compacted()) {
    for (auto& s : _segs) {
        _probe->add_initial_segment(*s);
        if (_compaction_enabled) {
            s->mark_as_compacted_segment();
        }
        add_segment_bytes(s, s->size_bytes());
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

    vlog(
      stlog.debug, "waiting for locks before removing log {}", config().ntp());

    // Request abort of compaction before obtaining rewrite lock holder.
    _compaction_as.request_abort();

    // To prevent a race between prefix truncation and closing, obtain the
    // rewrite lock here and indicate it as broken for any future waiters.
    auto rewrite_lock_holder = co_await _segment_rewrite_lock.get_units();
    _segment_rewrite_lock.broken();

    // To prevent racing with a new segment being rolled, obtain the mutex here,
    // and indicate it as broken for any future waiters.
    auto roll_lock_holder = co_await _segments_rolling_lock.get_units();
    _segments_rolling_lock.broken();

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
    co_await _offset_translator.remove_persistent_state();

    co_await _readers_cache->stop()
      .then([this, permanent_delete = std::move(permanent_delete)]() mutable {
          // wait for all futures
          return ss::when_all_succeed(
                   permanent_delete.begin(), permanent_delete.end())
            .then([this]() {
                vlog(stlog.info, "Finished removing all segments:{}", config());
            })
            .then([this] {
                return remove_kvstore_state(config().ntp(), _kvstore);
            });
      })
      .finally([this] { _probe->clear_metrics(); });
}

ss::future<> disk_log_impl::start(
  std::optional<truncate_prefix_config> truncate_cfg, ss::abort_source& as) {
    auto is_new = is_new_log();
    co_await offset_translator().start(
      storage::offset_translator::must_reset{is_new});
    if (truncate_cfg.has_value()) {
        co_await truncate_prefix(truncate_cfg.value());
    }
    // Reset or load the offset translator state, depending on whether this is
    // a brand new log.
    if (!is_new) {
        co_await offset_translator().sync_with_log(*this, as);
    }
}

ss::future<std::optional<ss::sstring>> disk_log_impl::close() {
    vassert(!_closed, "Invalid double closing of log - {}", *this);

    vlog(
      stlog.debug, "waiting for locks before closing log {}", config().ntp());

    // Request abort of compaction before obtaining rewrite lock holder.
    _compaction_as.request_abort();

    // To prevent a race between prefix truncation and closing, obtain the
    // rewrite lock here and indicate it as broken for any future waiters.
    auto rewrite_lock_holder = co_await _segment_rewrite_lock.get_units();
    _segment_rewrite_lock.broken();

    // To prevent racing with a new segment being rolled, obtain the mutex here,
    // and indicate it as broken for any future waiters.
    auto roll_lock_holder = co_await _segments_rolling_lock.get_units();
    _segments_rolling_lock.broken();

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
disk_log_impl::size_based_gc_max_offset(gc_config cfg) const {
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
        ret = segment->offsets().get_dirty_offset();
    }
    return ret;
}

std::optional<model::offset>
disk_log_impl::time_based_gc_max_offset(gc_config cfg) const {
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
    // with Redpanda v23.3, segment_index tracks broker_timestamps, and it's the
    // value returned by segment_index::retention_timestamp() for **new**
    // segments. for older segments, the issue still remains and
    // storage_ignore_timestamps_in_future_secs is another way to deal with it

    // this will retrive cluster configs, to reused them during the scan since
    // there are no scheduling points

    const auto retention_cfg = time_based_retention_cfg::make(
      _feature_table.local());

    // if the segment max timestamp is bigger than now plus threshold we
    // will report the segment max timestamp as bogus timestamp
    vlog(
      gclog.debug,
      "[{}] time retention timestamp: {}, first segment retention timestamp: "
      "{}, retention_cfg: {}",
      config().ntp(),
      cfg.eviction_time,
      _segs.empty() ? model::timestamp::min()
                    : _segs.front()->index().retention_timestamp(retention_cfg),
      retention_cfg);

    auto validated_timestamps = _feature_table.local().is_active(
      features::feature::validated_batch_timestamps);
    static constexpr auto const_threshold = 1min;
    auto bogus_threshold = model::timestamp(
      model::timestamp::now().value() + const_threshold / 1ms);

    auto it = std::find_if(
      std::cbegin(_segs),
      std::cend(_segs),
      [this,
       time = cfg.eviction_time,
       bogus_threshold,
       retention_cfg,
       validated_timestamps](const ss::lw_shared_ptr<segment>& s) {
          auto retention_ts = s->index().retention_timestamp(retention_cfg);

          if (!validated_timestamps) {
              // This legacy behavior of warning on timestamps above
              // `bogus_threshold` is only needed in older clusters that didn't
              // validate timestamps.
              if (retention_ts > bogus_threshold) {
                  // Warn on timestamps more than the "bogus" threshold in
                  // future this should not fire for segments created after
                  // v23.3
                  vlog(
                    gclog.warn,
                    "[{}] found segment with bogus retention timestamp: {} "
                    "(base "
                    "{}, max {}) - {}",
                    config().ntp(),
                    retention_ts,
                    s->index().base_timestamp(),
                    s->index().max_timestamp(),
                    s->index().broker_timestamp(),
                    s);
              }
          }

          // first that is not going to be collected
          return retention_ts > time;
      });

    if (it == _segs.cbegin()) {
        return std::nullopt;
    }

    it = std::prev(it);
    return (*it)->offsets().get_committed_offset();
}

ss::future<model::offset>
disk_log_impl::monitor_eviction(ss::abort_source& as) {
    if (_eviction_monitor) {
        throw std::logic_error(
          "Eviction promise already registered. Eviction "
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

model::offset
disk_log_impl::request_eviction_until_offset(model::offset max_offset) {
    const auto min_offset = [this]() -> std::optional<model::offset> {
        if (_segs.size() > 1) {
            return _segs.front()->offsets().get_committed_offset();
        }
        return std::nullopt;
    }();

    const auto have_segments_to_evict = min_offset.has_value()
                                        && min_offset.value() <= max_offset;

    if (_eviction_monitor && have_segments_to_evict) {
        _eviction_monitor->promise.set_value(max_offset);
        _eviction_monitor.reset();

        vlog(
          gclog.debug,
          "[{}] requested eviction of segments up to {} offset",
          config().ntp(),
          max_offset);

        return model::next_offset(max_offset);
    }

    vlog(
      gclog.debug,
      "[{}] skipping eviction: monitor {} min offset {} max offset {}",
      config().ntp(),
      _eviction_monitor.has_value(),
      min_offset,
      max_offset);

    return _start_offset;
}

ss::future<compaction_result> disk_log_impl::segment_self_compact(
  compaction::compaction_config cfg,
  ss::lw_shared_ptr<segment> seg,
  bool force_compaction) {
    co_return co_await storage::internal::self_compact_segment(
      seg,
      _stm_manager,
      cfg,
      *_probe,
      *_readers_cache,
      _manager.resources(),
      _feature_table,
      force_compaction);
}

ss::future<> disk_log_impl::adjacent_merge_compact(
  segment_set segments,
  compaction::compaction_config cfg,
  std::optional<model::offset> new_start_offset) {
    vlog(
      gclog.trace,
      "[{}] applying 'compaction' log cleanup policy with config: {}",
      config().ntp(),
      cfg);

    // create a logging predicate for offsets..
    auto offsets_compactible = [&cfg, &new_start_offset, this](segment& s) {
        if (
          new_start_offset
          && s.offsets().get_base_offset() < *new_start_offset) {
            vlog(
              gclog.trace,
              "[{}] segment {} base offs {}, new start offset {}, "
              "skipping self compaction.",
              config().ntp(),
              s.reader().filename(),
              s.offsets().get_base_offset(),
              *new_start_offset);
            return false;
        }

        if (s.is_compactible(cfg)) {
            vlog(
              gclog.trace,
              "[{}] segment {} stable offs {}, max compactible {}, "
              "compacting.",
              config().ntp(),
              s.reader().filename(),
              s.offsets().get_stable_offset(),
              cfg.max_removable_local_log_offset);
            return true;
        } else {
            vlog(
              gclog.trace,
              "[{}] segment {} stable offs {} > max compactible offs {}, "
              "skipping self compaction.",
              config().ntp(),
              s.reader().filename(),
              s.offsets().get_stable_offset(),
              cfg.max_removable_local_log_offset);
            return false;
        }
    };

    for (auto& seg : segments) {
        if (cfg.asrc) {
            cfg.asrc->check();
        }

        auto is_compactible_segment = !seg->is_closed() && !seg->has_appender()
                                      && seg->is_compacted_segment()
                                      && offsets_compactible(*seg);
        // Skip over segments that that being truncated, or are uncompactible.
        if (is_compactible_segment) {
            auto result = co_await segment_self_compact(cfg, seg);

            if (seg->is_closed()) {
                // We can race here with a truncation operation that removes the
                // full segment, since we are not under a rewrite lock.
                continue;
            }
            vlog(
              gclog.debug,
              "[{}] segment {} compaction result: {}",
              config().ntp(),
              seg->reader().filename(),
              result);
            if (result.did_compact()) {
                _compaction_ratio.update(result.compaction_ratio());
                const ssize_t removed_bytes = ssize_t(result.size_before)
                                              - ssize_t(result.size_after);
                subtract_segment_bytes(seg, removed_bytes);
            }
        }
    }

    co_await compact_adjacent_segment_ranges(cfg, new_start_offset);
}

segment_set disk_log_impl::find_sliding_range(
  const compaction::compaction_config& cfg,
  std::optional<model::offset> new_start_offset) {
    if (
      _last_compaction_window_start_offset.has_value()
      && (_last_compaction_window_start_offset.value()
           <= _segs.front()->offsets().get_base_offset())) {
        // If this evaluates to true, it is likely because local retention has
        // removed segments. e.g, segments ([0],[1],[2]), have been garbage
        // collected to segments ([2]), while _last_window_compaction_offset
        // == 1. To avoid compaction getting stuck in this situation, we reset
        // the compaction window offset here.
        vlog(
          gclog.debug,
          "[{}] start offset ({}) <= base offset of front segment ({}), "
          "resetting compaction window start offset",
          config().ntp(),
          _last_compaction_window_start_offset.value(),
          _segs.front()->offsets().get_base_offset());
        _probe->add_sliding_window_round_complete();
        _last_compaction_window_start_offset.reset();
    }

    // Collect all segments that are compactible.
    segment_set::underlying_t buf;
    for (const auto& seg : _segs) {
        if (seg->has_appender() || !seg->is_compactible(cfg)) {
            // Stop once we get to an uncompactible segment.
            break;
        }
        if (
           _last_compaction_window_start_offset.has_value()
           && (seg->offsets().get_base_offset()
                >= _last_compaction_window_start_offset.value())) {
            // Force clean segment production by compacting down to the
            // start of the log before considering new segments in the
            // compaction window.
            break;
        }
        if (
          new_start_offset
          && seg->offsets().get_base_offset() < new_start_offset.value()) {
            // Skip over segments that are being truncated.
            continue;
        }

        buf.emplace_back(seg);
    }

    segment_set segs(std::move(buf));
    return segs;
}

ss::future<bool> disk_log_impl::sliding_window_compact(
  const compaction::compaction_config& cfg,
  std::optional<model::offset> new_start_offset) {
    vlog(gclog.debug, "[{}] running sliding window compaction", config().ntp());
    auto segs = find_sliding_range(cfg, new_start_offset);
    if (segs.empty()) {
        vlog(
          gclog.debug, "[{}] no segments in range to compact", config().ntp());
        co_return false;
    }
    bool has_self_compacted = false;
    for (auto& seg : segs) {
        if (cfg.asrc) {
            cfg.asrc->check();
        }

        auto result = co_await segment_self_compact(cfg, seg);

        if (result.did_compact() == false) {
            continue;
        }

        if (seg->is_closed()) {
            // We can race here with a truncation operation that removes the
            // full segment, since we are not under a rewrite lock.
            continue;
        }

        const ssize_t removed_bytes = result.size_before - result.size_after;
        subtract_segment_bytes(seg, removed_bytes);
        vlog(
          gclog.debug,
          "[{}] segment {} self compaction result: {}",
          config().ntp(),
          seg,
          result);
        has_self_compacted = true;
    }

    // Remove any of the segments that have already been cleanly compacted. They
    // would be no-ops to compact.
    while (!segs.empty()) {
        if (segs.back()->has_clean_compact_timestamp()) {
            segs.pop_back();
        } else {
            break;
        }
    }

    // Remove any of the beginning segments that don't have any
    // compactible records. They would be no-ops to compact.
    while (!segs.empty()) {
        if (segs.front()->may_have_compactible_records()) {
            break;
        }
        // For all intents and purposes, these segments are already cleanly
        // compacted.
        auto seg = segs.front();
        bool marked_as_cleanly_compacted
          = co_await internal::mark_segment_as_finished_window_compaction(
            seg, true, *_probe);
        if (marked_as_cleanly_compacted) {
            subtract_dirty_segment_bytes(seg->size_bytes());
        }
        segs.pop_front();
    }
    if (segs.empty()) {
        vlog(
          gclog.debug,
          "[{}] no segments left in sliding window to compact (all segments "
          "were already cleanly compacted, or did not have any compactible "
          "records)",
          config().ntp());
        co_return has_self_compacted;
    }
    vlog(
      gclog.info,
      "[{}] window compacting {} segments in interval [{}, {}]",
      config().ntp(),
      segs.size(),
      segs.front()->filename(),
      segs.back()->filename());

    // TODO: add configuration to use simple_key_offset_map.
    std::unique_ptr<compaction::simple_key_offset_map> simple_map;
    if (cfg.hash_key_map) {
        co_await cfg.hash_key_map->reset();
    } else {
        simple_map = std::make_unique<compaction::simple_key_offset_map>(
          cfg.key_offset_map_max_keys);
    }
    compaction::key_offset_map& map
      = cfg.hash_key_map
          ? dynamic_cast<compaction::key_offset_map&>(*cfg.hash_key_map)
          : dynamic_cast<compaction::key_offset_map&>(*simple_map);
    model::offset idx_start_offset;
    bool needs_chunked_sliding_window_compact = false;
    try {
        idx_start_offset = co_await build_offset_map(
          cfg,
          segs,
          _stm_manager,
          _manager.resources(),
          *_probe,
          map,
          _feature_table);
    } catch (const zero_segments_indexed_exception&) {
        // We failed to index even one segment (the last entry of the segs set).
        // Perform chunked compaction on it.
        needs_chunked_sliding_window_compact = true;
    } catch (...) {
        auto eptr = std::current_exception();
        if (ssx::is_shutdown_exception(eptr)) {
            // Pass through shutdown errors.
            std::rethrow_exception(eptr);
        }
        vlog(
          gclog.warn,
          "[{}] failed to build offset map. Stopping compaction: {}",
          config().ntp(),
          std::current_exception());
        // Reset the sliding window start offset so that compaction may still
        // make progress in the future. Otherwise, we will fail to build the
        // offset map for the same segment over and over.
        _last_compaction_window_start_offset.reset();
        co_return false;
    }

    if (needs_chunked_sliding_window_compact) {
        bool did_compact = false;
        try {
            did_compact = co_await chunked_sliding_window_compact(
              cfg, segs, map);
        } catch (...) {
            vlog(
              gclog.debug,
              "[{}] failed to perform chunked sliding window compaction. "
              "Stopping compaction: {}",
              config().ntp(),
              std::current_exception());
        }
        co_return did_compact;
    }

    vlog(
      gclog.debug,
      "[{}] built offset map with {} keys (max allowed {}), min segment fully "
      "indexed base offset: {}, max indexed key offset: {}",
      config().ntp(),
      map.size(),
      map.capacity(),
      idx_start_offset,
      map.max_offset());

    std::optional<model::offset> next_window_start_offset = idx_start_offset;
    if (idx_start_offset == segs.front()->offsets().get_base_offset()) {
        // We have cleanly compacted up to the first segment in the sliding
        // range (not necessarily equivalent to the first segment in the log-
        // segments may have been removed from the sliding range if they were
        // already cleanly compacted or had no compactible offsets). Reset the
        // start offset to allow new segments into the sliding window range.
        vlog(
          gclog.debug,
          "[{}] fully de-duplicated up to start of sliding range with offset "
          "{}, resetting sliding window start offset",
          config().ntp(),
          idx_start_offset);
        _probe->add_sliding_window_round_complete();
        next_window_start_offset.reset();
    }

    auto segment_modify_lock = co_await _segment_rewrite_lock.get_units();
    for (auto& seg : segs) {
        if (cfg.asrc) {
            cfg.asrc->check();
        }

        if (seg->is_closed()) {
            // We may have raced with a prefix truncation while waiting for the
            // _segment_rewrite_lock, meaning some segments in the window may
            // have been closed. It is safe to `continue` and rewrite the other,
            // non-closed segments in the window.
            continue;
        }

        // A segment is considered "clean" if it has been fully indexed (all
        // keys are de-duplicated)
        const bool is_finished_window_compaction = true;
        const bool is_clean_compacted = seg->offsets().get_base_offset()
                                        >= idx_start_offset;

        const bool segment_needs_rewrite
          = internal::may_have_removable_tombstones(seg, cfg)
            || internal::has_removable_transaction_batches(
              seg,
              cfg,
              compaction::is_tx_batch_compaction_enabled(_feature_table))
            || co_await segment_needs_rewrite_with_offset_map(cfg, seg, map);
        if (!segment_needs_rewrite) {
            vlog(
              gclog.trace,
              "[{}] segment does not require rewrite with provided offset map: "
              "{}",
              config().ntp(),
              seg->filename());
            bool marked_as_cleanly_compacted
              = co_await internal::mark_segment_as_finished_window_compaction(
                seg, is_clean_compacted, *_probe);
            if (marked_as_cleanly_compacted) {
                subtract_dirty_segment_bytes(seg->size_bytes());
            }

            continue;
        }

        co_await rewrite_segment_with_offset_map(
          cfg, seg, map, is_finished_window_compaction, is_clean_compacted);
    }

    _last_compaction_window_start_offset = next_window_start_offset;

    co_return true;
}

std::optional<
  chunked_vector<std::pair<segment_set::iterator, segment_set::iterator>>>
disk_log_impl::find_adjacent_compaction_ranges(
  const compaction::compaction_config& cfg,
  std::optional<model::offset> new_start_offset) {
    {
        // Early return if cluster configured values effectively disables
        // adjacent merge compaction.
        auto max_ranges_size
          = config::shard_local_cfg().log_compaction_merge_max_ranges();
        if (max_ranges_size.has_value() && max_ranges_size < 1) {
            return std::nullopt;
        }

        auto max_segments_in_range
          = config::shard_local_cfg()
              .log_compaction_merge_max_segments_per_range();
        if (max_segments_in_range.has_value() && max_segments_in_range < 2) {
            return std::nullopt;
        }
    }

    if (_segs.size() < 2) {
        return std::nullopt;
    }

    // the chosen segments all need to be stable.
    // Each participating segment should individually pass the
    // compactible offset check for the compacted segment to be stable.
    // Additionally each segment should have finished self compaction.
    // This is needed by transactions because the compaction index of
    // segments with transactional batches is only populated during self
    // compaction. Not having this check would result in concatenating
    // with an empty compaction index and a data loss.
    auto unstable =
      [&cfg, new_start_offset](ss::lw_shared_ptr<segment>& seg) -> bool {
        // Don't bother considering segments below the new start offset for
        // adjacent compaction.
        if (
          new_start_offset.has_value()
          && seg->offsets().get_base_offset() < new_start_offset.value()) {
            return true;
        }
        return !seg->has_self_compact_timestamp() || seg->has_appender()
               || !seg->is_compactible(cfg);
    };

    // Ensure that the adjacent segments do not span an offset space greater
    // than the maximum value that can be represented by a uint32_t. This must
    // be enforced due to use of roaring::bitmap in the compacted_offset_list,
    // which is used to deduplicate records during self compaction and sliding
    // window compaction. Overflows in this area could lead to incorrect
    // compaction.
    auto valid_offset_range = [](
                                ss::lw_shared_ptr<segment>& first,
                                ss::lw_shared_ptr<segment>& last) -> bool {
        auto base_offset_first_seg = first->offsets().get_base_offset();
        auto dirty_offset_last_seg = last->offsets().get_dirty_offset();
        int64_t offset_delta = dirty_offset_last_seg()
                               - base_offset_first_seg();
        static constexpr int64_t u32_max = static_cast<int64_t>(
          std::numeric_limits<uint32_t>::max());
        return offset_delta <= u32_max;
    };

    chunked_vector<std::pair<segment_set::iterator, segment_set::iterator>>
      ranges;

    auto it = _segs.begin();
    size_t current_size{0};
    model::term_id current_term{(*it)->offsets().get_term()};

    std::pair<segment_set::iterator, segment_set::iterator> current_range = {
      it, it};
    while (it != _segs.end()) {
        auto& seg = *it;
        auto seg_size = seg->size_bytes();
        auto seg_term = seg->offsets().get_term();

        current_size += seg_size;

        auto num_segments_in_range = std::distance(
          current_range.first, current_range.second);

        bool term_boundary = seg_term != current_term;
        bool size_boundary = current_size
                             > _manager.config().max_compacted_segment_size();
        bool is_unstable = unstable(seg);
        auto max_segments_in_range
          = config::shard_local_cfg()
              .log_compaction_merge_max_segments_per_range();
        bool reached_max_range_size = max_segments_in_range.has_value()
                                      && num_segments_in_range
                                           >= max_segments_in_range.value();
        bool is_valid_offset_range = valid_offset_range(
          *current_range.first, seg);

        if (
          term_boundary || size_boundary || reached_max_range_size
          || is_unstable || !is_valid_offset_range) {
            if (num_segments_in_range > 1) {
                ranges.push_back(current_range);
                auto max_ranges_size
                  = config::shard_local_cfg().log_compaction_merge_max_ranges();
                if (
                  max_ranges_size.has_value()
                  && ranges.size() >= max_ranges_size.value()) {
                    return ranges;
                }
            }

            auto next_it = is_unstable ? std::next(it) : it;
            auto next_size = is_unstable ? 0 : seg_size;
            auto next_term = next_it != _segs.end()
                               ? (*next_it)->offsets().get_term()
                               : model::term_id{};
            current_range = {next_it, next_it};
            current_size = next_size;
            current_term = next_term;

            if (!is_unstable) {
                ++current_range.second;
            }

        } else {
            ++current_range.second;
        }
        ++it;
    }

    if (std::distance(current_range.first, current_range.second) > 1) {
        ranges.push_back(current_range);
    }

    if (ranges.empty()) {
        return std::nullopt;
    }

    return ranges;
}

ss::future<std::optional<chunked_vector<compaction_result>>>
disk_log_impl::compact_adjacent_segment_ranges(
  compaction::compaction_config cfg,
  std::optional<model::offset> new_start_offset) {
    chunked_vector<compaction_result> rs;
    if (auto ranges = find_adjacent_compaction_ranges(cfg, new_start_offset);
        ranges) {
        // lightweight copy of segments in all of the found ranges. once a
        // scheduling event occurs in this method we can't rely on the iterators
        // in the range remaining valid. for example, a concurrent truncate may
        // erase an element from the range. The act of compacting adjacent
        // segments will also invalidate iterators pointing to the original
        // _segs vector.
        using vec_t = chunked_vector<ss::lw_shared_ptr<segment>>;
        chunked_vector<vec_t> seg_ranges;
        seg_ranges.reserve(ranges->size());
        for (auto& range : *ranges) {
            seg_ranges.emplace_back(range.first, range.second);
        }
        for (auto& range : seg_ranges) {
            if (cfg.asrc) {
                cfg.asrc->check();
            }

            auto r = co_await do_compact_adjacent_segments(range, cfg);
            vlog(
              gclog.debug,
              "Adjacent segments of {}, compaction result: {}",
              config().ntp(),
              r);
            if (r.did_compact()) {
                _compaction_ratio.update(r.compaction_ratio());
            }
            rs.push_back(r);
        }
    } else {
        vlog(
          gclog.debug,
          "Adjacent segments of {}, no adjacent ranges",
          config().ntp());
    }
    co_return rs;
}

ss::future<compaction_result> disk_log_impl::do_compact_adjacent_segments(
  chunked_vector<ss::lw_shared_ptr<segment>>& segments,
  compaction::compaction_config cfg) {
    // This shouldn't be the case for any ranges returned from
    // find_adjacent_compaction_ranges(), but it is checked regardless.
    if (segments.size() < 2) {
        const auto total_size = std::accumulate(
          segments.begin(),
          segments.end(),
          size_t(0),
          [](size_t acc, ss::lw_shared_ptr<segment>& seg) {
              return acc + seg->size_bytes();
          });
        co_return compaction_result{total_size};
    }

    const bool all_segments_self_compacted = std::ranges::all_of(
      segments, &segment::has_self_compact_timestamp);

    const bool all_segments_cleanly_compacted = std::ranges::all_of(
      segments, &segment::has_clean_compact_timestamp);

    // In the case we are merging a cleanly compacted segment with a non-cleanly
    // compacted segment, we need to consider the bytes of the cleanly compacted
    // segment "dirty" again, since they will be deducted once the replacement
    // segment has finally been marked clean.
    ssize_t clean_turning_dirty_bytes = 0;
    if (!all_segments_cleanly_compacted) {
        for (const auto& seg : segments) {
            if (seg->has_clean_compact_timestamp()) {
                clean_turning_dirty_bytes += seg->size_bytes();
            }
        }
    }

    if (unlikely(!all_segments_self_compacted)) {
        const auto total_size = std::accumulate(
          segments.begin(),
          segments.end(),
          size_t(0),
          [](size_t acc, ss::lw_shared_ptr<segment>& seg) {
              return acc + seg->size_bytes();
          });
        co_return compaction_result{total_size};
    }

    vlog(
      gclog.info,
      "Compacting {} adjacent segments over range: [{}:{}]",
      segments.size(),
      segments.front()->filename(),
      segments.back()->filename());

    // Important that we take this lock _before_ any segment locks.
    auto segment_modify_lock = co_await _segment_rewrite_lock.get_units();

    // the segment which will be expanded to replace
    auto target = segments.front();

    std::optional<compaction_result> opt_ret;
    try {
        opt_ret
          = co_await storage::internal::concatenate_and_rebuild_target_segment(
            target,
            segments,
            _stm_manager,
            cfg,
            *_probe,
            *_readers_cache,
            _manager.resources(),
            _feature_table,
            _segment_rewrite_lock);
    } catch (const generation_id_mismatch_exception& e) {
        // Early abort
        vlog(gclog.info, "{}", e.what());
        const auto total_size = std::accumulate(
          segments.begin(),
          segments.end(),
          size_t(0),
          [](size_t acc, ss::lw_shared_ptr<segment>& seg) {
              return acc + seg->size_bytes();
          });
        co_return compaction_result{total_size};
    }

    // This is guaranteed to have a value.
    auto ret = *opt_ret;

    // remove the now redundant segments, if they haven't already been removed.
    // this could occur if racing with functions like truncate which manipulate
    // the segment set before acquiring segment locks. this also means that the
    // input iterator range may not longer be valid so we must manually search
    // the segment set.
    for (auto seg_it = std::next(segments.begin()); seg_it != segments.end();
         ++seg_it) {
        auto segment_to_remove = *seg_it;

        auto it = std::find(_segs.begin(), _segs.end(), segment_to_remove);
        if (it != _segs.end()) {
            // We are going to remove one of the segments, but its bytes have
            // not technically been removed- just moved to the new segment,
            // `target`. remove_segment_permanently() will remove bytes, so to
            // make sure the book-keeping stays correct, we must first add them
            // here to ensure a net zero change post adjacent merge.
            add_segment_bytes(
              segment_to_remove, segment_to_remove->size_bytes());

            _segs.erase(it, std::next(it));
            co_await remove_segment_permanently(
              segment_to_remove, "compact_adjacent_segments");
        }
    }

    _probe->add_adjacent_segments_compacted(segments.size() - 1);

    add_dirty_segment_bytes(clean_turning_dirty_bytes);
    const ssize_t removed_bytes = ssize_t(ret.size_before)
                                  - ssize_t(ret.size_after);
    subtract_segment_bytes(target, removed_bytes);

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

gc_config
disk_log_impl::maybe_apply_local_storage_overrides(gc_config cfg) const {
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
     * NOTE: both retention_local_strict and retention_local_strict_override
     * need to be set to true to honor strict local retention. Otherwise, local
     * retention is advisory.
     */
    bool strict_local_retention
      = config::shard_local_cfg().retention_local_strict()
        && config::shard_local_cfg().retention_local_strict_override();
    if (!strict_local_retention) {
        vlog(
          gclog.trace,
          "[{}] Skipped local retention override for topic with remote write "
          "enabled: {}",
          config().ntp(),
          cfg);
        return cfg;
    }

    cfg = apply_local_storage_overrides(cfg);

    vlog(
      gclog.trace,
      "[{}] Overrode retention for topic with remote write enabled: {}",
      config().ntp(),
      cfg);

    return cfg;
}

gc_config disk_log_impl::apply_local_storage_overrides(gc_config cfg) const {
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

    if (!local_retention_ms.is_engaged()) {
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
gc_config
disk_log_impl::apply_kafka_retention_overrides(gc_config defaults) const {
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
    auto ret = apply_kafka_retention_overrides(defaults);
    return maybe_apply_local_storage_overrides(ret);
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
     * Compaction. Could factor out into a public interface like gc/retention if
     * there is a need to run it separately.
     */
    if (config().is_locally_compacted() && !_segs.empty()) {
        scoped_file_tracker::set_t leftovers;
        cfg.compact.files_to_cleanup = &leftovers;
        auto fut = co_await ss::coroutine::as_future(
          do_compact(cfg.compact, new_start_offset));

        while (!leftovers.empty()) {
            auto first_leftover = leftovers.begin();
            auto file = first_leftover->string();
            vlog(gclog.debug, "Cleaning up leftover file: {}", file);
            try {
                if (co_await ss::file_exists(file)) {
                    co_await ss::remove_file(file);
                }
            } catch (...) {
                vlog(
                  gclog.warn,
                  "Error while cleaning up {} after aborted compaction: {}",
                  file,
                  std::current_exception());
            }
            leftovers.erase(first_leftover);
        }
        if (fut.failed()) {
            std::rethrow_exception(fut.get_exception());
        }
    }

    _probe->set_compaction_ratio(_compaction_ratio.get());
}

ss::future<> disk_log_impl::do_compact(
  compaction::compaction_config compact_cfg,
  std::optional<model::offset> new_start_offset) {
    compact_cfg.asrc = &_compaction_as;

    auto compact_fut = ss::now();

    auto use_adjacent_merge
      = !config::shard_local_cfg().log_compaction_use_sliding_window()
        || config::shard_local_cfg().log_compaction_pause_use_sliding_window()
        || (compact_cfg.hash_key_map && compact_cfg.hash_key_map->capacity() == 0);

    if (use_adjacent_merge) {
        // Make a copy of `_segs` to avoid iterator invalidation in
        // `adjacent_merge_compact`.
        compact_fut = adjacent_merge_compact(
          _segs.copy(), compact_cfg, new_start_offset);
    } else {
        compact_fut = sliding_window_compact(compact_cfg, new_start_offset)
                        .then([&](bool compacted) {
                            vlog(
                              gclog.info,
                              "Sliding compaction of {} did {}compact "
                              "data, proceeding to adjacent "
                              "segment compaction",
                              config().ntp(),
                              compacted ? "" : "not ");
                            return compact_adjacent_segment_ranges(
                                     compact_cfg, new_start_offset)
                              .discard_result();
                        });
    }

    auto compact_result = co_await ss::coroutine::as_future(
      std::move(compact_fut));
    if (compact_result.failed()) {
        auto eptr = compact_result.get_exception();
        if (ssx::is_shutdown_exception(eptr)) {
            vlog(
              gclog.debug,
              "Compaction of {} stopped because of shutdown",
              config().ntp());
            co_return;
        }
        std::rethrow_exception(eptr);
    }
}

ss::future<bool> disk_log_impl::chunked_sliding_window_compact(
  const compaction::compaction_config& compact_cfg,
  const segment_set& segs,
  compaction::key_offset_map& map) {
    // The last segment in the segment set is the first segment we attempted and
    // failed to index (the "unindexed" segment)
    auto seg = segs.back();
    vlog(
      gclog.info,
      "Unable to fully index segment with max allowed keys {}. Performing "
      "chunked sliding window compaction for segment {}",
      map.capacity(),
      seg);

    auto last_indexed_offset = model::offset{};
    bool segment_fully_indexed = false;

    // Hold the segment rewrite lock for the entirety of the chunked compaction
    // routine. index_chunk_of_segment_for_map() will repeatedly obtain and
    // release a seg->read_lock(), and rewriting of each segment in the window
    // happens multiple times in the below while() loop
    auto segment_modify_lock = co_await _segment_rewrite_lock.get_units();

    if (seg->is_closed()) {
        // We may have raced with a prefix truncation while waiting for
        // the _segment_rewrite_lock. Do not proceed with chunked compaction on
        // `seg`.
        co_return false;
    }

    // The key offset map will be repeatedly built from "chunks" of the
    // unindexed segment and used to rewrite the segments in the sliding window
    // until the entirety of the unindexed segment has been indexed
    while (!segment_fully_indexed) {
        if (compact_cfg.asrc) {
            compact_cfg.asrc->check();
        }

        // Build up the key offset map for a chunk of the unindexed segment,
        // starting from the last_indexed_offset
        segment_fully_indexed = co_await index_chunk_of_segment_for_map(
          compact_cfg, seg, map, *_probe, std::ref(last_indexed_offset));

        // Deduplicate all of the segments in the sliding window using the key
        // offset map built from the current chunk of the unindexed segment
        for (auto& s : segs) {
            if (compact_cfg.asrc) {
                compact_cfg.asrc->check();
            }

            if (s->is_closed()) {
                // We may have raced with a prefix truncation while waiting for
                // the _segment_rewrite_lock, meaning some segments in the
                // window may have been closed. It is safe to `continue` and
                // rewrite the other, non-closed segments in the window.
                continue;
            }

            const bool segment_needs_rewrite
              = internal::may_have_removable_tombstones(s, compact_cfg)
                || internal::has_removable_transaction_batches(
                  s,
                  compact_cfg,
                  compaction::is_tx_batch_compaction_enabled(_feature_table))
                || co_await segment_needs_rewrite_with_offset_map(
                  compact_cfg, s, map);
            if (!segment_needs_rewrite) {
                vlog(
                  gclog.trace,
                  "[{}] segment does not require rewrite with provided offset "
                  "map: {}",
                  config().ntp(),
                  seg->filename());
                continue;
            }

            // Neither of these flags are true until chunked compaction is
            // complete
            static const bool is_finished_window_compaction = false;
            static const bool is_clean_compacted = false;
            co_await rewrite_segment_with_offset_map(
              compact_cfg,
              s,
              map,
              is_finished_window_compaction,
              is_clean_compacted);
        }
    }

    // Segments can now be marked as finished window compaction
    for (auto& s : segs) {
        std::ignore
          = co_await internal::mark_segment_as_finished_window_compaction(
            s, false, *_probe);
    }

    // We can also mark the last segment as cleanly compacted now.
    bool marked_as_cleanly_compacted
      = co_await internal::mark_segment_as_finished_window_compaction(
        seg, true, *_probe);

    // This should always be true - the unindexed segment would not have been
    // cleanly compacted to begin with.
    if (marked_as_cleanly_compacted) {
        subtract_dirty_segment_bytes(seg->size_bytes());
    }

    _last_compaction_window_start_offset = seg->offsets().get_base_offset();
    _probe->add_chunked_compaction_run();

    // It is possible that chunked compaction on the last segment in the set
    // resulted in all of the segments with compactible records being cleanly
    // compacted. In this case, we need to reset the
    // _last_compaction_window_start_offset.
    const bool sliding_window_round_complete = std::ranges::all_of(
      segs, [](const auto& s) {
          if (!s->may_have_compactible_records()) {
              return true;
          }
          return s->has_clean_compact_timestamp();
      });

    if (sliding_window_round_complete) {
        _probe->add_sliding_window_round_complete();
        _last_compaction_window_start_offset.reset();
    }

    vlog(
      gclog.trace,
      "Finished chunked sliding window compaction for segment {}",
      seg);

    co_return true;
}

ss::future<> disk_log_impl::rewrite_segment_with_offset_map(
  const compaction::compaction_config& cfg,
  ss::lw_shared_ptr<segment> seg,
  compaction::key_offset_map& map,
  bool is_finished_window_compaction,
  bool is_clean_compacted) {
    if (seg->offsets().get_base_offset() > map.max_offset()) {
        // The map was built from newest to oldest segments within this
        // sliding range. If we see a new segment whose offsets are all
        // higher than those indexed, it may be because the segment is
        // entirely comprised of non-data batches. Mark it as compacted so
        // we can progress through compactions.
        bool marked_as_cleanly_compacted
          = co_await internal::mark_segment_as_finished_window_compaction(
            seg, is_clean_compacted, *_probe);
        if (marked_as_cleanly_compacted) {
            subtract_dirty_segment_bytes(seg->size_bytes());
        }

        vlog(
          gclog.trace,
          "[{}] treating segment as compacted, offsets fall above highest "
          "indexed key {}, likely because they are non-data batches: {}",
          config().ntp(),
          map.max_offset(),
          seg->filename());
        co_return;
    }
    if (!seg->may_have_compactible_records()) {
        // All data records are already compacted away. Skip to avoid a
        // needless rewrite.
        bool marked_as_cleanly_compacted
          = co_await internal::mark_segment_as_finished_window_compaction(
            seg, is_clean_compacted, *_probe);
        if (marked_as_cleanly_compacted) {
            subtract_dirty_segment_bytes(seg->size_bytes());
        }

        vlog(
          gclog.trace,
          "[{}] treating segment as compacted, either all non-data "
          "records or the only record is a data record: {}",
          config().ntp(),
          seg->filename());
        co_return;
    }

    // TODO: implement a segment replacement strategy such that each term
    // tries to write only one segment (or more if the term had a large
    // amount of data), rather than replacing N segments with N segments.
    const auto tmpname = seg->reader().path().to_compaction_staging();
    const auto cmp_idx_tmpname = tmpname.to_compacted_index();
    auto staging_to_clean = scoped_file_tracker{
      cfg.files_to_cleanup, {tmpname, cmp_idx_tmpname}};

    const auto size_before = seg->size_bytes();
    auto appender = co_await internal::make_segment_appender(
      tmpname, size_before, resources(), cfg.sanitizer_config);

    auto cmp_idx_name = seg->path().to_compacted_index();
    auto compacted_idx_writer = make_file_backed_compacted_index(
      cmp_idx_tmpname, true, resources(), cfg.sanitizer_config);

    vlog(
      gclog.debug,
      "[{}] Deduplicating data from segment {} to {}: {}",
      config().ntp(),
      seg->path(),
      tmpname,
      seg);
    auto initial_generation_id = seg->get_generation_id();
    std::exception_ptr eptr;
    index_state new_idx;
    try {
        new_idx = co_await deduplicate_segment(
          cfg,
          map,
          seg,
          *appender,
          *compacted_idx_writer,
          _stm_manager,
          *_probe,
          storage::internal::should_apply_delta_time_offset(_feature_table),
          _feature_table);
    } catch (...) {
        eptr = std::current_exception();
    }
    // We must close the segment apender
    co_await compacted_idx_writer->close();
    co_await appender->close();
    if (eptr) {
        std::rethrow_exception(eptr);
    }

    vlog(
      gclog.debug,
      "[{}] Replacing segment {} with {}",
      config().ntp(),
      seg->path(),
      tmpname);

    auto rdr_holder = co_await _readers_cache->evict_segment_readers(seg);
    auto write_lock = co_await seg->write_lock();
    if (initial_generation_id != seg->get_generation_id()) {
        throw std::runtime_error(
          fmt::format(
            "Aborting compaction of segment: {}, segment was mutated "
            "while compacting",
            seg->path()));
    }
    if (seg->is_closed()) {
        throw segment_closed_exception();
    }
    const auto size_after = appender->file_byte_offset();

    // Clear our indexes before swapping the data files (note, the new
    // compaction index was opened with the truncate option above).
    co_await seg->index().drop_all_data();

    // Rename the data file.
    co_await internal::do_swap_data_file_handles(
      tmpname, seg, cfg, *_probe, compacted_idx_writer->size_bytes());

    // Persist the state of our indexes in their new names.
    seg->index().swap_index_state(std::move(new_idx));
    seg->force_set_commit_offset_from_index();
    co_await seg->reset_batch_cache_index();

    const ssize_t removed_bytes = ssize_t(size_before) - ssize_t(size_after);

    // We must deduct the removed bytes from the dirty segment bytes.
    // However, if the segment is marked as cleanly compacted below, the
    // entire seg size (before compaction) will instead be removed from dirty
    // segment bytes.
    ssize_t dirty_removed_bytes = seg->has_clean_compact_timestamp()
                                    ? 0
                                    : removed_bytes;

    if (is_finished_window_compaction) {
        // Mark the segment as completed window compaction, and possibly set the
        // clean_compact_timestamp in it's index.
        bool marked_as_cleanly_compacted
          = co_await internal::mark_segment_as_finished_window_compaction(
            seg, is_clean_compacted, *_probe);
        if (marked_as_cleanly_compacted) {
            dirty_removed_bytes = ssize_t(size_before);
        }
    }

    subtract_dirty_segment_bytes(dirty_removed_bytes);
    subtract_closed_segment_bytes(removed_bytes);

    co_await seg->index().flush();
    co_await ss::rename_file(cmp_idx_tmpname.string(), cmp_idx_name.string());
    _probe->segment_compacted();
    _probe->add_compaction_removed_bytes(removed_bytes);

    compaction_result res(size_before, size_after);
    _compaction_ratio.update(res.compaction_ratio());
    seg->advance_generation();
    staging_to_clean.clear();
    vlog(gclog.debug, "[{}] Final compacted segment {}", config().ntp(), seg);
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

        if (!is_cloud_retention_active()) {
            vlog(
              gclog.warn,
              "[{}] expected remote retention to be active",
              config().ntp());
            co_return std::nullopt;
        }

        vlog(
          gclog.info,
          "[{}] applying 'deletion' log cleanup with remote retention override "
          "offset {} and config {}",
          config().ntp(),
          offset,
          cfg);

        co_return request_eviction_until_offset(offset);
    }

    if (!config().is_locally_collectable()) {
        co_return std::nullopt;
    }

    vlog(
      gclog.trace,
      "[{}] applying 'deletion' log cleanup policy with config: {}",
      config().ntp(),
      cfg);

    auto max_offset = co_await maybe_adjusted_retention_offset(cfg);

    if (max_offset) {
        co_return request_eviction_until_offset(*max_offset);
    }

    co_return std::nullopt;
}

ss::future<> disk_log_impl::maybe_adjust_retention_timestamps() {
    auto validated_timestamps = _feature_table.local().is_active(
      features::feature::validated_batch_timestamps);
    if (validated_timestamps) {
        // Legacy behavior of adjusting retention timestamps is only needed in
        // older clusters that didn't validate timestamps.
        co_return;
    }
    // Correct any timestamps too far in the future, meant to be called before
    // calculating the retention offset for garbage collection.
    // It's expected that this will be used only for segments pre v23.3,
    // without a proper broker_timestamps
    auto ignore_in_future
      = config::shard_local_cfg().storage_ignore_timestamps_in_future_sec();
    if (!ignore_in_future.has_value()) {
        co_return;
    }
    auto ignore_threshold = model::timestamp(
      model::timestamp::now().value() + ignore_in_future.value() / 1ms);

    auto retention_cfg = time_based_retention_cfg::make(
      _feature_table.local()); // this will retrieve cluster cfgs

    chunked_vector<segment_set::type> segs_all_bogus;
    for (const auto& s : _segs) {
        auto max_ts = s->index().retention_timestamp(retention_cfg);

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
        auto max_ts = s->index().retention_timestamp(retention_cfg);
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
    co_await maybe_adjust_retention_timestamps();
    co_return retention_offset(cfg);
}

std::optional<model::offset>
disk_log_impl::retention_offset(gc_config cfg) const {
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
    return _segs.back()->offsets().get_term();
}

bool disk_log_impl::is_new_log() const {
    static constexpr model::offset not_initialized{};
    const auto os = offsets();
    return segment_count() == 0 && os.dirty_offset == not_initialized
           && os.start_offset == not_initialized;
}

model::offset_delta disk_log_impl::offset_delta(model::offset o) const {
    return model::offset_delta{_offset_translator.state()->delta(o)};
}

model::offset disk_log_impl::from_log_offset(model::offset log_offset) const {
    return _offset_translator.state()->from_log_offset(log_offset);
}

model::offset disk_log_impl::to_log_offset(model::offset data_offset) const {
    return _offset_translator.state()->to_log_offset(data_offset);
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

    auto it = std::find_if(
      _segs.rbegin(), _segs.rend(), [](const segment_set::type& s) {
          return !s->empty();
      });

    if (it == _segs.rend()) {
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
    const auto& eof = (*it)->offsets();

    const auto start_offset = _start_offset() >= 0 ? _start_offset
                                                   : bof.get_base_offset();

    return storage::offset_stats{
      .start_offset = start_offset,

      .committed_offset = eof.get_committed_offset(),
      .committed_offset_term = eof.get_term(),

      .dirty_offset = eof.get_dirty_offset(),
      .dirty_offset_term = eof.get_term(),
    };
}

model::offset disk_log_impl::find_last_term_start_offset() const {
    if (_segs.empty()) {
        return {};
    }

    segment_set::type end;
    segment_set::type term_start;
    for (auto it = _segs.rbegin(); it != _segs.rend(); ++it) {
        auto& seg = *it;
        if (!seg->empty()) {
            if (!end) {
                end = seg;
            }
            // find term start offset
            if (seg->offsets().get_term() < end->offsets().get_term()) {
                break;
            }
            term_start = seg;
        }
    }

    if (!end) {
        return {};
    }

    return term_start->offsets().get_base_offset();
}

model::timestamp disk_log_impl::start_timestamp() const {
    if (_segs.empty()) {
        return model::timestamp{};
    }

    const auto start_offset = _start_offset >= model::offset{0}
                                ? _start_offset
                                : _segs.front()->offsets().get_base_offset();

    auto seg = _segs.lower_bound(start_offset);
    if (seg == _segs.end()) {
        return model::timestamp{};
    }

    return (*seg)->index().base_timestamp();
}

ss::future<> disk_log_impl::new_segment(model::offset o, model::term_id t) {
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
        config::shard_local_cfg().storage_read_buffer_size(),
        config::shard_local_cfg().storage_read_readahead_count(),
        _max_segment_size)
      .then([this](ss::lw_shared_ptr<segment> handles) mutable {
          return remove_empty_segments().then(
            [this, h = std::move(handles)]() mutable {
                throw_if_closed();
                if (config().is_locally_compacted()) {
                    h->mark_as_compacted_segment();
                }
                _segs.add(std::move(h));
                _probe->segment_created();
                _stm_manager->make_snapshot_in_background();
                _stm_dirty_bytes_units.return_all();
            });
      });
}
namespace {
model::offset get_next_append_offset(const offset_stats& offsets) {
    /**
     * When dirty_offset < start_offset, the log is empty. In this case we use
     * the start offset as the next offset for appender
     */
    if (offsets.dirty_offset < offsets.start_offset) {
        return offsets.start_offset;
    }
    // otherwise next batch will be appended at dirty_offset + 1
    return model::next_offset(offsets.dirty_offset);
}
} // namespace

// config timeout is for the one calling reader consumer
log_appender disk_log_impl::make_appender(log_append_config cfg) {
    throw_if_closed();
    auto now = log_clock::now();
    auto ofs = offsets();
    model::offset next_offset = get_next_append_offset(ofs);

    vlog(
      stlog.trace,
      "creating log appender for: {}, next offset: {}, log offsets: {}",
      config().ntp(),
      next_offset,
      ofs);
    return log_appender(
      std::make_unique<disk_log_appender>(*this, cfg, now, next_offset));
}

ss::future<> disk_log_impl::flush() {
    throw_if_closed();
    if (_segs.empty()) {
        return ss::make_ready_future<>();
    }
    vlog(
      stlog.trace, "flush on segment with offsets {}", _segs.back()->offsets());
    return _segs.back()->flush();
}

size_t disk_log_impl::max_segment_size() const {
    // override for segment size
    size_t result;
    if (config().has_overrides() && config().get_overrides().segment_size) {
        result = *config().get_overrides().segment_size;
    } else {
        // no overrides use defaults
        result = config().is_locally_compacted()
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
    if (_segs.empty()) {
        return 0;
    }
    uint64_t size = 0;
    for (auto it = _segs.rbegin(); it != _segs.rend(); ++it) {
        auto& seg = *it;
        auto& offsets = seg->offsets();
        if (offsets.get_base_offset() < o) {
            if (offsets.get_dirty_offset() <= o) {
                return size;
            }
            // use an index to find the approximate size of the segment data
            // that are followed by the requested offset.
            //
            // NOTE: this is an approximation with the accuracy of the index
            // entry buffer size which is currently equal to 32KiB.
            auto entry = seg->index().find_nearest(o);
            if (entry) {
                size += seg->size_bytes() - entry->filepos;
            }
            return size;
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

void disk_log_impl::bg_checkpoint_offset_translator() {
    ssx::spawn_with_gate(_compaction_housekeeping_gate, [this] {
        return _offset_translator.maybe_checkpoint();
    });
}

ss::future<> disk_log_impl::force_roll() {
    auto roll_lock_holder = co_await _segments_rolling_lock.get_units();
    auto t = term();
    auto next_offset = model::next_offset(offsets().dirty_offset);
    vlog(
      stlog.debug,
      "{} force_roll: new segment start offset {}",
      config().ntp(),
      next_offset);
    if (_segs.empty()) {
        co_return co_await new_segment(next_offset, t);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender()) {
        co_return co_await new_segment(next_offset, t);
    }

    add_segment_bytes(ptr, ptr->size_bytes());
    co_return co_await ptr->release_appender(_readers_cache.get())
      .then([this, next_offset, t] { return new_segment(next_offset, t); });
}

ss::future<> disk_log_impl::maybe_roll_unlocked(
  model::term_id t, model::offset next_offset) {
    vassert(
      !_segments_rolling_lock.ready(),
      "Must have taken _segments_rolling_lock");

    vassert(t >= term(), "Term:{} must be greater than base:{}", t, term());
    if (_segs.empty()) {
        co_return co_await new_segment(next_offset, t);
    }
    auto ptr = _segs.back();
    if (!ptr->has_appender() || ptr->is_tombstone()) {
        co_return co_await new_segment(next_offset, t);
    }
    bool size_should_roll = false;

    if (ptr->appender().file_byte_offset() >= _max_segment_size) {
        size_should_roll = true;
    }
    if (t != term() || size_should_roll) {
        add_segment_bytes(ptr, ptr->size_bytes());
        co_await ptr->release_appender(_readers_cache.get());
        co_await new_segment(next_offset, t);
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

    // Find the max allowable age of a segment. There are two factors:
    // 1. `segment.ms` configuration.
    // 2. For a compacted topic, `max.compaction.lag.ms` configuration,
    //    because this value is the maximum amount of time a segment
    //    can be ineligible for compaction, and an active segment is
    //    ineligible.
    std::optional<std::chrono::milliseconds> max_age;
    auto seg_ms = config().segment_ms();
    auto& local_config = config::shard_local_cfg();
    static constexpr auto rate_limit = std::chrono::minutes(5);
    thread_local static ss::logger::rate_limit rate(rate_limit);
    if (seg_ms.has_value()) {
        // Clamp user provided value with (hopefully sane) server min and max
        // values, this should protect against overflow UB.
        const auto clamped_seg_ms = std::clamp(
          seg_ms.value(),
          local_config.log_segment_ms_min(),
          local_config.log_segment_ms_max());

        if (seg_ms < clamped_seg_ms) {
            stlog.log(
              ss::log_level::warn,
              rate,
              "Configured segment.ms={} is lower than the configured cluster "
              "bound {}={}",
              seg_ms.value(),
              local_config.log_segment_ms_min.name(),
              local_config.log_segment_ms_min());
        }

        if (seg_ms > clamped_seg_ms) {
            stlog.log(
              ss::log_level::warn,
              rate,
              "Configured segment.ms={} is higher than the configured cluster "
              "bound {}={}",
              seg_ms.value(),
              local_config.log_segment_ms_max.name(),
              local_config.log_segment_ms_max());
        }

        max_age = clamped_seg_ms;
    }

    // Take max compaction lag into account if the topic is compacted.
    // Skip if the configuration is the default, which is effectively
    // no max compaction lag.
    // Technically, we should roll if a message is too old based on
    // its own timestamp; however, doing this simpler thing matches
    // what Kafka does exactly.
    auto max_lag = config().max_compaction_lag_ms();
    if (
      config().is_locally_compacted()
      && max_lag != local_config.max_compaction_lag_ms.default_value()) {
        // Clamp for the same reasons.
        const auto clamped_max_lag = std::clamp(
          seg_ms.value(),
          local_config.log_segment_ms_min(),
          local_config.log_segment_ms_max());
        max_age = max_age.has_value()
                    ? std::min(max_age.value(), clamped_max_lag)
                    : clamped_max_lag;

        // If we're applying max_lag to roll segments, that's likely a
        // misconfiguration, and it's definitely worth noting.
        if (max_age == max_lag) {
            stlog.log(
              ss::log_level::warn,
              rate,
              "Segment roll controlled by max.compaction.lag.ms ({}): this is "
              "likely a configuration problem",
              max_lag);
        }
    }

    if (!max_age.has_value()) {
        // No time-based rolling.
        co_return;
    }

    if (first_write_ts.value() + max_age.value() > ss::lowres_clock::now()) {
        // skip, time hasn't expired
        co_return;
    }

    add_segment_bytes(last, last->size_bytes());
    co_await last->release_appender(_readers_cache.get());
    auto offsets = last->offsets();
    auto new_so = model::next_offset(offsets.get_committed_offset());
    co_await new_segment(new_so, offsets.get_term());
    vlog(
      stlog.trace,
      "{} segment rolled, new segment start offset: {}",
      config().ntp(),
      new_so);
}

ss::future<model::record_batch_reader>
disk_log_impl::make_unchecked_reader(local_log_reader_config config) {
    throw_if_closed();
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), cfg, *_probe, get_offset_translator_state());
      });
}

ss::future<model::record_batch_reader>
disk_log_impl::make_cached_reader(local_log_reader_config config) {
    throw_if_closed();

    auto rdr = _readers_cache->get_reader(config);
    if (rdr) {
        return ss::make_ready_future<model::record_batch_reader>(
          std::move(*rdr));
    }
    return _lock_mngr.range_lock(config)
      .then([this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          return std::make_unique<log_reader>(
            std::move(lease), cfg, *_probe, get_offset_translator_state());
      })
      .then([this](auto rdr) { return _readers_cache->put(std::move(rdr)); });
}

namespace details {
// This accumulator is used to compute size of the on-disk representation of the
// record batches
struct batch_size_accumulator {
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        vassert(
          result_size_bytes != nullptr && base_timestamp != nullptr
            && max_timestamp != nullptr,
          "batch_size_accumulator is not initialized properly");
        // Target is exclusive:
        // 'target' offset corresponds to the base offset of the
        // batch that shouldn't be added to the result:
        //
        //            target---v
        //     |++++++++++++++[X      ]           |
        //
        // Target is inclusive:
        // 'target' offset corresponds to the last offset of the
        // batch that should be added to the final result:
        //
        //                  target---v
        //     |++++++++++++++[+++++++]X          |
        //
        *base_timestamp = b.header().first_timestamp;
        if (boundary == boundary_type::inclusive) {
            if (b.last_offset() > target) {
                *max_timestamp = std::min(
                  *max_timestamp, b.header().max_timestamp);
                co_return ss::stop_iteration::yes;
            }
            *result_size_bytes += model::packed_record_batch_header_size
                                  + b.data().size_bytes();
            if (b.header().last_offset() == target) {
                *max_timestamp = b.header().max_timestamp;
            }
            co_return ss::stop_iteration::no;
        } else {
            if (b.base_offset() >= target) {
                co_return ss::stop_iteration::yes;
            }
            *result_size_bytes += model::packed_record_batch_header_size
                                  + b.data().size_bytes();
            *max_timestamp = b.header().max_timestamp;
            co_return ss::stop_iteration::no;
        }
    }
    bool end_of_stream() const { return false; }

    size_t* result_size_bytes{nullptr};
    model::offset target;
    boundary_type boundary;
    model::timestamp* base_timestamp{nullptr};
    model::timestamp* max_timestamp{nullptr};
};
} // namespace details

auto disk_log_impl::get_file_offset(
  ss::lw_shared_ptr<segment> s,
  std::optional<segment_index::entry> maybe_index_entry,
  model::offset target,
  boundary_type boundary) -> ss::future<file_offset_t> {
    auto index_entry = maybe_index_entry.value_or(
      segment_index::entry{
        .offset = s->offsets().get_base_offset(),
        .timestamp = s->index().base_timestamp(),
        .filepos = 0,
      });
    size_t size_bytes{index_entry.filepos};
    model::timestamp base_timestamp = index_entry.timestamp;
    model::timestamp max_timestamp = model::timestamp::max();
    details::batch_size_accumulator acc{
      .result_size_bytes = &size_bytes,
      .target = target,
      .boundary = boundary,
      .base_timestamp = &base_timestamp,
      .max_timestamp = &max_timestamp,
    };

    auto reader_start_offset = index_entry.offset;

    storage::local_log_reader_config reader_cfg(reader_start_offset, target);

    reader_cfg.skip_batch_cache = true;
    reader_cfg.skip_readers_cache = true;

    auto lock = s->try_hold_read_lock();

    if (!lock.has_value()) {
        vlog(
          stlog.info,
          "{}: Something prevented access to the segment's read lock. "
          "aborting...",
          config().ntp());
        throw ss::semaphore_timed_out();
    }

    auto reader = model::make_record_batch_reader<single_segment_reader>(
      s, std::move(lock).value(), reader_cfg, *_probe);

    try {
        co_await std::move(reader).consume(acc, model::no_timeout);
    } catch (...) {
        vlog(
          stlog.error,
          "Error detected while consuming {}",
          std::current_exception());
        throw;
    }

    co_return file_offset_t{
      .position = size_bytes,
      .base_timestamp = base_timestamp,
      .last_timestamp = max_timestamp,
    };
}

model::offset disk_log_impl::cleanly_compacted_prefix_offset() const {
    _cleanly_compacted_offset = std::max(
      _cleanly_compacted_offset, _start_offset);
    if (!_segs.empty()) {
        auto first_seg_to_check_it = _segs.lower_bound(
          _cleanly_compacted_offset);
        auto first_non_clean_seg_it = std::find_if(
          first_seg_to_check_it, _segs.end(), [](const segment_set::type& seg) {
              return !seg->index().has_clean_compact_timestamp();
          });
        if (first_non_clean_seg_it == _segs.end()) [[unlikely]] {
            // This should not happen, as we never compact the last segment.
            // If it does for any reason, underreport cleanly compacted offset
            // by the length of the last segment rather than use its max_offset
            // which may be uninitialized.
            --first_non_clean_seg_it;
        }
        _cleanly_compacted_offset
          = (*first_non_clean_seg_it)->index().base_offset();
    }
    return _cleanly_compacted_offset;
}

bool disk_log_impl::log_contains_offset(model::offset o) const noexcept {
    auto log_offsets = offsets();
    auto log_interval = model::bounded_offset_interval::optional(
      log_offsets.start_offset, log_offsets.committed_offset);

    if (!log_interval.has_value()) {
        // Log is empty
        return false;
    }

    return log_interval->contains(o);
}

bool disk_log_impl::log_contains_offset_range(
  model::offset first, model::offset last) const noexcept {
    auto log_offsets = offsets();
    auto log_interval = model::bounded_offset_interval::optional(
      log_offsets.start_offset, log_offsets.committed_offset);

    if (!log_interval.has_value()) {
        // Log is empty
        return false;
    }

    return log_interval->contains(first) && log_interval->contains(last);
}

ss::future<std::optional<log::offset_range_size_result_t>>
disk_log_impl::offset_range_size(
  model::offset first, model::offset last, ss::semaphore::time_point timeout) {
    vlog(
      stlog.debug,
      "Offset range size, first: {}, last: {}, lstat: {}",
      first,
      last,
      offsets());

    if (!log_contains_offset_range(first, last)) {
        vlog(stlog.debug, "Log does not include entire range");
        co_return std::nullopt;
    }

    // build the collection
    const auto segments = [&] {
        auto base_it = _segs.lower_bound(first);
        std::vector<ss::lw_shared_ptr<storage::segment>> segments;
        for (auto it = base_it; it != _segs.end(); it++) {
            const auto& offsets = it->get()->offsets();
            if (offsets.get_committed_offset() < first) {
                continue;
            }
            if (offsets.get_base_offset() > last) {
                break;
            }
            segments.push_back(*it);
        }
        return segments;
    }();

    // The following checks are needed to maintain the following invariants:
    // - the 'first' offset of the offset range exists and the first segment in
    //   the 'segments' collection points to it.
    // - if the log was truncated and start offset is greater than 'first' the
    //   method should return nullopt.
    // - the 'last' offset of the offset range exists and the the last segment
    //   in the 'segments' collection points to it.
    // - if the last offset of the log is less than 'last' the method throws.
    if (segments.empty()) {
        vlog(stlog.debug, "Can't find log segments to lock");
        co_return std::nullopt;
    }
    if (segments.front()->offsets().get_base_offset() > first) {
        vlog(
          stlog.debug,
          "Offset {} is out of range, {}",
          first,
          segments.front()->offsets());
        co_return std::nullopt;
    }
    if (segments.back()->offsets().get_committed_offset() < last) {
        vlog(
          stlog.debug,
          "Offset {} is out of range, {}",
          last,
          segments.back()->offsets());
        co_return std::nullopt;
    }

    std::vector<ss::future<ss::rwlock::holder>> f_locks;
    f_locks.reserve(segments.size());
    for (auto& s : segments) {
        f_locks.emplace_back(s->read_lock(timeout));
    }

    auto holders = co_await ss::when_all_succeed(
      std::begin(f_locks), std::end(f_locks));

    // Check if anything was closed after the scheduling point.
    for (const auto& s : segments) {
        if (s->is_closed()) {
            co_return std::nullopt;
        }
    }

    // Recheck log offsets as well, as we could race with a prefix truncation
    // while waiting for segment locks.
    if (!log_contains_offset_range(first, last)) {
        vlog(stlog.debug, "Log does not include entire range");
        co_return std::nullopt;
    }

    // Left subscan
    auto ix_left = segments.front()->index().find_nearest(first);
    if (ix_left.has_value()) {
        // We have found an index entry.
        vlog(
          stlog.debug,
          "{}: Scanning (left) log segment {} from the file offset {} (RP "
          "offset "
          "{} ; timestamp {})",
          config().ntp(),
          segments.front()->offsets(),
          ix_left->filepos,
          ix_left->offset,
          ix_left->timestamp);
    } else {
        // Scan from the beginning of the segment.
        vlog(
          stlog.debug,
          "{}: Scanning (left) log segment {} from the start",
          config().ntp(),
          segments.front()->offsets());
    }
    auto left_scan_offset = co_await get_file_offset(
      segments.front(), ix_left, first, boundary_type::exclusive);
    auto left_scan_bytes = left_scan_offset.position;

    // Right subscan
    auto ix_right = segments.back()->index().find_nearest(last);
    if (ix_right.has_value()) {
        vlog(
          stlog.debug,
          "{}: Scanning (right) log segment {} from the file offset {} (RP "
          "offset "
          "{} ; timestamp {})",
          config().ntp(),
          segments.back()->offsets(),
          ix_right->filepos,
          ix_right->offset,
          ix_right->timestamp);
    } else {
        // Scan from the beginning of the segment.
        vlog(
          stlog.debug,
          "{}: Scanning (right) log segment {} from the start",
          config().ntp(),
          segments.back()->offsets());
    }
    auto right_scan_offset = co_await get_file_offset(
      segments.back(), ix_right, last, boundary_type::inclusive);
    auto right_scan_bytes = right_scan_offset.position;

    // compute size
    size_t total_size = 0;
    if (segments.size() > 1) {
        size_t mid_size = 0;
        size_t ix_last = segments.size() - 1;
        size_t left_size = segments.front()->size_bytes() - left_scan_bytes;
        size_t right_size = right_scan_bytes;
        for (size_t i = 1; i < ix_last; i++) {
            mid_size += segments[i]->size_bytes();
        }
        total_size = left_size + mid_size + right_size;
        vlog(
          stlog.debug,
          "Computed size components: {}-{}-{}, total: {}, left scan bytes: {}, "
          "right scan bytes: {}, num segments: {}, "
          "offset range: {}-{}",
          left_size,
          mid_size,
          right_size,
          total_size,
          left_scan_bytes,
          right_scan_bytes,
          segments.size(),
          first,
          last);
    } else {
        // Both left and right scans were performed on the same segment.
        total_size = right_scan_bytes - left_scan_bytes;
        vlog(
          stlog.debug,
          "Computed size total: {}, left scan bytes: {}, right scan bytes: {}, "
          "num segments: {}, offset range: {}-{}",
          total_size,
          left_scan_bytes,
          right_scan_bytes,
          segments.size(),
          first,
          last);
    }
    co_return offset_range_size_result_t{
      .on_disk_size = total_size,
      .last_offset = last,
      .first_timestamp = left_scan_offset.base_timestamp,
      .last_timestamp = right_scan_offset.last_timestamp,
    };
}

ss::future<std::optional<log::offset_range_size_result_t>>
disk_log_impl::offset_range_size(
  model::offset first, offset_range_size_requirements_t target) {
    vlog(
      stlog.debug,
      "Offset range size, first: {}, target size: {}/{}, lstat: {}",
      first,
      target.target_size,
      target.min_size,
      offsets());

    if (!log_contains_offset(first)) {
        vlog(stlog.debug, "Log does not include offset {}", first);
        co_return std::nullopt;
    }

    auto base_it = _segs.lower_bound(first);

    // Invariant: 'first' offset should be present in the log. If the segment is
    // compacted it's OK if it's missing if the segment that stores the offset
    // range that includes the offset still exists.
    if (base_it == _segs.end()) {
        vlog(
          stlog.debug,
          "Offset {} is not present in the log {}",
          first,
          offsets());
        co_return std::nullopt;
    } else {
        auto lstat = base_it->get()->offsets();
        if (
          first < lstat.get_base_offset()
          || first > lstat.get_committed_offset()) {
            vlog(
              stlog.debug,
              "Offset {} is not present in the segment {}",
              first,
              lstat);
            co_return std::nullopt;
        }
    }

    file_offset_t first_segment_file_offsets{};
    auto first_segment = *base_it;
    size_t first_segment_size = first_segment->file_size();
    auto first_segment_offsets = first_segment->offsets();

    auto [f_locks, segments] = [&]() {
        std::vector<ss::future<ss::rwlock::holder>> f_locks;
        std::vector<ss::lw_shared_ptr<segment>> segments;
        size_t locked_range_size = 0;
        model::offset last_locked_offset;
        for (auto& s : _segs) {
            locked_range_size += s->size_bytes();
            f_locks.emplace_back(s->read_lock());
            segments.emplace_back(s);
            last_locked_offset = s->offsets().get_committed_offset();
            if (locked_range_size > (target.target_size + first_segment_size)) {
                // The size of the locked range consist of full segments only.
                // It's not guaranteed that the first segment will contribute
                // much to the resulting size because the staring point could be
                // at the end of the segment. Because of that we're not taking
                // its size into account here.
                break;
            }
        }
        return std::make_tuple(std::move(f_locks), std::move(segments));
    }();
    auto holders = co_await ss::when_all_succeed(
      std::begin(f_locks), std::end(f_locks));

    // Check if anything was closed after the scheduling point.
    for (const auto& s : segments) {
        if (s->is_closed()) {
            co_return std::nullopt;
        }
    }

    // Recheck log offsets as well, as we could race with a prefix truncation
    // while waiting for segment locks.
    if (!log_contains_offset(first)) {
        vlog(stlog.debug, "Log does not include offset {}", first);
        co_return std::nullopt;
    }

    if (
      first_segment_offsets.get_base_offset() <= first
      && first_segment_offsets.get_committed_offset() >= first) {
        // The first segment is accounted only partially. We're doing subscan
        // here but most of the time it won't require the actual scanning
        // because in this mode the offset range will end on index entry or
        // segment end. When we subsequently creating uploads using this method
        // only the first upload will have to do scanning to find the
        // offset.
        auto ix_res = first_segment->index().find_nearest(first);

        first_segment_file_offsets = co_await get_file_offset(
          first_segment, ix_res, first, boundary_type::exclusive);
    } else {
        // We expect to find first offset inside the first segment.
        // If this is not the case the log was likely truncated concurrently.
        vlog(
          stlog.debug,
          "First segment out of range, offsets: {}, expected offset: {}",
          first_segment_offsets,
          first);
        co_return std::nullopt;
    }

    size_t first_segment_file_pos = first_segment_file_offsets.position;

    // No scheduling points below this point, some invariants has to be
    // validated

    // Collect relevant segments
    size_t current_size = 0;
    // Last offset included to the result, default value means that we didn't
    // find anything
    model::offset last_included_offset = {};
    size_t num_segments = 0;
    auto it = _segs.lower_bound(first);
    for (; it < _segs.end(); it++) {
        if (it->get()->is_closed()) {
            co_return std::nullopt;
        }
        if (*it == first_segment) {
            vlog(
              stlog.debug,
              "First offset {} located at {}, offset range size: {}, Segment "
              "offsets: {}",
              first,
              first_segment_file_pos,
              first_segment_size - first_segment_file_pos,
              first_segment_offsets);
            current_size += first_segment_size - first_segment_file_pos;
        } else {
            auto sz = it->get()->file_size();
            current_size += sz;
            vlog(
              stlog.debug,
              "Adding {} bytes to the offset range. Segment offsets: {}, "
              "current_size: {}",
              sz,
              it->get()->offsets(),
              current_size);
        }
        num_segments++;
        if (current_size > target.target_size) {
            // Segment size overshoots and we need to use segment index
            // to find the end offset and its file pos.
            // We accumulated enough segments to satisfy the query. There're
            // few cases:
            // - The beginning and the end of the range is inside the same
            // segment. The 'num_segments' will be equal to one if this is the
            // case.
            // - The beginning and the end are located in different
            // segments. In both cases we need to find the finish line. The
            // size calculation is affected by these two possibilities.
            vlog(
              stlog.debug,
              "Offset range size overshoot by {}, current segment size "
              "{}, current offset range size: {}",
              current_size - target.target_size,
              it->get()->file_size(),
              current_size);

            size_t truncate_after = 0;
            if (num_segments == 1) {
                // There are two cases here.
                // 1. The segment has at least target_size bytes after
                // base_file_pos,
                // 2. The segment is too small. In this case we need to clamp
                // the result.
                truncate_after = first_segment_file_pos + target.target_size;
                truncate_after = std::clamp(
                  truncate_after,
                  first_segment_file_pos,
                  it->get()->file_size());
            } else {
                // In this case we need to find the truncation point
                // always starting from the beginning of the segment.
                //
                // prev is guaranteed to be smaller than target_size
                // because we reached this branch.
                auto prev = current_size - it->get()->file_size();
                auto delta = target.target_size - prev;
                truncate_after = delta;
            }
            auto committed_offset = it->get()->offsets().get_committed_offset();
            if (committed_offset == model::offset{}) {
                co_return std::nullopt;
            }
            auto last_index_entry = it->get()->index().find_above_size_bytes(
              truncate_after);
            if (
              last_index_entry.has_value()
              && last_index_entry->offset > committed_offset) {
                // The index entry overshoots the committed offset
                vlog(
                  stlog.debug,
                  "Index lookup overshoot committed offset {}, index entry "
                  "found: {}",
                  committed_offset,
                  last_index_entry->offset);
                last_index_entry = it->get()->index().find_nearest(
                  committed_offset);
            }
            if (
              last_index_entry.has_value()
              && model::prev_offset(last_index_entry->offset) > first) {
                vlog(
                  stlog.debug,
                  "Setting offset range to {} - {}, {} bytes of the last "
                  "segment are included",
                  first,
                  last_index_entry->offset,
                  last_index_entry->filepos);

                last_included_offset = model::prev_offset(
                  last_index_entry->offset);

                // We're including only part of the file so the total size
                // of the range has to be adjusted
                current_size
                  -= (it->get()->file_size() - last_index_entry->filepos);
            } else {
                // There is no index entry that we can use to find the size
                // of the offset range in this case
                vlog(
                  stlog.debug,
                  "Setting offset range to {} - {} (end of segment), "
                  "truncation point: {}, base_file_pos: {}",
                  first,
                  it->get()->offsets().get_committed_offset(),
                  truncate_after,
                  first_segment_file_pos);
                last_included_offset
                  = it->get()->offsets().get_committed_offset();
            }
            break;
        } else if (current_size > target.min_size) {
            vlog(
              stlog.debug,
              "Setting offset range to {} - {}",
              first,
              it->get()->offsets().get_committed_offset());
            // We can include full segment to the list of segments
            last_included_offset = it->get()->offsets().get_committed_offset();
            continue;
        }
    }

    vlog(
      stlog.debug,
      "Discovered offset size: {}, last included offset: {}",
      current_size,
      last_included_offset);

    if (current_size < target.min_size) {
        vlog(stlog.debug, "Discovered offset range is not large enough");
        co_return std::nullopt;
    }

    if (
      num_segments == 0 || current_size == 0
      || last_included_offset == model::offset{}) {
        vlog(stlog.debug, "Can't find log segments");
        co_return std::nullopt;
    }

    while (it == _segs.end()
           || it->get()->offsets().get_base_offset() > last_included_offset) {
        it = std::prev(it);
    }
    auto ix_right = it->get()->index().find_nearest(last_included_offset);
    auto last_segment_file_offsets = co_await get_file_offset(
      *it, ix_right, last_included_offset, boundary_type::inclusive);

    co_return offset_range_size_result_t{
      .on_disk_size = current_size,
      .last_offset = last_included_offset,
      .first_timestamp = first_segment_file_offsets.base_timestamp,
      .last_timestamp = last_segment_file_offsets.last_timestamp,
    };
}

bool disk_log_impl::is_compacted(
  model::offset first, model::offset last) const {
    for (auto it = _segs.lower_bound(first); it != _segs.end(); it++) {
        const auto& lstat = it->get()->offsets();
        if (lstat.get_base_offset() > last) {
            break;
        }
        if (it->get()->is_compacted_segment()) {
            return true;
        }
    }
    return false;
}

bool disk_log_impl::eligible_for_compacted_reupload(
  model::offset first, model::offset last) const {
    if (auto mco = max_eligible_for_compacted_reupload_offset(first);
        mco.has_value()) {
        return last <= mco.value();
    }
    return false;
}

std::optional<model::offset>
disk_log_impl::max_eligible_for_compacted_reupload_offset(
  model::offset first) const {
    auto it = _segs.lower_bound(first);
    if (it == _segs.end()) {
        return std::nullopt;
    }
    auto compaction_complete = [this](const ss::lw_shared_ptr<segment>& seg) {
        if (!seg->is_compacted_segment()) {
            return false;
        }
        if (config::shard_local_cfg().log_compaction_use_sliding_window) {
            // if deletion is enabled, we don't want to wait so long for
            // perfectly compacted data that truncation leaves us with
            // uncompacted data in cloud storage. so in this case, a single
            // round of windowed compaction is 'enough'.
            if (config().is_locally_collectable()) {
                return seg->finished_windowed_compaction();
            }

            // however, if deletion is not enabled, then we may as well wait
            // for the best possible compacted version of the data.
            return seg->has_clean_compact_timestamp();
        }
        return seg->has_self_compact_timestamp();
    };

    if (!compaction_complete(*it)) {
        return std::nullopt;
    }

    // perform a linear search here because segment_set may not be partitioned
    // with respect to has_clean_compact_timestamp
    auto first_uncompacted = std::find_if_not(
      it, _segs.end(), compaction_complete);

    if (first_uncompacted == _segs.end()) {
        // in this case everything is compacted enough for reupload
        return _segs.back()->offsets().get_committed_offset();
    }

    return model::prev_offset(
      first_uncompacted->get()->offsets().get_base_offset());
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(local_log_reader_config config) {
    throw_if_closed();
    if (config.start_offset < _start_offset) {
        return ss::make_exception_future<model::record_batch_reader>(
          std::runtime_error(
            fmt::format(
              "Reader cannot read before start of the log {} < {}",
              config.start_offset,
              _start_offset)));
    }

    if (config.start_offset > config.max_offset) {
        auto lease = std::make_unique<lock_manager::lease>(segment_set({}));
        auto empty = model::make_record_batch_reader<log_reader>(
          std::move(lease), config, *_probe, get_offset_translator_state());
        return ss::make_ready_future<model::record_batch_reader>(
          std::move(empty));
    }
    if (config.skip_readers_cache) {
        return make_unchecked_reader(config);
    }
    return make_cached_reader(config);
}

ss::future<model::record_batch_reader>
disk_log_impl::make_reader(timequery_config config) {
    throw_if_closed();
    return _lock_mngr.range_lock(config).then(
      [this, cfg = config](std::unique_ptr<lock_manager::lease> lease) {
          auto start_offset = cfg.min_offset;

          if (!lease->range.empty()) {
              const ss::lw_shared_ptr<segment>& segment = *lease->range.begin();
              std::optional<segment_index::entry> index_entry = std::nullopt;

              // The index (and hence, binary search) is used only if the
              // timestamps on the batches are monotonically increasing.
              if (segment->index().batch_timestamps_are_monotonic()) {
                  index_entry = segment->index().find_nearest(cfg.time);
                  if (index_entry) {
                      vlog(
                        stlog.debug,
                        "Batch timestamps have monotonically increasing "
                        "timestamps; used segment index to find first batch "
                        "before timestamp {}: offset={} with ts={}",
                        cfg.time,
                        index_entry->offset,
                        index_entry->timestamp);
                  }
              }

              auto offset_within_segment
                = index_entry ? index_entry->offset
                              : segment->offsets().get_base_offset();

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

          local_log_reader_config config(
            start_offset,
            cfg.max_offset,
            2048, // We just need one record batch
            cfg.type_filter,
            cfg.time,
            cfg.abort_source);
          return model::make_record_batch_reader<log_reader>(
            std::move(lease), config, *_probe, get_offset_translator_state());
      });
}

std::optional<model::term_id> disk_log_impl::get_term(model::offset o) const {
    auto it = _segs.lower_bound(o);
    if (it != _segs.end() && o >= _start_offset) {
        return (*it)->offsets().get_term();
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

    if ((*it)->offsets().get_term() == term) {
        return (*it)->offsets().get_dirty_offset();
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

std::optional<model::offset>
disk_log_impl::index_batch_base_offset_lower_bound(model::offset o) const {
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
    // find nearest always returns a base offset.
    if (auto entry = idx.find_nearest(o)) {
        return entry->offset;
    }
    return std::nullopt;
}

ss::future<std::optional<timequery_result>>
disk_log_impl::timequery(timequery_config cfg) {
    throw_if_closed();
    if (_segs.empty()) {
        co_return std::nullopt;
    }

    auto reader = co_await make_reader(cfg);
    auto batches = co_await model::consume_reader_to_memory(
      std::move(reader), model::no_timeout);

    if (
      !batches.empty() && batches.front().header().max_timestamp >= cfg.time) {
        co_return co_await batch_timequery(
          std::move(batches.front()), cfg.min_offset, cfg.time, cfg.max_offset);
    }

    co_return std::nullopt;
}

ss::future<> disk_log_impl::remove_segment_permanently(
  ss::lw_shared_ptr<segment> s, std::string_view ctx) {
    vlog(stlog.info, "Removing \"{}\" ({}, {})", s->filename(), ctx, s);
    // stats accounting must happen synchronously
    _probe->delete_segment(*s);
    // Only deduct bytes if the segment was previously closed.
    if (!s->has_appender()) {
        subtract_segment_bytes(s, s->size_bytes());
    }
    // background close
    s->tombstone();

    // An arbitrary length of time for the watchdog timeout. A segment exceeding
    // this length of time to close may indicate some sort of lock contention OR
    // a potentially bad cluster state.
    static constexpr auto segment_wd_timeout = 30s;
    ssx::watchdog wd(
      segment_wd_timeout,
      [filename = s->filename(),
       had_outstanding_locks = s->has_outstanding_locks(),
       ctx = ss::sstring(ctx)] {
          vlog(
            stlog.warn,
            "Segment {} exceeding {} to close{} ({})",
            filename,
            segment_wd_timeout,
            had_outstanding_locks ? ", had outstanding locks" : "",
            ctx);
      });

    co_await _readers_cache->evict_segment_readers(s)
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
                 || std::max(
                      _segs.back()->offsets().get_base_offset(), _start_offset)
                      < o;
      },
      [this] {
          auto ptr = _segs.back();
          _segs.pop_back();
          return remove_segment_permanently(ptr, "remove_full_segments");
      });
}
namespace {
bool keep_segment_after_prefix_truncate(
  const ss::lw_shared_ptr<segment>& segment,
  model::offset prefix_truncate_offset) {
    auto& offsets = segment->offsets();
    return offsets.get_base_offset() >= prefix_truncate_offset
           || offsets.get_dirty_offset() >= prefix_truncate_offset;
}
} // namespace
ss::future<>
disk_log_impl::remove_prefix_full_segments(truncate_prefix_config cfg) {
    return ss::do_until(
      [this, cfg] {
          // base_offset check is for the case of an empty segment
          // (where dirty = base - 1). We don't want to remove it because
          // batches may be concurrently appended to it and we should keep them.
          return _segs.empty()
                 || keep_segment_after_prefix_truncate(
                   _segs.front(), cfg.start_offset);
      },
      [this, prefix_truncate_offset = cfg.start_offset] {
          // it is safe to capture the front segment pointer here. This
          // operation is executed under the segment_rewrite_lock, we are
          // guaranteed that no other operation will remove the segment from the
          // segment list head (front).
          auto ptr = _segs.front();
          return do_remove_prefix_full_segment(
            std::move(ptr), prefix_truncate_offset);
      });
}

ss::future<> disk_log_impl::do_remove_prefix_full_segment(
  ss::lw_shared_ptr<segment> ptr, model::offset prefix_truncate_offset) {
    auto cache_lock = co_await _readers_cache->evict_segment_readers(ptr);

    // We will be calling `remove_segment_permanently()` below on what is
    // potentially the active segment. If there is a concurrent append occuring,
    // we may get into a race with the `disk_log_appender` since we return the
    // units in `lock_holder` (i.e the segment's write lock) while the future
    // for `remove_segment_permanently()` is still unresolved. In this case, we
    // need to obtain units from `_segments_rolling_lock` to prevent any
    // concurrency issues.
    std::optional<ssx::semaphore_units> seg_rolling_units;
    if (ptr->has_appender()) {
        // This cannot throw due to a broken semaphore, as we are already
        // holding `_segment_rewrite_lock`.
        seg_rolling_units = co_await _segments_rolling_lock.get_units();
    }

    auto lock_holder = co_await ptr->write_lock();

    // after the locks are acquired, check if the segment is
    // still eligible for deletion as there might have been
    // concurrent appends. If segments collection is empty we
    // can skip prefix truncation as the segments were removed
    if (
      keep_segment_after_prefix_truncate(ptr, prefix_truncate_offset)
      || _segs.empty()) {
        co_return;
    }
    _segs.pop_front();
    _probe->add_bytes_prefix_truncated(ptr->file_size());
    // first call the remove segments, then release the lock
    // before waiting for future to finish
    auto f = remove_segment_permanently(ptr, "remove_prefix_full_segments");
    lock_holder.return_all();

    co_return co_await std::move(f);
}

ss::future<> disk_log_impl::truncate_prefix(truncate_prefix_config cfg) {
    throw_if_closed();
    co_await _failure_probes.truncate_prefix().then([this, cfg]() mutable {
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
    // We truncate the segments before truncating offset translator to wait for
    // readers that started reading from the start of the log before we advanced
    // the start offset and thus can still need offset translation info.
    co_await _offset_translator.prefix_truncate(
      model::prev_offset(cfg.start_offset), cfg.force_truncate_delta);
}

ss::future<> disk_log_impl::do_truncate_prefix(truncate_prefix_config cfg) {
    vlog(
      stlog.trace,
      "prefix truncate {} at {}",
      config().ntp(),
      cfg.start_offset);
    /*
     * Persist the desired starting offset
     */
    co_await update_start_offset(cfg.start_offset);

    /*
     * Then delete all segments (potentially including the active segment)
     * whose max offset falls below the new starting offset.
     */
    {
        ssx::semaphore_units seg_rewrite_units
          = co_await _segment_rewrite_lock.get_units();
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
    throw_if_closed();
    // We are truncating the offset translator before truncating the log
    // because if saving offset translator state fails (e.g. because of a
    // crash), we can retry and eventually log and offset translator will
    // become consistent. OTOH if log truncation were first and saving offset
    // translator state failed, we wouldn't retry and log and offset translator
    // could diverge.
    co_await _offset_translator.truncate(cfg.base_offset);

    co_await _failure_probes.truncate().then([this, cfg]() mutable {
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

    vlog(stlog.trace, "do_truncate at {}, log {}", cfg.base_offset, stats);
    if (cfg.base_offset > stats.dirty_offset || _segs.empty()) {
        co_return;
    }

    cfg.base_offset = std::max(cfg.base_offset, _start_offset);
    // Note different from the stats variable above because
    // we want to delete even empty segments.
    if (
      cfg.base_offset
      < std::max(_segs.back()->offsets().get_base_offset(), _start_offset)) {
        co_await remove_full_segments(cfg.base_offset);
        // recurse
        co_return co_await do_truncate(cfg, std::move(lock_guards));
    }

    auto last = _segs.back();
    if (cfg.base_offset > last->offsets().get_dirty_offset()) {
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
    model::offset start = last->offsets().get_base_offset();

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
    vlog(
      stlog.trace,
      "do_truncate at {}, start {}, initial_size {}",
      cfg.base_offset,
      start,
      initial_size);

    // an unchecked reader is created which does not enforce the logical
    // starting offset. this is needed because we really do want to read
    // all the data in the segment to find the correct physical offset.
    auto reader = co_await make_unchecked_reader(
      local_log_reader_config(start, model::offset::max()));
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
    auto [new_max_offset, file_position, new_max_timestamp] = phs.value();
    vlog(
      stlog.trace,
      "do_truncate at {}, new_max_offset {}, file_position {}",
      cfg.base_offset,
      new_max_offset,
      file_position);

    if (file_position == 0) {
        _segs.pop_back();
        _suffix_truncation_indicator++;
        co_return co_await remove_segment_permanently(
          last_ptr, "truncate[post-translation]");
    }

    // In a compacted segment with an appender, last_ptr->truncate() will
    // release_appender and force a segment roll. We need to add the file size
    // to dirty and closed bytes here.
    if (last_ptr->is_compacted_segment() && last_ptr->has_appender()) {
        add_segment_bytes(last_ptr, last_ptr->size_bytes());
    }

    // But, we also have to remove the bytes according to truncation for a
    // closed (or to-be-closed, in the case of a compacted) segment.
    const ssize_t removed_bytes = last_ptr->size_bytes() - file_position;
    _probe->remove_partition_bytes(removed_bytes);
    if (!last_ptr->has_appender() || last_ptr->is_compacted_segment()) {
        subtract_segment_bytes(last_ptr, removed_bytes);
    }
    auto cache_lock = co_await _readers_cache->evict_truncate(cfg.base_offset);

    try {
        _suffix_truncation_indicator++;
        co_return co_await last_ptr->truncate(
          new_max_offset, file_position, new_max_timestamp);
    } catch (...) {
        vassert(
          false,
          "Could not truncate:{} logical max:{}, physical "
          "offset:{}, new max timestamp:{} on segment:{} - log:{}",
          std::current_exception(),
          new_max_offset,
          file_position,
          new_max_timestamp,
          last,
          *this);
    }
}

size_t disk_log_impl::get_log_truncation_counter() const noexcept {
    return _suffix_truncation_indicator;
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

bool disk_log_impl::notify_compaction_update() {
    bool new_compaction_enabled = config().is_locally_compacted();
    bool result = (_compaction_enabled != new_compaction_enabled);
    _compaction_enabled = new_compaction_enabled;

    // enable compaction
    if (!_compaction_enabled && new_compaction_enabled) {
        for (auto& s : _segs) {
            s->mark_as_compacted_segment();
        }
    }
    // disable compaction
    if (_compaction_enabled && !new_compaction_enabled) {
        for (auto& s : _segs) {
            s->unmark_as_compacted_segment();
        }
    }

    return result;
}

void disk_log_impl::set_overrides(ntp_config::default_overrides o) {
    mutable_config().set_overrides(o);
}

/**
 * We express compaction backlog as the size of data that has to be read to
 * perform full compaction.
 */
int64_t disk_log_impl::compaction_backlog() {
    if (!config().is_locally_compacted() || _segs.empty()) {
        return 0;
    }

    if (!needs_compaction()) {
        return 0;
    }

    // Find the last closed yet dirty segment in the log, and sum over all bytes
    // before it.
    auto last_dirty_it = std::find_if(
      _segs.rbegin(), _segs.rend(), [](const segment_set::type& s) {
          return !s->has_appender() && !s->has_clean_compact_timestamp();
      });

    if (last_dirty_it == _segs.rend()) {
        return 0;
    }

    int64_t backlog = std::accumulate(
      _segs.begin(),
      last_dirty_it.base(),
      int64_t{0},
      [](int64_t acc, ss::lw_shared_ptr<segment>& seg) {
          return acc + seg->size_bytes();
      });

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
  raft::group_id group,
  log_manager& manager,
  segment_set segs,
  kvstore& kvstore,
  ss::sharded<features::feature_table>& feature_table,
  std::vector<model::record_batch_type> translator_batch_types) {
    auto disk_log = ss::make_shared<disk_log_impl>(
      std::move(cfg),
      group,
      manager,
      std::move(segs),
      kvstore,
      feature_table,
      std::move(translator_batch_types));
    return disk_log;
}

/*
 * assumes that the compaction gate is held.
 */
ss::future<std::pair<usage, reclaim_size_limits>>
disk_log_impl::disk_usage_and_reclaimable_space(gc_config input_cfg) {
    std::optional<model::offset> max_offset;
    if (config().is_locally_collectable()) {
        auto cfg = apply_overrides(input_cfg);
        max_offset = retention_offset(cfg);
    }

    /*
     * offset calculation for local retention reclaimable is different than the
     * retention above and takes into account local retention advisory flag.
     */
    auto local_retention_cfg = apply_kafka_retention_overrides(input_cfg);
    local_retention_cfg = apply_local_storage_overrides(local_retention_cfg);
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
     * max removable offset from stm. A future refactoring may consider moving
     * more of the retention controls into a higher level location.
     */
    const auto max_removable = stm_manager()->max_removable_local_log_offset();
    const auto retention_offset = [&]() -> std::optional<model::offset> {
        if (max_offset.has_value()) {
            return std::min(max_offset.value(), max_removable);
        }
        return std::nullopt;
    }();

    /*
     * truncate_prefix() dry run. the available segments are tabulated even when
     * retention is not enabled data may be available in cloud storage and
     * still be subject to reclaim in low disk space situations.
     */
    chunked_vector<segment_set::type> retention_segments;
    chunked_vector<segment_set::type> available_segments;
    chunked_vector<segment_set::type> remaining_segments;
    chunked_vector<segment_set::type> local_retention_segments;
    for (auto& seg : _segs) {
        if (
          retention_offset.has_value()
          && seg->offsets().get_dirty_offset() <= retention_offset.value()) {
            retention_segments.push_back(seg);
        } else if (
          is_cloud_retention_active()
          && seg->offsets().get_dirty_offset() <= max_removable) {
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
          && seg->offsets().get_dirty_offset() <= max_removable
          && local_retention_offset.has_value()
          && seg->offsets().get_dirty_offset()
               <= local_retention_offset.value()) {
            local_retention_segments.push_back(seg);
        }
    }

    ss::semaphore limit(
      std::max<size_t>(
        1,
        config::shard_local_cfg().space_management_max_segment_concurrency()));

    auto [retention, available, remaining, lcl] = co_await ss::when_all_succeed(
      // reduce segment subject to retention policy
      ss::map_reduce(
        retention_segments,
        [&limit](const segment_set::type& seg) {
            return ss::with_semaphore(
              limit, 1, [&seg] { return seg->persistent_size(); });
        },
        usage{},
        [](usage acc, usage u) { return acc + u; }),

      // reduce segments available for reclaim
      ss::map_reduce(
        available_segments,
        [&limit](const segment_set::type& seg) {
            return ss::with_semaphore(
              limit, 1, [&seg] { return seg->persistent_size(); });
        },
        usage{},
        [](usage acc, usage u) { return acc + u; }),

      // reduce segments not available for reclaim
      ss::map_reduce(
        remaining_segments,
        [&limit](const segment_set::type& seg) {
            return ss::with_semaphore(
              limit, 1, [&seg] { return seg->persistent_size(); });
        },
        usage{},
        [](usage acc, usage u) { return acc + u; }),

      // reduce segments not available for reclaim
      ss::map_reduce(
        local_retention_segments,
        [&limit](const segment_set::type& seg) {
            return ss::with_semaphore(
              limit, 1, [&seg] { return seg->persistent_size(); });
        },
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
    _reclaimable_size_bytes = reclaim.available;

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

    cfg = apply_kafka_retention_overrides(cfg);

    /*
     * compacted topics are always stored whole on local storage such that local
     * retention settings do not come into play.
     */
    if (config().is_locally_compacted()) {
        /*
         * if there is no delete policy enabled for a compacted topic then its
         * local capacity requirement is limited only by compaction process and
         * how much data is written. for this case we use a heuristic of to
         * report space wanted as `factor * current capacity` to reflect that we
         * want space for continued growth.
         */
        if (
          !config().is_locally_collectable()
          || deletion_exempt(config().ntp())) {
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
        cfg = maybe_apply_local_storage_overrides(cfg);
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
    co_await maybe_adjust_retention_timestamps();

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
    chunked_vector<segment_set::type> segments;
    for (; it != std::cend(_segs); ++it) {
        segments.push_back(*it);
    }

    /*
     * not enough data. caller will substitute in a reasonable value.
     */
    if (segments.size() < 2) {
        vlog(
          stlog.trace,
          "time-based-usage: skipping log with too few segments ({}) ntp {}",
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

    ss::semaphore limit(
      std::max<size_t>(
        1,
        config::shard_local_cfg().space_management_max_segment_concurrency()));

    // roll up the amount of disk space taken by these segments
    auto usage = co_await ss::map_reduce(
      segments,
      [&limit](const segment_set::type& seg) {
          return ss::with_semaphore(
            limit, 1, [&seg] { return seg->persistent_size(); });
      },
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
    usage use;
    reclaim_size_limits reclaim;

    auto usage_and_reclaim_fut = co_await ss::coroutine::as_future(
      disk_usage_and_reclaimable_space(cfg));

    if (usage_and_reclaim_fut.failed()) {
        // The disk usage/reclaimable space could not be computed for some
        // reason. Return the used size, but with 0 bytes indicated as
        // reclaimable.
        auto e = usage_and_reclaim_fut.get_exception();
        ss::semaphore limit(
          std::max<size_t>(
            1,
            config::shard_local_cfg()
              .space_management_max_segment_concurrency()));

        use = co_await ss::map_reduce(
          _segs,
          [&limit](const segment_set::type& seg) {
              return ss::with_semaphore(
                limit, 1, [&seg] { return seg->persistent_size(); });
          },
          usage{},
          [](usage acc, usage u) { return acc + u; });

        vlog(
          gclog.warn,
          "Unable to collect disk usage for ntp {}: {}, reporting usage of {} "
          "with no reclaimable bytes",
          config().ntp(),
          e,
          use.total());
    } else {
        auto usage_and_reclaim = usage_and_reclaim_fut.get();
        use = usage_and_reclaim.first;
        reclaim = usage_and_reclaim.second;
    }

    /*
     * compute target capacities such as minimum required capacity as well as
     * capacities needed to meet goals such as local retention.
     */
    auto target = co_await disk_usage_target(cfg, use);

    /*
     * the intention here is to establish a needed <= wanted relationship which
     * should generally provide a nicer set of numbers for consumers to use.
     */
    target.min_capacity_wanted = std::max(
      target.min_capacity_wanted, target.min_capacity);

    co_return usage_report(use, reclaim, target);
}

chunked_vector<ss::lw_shared_ptr<segment>>
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
     * may signal restrictions through the max removable offset. for cloud
     * topics max removable will include a reflection of how much data has
     * been uploaded into the cloud.
     */
    const auto max_removable = stm_manager()->max_removable_local_log_offset();

    // collect eligible segments
    chunked_vector<segment_set::type> segments;
    for (auto remaining = _segs.size() - keep_segs; auto& seg : _segs) {
        if (seg->offsets().get_committed_offset() <= max_removable) {
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
     * removable offset unreliable. the read replica topics still have a
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
    cfg = apply_kafka_retention_overrides(cfg);
    cfg = apply_local_storage_overrides(cfg);
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
    const auto max_removable = stm_manager()->max_removable_local_log_offset();

    /*
     * lightweight segment set copy for safe iteration
     */
    chunked_vector<segment_set::type> segments;
    for (const auto& seg : _segs) {
        segments.push_back(seg);
    }

    /*
     * currently we use two segments as the low space size
     */
    std::optional<model::offset> low_space_offset;
    if (segments.size() >= 2) {
        low_space_offset
          = segments[segments.size() - 2]->offsets().get_base_offset();
    }

    vlog(
      stlog.debug,
      "Categorizing {} {} segments for {} with local retention {} low "
      "space {} max removable {}",
      segments.size(),
      (hinted ? "hinted" : "non-hinted"),
      config().ntp(),
      local_retention_offset,
      low_space_offset,
      max_removable);

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
             *   making its current full size reclaimable as max removable
             *   increases to cover the entire segment.
             *
             *   2. finally, we don't report it if max removable hasn't even
             *   made it to the active segment yet.
             */
            if (seg->offsets().get_base_offset() <= max_removable) {
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
          .offset = seg->offsets().get_dirty_offset(),
          .size = seg_size,
        };

        /*
         * if the current segment is not fully collectible, then subsequent
         * segments will not be either, and don't require consideration.
         */
        if (point.offset > max_removable) {
            vlog(
              stlog.trace,
              "Stopping collection at offset {}:{} above max removable {}",
              point.offset,
              human::bytes(point.size),
              max_removable);
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

size_t disk_log_impl::reclaimable_size_bytes() const {
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
    return _reclaimable_size_bytes;
}

ss::future<> disk_log_impl::copy_kvstore_state(
  model::ntp ntp,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& storage) {
    const auto ks = kvstore::key_space::storage;
    std::optional<iobuf> start_offset = source_kvs.get(
      ks, internal::start_offset_key(ntp));
    std::optional<iobuf> clean_segment = source_kvs.get(
      ks, internal::clean_segment_key(ntp));

    co_await storage.invoke_on(target_shard, [&](storage::api& api) {
        const auto ks = kvstore::key_space::storage;
        std::vector<ss::future<>> write_futures;
        write_futures.reserve(3);
        if (start_offset) {
            write_futures.push_back(api.kvs().put(
              ks, internal::start_offset_key(ntp), start_offset->copy()));
        }
        if (clean_segment) {
            write_futures.push_back(api.kvs().put(
              ks, internal::clean_segment_key(ntp), clean_segment->copy()));
        }
        return ss::when_all_succeed(std::move(write_futures));
    });
}

ss::future<> disk_log_impl::remove_kvstore_state(
  const model::ntp& ntp, storage::kvstore& kvs) {
    const auto ks = kvstore::key_space::storage;
    return ss::when_all_succeed(
             kvs.remove(ks, internal::start_offset_key(ntp)),
             kvs.remove(ks, internal::clean_segment_key(ntp)))
      .discard_result();
}

double disk_log_impl::dirty_ratio() const {
    // Sanity check/safety hatch against negative values
    if (_dirty_segment_bytes < 0 || _closed_segment_bytes < 0) {
        vlog(
          stlog.info,
          "{}: _dirty_segment_bytes ({}) and/or _closed_segment_bytes ({}) "
          "have become negative - clearing counters and resumming over {} "
          "segments in the log.",
          config().ntp(),
          _dirty_segment_bytes,
          _closed_segment_bytes,
          _segs.size());

        reset_dirty_and_closed_bytes();
    }
    // Avoid division by 0.
    return (_closed_segment_bytes == 0)
             ? 0.0
             : static_cast<double>(_dirty_segment_bytes)
                 / static_cast<double>(_closed_segment_bytes);
}

void disk_log_impl::add_dirty_segment_bytes(ssize_t bytes) {
    _dirty_segment_bytes += bytes;
    _probe->set_dirty_segment_bytes(_dirty_segment_bytes);
}

void disk_log_impl::add_closed_segment_bytes(ssize_t bytes) {
    _closed_segment_bytes += bytes;
    _probe->set_closed_segment_bytes(_closed_segment_bytes);
}

void disk_log_impl::subtract_dirty_segment_bytes(ssize_t bytes) {
    _dirty_segment_bytes -= bytes;
    dassert(
      _dirty_segment_bytes >= 0, "_dirty_segment_bytes must not be negative.");
    _probe->set_dirty_segment_bytes(_dirty_segment_bytes);
}

void disk_log_impl::subtract_closed_segment_bytes(ssize_t bytes) {
    _closed_segment_bytes -= bytes;
    dassert(
      _closed_segment_bytes >= 0,
      "_closed_segment_bytes must not be negative.");
    _probe->set_closed_segment_bytes(_closed_segment_bytes);
}

void disk_log_impl::add_segment_bytes(
  ss::lw_shared_ptr<segment> s, ssize_t bytes) {
    if (!s->has_clean_compact_timestamp()) {
        add_dirty_segment_bytes(bytes);
    }
    add_closed_segment_bytes(bytes);
}

void disk_log_impl::subtract_segment_bytes(
  ss::lw_shared_ptr<segment> s, ssize_t bytes) {
    if (!s->has_clean_compact_timestamp()) {
        subtract_dirty_segment_bytes(bytes);
    }
    subtract_closed_segment_bytes(bytes);
}

void disk_log_impl::reset_dirty_and_closed_bytes() const {
    ssize_t dirty{0};
    ssize_t closed{0};
    for (const auto& seg : _segs) {
        if (!seg->has_appender()) {
            if (!seg->has_clean_compact_timestamp()) {
                dirty += seg->size_bytes();
            }
            closed += seg->size_bytes();
        }
    }
    _dirty_segment_bytes = dirty;
    _closed_segment_bytes = closed;
}

std::optional<model::timestamp>
disk_log_impl::earliest_dirty_segment_ts() const {
    auto dirty_segments_ts = _segs | std::views::filter([](const auto& seg) {
                                 return !seg->has_appender()
                                        && !seg->has_clean_compact_timestamp();
                             })
                             | std::views::transform([](const auto& seg) {
                                   return seg->index().base_timestamp();
                               });
    if (std::ranges::empty(dirty_segments_ts)) {
        return std::nullopt;
    }
    return *std::ranges::min_element(dirty_segments_ts);
}

bool disk_log_impl::needs_compaction() const {
    auto max_lag = config().max_compaction_lag_ms();
    const auto now = to_time_point(model::timestamp::now());
    const auto earliest_dirty_ts = earliest_dirty_segment_ts();
    const auto exceed_compact_lag
      = earliest_dirty_ts.has_value()
        && (now - to_time_point(earliest_dirty_ts.value()) > max_lag);

    auto dr = dirty_ratio();
    return dr >= config().min_cleanable_dirty_ratio() || exceed_compact_lag;
}

void disk_log_impl::throw_if_closed() const {
    if (_closed) {
        throw ss::abort_requested_exception();
    }
}

} // namespace storage
