// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_manager.h"

#include "absl/container/btree_map.h"
#include "base/likely.h"
#include "base/vlog.h"
#include "compaction/key_offset_map.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/abort_source.h"
#include "ssx/async-clear.h"
#include "ssx/future-util.h"
#include "ssx/mutex.h"
#include "ssx/watchdog.h"
#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/disk_log_impl.h"
#include "storage/file_sanitizer.h"
#include "storage/fs_utils.h"
#include "storage/kvstore.h"
#include "storage/log.h"
#include "storage/log_manager_probe.h"
#include "storage/logger.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/storage_resources.h"
#include "storage/types.h"
#include "utils/directory_walker.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>
#include <seastar/util/later.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <filesystem>
#include <functional>
#include <optional>
#include <ranges>

using namespace std::chrono_literals;

namespace {

class priority_compaction_exception final : public std::runtime_error {
public:
    explicit priority_compaction_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};

} // namespace

namespace storage {
using logs_type = absl::flat_hash_map<model::ntp, log_housekeeping_meta>;

log_config::log_config(
  ss::sstring directory,
  size_t segment_size,

  std::optional<file_sanitize_config> file_cfg) noexcept
  : base_dir(std::move(directory))
  , max_segment_size(config::mock_binding<size_t>(std::move(segment_size)))
  , segment_size_jitter(0) // For deterministic behavior in unit tests.
  , compacted_segment_size(config::mock_binding<size_t>(256_MiB))
  , max_compacted_segment_size(config::mock_binding<size_t>(5_GiB))
  , retention_bytes(config::mock_binding<std::optional<size_t>>(std::nullopt))
  , compaction_interval(
      config::mock_binding<std::chrono::milliseconds>(std::chrono::minutes(10)))
  , log_retention(
      config::mock_binding<std::optional<std::chrono::milliseconds>>(
        std::chrono::minutes(10080)))
  , file_config(std::move(file_cfg)) {}

log_config::log_config(
  ss::sstring directory,
  size_t segment_size,

  with_cache with,
  std::optional<file_sanitize_config> file_cfg) noexcept
  : log_config(std::move(directory), segment_size, std::move(file_cfg)) {
    cache = with;
}

log_config::log_config(
  ss::sstring directory,
  config::binding<size_t> segment_size,
  config::binding<size_t> compacted_segment_size,
  config::binding<size_t> max_compacted_segment_size,
  jitter_percents segment_size_jitter,

  config::binding<std::optional<size_t>> ret_bytes,
  config::binding<std::chrono::milliseconds> compaction_ival,
  config::binding<std::optional<std::chrono::milliseconds>> log_ret,
  with_cache c,
  batch_cache::reclaim_options recopts,
  std::chrono::milliseconds rdrs_cache_eviction_timeout,
  ss::scheduling_group compaction_sg,
  std::optional<file_sanitize_config> file_cfg) noexcept
  : base_dir(std::move(directory))
  , max_segment_size(std::move(segment_size))
  , segment_size_jitter(segment_size_jitter)
  , compacted_segment_size(std::move(compacted_segment_size))
  , max_compacted_segment_size(std::move(max_compacted_segment_size))

  , retention_bytes(std::move(ret_bytes))
  , compaction_interval(std::move(compaction_ival))
  , log_retention(std::move(log_ret))
  , cache(c)
  , reclaim_opts(recopts)
  , readers_cache_eviction_timeout(rdrs_cache_eviction_timeout)
  , compaction_sg(compaction_sg)
  , file_config(std::move(file_cfg)) {}

log_manager::log_manager(
  log_config config,
  kvstore& kvstore,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table) noexcept
  : _config(std::move(config))
  , _kvstore(kvstore)
  , _resources(resources)
  , _feature_table(feature_table)
  , _housekeeping_jitter(_config.compaction_interval())
  , _trigger_gc_jitter(0s, 5s)
  , _batch_cache(_config.reclaim_opts)
  , _probe(std::make_unique<log_manager_probe>()) {
    _config.compaction_interval.watch([this]() {
        _housekeeping_jitter = simple_time_jitter<ss::lowres_clock>{
          _config.compaction_interval()};
        _housekeeping_sem.signal();
    });
}

log_manager::~log_manager() = default;

ss::future<> log_manager::clean_close(ss::shared_ptr<storage::log> log) {
    auto clean_segment = co_await log->close();

    if (clean_segment) {
        vlog(
          stlog.debug,
          "writing clean record for: {} {}",
          log->config().ntp(),
          clean_segment.value());
        co_await _kvstore.put(
          kvstore::key_space::storage,
          internal::clean_segment_key(log->config().ntp()),
          serde::to_iobuf(
            internal::clean_segment_value{
              .segment_name = std::filesystem::path(clean_segment.value())
                                .filename()
                                .string()}));
    }
}

ss::future<> log_manager::start() {
    _probe->setup_metrics();
    if (
      unlikely(
        config::shard_local_cfg().log_disable_housekeeping_for_tests.value())) {
        co_return;
    }

    // The main housekeeping job loop (triggered by log_compaction_interval_ms).
    ssx::spawn_with_gate(_gate, [this] {
        return ss::with_scheduling_group(_config.compaction_sg, [this]() {
            return run_housekeeping_job(
              [this]() { return housekeeping_loop(); }, "housekeeping");
        });
    });
    // The urgent garbage collection loop (triggered by disk pressure).
    ssx::spawn_with_gate(_gate, [this] {
        return run_housekeeping_job([this]() { return gc_loop(); }, "gc");
    });
    co_return;
}

ss::future<> log_manager::stop() {
    _abort_source.request_abort();
    _housekeeping_sem.broken();
    _gc_sem.broken();

    co_await _gate.close();
    co_await ss::coroutine::parallel_for_each(
      _logs, [this](logs_type::value_type& entry) -> ss::future<> {
          auto close_fut = entry.second->housekeeping_gate.close();
          return clean_close(entry.second->handle)
            .then(
              [f = std::move(close_fut)]() mutable { return std::move(f); });
      });
    co_await _batch_cache.stop();
    co_await ssx::async_clear(_logs);
    if (_compaction_hash_key_map) {
        // Clear memory used for the compaction hash map, if any.
        co_await _compaction_hash_key_map->initialize(0);
        _compaction_hash_key_map.reset();
    }

    _probe->clear_metrics();
}

/**
 * `housekeeping_scan` scans over every current log in a single pass.
 */
ss::future<>
log_manager::housekeeping_scan(model::timestamp collection_threshold) {
    // reset flags for the next two loops, segment_ms and compaction.
    // since there are suspension points during the traversal of _logs_list, the
    // algorithm is: mark the logs visited, rotate _logs_list, op, and loop
    // until empty or reaching a marked log
    clear_log_meta_flags(_logs_list);

    /*
     * Apply segment ms will roll the active segment if it is old enough. This
     * is best done prior to running gc or compaction because it ensures that
     * an inactive partition eventually makes data in its most recent segment
     * eligible for these housekeeping processes.
     *
     * TODO:
     *   handle this after compaction? handle segment.ms sequentially, since
     *   compaction is already sequential when this will be unified with
     *   compaction, the whole task could be made concurrent
     */
    co_await apply_segment_ms_to_logs(_logs_list);

    if (_abort_source.abort_requested()) {
        co_return;
    }

    if (
      config::shard_local_cfg().log_compaction_use_sliding_window.value()
      && !_compaction_hash_key_map
      && (!_logs_list.empty() || !_priority_logs_list.empty())) {
        auto compaction_mem_bytes
          = memory_groups().compaction_reserved_memory();
        auto compaction_map
          = std::make_unique<compaction::hash_key_offset_map>();
        co_await compaction_map->initialize(compaction_mem_bytes);
        _compaction_hash_key_map = std::move(compaction_map);
    }

    sort_logs_by_compaction_heuristic(_logs_list);

    // Housekeep priority partitions first.
    co_await priority_housekeeping_scan(collection_threshold);

    while (
      !_logs_list.empty()
      && is_not_set(_logs_list.front().flags, bflags::compaction_checked)) {
        if (_abort_source.abort_requested()) {
            co_return;
        }

        // Before each regular housekeeping application, housekeep any priority
        // logs that need it. This ensures priority partitions (e.g.
        // __consumer_offsets) are not starved by long-running compactions.
        if (priority_logs_need_compaction()) {
            co_await priority_housekeeping_scan(collection_threshold);
        }

        // Re-check log list size before accessing front() after the above
        // scheduling point.
        if (_logs_list.empty()) {
            co_return;
        }

        auto& current_log = _logs_list.front();
        _logs_list.shift_forward();

        current_log.flags |= bflags::compaction_checked;

        // Hold the housekeeping gate to prevent issues with concurrent removal
        // of the log meta.
        auto gate = current_log.housekeeping_gate.hold();

        if (is_not_set(current_log.flags, bflags::should_compact)) {
            // Still perform gc() here on a regular `log_compaction_interval_ms`
            // basis. Use `try_get_units()` to avoid concurrent garbage
            // collection with `gc_loop()`- if we fail to obtain units,
            // it is because urgent garbage collection is already underway for
            // this log.
            auto units = current_log.housekeeping_lock.try_get_units();
            if (units.has_value()) {
                co_await current_log.handle->gc(
                  gc_config(collection_threshold, _config.retention_bytes()));
            }

            continue;
        }

        // Obtain housekeeping lock to prevent concurrency of
        // log->housekeeping() with gc fibre.
        auto housekeeping_lock_holder
          = co_await current_log.housekeeping_lock.get_units();

        if (!current_log.link.is_linked()) {
            continue;
        }

        // Set up timer-based preemption: if compaction exceeds the configured
        // timeout and a priority partition needs compaction, abort so priority
        // partitions can be serviced. If no priority partition needs
        // compaction, rearm the timer to check on a shorter interval to check
        // more frequently after `log_compaction_max_priority_wait_ms` has
        // already passed.
        ss::abort_source preempt_as;
        const auto& ntp = current_log.handle->config().ntp();
        auto timeout
          = config::shard_local_cfg().log_compaction_max_priority_wait_ms();
        ss::timer<ss::lowres_clock> preempt_timer;
        preempt_timer.set_callback(
          [this, &preempt_as, &ntp, &preempt_timer, timeout] {
              bool priority_needs_compaction = priority_logs_need_compaction();
              if (priority_needs_compaction) {
                  vlog(
                    gclog.info,
                    "{}: compaction exceeded {}ms, preempting for priority "
                    "partitions",
                    ntp,
                    timeout.count());
                  preempt_as.request_abort_ex(priority_compaction_exception(
                    "Compaction pre-empted for compaction of priority "
                    "partition"));
              } else {
                  // Rearm at a fraction of the initial timeout
                  preempt_timer.arm(timeout / 8);
              }
          });
        preempt_timer.arm(timeout);

        try {
            co_await do_housekeeping(
              current_log, collection_threshold, preempt_as);
        } catch (const priority_compaction_exception& e) {
            // Preempted for priority partition compaction.
            vlog(gclog.info, "{} - {}", ntp, e);
        }
    }
}

ss::future<> log_manager::run_housekeeping_job(
  std::function<ss::future<>()> loop_func, std::string_view ctx) {
    while (!_gate.is_closed()) {
        try {
            co_await loop_func();
        } catch (...) {
            /*
             * continue on shutdown exception because it may be bubbling up from
             * a partition shutting down. we only stop running housekeeping when
             * the log manager stops.
             */
            auto e = std::current_exception();
            if (ssx::is_shutdown_exception(e)) {
                vlog(
                  gclog.debug,
                  "Shutdown error caught in run_housekeeping_job({}): {}",
                  ctx,
                  e);
                continue;
            }
            vlog(
              gclog.info,
              "Error processing run_housekeeping_job({}): {}",
              ctx,
              e);
        }
    }
}

model::timestamp log_manager::lowest_ts_to_retain() const {
    if (!_config.log_retention().has_value()) {
        return model::timestamp(0);
    }
    const auto now = model::timestamp::now().value();
    const auto retention = _config.log_retention().value().count();
    return model::timestamp(now - retention);
}

ss::future<> log_manager::housekeeping_loop() {
    /*
     * data older than this threshold may be garbage collected
     */
    while (true) {
        const auto prev_jitter_base = _housekeeping_jitter.base_duration();
        try {
            co_await _housekeeping_sem.wait(
              _housekeeping_jitter.next_duration(),
              std::max(_housekeeping_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // time for some chores
        }

        /*
         * if it appears that the compaction interval config changed while
         * we were sleeping then reschedule rather than run immediately.
         * this attempts to avoid thundering herd since config changes are
         * delivered immediately to all shards.
         */
        if (_housekeeping_jitter.base_duration() != prev_jitter_base) {
            continue;
        }

        /*
         * Perform compaction. Additional scheduling heuristics will be added
         * here, including:
         *
         * - Logs can be compacted in order of most space savings first, but the
         *   estimation will be harder, most likely based on recent compaction
         *   ratio acehived.
         *
         * - It may be wise to skip compaction completely in extreme low-disk
         *   situations because the compaction process itself requires
         *   additional disk space to stage new segments and indices.
         *
         * - Enhance the `disk_usage` interface to estimate when new data will
         *   become reclaimable and cancel non-impactful housekeeping work.
         *
         * - Early out compaction process if a new disk space alert arrives
         */
        try {
            co_await housekeeping_scan(lowest_ts_to_retain());
        } catch (...) {
            auto eptr = std::current_exception();
            if (ssx::is_shutdown_exception(eptr)) {
                std::rethrow_exception(eptr);
            }
            vlog(stlog.warn, "Error processing housekeeping(): {}", eptr);
        }
    }
}

ss::future<> log_manager::gc_loop() {
    /*
     * data older than this threshold may be garbage collected
     */
    while (true) {
        co_await _gc_sem.wait(std::max(_gc_sem.current(), size_t(1)));

        /*
         * When we are in a low disk space situation we would like to reclaim
         * data as fast as possible since being in that state may cause all
         * sorts of problems like blocking producers, or even crashing the
         * system. The fastest way to do this is to apply retention rules to
         * partitions in the order of most to least amount of reclaimable data.
         * This amount can be estimated using the log::disk_usage(gc_config)
         * interface.
         */
        if (
          _gc_triggered || _disk_space_alert == disk_space_alert::degraded
          || _disk_space_alert == disk_space_alert::low_space) {
            // it is expected that callers set the flag whenever they want the
            // next round of housekeeping to priortize gc.
            _gc_triggered = false;
            _probe->urgent_gc_run();

            /*
             * build a schedule of partitions to gc ordered by amount of
             * estimated reclaimable space. since logs may be asynchronously
             * deleted doing this safely is tricky.
             */
            absl::btree_multimap<size_t, model::ntp, std::greater<>>
              ntp_by_gc_size;

            /*
             * first we build a collection of ntp's as their estimated
             * reclaimable disk space. the loop and disk_usage() are safe
             * against concurrent log removals.
             */
            for (auto& log_meta : _logs_list) {
                /*
                 * applying segment.ms will make reclaimable data from the
                 * active segment visible.
                 */

                // Hold the housekeeping gate to prevent issues with concurrent
                // removal of the log meta.
                auto gate = log_meta.housekeeping_gate.hold();

                auto housekeeping_lock_holder
                  = co_await log_meta.housekeeping_lock.get_units();
                if (!log_meta.link.is_linked()) {
                    continue;
                }

                co_await log_meta.handle->apply_segment_ms();
                if (!log_meta.link.is_linked()) {
                    continue;
                }

                auto ntp = log_meta.handle->config().ntp();
                auto usage = co_await log_meta.handle->disk_usage(
                  gc_config(lowest_ts_to_retain(), _config.retention_bytes()));

                /*
                 * NOTE: this estimate is for local retention policy only. for a
                 * policy that considers removing data uploaded to the cloud,
                 * this estimate is available in `usage.reclaim.available`.
                 */
                ntp_by_gc_size.emplace(usage.reclaim.retention, std::move(ntp));
            }

            /*
             * Since we don't hold on to a per-log gate after we've recorded it
             * in the container above, we need to lookup the ntp by name in the
             * official log registry to avoid problems with concurrent removals
             * since the log interface does not tolerate ops on closed logs.
             */
            static constexpr size_t max_concurrent_gc = 20;
            co_await ss::max_concurrent_for_each(
              ntp_by_gc_size.begin(),
              ntp_by_gc_size.end(),
              max_concurrent_gc,
              [this](const auto& candidate) {
                  auto* log_meta = get_log_meta(candidate.second);
                  if (!log_meta) {
                      return ss::now();
                  }

                  auto log = log_meta->handle;

                  if (!log) {
                      return ss::now();
                  }

                  // Hold the housekeeping gate to prevent issues with
                  // concurrent removal of the log meta.
                  auto gate = log_meta->housekeeping_gate.hold();

                  auto& housekeeping_lock = log_meta->housekeeping_lock;
                  auto units = housekeeping_lock.try_get_units();
                  if (!units.has_value()) {
                      return ss::now();
                  }

                  return log
                    ->gc(gc_config(
                      lowest_ts_to_retain(), _config.retention_bytes()))
                    .finally(
                      [units = std::move(units), g = std::move(gate)] {});
              });
        }
    }
}

void log_manager::clear_log_meta_flags(compaction_list_type& logs) {
    for (auto& log_meta : logs) {
        log_meta.flags &= ~(
          bflags::compacted | bflags::lifetime_checked
          | bflags::compaction_checked | bflags::should_compact);
    }
}

ss::future<> log_manager::apply_segment_ms_to_logs(compaction_list_type& logs) {
    while (!logs.empty()
           && is_not_set(logs.front().flags, bflags::lifetime_checked)) {
        if (_abort_source.abort_requested()) {
            co_return;
        }

        auto& current_log = logs.front();
        logs.shift_forward();

        current_log.flags |= bflags::lifetime_checked;

        // Hold the housekeeping gate to prevent issues with concurrent removal
        // of the log meta.
        auto gate = current_log.housekeeping_gate.hold();

        // Obtain housekeeping lock to prevent concurrency of
        // log->apply_segment_ms() with gc fibre.
        auto housekeeping_lock_holder
          = co_await current_log.housekeeping_lock.get_units();

        if (!current_log.link.is_linked()) {
            continue;
        }

        // NOTE: apply_segment_ms holds _compaction_housekeeping_gate, that
        // prevents the removal of the parent object. this makes awaiting
        // apply_segment_ms safe against removal of segments from logs
        co_await current_log.handle->apply_segment_ms();
    }
}

void log_manager::sort_logs_by_compaction_heuristic(
  compaction_list_type& logs) {
    using compaction_heuristic_t = uint64_t;
    // There can be no scheduling points between here and the sorting done
    // below, as we are holding pointers to log_housekeeping_meta in this
    // btree_map.
    // This needs to be sorted in ascending order, as we are pushing `log_meta`s
    // to the front of `logs`.
    absl::
      btree_map<compaction_heuristic_t, chunked_vector<log_housekeeping_meta*>>
        compaction_heuristic_to_log_metas;
    for (auto& log_meta : logs) {
        auto should_compact_log = [](ss::shared_ptr<log> l) {
            auto needs_compact = l->needs_compaction();
            if (!needs_compact) {
                vlog(
                  gclog.trace,
                  "{}: dirty ratio ({}) < min.cleanable.dirty.ratio ({}) and "
                  "time since earliest dirty timestamp does not exceed "
                  "max.compaction.lag.ms ({}), skipping compaction.",
                  l->config().ntp(),
                  l->dirty_ratio(),
                  l->config().min_cleanable_dirty_ratio(),
                  l->config().max_compaction_lag_ms());
            }
            return needs_compact;
        };

        const auto compact_log = should_compact_log(log_meta.handle);

        if (compact_log) {
            log_meta.flags |= bflags::should_compact;

            // Order ntps by compaction heuristic.
            // Currently, this is just the dirty ratio.
            auto compute_compaction_heuristic =
              [](ss::shared_ptr<log> l) -> compaction_heuristic_t {
                auto res = (100.0 * l->dirty_ratio());
                return static_cast<compaction_heuristic_t>(res);
            };

            auto compaction_heuristic_weight = compute_compaction_heuristic(
              log_meta.handle);
            compaction_heuristic_to_log_metas[compaction_heuristic_weight]
              .push_back(&log_meta);
        }
    }

    for (const auto& [weight, log_metas] : compaction_heuristic_to_log_metas) {
        for (auto* meta_ptr : log_metas) {
            meta_ptr->link.unlink();
            logs.push_front(*meta_ptr);
        }
    }
}

bool log_manager::is_priority_ntp(const model::ntp& ntp) const {
    return model::is_consumer_offsets_topic(ntp);
}

bool log_manager::priority_logs_need_compaction() const {
    return std::ranges::any_of(_priority_logs_list, [](const auto& l) {
        return l.handle->needs_compaction();
    });
}

ss::future<>
log_manager::priority_housekeeping_scan(model::timestamp collection_threshold) {
    // reset flags for the next two loops, segment_ms and compaction.
    clear_log_meta_flags(_priority_logs_list);

    co_await apply_segment_ms_to_logs(_priority_logs_list);

    if (_abort_source.abort_requested()) {
        co_return;
    }

    sort_logs_by_compaction_heuristic(_priority_logs_list);

    while (!_priority_logs_list.empty()
           && is_not_set(
             _priority_logs_list.front().flags, bflags::compaction_checked)) {
        if (_abort_source.abort_requested()) {
            co_return;
        }

        auto& current_log = _priority_logs_list.front();
        _priority_logs_list.shift_forward();

        current_log.flags |= bflags::compaction_checked;

        auto gate = current_log.housekeeping_gate.hold();

        if (is_not_set(current_log.flags, bflags::should_compact)) {
            // Still perform gc() here.
            auto units = current_log.housekeeping_lock.try_get_units();
            if (units.has_value()) {
                co_await current_log.handle->gc(
                  gc_config(collection_threshold, _config.retention_bytes()));
            }

            continue;
        }

        auto housekeeping_lock_holder
          = co_await current_log.housekeeping_lock.get_units();

        if (!current_log.link.is_linked()) {
            continue;
        }

        co_await do_housekeeping(current_log, collection_threshold);
    }
}

ss::future<> log_manager::do_housekeeping(
  log_housekeeping_meta& meta,
  model::timestamp collection_threshold,
  model::opt_abort_source_t preempt_source) {
    if (!meta.link.is_linked()) {
        co_return;
    }

    auto ntp = meta.handle->config().ntp();
    auto ntp_sanitizer_cfg = _config.maybe_get_ntp_sanitizer_config(ntp);

    // Until we better implement bailing out of compaction, the best thing
    // we can do for observability is add a watchdog here.
    ssx::watchdog wd5m(5min, [ntp] {
        vlog(gclog.warn, "{}: Housekeeping process exceeding 5 minutes", ntp);
    });

    // Create a composite abort source that triggers on shutdown or
    // preemption. If no preempt_source is provided, just use _abort_source
    // directly.
    std::optional<ssx::composite_abort_source> composite_as;
    auto& as = [this, &preempt_source, &composite_as]() -> ss::abort_source& {
        if (preempt_source.has_value()) {
            composite_as.emplace(_abort_source, preempt_source->get());
            return composite_as->as();
        }
        return _abort_source;
    }();

    // NOTE: housekeeping holds _compaction_housekeeping_gate, that prevents
    // the removal of the parent object. this makes awaiting housekeeping
    // safe against removal of segments from _logs_list
    auto& log = meta.handle;
    auto pinned_kafka_offset = log->stm_hookset()->lowest_pinned_data_offset();
    std::optional<model::offset> max_unpinned_offset;
    if (pinned_kafka_offset) {
        auto local_log_start = log->offsets().start_offset;
        auto kafka_local_start = model::offset_cast(
          log->from_log_offset(local_log_start));
        if (*pinned_kafka_offset >= kafka_local_start) {
            max_unpinned_offset = model::prev_offset(
              log->to_log_offset(kafka::offset_cast(*pinned_kafka_offset)));
        } else {
            max_unpinned_offset = model::prev_offset(
              log->offsets().start_offset);
        }
    }

    model::offset max_compactible_offset
      = log->stm_hookset()->max_removable_local_log_offset();
    model::offset max_tombstone_remove_offset
      = log->stm_hookset()->max_tombstone_remove_offset();
    model::offset max_tx_end_remove_offset
      = log->stm_hookset()->max_tx_end_remove_offset();
    model::offset tx_snapshot_offset = log->stm_hookset()->tx_snapshot_offset();
    // We clamp the offset up to which we can remove transactional control
    // batches to the last snapshot taken by the transactional stm. This
    // ensures that we do not remove control batches that may be needed to
    // reconstruct the state machine during recovery.
    model::offset max_tx_remove_offset = std::min(
      max_tx_end_remove_offset, tx_snapshot_offset);

    vlog(
      gclog.trace,
      "{}: max tombstone remove offset: {}, max tx remove offset: {}, max "
      "tx end remove snapshot: {}, tx_snapshot_offset: {}",
      ntp,
      max_tombstone_remove_offset,
      max_tx_remove_offset,
      max_tx_end_remove_offset,
      tx_snapshot_offset);

    if (max_unpinned_offset && *max_unpinned_offset < max_compactible_offset) {
        vlog(
          gclog.debug,
          "{}: Compaction is pinned by offset: pinned Kafka offset: {}, "
          "log offsets max unpinned {} < max removable {}",
          ntp,
          *pinned_kafka_offset,
          *max_unpinned_offset,
          max_compactible_offset);
        max_compactible_offset = *max_unpinned_offset;
    }

    co_await log->housekeeping(
      housekeeping_config::make_config(
        collection_threshold,
        _config.retention_bytes(),
        max_compactible_offset,
        max_tombstone_remove_offset,
        max_tx_remove_offset,
        log->config().delete_retention_ms(),
        log->config().delete_retention_ms(),
        log->config().min_compaction_lag_ms(),
        as,
        std::move(ntp_sanitizer_cfg),
        _compaction_hash_key_map.get()));

    meta.flags |= log_housekeeping_meta::bitflags::compacted;
    meta.last_compaction = ss::lowres_clock::now();

    _probe->housekeeping_log_processed();
}

/**
 *
 * @param read_buf_size size of underlying ss::input_stream's buffer
 */
ss::future<ss::lw_shared_ptr<segment>> log_manager::make_log_segment(
  const ntp_config& ntp,
  model::offset base_offset,
  model::term_id term,
  size_t read_buf_size,
  unsigned read_ahead,
  size_t segment_size_hint,
  record_version_type version) {
    auto gate_holder = _gate.hold();

    auto ntp_sanitizer_cfg = _config.maybe_get_ntp_sanitizer_config(ntp.ntp());

    co_return co_await make_segment(
      ntp,
      base_offset,
      term,
      version,
      read_buf_size,
      read_ahead,
      create_cache(ntp.cache_enabled()),
      _resources,
      _feature_table,
      std::move(ntp_sanitizer_cfg),
      segment_size_hint,
      _probe->get_appender_stats());
}

std::optional<batch_cache_index>
log_manager::create_cache(with_cache ntp_cache_enabled) {
    if (
      unlikely(
        _config.cache == with_cache::no
        || ntp_cache_enabled == with_cache::no)) {
        return std::nullopt;
    }

    return batch_cache_index(_batch_cache);
}

ss::future<ss::shared_ptr<log>> log_manager::manage(
  ntp_config cfg,
  raft::group_id group,
  std::vector<model::record_batch_type> translator_batch_types) {
    auto gate = _gate.hold();
    if (!translator_batch_types.empty()) {
        // Sanity check to avoid multiple logs overwriting each others'
        // translator state in the kvstore, which is keyed by group id.
        vassert(
          group != raft::group_id{},
          "When configured to translate offsets, must supply a valid group id");
    }

    auto units = co_await _resources.get_recovery_units();
    co_return co_await do_manage(
      std::move(cfg), group, std::move(translator_batch_types));
}

ss::future<> log_manager::maybe_clear_kvstore(const ntp_config& cfg) {
    if (co_await ss::file_exists(cfg.work_directory())) {
        co_return;
    }
    // directory was deleted, make sure we do not have any state in KV
    // store.
    // NOTE: this only removes state in the storage key space.
    co_await disk_log_impl::remove_kvstore_state(cfg.ntp(), _kvstore);
}

ss::future<ss::shared_ptr<log>> log_manager::do_manage(
  ntp_config cfg,
  raft::group_id group,
  std::vector<model::record_batch_type> translator_batch_types) {
    if (_config.base_dir.empty()) {
        throw std::runtime_error(
          "log_manager:: cannot have empty config.base_dir");
    }

    vassert(
      _logs.find(cfg.ntp()) == _logs.end(), "cannot double register same ntp");

    std::optional<ss::sstring> last_clean_segment;
    auto clean_iobuf = _kvstore.get(
      kvstore::key_space::storage, internal::clean_segment_key(cfg.ntp()));
    if (clean_iobuf) {
        last_clean_segment = serde::from_iobuf<internal::clean_segment_value>(
                               std::move(clean_iobuf.value()))
                               .segment_name;
    }

    co_await maybe_clear_kvstore(cfg);

    with_cache cache_enabled = cfg.cache_enabled();
    auto ntp_sanitizer_cfg = _config.maybe_get_ntp_sanitizer_config(cfg.ntp());

    auto segments = co_await recover_segments(
      partition_path(cfg),
      cfg.is_locally_compacted(),
      [this, cache_enabled] { return create_cache(cache_enabled); },
      _abort_source,
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      last_clean_segment,
      _resources,
      _feature_table,
      std::move(ntp_sanitizer_cfg));

    auto l = storage::make_disk_backed_log(
      std::move(cfg),
      group,
      *this,
      std::move(segments),
      _kvstore,
      _feature_table,
      std::move(translator_batch_types));
    auto [it, success] = _logs.emplace(
      l->config().ntp(), std::make_unique<log_housekeeping_meta>(l));

    // Add to appropriate list based on whether this is a priority NTP.
    if (is_priority_ntp(l->config().ntp())) {
        _priority_logs_list.push_back(*it->second);
        vlog(
          stlog.debug,
          "Tracking {} as priority partition for compaction",
          l->config().ntp());
    } else {
        _logs_list.push_back(*it->second);
    }

    update_log_count();
    vassert(success, "Could not keep track of:{} - concurrency issue", l);
    co_return l;
}

ss::future<> log_manager::shutdown(model::ntp ntp) {
    vlog(stlog.debug, "Asked to shutdown: {}", ntp);
    auto gate = _gate.hold();
    auto handle = _logs.extract(ntp);
    if (!handle) {
        co_return;
    }

    auto close_fut = handle->second->housekeeping_gate.close();

    co_await clean_close(handle.value().second->handle);

    co_await std::move(close_fut);
    vlog(stlog.debug, "Shutdown: {}", ntp);
}

ss::future<> log_manager::remove(model::ntp ntp) {
    vlog(stlog.info, "Asked to remove: {}", ntp);
    auto g = _gate.hold();
    auto handle = _logs.extract(ntp);
    update_log_count();
    if (!handle) {
        co_return;
    }

    auto close_fut = handle->second->housekeeping_gate.close();

    // 'ss::shared_ptr<>' make a copy
    auto lg = handle.value().second->handle;
    vlog(stlog.info, "Removing: {}", lg);
    // NOTE: it is ok to *not* externally synchronize the log here
    // because remove, takes a write lock on each individual segments
    // waiting for all of them to be closed before actually removing the
    // underlying log. If there is a background operation like
    // compaction or so, it will block correctly.
    auto ntp_dir = lg->config().work_directory();
    ss::sstring topic_dir = lg->config().topic_directory().string();
    co_await lg->remove();
    directory_walker walker;
    co_await walker.walk(
      ntp_dir, [&ntp_dir](const ss::directory_entry& de) -> ss::future<> {
          // Concurrent truncations and compactions may conflict with each
          // other, resulting in a mutual inability to use their staging files.
          // If this has happened, clean up all staging files so we can fully
          // remove the NTP directory.
          //
          // TODO: we should more consistently clean up the staging operations
          // to clean up after themselves on failure.
          static constexpr auto suffixes_to_remove = std::to_array(
            {".staging", ".cannotrecover", ".ignore_have_newer"});
          const auto should_remove = std::ranges::any_of(
            suffixes_to_remove,
            [&](const auto& v) { return de.name.ends_with(v); });

          if (should_remove) {
              // It isn't necessarily problematic to get here since we can
              // proceed with removal, but it points to a missing cleanup which
              // can be problematic for users, as it needlessly consumes space.
              // Log verbosely to make it easier to catch.
              auto file_path = fmt::format("{}/{}", ntp_dir, de.name);
              vlog(stlog.warn, "Leftover file found, removing: {}", file_path);
              return ss::remove_file(file_path);
          }
          return ss::make_ready_future<>();
      });
    co_await remove_file(ntp_dir);
    // We always dispatch topic directory deletion to core 0 as requests may
    // come from different cores
    co_await dispatch_topic_dir_deletion(topic_dir);

    co_await std::move(close_fut);
}

ss::future<> remove_orphan_partition_files(
  ss::sstring topic_directory_path,
  model::topic_namespace nt,
  ss::noncopyable_function<bool(model::ntp, partition_path::metadata)>&
    orphan_filter) {
    return directory_walker::walk(
      topic_directory_path,
      [topic_directory_path, nt, &orphan_filter](
        ss::directory_entry entry) -> ss::future<> {
          auto ntp_directory_data = partition_path::parse_partition_directory(
            entry.name);
          if (!ntp_directory_data) {
              return ss::now();
          }

          auto ntp = model::ntp(nt.ns, nt.tp, ntp_directory_data->partition_id);
          if (orphan_filter(ntp, *ntp_directory_data)) {
              auto ntp_directory = std::filesystem::path(topic_directory_path)
                                   / std::filesystem::path(entry.name);
              vlog(stlog.info, "Cleaning up ntp directory {} ", ntp_directory);
              return ss::recursive_remove_directory(ntp_directory)
                .handle_exception_type([ntp_directory](
                                         const std::filesystem::
                                           filesystem_error& err) {
                    vlog(
                      stlog.error,
                      "Exception while cleaning orphan files for {} Error: {}",
                      ntp_directory,
                      err);
                });
          }
          return ss::now();
      });
}

ss::future<> log_manager::remove_orphan_files(
  ss::sstring data_directory_path,
  absl::flat_hash_set<model::ns> namespaces,
  ss::noncopyable_function<bool(model::ntp, partition_path::metadata)>
    orphan_filter) {
    auto holder = _gate.hold();
    auto data_directory_exist = co_await ss::file_exists(data_directory_path);
    if (!data_directory_exist) {
        co_return;
    }

    for (const auto& ns : namespaces) {
        if (_gate.is_closed()) {
            co_return;
        }
        auto namespace_directory = std::filesystem::path(data_directory_path)
                                   / std::filesystem::path(ss::sstring(ns));
        auto namespace_directory_exist = co_await ss::file_exists(
          namespace_directory.string());
        if (!namespace_directory_exist) {
            continue;
        }
        co_await directory_walker::walk(
          namespace_directory.string(),
          [this, &namespace_directory, &ns, &orphan_filter](
            ss::directory_entry entry) -> ss::future<> {
              if (entry.type != ss::directory_entry_type::directory) {
                  return ss::now();
              }
              auto topic_directory = namespace_directory
                                     / std::filesystem::path(entry.name);
              return remove_orphan_partition_files(
                       topic_directory.string(),
                       model::topic_namespace(ns, model::topic(entry.name)),
                       orphan_filter)
                .then([this, topic_directory]() {
                    vlog(
                      stlog.info,
                      "Trying to clean up topic directory {} ",
                      topic_directory);
                    return dispatch_topic_dir_deletion(
                      topic_directory.string());
                })
                .handle_exception_type(
                  [](const std::filesystem::filesystem_error& err) {
                      auto lvl = err.code()
                                     == std::errc::no_such_file_or_directory
                                   ? ss::log_level::trace
                                   : ss::log_level::info;
                      vlogl(
                        stlog,
                        lvl,
                        "Exception while cleaning orphan files {}",
                        err);
                  });
          })
          .handle_exception_type(
            [](const std::filesystem::filesystem_error& err) {
                vlog(
                  stlog.error, "Exception while cleaning orphan files {}", err);
            });
    }
    co_return;
}

ss::future<> log_manager::dispatch_topic_dir_deletion(ss::sstring dir) {
    return ss::smp::submit_to(
             0,
             [dir = std::move(dir)]() mutable {
                 static thread_local ssx::mutex fs_lock{
                   "dispatch_topic_dir_deletion"};
                 return fs_lock.with([dir = std::move(dir)] {
                     return ss::file_exists(dir).then([dir](bool exists) {
                         if (!exists) {
                             return ss::now();
                         }
                         return directory_walker::empty(
                                  std::filesystem::path(dir))
                           .then([dir](bool empty) {
                               if (!empty) {
                                   return ss::now();
                               }
                               return ss::remove_file(dir);
                           });
                     });
                 });
             })
      .handle_exception_type([](const std::filesystem::filesystem_error& e) {
          // directory might have already been used by different shard,
          // just ignore the error
          if (e.code() == std::errc::directory_not_empty) {
              return ss::now();
          }
          return ss::make_exception_future<>(e);
      });
}

absl::flat_hash_set<model::ntp> log_manager::get_all_ntps() const {
    absl::flat_hash_set<model::ntp> r;
    for (const auto& p : _logs) {
        r.insert(p.first);
    }
    return r;
}
int64_t log_manager::compaction_backlog() const {
    return std::accumulate(
      _logs.begin(),
      _logs.end(),
      int64_t(0),
      [](int64_t acc, const logs_type::value_type& p) {
          return acc + p.second->handle->compaction_backlog();
      });
}

std::ostream& operator<<(std::ostream& o, const log_config& c) {
    o << "{base_dir:" << c.base_dir
      << ", max_segment.size:" << c.max_segment_size()
      << ", file_sanitize_config:" << c.file_config << ", retention_bytes:";
    if (c.retention_bytes()) {
        o << *(c.retention_bytes());
    } else {
        o << "nullopt";
    }
    return o
           << ", compaction_interval_ms:" << c.compaction_interval().count()
           << ", log_retention_ms:"
           << c.log_retention().value_or(std::chrono::milliseconds(-1)).count()
           << ", with_cache:" << c.cache << ", reclaim_opts:" << c.reclaim_opts
           << "}";
}
std::ostream& operator<<(std::ostream& o, const log_manager& m) {
    return o << "{config:" << m._config << ", logs.size:" << m._logs.size()
             << ", cache:" << m._batch_cache << "}";
}

ss::future<usage_report> log_manager::disk_usage() {
    /*
     * settings here should mirror those in housekeeping.
     *
     * TODO: this will be factored out to make the sharing of settings easier to
     * maintain.
     */
    auto cfg = default_gc_config();

    chunked_vector<ss::shared_ptr<log>> logs;
    for (auto& it : _logs) {
        logs.push_back(it.second->handle);
    }

    ss::semaphore limit(
      std::max<size_t>(
        1, config::shard_local_cfg().space_management_max_log_concurrency()));

    co_return co_await ss::map_reduce(
      logs.begin(),
      logs.end(),
      [&limit, cfg](ss::shared_ptr<log> log) {
          return ss::with_semaphore(
                   limit, 1, [cfg, log] { return log->disk_usage(cfg); })
            .then_wrapped([log](ss::future<usage_report> f) {
                if (f.failed()) {
                    auto e = f.get_exception();
                    vlog(
                      gclog.warn,
                      "Unable to collect disk usage from ntp {}: {}",
                      log->config().ntp(),
                      e);
                    return ss::make_ready_future<usage_report>();
                } else {
                    return f;
                }
            });
      },
      usage_report{},
      [](usage_report acc, usage_report update) { return acc + update; });
}

/*
 *
 */
void log_manager::handle_disk_notification(storage::disk_space_alert alert) {
    /*
     * early debounce: disk alerts are delivered periodically by the health
     * manager even when the state doesn't change. only pass on new states.
     * beware of flapping alerts here. we only deliver new, non-ok alerts, which
     * should help, but may prove to be insufficient.
     */
    if (_disk_space_alert != alert) {
        _disk_space_alert = alert;
        if (alert != disk_space_alert::ok) {
            _gc_sem.signal();
        }
    }
}

void log_manager::trigger_gc() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::sleep_abortable(
                 _trigger_gc_jitter.next_duration(), _abort_source)
          .then([this] {
              _gc_triggered = true;
              _gc_sem.signal();
          });
    });
}

gc_config log_manager::default_gc_config() const {
    model::timestamp collection_threshold;
    if (!_config.log_retention()) {
        collection_threshold = model::timestamp(0);
    } else {
        collection_threshold = model::timestamp(
          model::timestamp::now().value() - _config.log_retention()->count());
    }
    return {collection_threshold, _config.retention_bytes()};
}

void log_manager::update_log_count() {
    auto count = _logs.size();

    _resources.update_partition_count(count);
    _probe->set_log_count(count);
}

} // namespace storage
