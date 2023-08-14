// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/async-clear.h"
#include "ssx/future-util.h"
#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/disk_log_impl.h"
#include "storage/file_sanitizer.h"
#include "storage/fs_utils.h"
#include "storage/kvstore.h"
#include "storage/log.h"
#include "storage/logger.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/segment_set.h"
#include "storage/segment_utils.h"
#include "storage/storage_resources.h"
#include "utils/directory_walker.h"
#include "utils/gate_guard.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/util/file.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <filesystem>
#include <optional>

namespace storage {
using logs_type = absl::flat_hash_map<model::ntp, log_housekeeping_meta>;

log_config::log_config(
  ss::sstring directory,
  size_t segment_size,
  ss::io_priority_class compaction_priority,
  std::optional<file_sanitize_config> file_cfg) noexcept
  : base_dir(std::move(directory))
  , max_segment_size(config::mock_binding<size_t>(std::move(segment_size)))
  , segment_size_jitter(0) // For deterministic behavior in unit tests.
  , compacted_segment_size(config::mock_binding<size_t>(256_MiB))
  , max_compacted_segment_size(config::mock_binding<size_t>(5_GiB))
  , compaction_priority(compaction_priority)
  , retention_bytes(config::mock_binding<std::optional<size_t>>(std::nullopt))
  , compaction_interval(
      config::mock_binding<std::chrono::milliseconds>(std::chrono::minutes(10)))
  , delete_retention(
      config::mock_binding<std::optional<std::chrono::milliseconds>>(
        std::chrono::minutes(10080)))
  , file_config(std::move(file_cfg)) {}

log_config::log_config(
  ss::sstring directory,
  size_t segment_size,
  ss::io_priority_class compaction_priority,
  with_cache with,
  std::optional<file_sanitize_config> file_cfg) noexcept
  : log_config(
    std::move(directory),
    segment_size,
    compaction_priority,
    std::move(file_cfg)) {
    cache = with;
}

log_config::log_config(
  ss::sstring directory,
  config::binding<size_t> segment_size,
  config::binding<size_t> compacted_segment_size,
  config::binding<size_t> max_compacted_segment_size,
  jitter_percents segment_size_jitter,
  ss::io_priority_class compaction_priority,
  config::binding<std::optional<size_t>> ret_bytes,
  config::binding<std::chrono::milliseconds> compaction_ival,
  config::binding<std::optional<std::chrono::milliseconds>> del_ret,
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
  , compaction_priority(compaction_priority)
  , retention_bytes(std::move(ret_bytes))
  , compaction_interval(std::move(compaction_ival))
  , delete_retention(std::move(del_ret))
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
  , _jitter(_config.compaction_interval())
  , _batch_cache(config.reclaim_opts) {
    _config.compaction_interval.watch([this]() {
        _jitter = simple_time_jitter<ss::lowres_clock>{
          _config.compaction_interval()};
        _housekeeping_sem.signal();
    });
}

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
          serde::to_iobuf(internal::clean_segment_value{
            .segment_name = std::filesystem::path(clean_segment.value())
                              .filename()
                              .string()}));
    }
}

ss::future<> log_manager::start() {
    if (unlikely(config::shard_local_cfg()
                   .log_disable_housekeeping_for_tests.value())) {
        co_return;
    }
    ssx::spawn_with_gate(_open_gate, [this] { return housekeeping(); });
    co_return;
}

ss::future<> log_manager::stop() {
    _abort_source.request_abort();
    _housekeeping_sem.broken();

    co_await _open_gate.close();
    co_await ss::coroutine::parallel_for_each(
      _logs, [this](logs_type::value_type& entry) {
          return clean_close(entry.second->handle);
      });
    co_await _batch_cache.stop();
    co_await ssx::async_clear(_logs)();
}

/**
 * `housekeeping_scan` scans over every current log in a single pass.
 */
ss::future<>
log_manager::housekeeping_scan(model::timestamp collection_threshold) {
    using bflags = log_housekeeping_meta::bitflags;

    if (_logs_list.empty()) {
        co_return;
    }

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
    for (auto& log_meta : _logs_list) {
        co_await log_meta.handle->apply_segment_ms();
    }

    for (auto& log_meta : _logs_list) {
        log_meta.flags &= ~bflags::compacted;
    }

    while ((_logs_list.front().flags & bflags::compacted) == bflags::none) {
        if (_abort_source.abort_requested()) {
            co_return;
        }

        auto& current_log = _logs_list.front();

        _logs_list.pop_front();
        _logs_list.push_back(current_log);

        current_log.flags |= bflags::compacted;
        current_log.last_compaction = ss::lowres_clock::now();

        auto ntp_sanitizer_cfg = _config.maybe_get_ntp_sanitizer_config(
          current_log.handle->config().ntp());
        co_await current_log.handle->housekeeping(housekeeping_config(
          collection_threshold,
          _config.retention_bytes(),
          current_log.handle->stm_manager()->max_collectible_offset(),
          _config.compaction_priority,
          _abort_source,
          std::move(ntp_sanitizer_cfg)));

        if (_logs_list.empty()) {
            co_return;
        }

        // bail out of compaction early in order to get back to gc
        if (_gc_triggered) {
            co_return;
        }
    }
}

ss::future<> log_manager::housekeeping() {
    while (!_open_gate.is_closed()) {
        try {
            co_await housekeeping_loop();
        } catch (...) {
            /*
             * continue on shutdown exception because it may be bubbling up from
             * a partition shutting down. we only stop running housekeeping when
             * the log manager stops.
             */
            if (ssx::is_shutdown_exception(std::current_exception())) {
                continue;
            }
            vlog(
              stlog.info,
              "Error processing housekeeping(): {}",
              std::current_exception());
        }
    }
}

ss::future<> log_manager::housekeeping_loop() {
    /*
     * data older than this threshold may be garbage collected
     */
    const auto collection_threshold = [this] {
        if (!_config.delete_retention().has_value()) {
            return model::timestamp(0);
        }
        const auto now = model::timestamp::now().value();
        const auto retention = _config.delete_retention().value().count();
        return model::timestamp(now - retention);
    };

    while (true) {
        try {
            const auto prev_jitter_base = _jitter.base_duration();
            co_await _housekeeping_sem.wait(
              _jitter.next_duration(),
              std::max(_housekeeping_sem.current(), size_t(1)));

            /*
             * if it appears that the compaction interval config changed while
             * we were sleeping then reschedule rather than run immediately.
             * this attempts to avoid thundering herd since config changes are
             * delivered immediately to all shards.
             */
            if (_jitter.base_duration() != prev_jitter_base) {
                continue;
            }
        } catch (const ss::semaphore_timed_out&) {
            // time for some chores
        }

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

            /*
             * build a schedule of partitions to gc ordered by amount of
             * estimated reclaimable space. since logs may be asynchronously
             * deleted doing this safely is tricky.
             */
            absl::btree_map<size_t, model::ntp, std::greater<>> ntp_by_gc_size;

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
                co_await log_meta.handle->apply_segment_ms();
                if (!log_meta.link.is_linked()) {
                    continue;
                }

                auto ntp = log_meta.handle->config().ntp();
                auto usage = co_await log_meta.handle->disk_usage(
                  gc_config(collection_threshold(), _config.retention_bytes()));

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
            for (const auto& candidate : ntp_by_gc_size) {
                auto log = get(candidate.second);
                if (!log) {
                    continue;
                }
                co_await log->gc(
                  gc_config(collection_threshold(), _config.retention_bytes()));
            }
        }

        /*
         * Fall through for an iteration of the original housekeeping loop which
         * will perform compaction. Additional scheduling heuristics will be
         * added here, including:
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
         * - Early out compaction process if a new disk space alert arives so
         *   that we return this main scheduling loop.
         */

        auto prev_sg = co_await ss::coroutine::switch_to(_config.compaction_sg);

        try {
            co_await housekeeping_scan(collection_threshold());
        } catch (const std::exception& e) {
            vlog(stlog.info, "Error processing housekeeping(): {}", e);
        }

        co_await ss::coroutine::switch_to(prev_sg);
    }
}

/**
 *
 * @param read_buf_size size of underlying ss::input_stream's buffer
 */
ss::future<ss::lw_shared_ptr<segment>> log_manager::make_log_segment(
  const ntp_config& ntp,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  size_t read_buf_size,
  unsigned read_ahead,
  record_version_type version) {
    auto gate_holder = _open_gate.hold();

    auto ntp_sanitizer_cfg = _config.maybe_get_ntp_sanitizer_config(ntp.ntp());

    co_return co_await make_segment(
      ntp,
      base_offset,
      term,
      pc,
      version,
      read_buf_size,
      read_ahead,
      create_cache(ntp.cache_enabled()),
      _resources,
      _feature_table,
      std::move(ntp_sanitizer_cfg));
}

std::optional<batch_cache_index>
log_manager::create_cache(with_cache ntp_cache_enabled) {
    if (unlikely(
          _config.cache == with_cache::no
          || ntp_cache_enabled == with_cache::no)) {
        return std::nullopt;
    }

    return batch_cache_index(_batch_cache);
}

ss::future<ss::shared_ptr<log>> log_manager::manage(ntp_config cfg) {
    auto gate = _open_gate.hold();

    auto units = co_await _resources.get_recovery_units();
    co_return co_await do_manage(std::move(cfg));
}

ss::future<> log_manager::recover_log_state(const ntp_config& cfg) {
    return ss::file_exists(cfg.work_directory())
      .then([this,
             offset_key = internal::start_offset_key(cfg.ntp()),
             segment_key = internal::clean_segment_key(cfg.ntp())](
              bool dir_exists) {
          if (dir_exists) {
              return ss::now();
          }
          // directory was deleted, make sure we do not have any state in KV
          // store.
          return _kvstore.remove(kvstore::key_space::storage, offset_key)
            .then([this, segment_key] {
                return _kvstore.remove(
                  kvstore::key_space::storage, segment_key);
            });
      });
}

ss::future<ss::shared_ptr<log>> log_manager::do_manage(ntp_config cfg) {
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

    co_await recover_log_state(cfg);

    with_cache cache_enabled = cfg.cache_enabled();
    auto ntp_sanitizer_cfg = _config.maybe_get_ntp_sanitizer_config(cfg.ntp());

    auto segments = co_await recover_segments(
      partition_path(cfg),
      cfg.is_compacted(),
      [this, cache_enabled] { return create_cache(cache_enabled); },
      _abort_source,
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      last_clean_segment,
      _resources,
      _feature_table,
      std::move(ntp_sanitizer_cfg));

    auto l = storage::make_disk_backed_log(
      std::move(cfg), *this, std::move(segments), _kvstore, _feature_table);
    auto [it, success] = _logs.emplace(
      l->config().ntp(), std::make_unique<log_housekeeping_meta>(l));
    _logs_list.push_back(*it->second);
    _resources.update_partition_count(_logs.size());
    vassert(success, "Could not keep track of:{} - concurrency issue", l);
    co_return l;
}

ss::future<> log_manager::shutdown(model::ntp ntp) {
    vlog(stlog.debug, "Asked to shutdown: {}", ntp);
    auto gate = _open_gate.hold();
    auto handle = _logs.extract(ntp);
    if (handle.empty()) {
        co_return;
    }
    co_await clean_close(handle.mapped()->handle);
    vlog(stlog.debug, "Shutdown: {}", ntp);
}

ss::future<> log_manager::remove(model::ntp ntp) {
    vlog(stlog.info, "Asked to remove: {}", ntp);
    gate_guard g(_open_gate);
    auto handle = _logs.extract(ntp);
    _resources.update_partition_count(_logs.size());
    if (handle.empty()) {
        co_return;
    }
    // 'ss::shared_ptr<>' make a copy
    auto lg = handle.mapped()->handle;
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
          if (boost::algorithm::ends_with(de.name, ".staging")) {
              // It isn't necessarily problematic to get here since we can
              // proceed with removal, but it points to a missing cleanup which
              // can be problematic for users, as it needlessly consumes space.
              // Log verbosely to make it easier to catch.
              auto file_path = fmt::format("{}/{}", ntp_dir, de.name);
              vlog(
                stlog.error,
                "Leftover staging file found, removing: {}",
                file_path);
              return ss::remove_file(file_path);
          }
          return ss::make_ready_future<>();
      });
    co_await remove_file(ntp_dir);
    // We always dispatch topic directory deletion to core 0 as requests may
    // come from different cores
    co_await dispatch_topic_dir_deletion(topic_dir);
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
                                         std::filesystem::
                                           filesystem_error const& err) {
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
    auto data_directory_exist = co_await ss::file_exists(data_directory_path);
    if (!data_directory_exist) {
        co_return;
    }

    for (const auto& ns : namespaces) {
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
                  [](std::filesystem::filesystem_error const& err) {
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
            [](std::filesystem::filesystem_error const& err) {
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
                 static thread_local mutex fs_lock;
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
    return o << ", compaction_interval_ms:" << c.compaction_interval().count()
             << ", delete_retention_ms:"
             << c.delete_retention()
                  .value_or(std::chrono::milliseconds(-1))
                  .count()
             << ", with_cache:" << c.cache
             << ", reclaim_opts:" << c.reclaim_opts << "}";
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

    fragmented_vector<ss::shared_ptr<log>> logs;
    for (auto& it : _logs) {
        logs.push_back(it.second->handle);
    }

    co_return co_await ss::map_reduce(
      logs.begin(),
      logs.end(),
      [cfg](ss::shared_ptr<log> log) { return log->disk_usage(cfg); },
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
            _housekeeping_sem.signal();
        }
    }
}

void log_manager::trigger_gc() {
    _gc_triggered = true;
    _housekeeping_sem.signal();
}

gc_config log_manager::default_gc_config() const {
    model::timestamp collection_threshold;
    if (!_config.delete_retention()) {
        collection_threshold = model::timestamp(0);
    } else {
        collection_threshold = model::timestamp(
          model::timestamp::now().value()
          - _config.delete_retention()->count());
    }
    return {collection_threshold, _config.retention_bytes()};
}

} // namespace storage
