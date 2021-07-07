// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_manager.h"

#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "resource_mgmt/io_priority.h"
#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
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
#include "utils/directory_walker.h"
#include "utils/file_sanitizer.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <filesystem>
#include <optional>

namespace storage {
using logs_type = absl::flat_hash_map<model::ntp, log_housekeeping_meta>;

log_manager::log_manager(log_config config, kvstore& kvstore) noexcept
  : _config(std::move(config))
  , _kvstore(kvstore)
  , _jitter(_config.compaction_interval)
  , _batch_cache(config.reclaim_opts) {
    _compaction_timer.set_callback([this] { trigger_housekeeping(); });
    _compaction_timer.rearm(_jitter());
}
void log_manager::trigger_housekeeping() {
    (void)ss::with_gate(_open_gate, [this] {
        auto next_housekeeping = _jitter();
        return housekeeping().finally([this, next_housekeeping] {
            // all of these *MUST* be in the finally
            if (_open_gate.is_closed()) {
                return;
            }

            _compaction_timer.rearm(next_housekeeping);
        });
    }).handle_exception([](std::exception_ptr e) {
        vlog(stlog.info, "Error processing housekeeping(): {}", e);
    });
}

ss::future<> log_manager::stop() {
    _compaction_timer.cancel();
    _abort_source.request_abort();
    return _open_gate.close()
      .then([this] {
          return ss::parallel_for_each(_logs, [](logs_type::value_type& entry) {
              return entry.second.handle.close();
          });
      })
      .then([this] { return _batch_cache.stop(); });
}

static inline logs_type::iterator find_next_non_compacted_log(logs_type& logs) {
    using bflags = log_housekeeping_meta::bitflags;
    return std::find_if(
      logs.begin(), logs.end(), [](const logs_type::value_type& l) {
          return bflags::none == (l.second.flags & bflags::compacted);
      });
}

ss::future<> log_manager::housekeeping() {
    auto collection_threshold = model::timestamp(
      model::timestamp::now().value() - _config.delete_retention.count());
    /**
     * Note that this loop does a double find - which is not fast. This solution
     * is the tradeoff to *not* lock the segment during log_manager::remove(ntp)
     * and to *not* use an ordered container.
     *
     * The issue with keeping an iterator and effectively doing a iterator++
     * is that during a concurrent log_manager::remove() we invalidate all the
     * iterators for the absl::flat_hash_map; - an alternative here would be to
     * use a lock
     *
     * Note that we also do not use an ordered container like an absl::tree-set
     * because finds are frequent on this datastructure and we want to serve
     * them as fast as we can since the majority of the time they will be in the
     * hotpath / (request-response)
     */
    using bflags = log_housekeeping_meta::bitflags;
    return ss::do_until(
             [this] {
                 auto it = find_next_non_compacted_log(_logs);
                 return it == _logs.end();
             },
             [this, collection_threshold] {
                 return ss::with_scheduling_group(
                   _config.compaction_sg, [this, collection_threshold] {
                       auto it = find_next_non_compacted_log(_logs);
                       if (it == _logs.end()) {
                           // must check again because between the stop
                           // condition and this continuation we might have
                           // removed the log
                           return ss::now();
                       }
                       it->second.flags |= bflags::compacted;
                       it->second.last_compaction = ss::lowres_clock::now();
                       return it->second.handle.compact(compaction_config(
                         collection_threshold,
                         _config.retention_bytes,
                         _config.compaction_priority,
                         _abort_source));
                   });
             })
      .finally([this] {
          for (auto& h : _logs) {
              h.second.flags &= ~bflags::compacted;
          }
      });
}
ss::future<ss::lw_shared_ptr<segment>> log_manager::make_log_segment(
  const ntp_config& ntp,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size) {
    return ss::with_gate(
      _open_gate, [this, &ntp, base_offset, term, pc, version, buf_size] {
          return make_segment(
            ntp,
            base_offset,
            term,
            pc,
            version,
            buf_size,
            _config.sanitize_fileops,
            create_cache(ntp.cache_enabled()));
      });
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

ss::future<log> log_manager::manage(ntp_config cfg) {
    return ss::with_gate(_open_gate, [this, cfg = std::move(cfg)]() mutable {
        return do_manage(std::move(cfg));
    });
}

ss::future<> log_manager::recover_log_state(const ntp_config& cfg) {
    return ss::file_exists(cfg.work_directory())
      .then(
        [this, key = internal::start_offset_key(cfg.ntp())](bool dir_exists) {
            if (dir_exists) {
                return ss::now();
            }
            // directory was deleted, make sure we do not have any state in KV
            // store.
            return _kvstore.remove(kvstore::key_space::storage, key);
        });
}

ss::future<log> log_manager::do_manage(ntp_config cfg) {
    if (_config.base_dir.empty()) {
        return ss::make_exception_future<log>(std::runtime_error(
          "log_manager:: cannot have empty config.base_dir"));
    }

    vassert(
      _logs.find(cfg.ntp()) == _logs.end(), "cannot double register same ntp");

    if (_config.stype == log_config::storage_type::memory) {
        auto path = cfg.work_directory();
        auto l = storage::make_memory_backed_log(std::move(cfg));
        _logs.emplace(l.config().ntp(), l);
        // in-memory needs to write vote_for configuration
        return ss::recursive_touch_directory(path).then([l] { return l; });
    }

    return recover_log_state(cfg).then([this, cfg = std::move(cfg)]() mutable {
        ss::sstring path = cfg.work_directory();
        with_cache cache_enabled = cfg.cache_enabled();
        return recover_segments(
                 std::filesystem::path(path),
                 _config.sanitize_fileops,
                 cfg.is_compacted(),
                 [this, cache_enabled] { return create_cache(cache_enabled); },
                 _abort_source)
          .then([this, cfg = std::move(cfg)](segment_set segments) mutable {
              auto l = storage::make_disk_backed_log(
                std::move(cfg), *this, std::move(segments), _kvstore);
              auto [_, success] = _logs.emplace(l.config().ntp(), l);
              vassert(
                success, "Could not keep track of:{} - concurrency issue", l);
              return l;
          });
    });
}

ss::future<> log_manager::shutdown(model::ntp ntp) {
    vlog(stlog.debug, "Asked to shutdown: {}", ntp);
    return ss::with_gate(_open_gate, [this, ntp = std::move(ntp)] {
        auto handle = _logs.extract(ntp);
        if (handle.empty()) {
            return ss::make_ready_future<>();
        }
        storage::log lg = handle.mapped().handle;
        return lg.close().finally([lg] {});
    });
}

ss::future<> log_manager::remove(model::ntp ntp) {
    vlog(stlog.info, "Asked to remove: {}", ntp);
    return ss::with_gate(_open_gate, [this, ntp = std::move(ntp)] {
        auto handle = _logs.extract(ntp);
        if (handle.empty()) {
            return ss::make_ready_future<>();
        }
        // 'ss::shared_ptr<>' make a copy
        storage::log lg = handle.mapped().handle;
        vlog(stlog.info, "Removing: {}", lg);
        // NOTE: it is ok to *not* externally synchronize the log here
        // because remove, takes a write lock on each individual segments
        // waiting for all of them to be closed before actually removing the
        // underlying log. If there is a background operation like
        // compaction or so, it will block correctly.
        auto ntp_dir = lg.config().work_directory();
        ss::sstring topic_dir = lg.config().topic_directory().string();
        return lg.remove()
          .then([dir = std::move(ntp_dir)] { return ss::remove_file(dir); })
          .then([this, dir = std::move(topic_dir)]() mutable {
              // We always dispatch topic directory deletion to core 0 as
              // requests may come from different cores
              return dispatch_topic_dir_deletion(std::move(dir));
          })
          .finally([lg] {});
    });
}

ss::future<> log_manager::dispatch_topic_dir_deletion(ss::sstring dir) {
    return ss::smp::submit_to(0, [dir = std::move(dir)]() mutable {
        static thread_local mutex fs_lock;
        return fs_lock.with([dir = std::move(dir)] {
            return ss::file_exists(dir).then([dir](bool exists) {
                if (!exists) {
                    return ss::now();
                }
                return directory_walker::empty(std::filesystem::path(dir))
                  .then([dir](bool empty) {
                      if (!empty) {
                          return ss::now();
                      }
                      return ss::remove_file(dir);
                  });
            });
        });
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
          return acc + p.second.handle.compaction_backlog();
      });
}

std::ostream& operator<<(std::ostream& o, log_config::storage_type t) {
    switch (t) {
    case log_config::storage_type::memory:
        return o << "{memory}";
    case log_config::storage_type::disk:
        return o << "{disk}";
    }
    return o << "{unknown-storage-type}";
}

std::ostream& operator<<(std::ostream& o, const log_config& c) {
    o << "{type:" << c.stype << ", base_dir:" << c.base_dir
      << ", max_segment.size:" << c.max_segment_size
      << ", debug_sanitize_fileops:" << c.sanitize_fileops
      << ", retention_bytes:";
    if (c.retention_bytes) {
        o << *c.retention_bytes;
    } else {
        o << "nullopt";
    }
    return o << ", compaction_interval_ms:" << c.compaction_interval.count()
             << ", delete_reteion_ms:" << c.delete_retention.count()
             << ", with_cache:" << c.cache
             << ", relcaim_opts:" << c.reclaim_opts << "}";
}
std::ostream& operator<<(std::ostream& o, const log_manager& m) {
    return o << "{config:" << m._config << ", logs.size:" << m._logs.size()
             << ", cache:" << m._batch_cache
             << ", compaction_timer.armed:" << m._compaction_timer.armed()
             << "}";
}
} // namespace storage
