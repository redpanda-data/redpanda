#include "storage/log_manager.h"

#include "config/configuration.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/fs_utils.h"
#include "storage/log.h"
#include "storage/log_replayer.h"
#include "storage/logger.h"
#include "storage/segment.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/segment_set.h"
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

#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <filesystem>
#include <optional>

namespace storage {
using logs_type = absl::flat_hash_map<model::ntp, log_housekeeping_meta>;

log_manager::log_manager(log_config config) noexcept
  : _config(std::move(config))
  , _jitter(_config.compaction_interval)
  , _batch_cache(config.reclaim_opts) {
    _compaction_timer.set_callback([this] { trigger_housekeeping(); });
    _compaction_timer.rearm(_jitter());
}
void log_manager::trigger_housekeeping() {
    (void)ss::with_gate(_open_gate, [this] {
        auto begin_housekeeping = _jitter();
        return housekeeping().finally([this, begin_housekeeping] {
            // all of these *MUST* be in the finally
            if (_open_gate.is_closed()) {
                return;
            }
            const auto next = _jitter();
            auto duration = std::chrono::milliseconds(0);
            if (next > begin_housekeeping) {
                duration = next - begin_housekeeping;
            }
            _compaction_timer.rearm(ss::lowres_clock::now() + duration);
        });
    }).handle_exception([](std::exception_ptr e) {
        vlog(stlog.info, "Error processing housekeeping(): {}", e);
    });
}

ss::future<> log_manager::stop() {
    _compaction_timer.cancel();
    _abort_source.request_abort();
    return _open_gate.close().then([this] {
        return ss::parallel_for_each(_logs, [](logs_type::value_type& entry) {
            return entry.second.handle.close();
        });
    });
}

inline logs_type::iterator find_next_non_compacted_log(logs_type& logs) {
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
                 auto it = find_next_non_compacted_log(_logs);
                 if (it == _logs.end()) {
                     // must check again because between the stop condition
                     // and this continuation we might have removed the log
                     return ss::now();
                 }
                 it->second.flags |= bflags::compacted;
                 it->second.last_compaction = ss::lowres_clock::now();
                 return it->second.handle.compact(compaction_config(
                   collection_threshold,
                   // TODO: [ch433] - this configuration needs to be updated
                   _config.retention_bytes,
                   // TODO: change default priority in application.cc
                   ss::default_priority_class(),
                   _abort_source));
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
          return do_make_log_segment(
            ntp, base_offset, term, pc, version, buf_size);
      });
}

ss::future<ss::file> make_writer(
  const std::filesystem::path& path, log_config::debug_sanitize_files debug) {
    ss::file_open_options opt{
      /// We fallocate the full file segment
      .extent_allocation_size_hint = 0,
      /// don't allow truncate calls
      .sloppy_size = false,
    };
    return ss::file_exists(path.string()).then([opt, path, debug](bool exists) {
        const auto flags = exists ? ss::open_flags::rw
                                  : ss::open_flags::create | ss::open_flags::rw;
        return ss::open_file_dma(path.string(), flags, opt)
          .then([debug](ss::file writer) {
              if (debug) {
                  return ss::file(
                    ss::make_shared(file_io_sanitizer(std::move(writer))));
              }
              return writer;
          });
    });
}

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  log_config::debug_sanitize_files debug,
  ss::io_priority_class iopc) {
    return make_writer(path, debug).then([iopc, path](ss::file writer) {
        try {
            // NOTE: This try-catch is needed to not uncover the real
            // exception during an OOM condition, since the appender allocates
            // 1MB of memory aligned buffers
            return ss::make_ready_future<compacted_index_writer>(
              make_file_backed_compacted_index(
                path.string(),
                writer,
                iopc,
                segment_appender::write_behind_memory / 2));
        } catch (...) {
            auto e = std::current_exception();
            vlog(stlog.error, "could not allocate compacted-index: {}", e);
            return writer.close().then_wrapped([writer, e = e](ss::future<>) {
                return ss::make_exception_future<compacted_index_writer>(e);
            });
        }
    });
}

ss::future<segment_appender_ptr> make_segment_appender(
  const std::filesystem::path& path,
  log_config::debug_sanitize_files debug,
  size_t number_of_chunks,
  ss::io_priority_class iopc) {
    return make_writer(path, debug)
      .then([number_of_chunks, iopc, path](ss::file writer) {
          try {
              // NOTE: This try-catch is needed to not uncover the real
              // exception during an OOM condition, since the appender allocates
              // 1MB of memory aligned buffers
              return ss::make_ready_future<segment_appender_ptr>(
                std::make_unique<segment_appender>(
                  writer, segment_appender::options(iopc, number_of_chunks)));
          } catch (...) {
              auto e = std::current_exception();
              vlog(stlog.error, "could not allocate appender: {}", e);
              return writer.close().then_wrapped([writer, e = e](ss::future<>) {
                  return ss::make_exception_future<segment_appender_ptr>(e);
              });
          }
      });
}

size_t number_of_chunks_from_config(const ntp_config& ntpc) {
    if (!ntpc.overrides) {
        return segment_appender::chunks_no_buffer;
    }
    auto& o = *ntpc.overrides;
    if (o.compaction_strategy) {
        return segment_appender::chunks_no_buffer / 2;
    }
    return segment_appender::chunks_no_buffer;
}

template<typename Func>
auto with_segment(ss::lw_shared_ptr<segment> s, Func&& f) {
    return f(s).then_wrapped([s](
                               ss::future<ss::lw_shared_ptr<segment>> new_seg) {
        try {
            auto ptr = new_seg.get0();
            return ss::make_ready_future<ss::lw_shared_ptr<segment>>(ptr);
        } catch (...) {
            return s->close()
              .then_wrapped([e = std::current_exception()](ss::future<>) {
                  return ss::make_exception_future<ss::lw_shared_ptr<segment>>(
                    e);
              })
              .finally([s] {});
        }
    });
}

// inneficient, but easy and rare
ss::future<ss::lw_shared_ptr<segment>> log_manager::do_make_log_segment(
  const ntp_config& ntpc,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size) {
    auto path = segment_path::make_segment_path(
      _config.base_dir, ntpc.ntp, base_offset, term, version);
    vlog(stlog.info, "Creating new segment {}", path.string());
    return open_segment(path, buf_size)
      .then([this, path, &ntpc, pc](ss::lw_shared_ptr<segment> seg) {
          return with_segment(
            seg, [this, path, &ntpc, pc](ss::lw_shared_ptr<segment> seg) {
                return make_segment_appender(
                         path,
                         _config.sanitize_fileops,
                         number_of_chunks_from_config(ntpc),
                         pc)
                  .then([seg](segment_appender_ptr a) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          std::move(seg->reader()),
                          std::move(seg->index()),
                          std::move(*a),
                          std::nullopt,
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()))
                            : std::nullopt));
                  });
            });
      })
      .then([this, path, &ntpc, pc](ss::lw_shared_ptr<segment> seg) {
          if (!(ntpc.overrides && ntpc.overrides->compaction_strategy)) {
              return ss::make_ready_future<ss::lw_shared_ptr<segment>>(seg);
          }
          return with_segment(
            seg, [this, path, pc](ss::lw_shared_ptr<segment> seg) {
                auto compacted_path = path;
                compacted_path.replace_extension(".compaction_index");
                return make_compacted_index_writer(
                         compacted_path, _config.sanitize_fileops, pc)
                  .then([seg](compacted_index_writer compact) {
                      return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                        ss::make_lw_shared<segment>(
                          seg->offsets(),
                          std::move(seg->reader()),
                          std::move(seg->index()),
                          std::move(seg->appender()),
                          std::move(compact),
                          seg->has_cache()
                            ? std::optional(std::move(seg->cache()))
                            : std::nullopt));
                  });
            });
      });
}

std::optional<batch_cache_index> log_manager::create_cache() {
    if (unlikely(_config.cache == log_config::with_cache::no)) {
        return std::nullopt;
    }

    return batch_cache_index(_batch_cache);
}

// Recover the last segment. Whenever we close a segment, we will likely
// open a new one to which we will direct new writes. That new segment
// might be empty. To optimize log replay, implement #140.
static ss::future<segment_set>
do_recover(segment_set&& segments, ss::abort_source& as) {
    return ss::async([segments = std::move(segments), &as]() mutable {
        if (segments.empty() || as.abort_requested()) {
            return std::move(segments);
        }
        segment_set::underlying_t good = std::move(segments).release();
        segment_set::underlying_t to_recover;
        to_recover.push_back(std::move(good.back()));
        good.pop_back(); // always recover last segment
        auto good_end = std::partition(
          good.begin(), good.end(), [](ss::lw_shared_ptr<segment>& ss) {
              auto& s = *ss;
              try {
                  // use the segment materialize instead of going through
                  // the index directly to hydrate the max_offset state
                  return s.materialize_index().get0();
              } catch (...) {
                  vlog(
                    stlog.info,
                    "Error materializing index:{}. Recovering parent "
                    "segment:{}. Details:{}",
                    s.index().filename(),
                    s.reader().filename(),
                    std::current_exception());
              }
              return false;
          });
        std::move(
          std::move_iterator(good_end),
          std::move_iterator(good.end()),
          std::back_inserter(to_recover));
        good.erase(
          good_end,
          good.end()); // remove all the ones we copied into recover

        for (auto& s : to_recover) {
            // check for abort
            if (unlikely(as.abort_requested())) {
                return segment_set(std::move(good));
            }
            auto stat = s->reader().stat().get0();
            if (stat.st_size == 0) {
                vlog(stlog.info, "Removing empty segment: {}", s);
                s->close().get();
                ss::remove_file(s->reader().filename()).get();
                ss::remove_file(s->index().filename()).get();
                continue;
            }
            auto replayer = log_replayer(*s);
            auto recovered = replayer.recover_in_thread(
              ss::default_priority_class());
            if (!recovered) {
                vlog(stlog.info, "Unable to recover segment: {}", s);
                s->close().get();
                ss::rename_file(
                  s->reader().filename(),
                  s->reader().filename() + ".cannotrecover")
                  .get();
                continue;
            }
            s->truncate(
               recovered.last_offset.value(),
               recovered.truncate_file_pos.value())
              .get();
            // persist index
            s->index().flush().get();
            vlog(stlog.info, "Recovered: {}", s);
            good.emplace_back(std::move(s));
        }
        return segment_set(std::move(good));
    });
}

ss::future<ss::lw_shared_ptr<segment>>
log_manager::open_segment(const std::filesystem::path& path, size_t buf_size) {
    auto const meta = segment_path::parse_segment_filename(
      path.filename().string());
    if (!meta || meta->version != record_version_type::v1) {
        return ss::make_exception_future<ss::lw_shared_ptr<segment>>(
          std::runtime_error(fmt::format(
            "Segment has invalid version {} != {} path {}",
            meta->version,
            record_version_type::v1,
            path)));
    }
    // note: this file _must_ be open in `ro` mode only. Seastar uses dma
    // files with no shared buffer cache around them. When we use a writer
    // w/ dma at the same time as the reader, we need a way to synchronize
    // filesytem metadata. In order to prevent expensive synchronization
    // primitives fsyncing both *reads* and *writes* we open this file in ro
    // mode and if raft requires truncation, we open yet-another handle w/
    // rw mode just for the truncation which gives us all the benefits of
    // preventing x-file synchronization This is fine, because truncation to
    // sealed segments are supposed to be very rare events. The hotpath of
    // truncating the appender, is optimized.
    return ss::open_file_dma(
             path.string(), ss::open_flags::ro | ss::open_flags::create)
      .then([](ss::file f) {
          return f.stat().then([f](struct stat s) {
              return ss::make_ready_future<uint64_t, ss::file>(s.st_size, f);
          });
      })
      .then([this, buf_size, path](uint64_t size, ss::file fd) {
          if (_config.sanitize_fileops) {
              fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
          }
          return std::make_unique<segment_reader>(
            path.string(), std::move(fd), size, buf_size);
      })
      .then([this, meta](std::unique_ptr<segment_reader> rdr) {
          auto ptr = rdr.get();
          auto index_name = std::filesystem::path(ptr->filename().c_str())
                              .replace_extension("base_index")
                              .string();
          return ss::open_file_dma(
                   index_name, ss::open_flags::create | ss::open_flags::rw)
            .then_wrapped([this, ptr, rdr = std::move(rdr), index_name, meta](
                            ss::future<ss::file> f) mutable {
                ss::file fd;
                try {
                    fd = f.get0();
                } catch (...) {
                    return ptr->close().then(
                      [rdr = std::move(rdr), e = std::current_exception()] {
                          return ss::make_exception_future<
                            ss::lw_shared_ptr<segment>>(e);
                      });
                }
                if (_config.sanitize_fileops) {
                    fd = ss::file(
                      ss::make_shared(file_io_sanitizer(std::move(fd))));
                }
                auto idx = segment_index(
                  index_name,
                  fd,
                  meta->base_offset,
                  segment_index::default_data_buffer_step);
                return ss::make_ready_future<ss::lw_shared_ptr<segment>>(
                  ss::make_lw_shared<segment>(
                    segment::offset_tracker(meta->term, meta->base_offset),
                    std::move(*rdr),
                    std::move(idx),
                    std::nullopt,
                    std::nullopt,
                    create_cache()));
            });
      });
}

ss::future<segment_set::underlying_t>
log_manager::open_segments(ss::sstring dir) {
    using segs_type = segment_set::underlying_t;
    return ss::do_with(
      segs_type{}, [this, dir = std::move(dir)](segs_type& segs) {
          auto f = directory_walker::walk(
            dir, [this, dir, &segs](ss::directory_entry seg) {
                // abort if requested
                if (_abort_source.abort_requested()) {
                    return ss::now();
                }
                /*
                 * Skip non-regular files (including links)
                 */
                if (
                  !seg.type || *seg.type != ss::directory_entry_type::regular) {
                    return ss::make_ready_future<>();
                }
                auto path = std::filesystem::path(
                  fmt::format("{}/{}", dir, seg.name));
                try {
                    auto is_valid = segment_path::parse_segment_filename(
                      path.filename().string());
                    if (!is_valid) {
                        return ss::make_ready_future<>();
                    }
                } catch (...) {
                    // not a reader filename
                    return ss::make_ready_future<>();
                }
                return open_segment(std::move(path))
                  .then([&segs](ss::lw_shared_ptr<segment> p) {
                      segs.push_back(std::move(p));
                  });
            });
          /*
           * if the directory walker returns an exceptional future then all
           * the segment readers that were created are cleaned up by
           * ss::do_with.
           */
          return f.then([&segs]() mutable {
              return ss::make_ready_future<segs_type>(std::move(segs));
          });
      });
}

ss::future<log> log_manager::manage(ntp_config cfg) {
    return ss::with_gate(_open_gate, [this, cfg = std::move(cfg)]() mutable {
        return do_manage(std::move(cfg));
    });
}

ss::future<segment_set>
log_manager::recover_segments(std::filesystem::path path) {
    return ss::recursive_touch_directory(path.string())
      .then(
        [this, path = std::move(path)] { return open_segments(path.string()); })
      .then([this](segment_set::underlying_t segs) {
          auto segments = segment_set(std::move(segs));
          return do_recover(std::move(segments), _abort_source);
      });
}

ss::future<log> log_manager::do_manage(ntp_config cfg) {
    if (_config.base_dir.empty()) {
        return ss::make_exception_future<log>(std::runtime_error(
          "log_manager:: cannot have empty config.base_dir"));
    }
    ss::sstring path = cfg.work_directory();
    vassert(
      _logs.find(cfg.ntp) == _logs.end(), "cannot double register same ntp");
    if (_config.stype == log_config::storage_type::memory) {
        auto l = storage::make_memory_backed_log(std::move(cfg));
        _logs.emplace(l.config().ntp, l);
        // in-memory needs to write vote_for configuration
        return ss::recursive_touch_directory(path).then([l] { return l; });
    }
    return recover_segments(std::filesystem::path(path))
      .then([this, cfg = std::move(cfg)](segment_set segments) mutable {
          auto l = storage::make_disk_backed_log(
            std::move(cfg), *this, std::move(segments));
          _logs.emplace(l.config().ntp, l);
          return l;
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
        return lg.remove().finally([lg] {});
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
