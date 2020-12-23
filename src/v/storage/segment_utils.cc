// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_utils.h"

#include "bytes/iobuf_parser.h"
#include "likely.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/index_state.h"
#include "storage/lock_manager.h"
#include "storage/log_reader.h"
#include "storage/logger.h"
#include "storage/parser_utils.h"
#include "storage/segment.h"
#include "units.h"
#include "utils/file_sanitizer.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <fmt/core.h>
#include <roaring/roaring.hh>

namespace storage::internal {
using namespace storage; // NOLINT

inline std::filesystem::path
data_segment_staging_name(const ss::lw_shared_ptr<segment>& s) {
    return std::filesystem::path(
      fmt::format("{}.staging", s->reader().filename()));
}

static inline ss::file wrap_handle(ss::file f, debug_sanitize_files debug) {
    if (debug) {
        return ss::file(ss::make_shared(file_io_sanitizer(std::move(f))));
    }
    return f;
}
static inline ss::future<ss::file> make_handle(
  const std::filesystem::path& path,
  ss::open_flags flags,
  ss::file_open_options opt,
  debug_sanitize_files debug) {
    return ss::open_file_dma(path.string(), flags, opt)
      .then([debug](ss::file writer) {
          return wrap_handle(std::move(writer), debug);
      });
}

static inline ss::file_open_options writer_opts() {
    return ss::file_open_options{
      /// We fallocate the full file segment
      .extent_allocation_size_hint = 0,
      /// don't allow truncate calls
      .sloppy_size = false,
    };
}

/// make file handle with default opts
ss::future<ss::file> make_writer_handle(
  const std::filesystem::path& path, storage::debug_sanitize_files debug) {
    return make_handle(
      path, ss::open_flags::rw | ss::open_flags::create, writer_opts(), debug);
}
/// make file handle with default opts
ss::future<ss::file> make_reader_handle(
  const std::filesystem::path& path, storage::debug_sanitize_files debug) {
    return make_handle(
      path,
      ss::open_flags::ro | ss::open_flags::create,
      ss::file_open_options{},
      debug);
}

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  debug_sanitize_files debug,
  ss::io_priority_class iopc) {
    return internal::make_writer_handle(path, debug)
      .then([iopc, path](ss::file writer) {
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
  debug_sanitize_files debug,
  size_t number_of_chunks,
  ss::io_priority_class iopc) {
    return internal::make_writer_handle(path, debug)
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
    if (!ntpc.has_overrides()) {
        return segment_appender::chunks_no_buffer;
    }
    auto& o = ntpc.get_overrides();
    if (o.compaction_strategy) {
        return segment_appender::chunks_no_buffer / 2;
    }
    return segment_appender::chunks_no_buffer;
}

ss::future<Roaring>
natural_index_of_entries_to_keep(compacted_index_reader reader) {
    reader.reset();
    return reader.consume(compaction_key_reducer(), model::no_timeout);
}

ss::future<> copy_filtered_entries(
  compacted_index_reader reader,
  Roaring to_copy_index,
  compacted_index_writer writer) {
    return ss::do_with(
      std::move(writer),
      [bm = std::move(to_copy_index),
       reader](compacted_index_writer& writer) mutable {
          reader.reset();
          return reader
            .consume(
              index_filtered_copy_reducer(std::move(bm), writer),
              model::no_timeout)
            // must be last
            .finally([&writer] {
                writer.set_flag(compacted_index::footer_flags::self_compaction);
                // do not handle exception on the close
                return writer.close();
            });
      });
}

static ss::future<> do_write_clean_compacted_index(
  compacted_index_reader reader, compaction_config cfg) {
    return natural_index_of_entries_to_keep(reader).then([reader,
                                                          cfg](Roaring bitmap) {
        const auto tmpname = std::filesystem::path(
          fmt::format("{}.staging", reader.filename()));
        return make_handle(
                 tmpname,
                 ss::open_flags::rw | ss::open_flags::truncate
                   | ss::open_flags::create,
                 writer_opts(),
                 cfg.sanitize)
          .then(
            [tmpname, cfg, reader, bm = std::move(bitmap)](ss::file f) mutable {
                auto writer = make_file_backed_compacted_index(
                  tmpname.string(),
                  std::move(f),
                  cfg.iopc,
                  // TODO: pass this memory from the cfg
                  segment_appender::write_behind_memory / 2);
                return copy_filtered_entries(
                  reader, std::move(bm), std::move(writer));
            })
          .then([old_name = tmpname.string(), new_name = reader.filename()] {
              // from glibc: If oldname is not a directory, then any
              // existing file named newname is removed during the
              // renaming operation
              return ss::rename_file(old_name, new_name);
          });
    });
}

ss::future<> write_clean_compacted_index(
  compacted_index_reader reader, compaction_config cfg) {
    // integrity verified in `do_detect_compaction_index_state`
    return do_write_clean_compacted_index(reader, cfg)
      .finally([reader]() mutable {
          return reader.close().then_wrapped(
            [reader](ss::future<>) { /*ignore*/ });
      });
}
ss::future<compacted_index::recovery_state> do_detect_compaction_index_state(
  std::filesystem::path p, compaction_config cfg) {
    using flags = compacted_index::footer_flags;
    return make_reader_handle(p, cfg.sanitize)
      .then([cfg, p](ss::file f) {
          return make_file_backed_compacted_reader(
            p.string(), std::move(f), cfg.iopc, 64_KiB);
      })
      .then([](compacted_index_reader reader) {
          return reader.verify_integrity()
            .then([reader]() mutable { return reader.load_footer(); })
            .then([](compacted_index::footer footer) {
                if (bool(footer.flags & flags::self_compaction)) {
                    return compacted_index::recovery_state::recovered;
                }
                return compacted_index::recovery_state::nonrecovered;
            })
            .finally([reader]() mutable { return reader.close(); });
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            stlog.warn,
            "detected error while attempting recovery, {}. marking as 'needs "
            "rebuild'. Common situation during crashes or hard shutdowns.",
            e);
          return compacted_index::recovery_state::needsrebuild;
      });
}

ss::future<compacted_index::recovery_state>
detect_compaction_index_state(std::filesystem::path p, compaction_config cfg) {
    return ss::file_exists(p.string()).then([p, cfg](bool exists) {
        if (exists) {
            return do_detect_compaction_index_state(p, cfg);
        }
        return ss::make_ready_future<compacted_index::recovery_state>(
          compacted_index::recovery_state::missing);
    });
}

ss::future<compacted_offset_list>
generate_compacted_list(model::offset o, compacted_index_reader reader) {
    reader.reset();
    return reader.consume(compacted_offset_list_reducer(o), model::no_timeout)
      .finally([reader] {});
}

ss::future<>
do_compact_segment_index(ss::lw_shared_ptr<segment> s, compaction_config cfg) {
    auto compacted_path = std::filesystem::path(s->reader().filename());
    compacted_path.replace_extension(".compaction_index");
    vlog(stlog.trace, "compacting index:{}", compacted_path);
    return make_reader_handle(compacted_path, cfg.sanitize)
      .then([cfg, compacted_path, s](ss::file f) {
          auto reader = make_file_backed_compacted_reader(
            compacted_path.string(), std::move(f), cfg.iopc, 64_KiB);
          return write_clean_compacted_index(reader, cfg);
      });
}
ss::future<storage::index_state> do_copy_segment_data(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  storage::probe& pb,
  ss::rwlock::holder h) {
    auto idx_path = std::filesystem::path(s->reader().filename());
    idx_path.replace_extension(".compaction_index");
    return make_reader_handle(idx_path, cfg.sanitize)
      .then([s, cfg, idx_path](ss::file f) {
          auto reader = make_file_backed_compacted_reader(
            idx_path.string(), std::move(f), cfg.iopc, 64_KiB);
          return generate_compacted_list(s->offsets().base_offset, reader)
            .finally([reader]() mutable {
                return reader.close().then_wrapped([](ss::future<>) {});
            });
      })
      .then(
        [cfg, s, &pb, h = std::move(h)](compacted_offset_list list) mutable {
            const auto tmpname = data_segment_staging_name(s);
            return make_segment_appender(
                     tmpname,
                     cfg.sanitize,
                     segment_appender::chunks_no_buffer,
                     cfg.iopc)
              .then([l = std::move(list), &pb, h = std::move(h), cfg, s](
                      segment_appender_ptr w) mutable {
                  auto raw = w.get();
                  auto red = copy_data_segment_reducer(std::move(l), raw);
                  auto r = create_segment_full_reader(s, cfg, pb, std::move(h));
                  return std::move(r)
                    .consume(std::move(red), model::no_timeout)
                    .finally([raw, w = std::move(w)]() mutable {
                        return raw->close()
                          .handle_exception([](std::exception_ptr e) {
                              vlog(
                                stlog.error,
                                "Error copying index to new segment:{}",
                                e);
                          })
                          .finally([w = std::move(w)] {});
                    });
              });
        });
}

model::record_batch_reader create_segment_full_reader(
  ss::lw_shared_ptr<storage::segment> s,
  storage::compaction_config cfg,
  storage::probe& pb,
  ss::rwlock::holder h) {
    auto o = s->offsets();
    auto reader_cfg = log_reader_config(
      o.base_offset, o.dirty_offset, cfg.iopc);
    reader_cfg.skip_batch_cache = true;
    segment_set::underlying_t set;
    set.reserve(1);
    set.push_back(s);
    auto lease = std::make_unique<lock_manager::lease>(
      segment_set(std::move(set)));
    lease->locks.push_back(std::move(h));
    return model::make_record_batch_reader<log_reader>(
      std::move(lease), reader_cfg, pb);
}

ss::future<> do_swap_data_file_handles(
  std::filesystem::path compacted,
  ss::lw_shared_ptr<storage::segment> s,
  storage::compaction_config cfg,
  probe& pb) {
    return s->reader()
      .close()
      .then([compacted, s] {
          ss::sstring old_name = compacted.string();
          return ss::rename_file(old_name, s->reader().filename());
      })
      .then([s, cfg] {
          auto to_open = std::filesystem::path(s->reader().filename().c_str());
          return make_reader_handle(to_open, cfg.sanitize);
      })
      .then([s, &pb](ss::file f) mutable {
          return f.stat()
            .then([f](struct stat s) {
                return ss::make_ready_future<std::tuple<uint64_t, ss::file>>(
                  std::make_tuple(s.st_size, f));
            })
            .then([s, &pb](std::tuple<uint64_t, ss::file> t) {
                auto& [size, fd] = t;
                auto r = segment_reader(
                  s->reader().filename(),
                  std::move(fd),
                  size,
                  default_segment_readahead_size);
                // update partition size probe
                pb.delete_segment(*s.get());
                std::swap(s->reader(), r);
                pb.add_initial_segment(*s.get());
            });
      });
}

ss::future<> do_self_compact_segment(
  ss::lw_shared_ptr<segment> s, compaction_config cfg, storage::probe& pb) {
    return s->read_lock()
      .then([cfg, s, &pb](ss::rwlock::holder h) {
          if (s->is_closed()) {
              return ss::make_exception_future<index_state>(
                segment_closed_exception());
          }

          return do_compact_segment_index(s, cfg)
            // copy the bytes after segment is good - note that we
            // need to do it with the READ-lock, not the write lock
            .then([cfg, s, h = std::move(h), &pb]() mutable {
                return do_copy_segment_data(s, cfg, pb, std::move(h));
            });
      })
      .then([s](storage::index_state idx) {
          return s->write_lock().then(
            [s, idx = std::move(idx)](ss::rwlock::holder h) mutable {
                using type = std::tuple<index_state, ss::rwlock::holder>;
                if (s->is_closed()) {
                    return ss::make_exception_future<type>(
                      segment_closed_exception());
                }
                return ss::make_ready_future<type>(
                  std::make_tuple(std::move(idx), std::move(h)));
            });
      })
      .then([cfg, s, &pb](std::tuple<index_state, ss::rwlock::holder> h) {
          return s->index()
            .drop_all_data()
            .then([s, cfg, &pb] {
                auto compacted_file = data_segment_staging_name(s);
                return do_swap_data_file_handles(compacted_file, s, cfg, pb);
            })
            .then([h = std::move(h), s]() mutable {
                auto& [idx, lock] = h;
                s->index().swap_index_state(std::move(idx));
                s->force_set_commit_offset_from_index();
                // FIXME(noah): crashes if we evic the cache
                // s->cache().purge();
                return s->index().flush().finally([l = std::move(lock)] {});
            });
      });
}

ss::future<> rebuild_compaction_index(
  model::record_batch_reader rdr,
  std::filesystem::path p,
  compaction_config cfg) {
    return make_compacted_index_writer(p, cfg.sanitize, cfg.iopc)
      .then([r = std::move(rdr)](compacted_index_writer w) mutable {
          auto u = std::make_unique<compacted_index_writer>(std::move(w));
          auto ptr = u.get();
          return std::move(r)
            .consume(index_rebuilder_reducer(ptr), model::no_timeout)
            .then_wrapped([x = std::move(u)](ss::future<> fut) mutable {
                return x->close()
                  .handle_exception([](std::exception_ptr e) {
                      vlog(stlog.warn, "error closing compacted index:{}", e);
                  })
                  .then([f = std::move(fut), x = std::move(x)]() mutable {
                      return std::move(f);
                  });
            });
      });
}

ss::future<> self_compact_segment(
  ss::lw_shared_ptr<segment> s, compaction_config cfg, storage::probe& pb) {
    if (s->has_appender()) {
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "Cannot compact an active segment. cfg:{} - segment:{}", cfg, s)));
    }
    if (s->finished_self_compaction()) {
        return ss::now();
    }
    auto idx_path = std::filesystem::path(s->reader().filename());
    idx_path.replace_extension(".compaction_index");
    return detect_compaction_index_state(idx_path, cfg)
      .then(
        [idx_path, s, cfg, &pb](compacted_index::recovery_state state) mutable {
            switch (state) {
            case compacted_index::recovery_state::recovered: {
                vlog(stlog.info, "detected {} is already compacted", idx_path);
                return ss::now();
            }
            case compacted_index::recovery_state::nonrecovered:
                return do_self_compact_segment(s, cfg, pb);
            case compacted_index::recovery_state::missing:
                [[fallthrough]];
            case compacted_index::recovery_state::needsrebuild: {
                vlog(
                  stlog.warn,
                  "Detected corrupt or missing index file:{}, recovering...",
                  idx_path);
                pb.corrupted_compaction_index();
                return s->read_lock()
                  .then([s, cfg, &pb, idx_path](ss::rwlock::holder h) {
                      return rebuild_compaction_index(
                        create_segment_full_reader(s, cfg, pb, std::move(h)),
                        idx_path,
                        cfg);
                  })
                  .then([s, cfg, &pb, idx_path] {
                      vlog(
                        stlog.info,
                        "recovered index: {}, attempting compaction again",
                        idx_path);
                      return self_compact_segment(s, cfg, pb);
                  });
            }
            default:
                __builtin_unreachable();
            }
        })
      .then([s] { s->mark_as_finished_self_compaction(); })
      .finally([&pb] { pb.segment_compacted(); });
}

std::filesystem::path compacted_index_path(std::filesystem::path segment_path) {
    return segment_path.replace_extension(".compaction_index");
}

size_t jitter_segment_size(size_t sz, jitter_percents jitter_percents) {
    vassert(
      jitter_percents >= 0 || jitter_percents <= 100,
      "jitter percents should be in range [0,100]. Requested {}",
      jitter_percents);
    // multiply by 10 to increase resolution
    auto p = jitter_percents() * 10;
    auto jit = random_generators::get_int<int64_t>(-p, p);

    int64_t jitter = jit * static_cast<int64_t>(sz) / 1000;
    return jitter + sz;
}

bytes start_offset_key(model::ntp ntp) {
    iobuf buf;
    reflection::serialize(buf, kvstore_key_type::start_offset, std::move(ntp));
    return iobuf_to_bytes(buf);
}

} // namespace storage::internal
