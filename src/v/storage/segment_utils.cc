// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_utils.h"

#include "bytes/iobuf_parser.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "ssx/future-util.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/fwd.h"
#include "storage/index_state.h"
#include "storage/lock_manager.h"
#include "storage/log_reader.h"
#include "storage/logger.h"
#include "storage/ntp_config.h"
#include "storage/parser_utils.h"
#include "storage/segment.h"
#include "storage/types.h"
#include "units.h"
#include "utils/file_sanitizer.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <roaring/roaring.hh>

template<>
struct fmt::formatter<storage::compacted_index::recovery_state> {
    using recovery_state = storage::compacted_index::recovery_state;
    constexpr auto parse(format_parse_context& ctx) { return ctx.end(); }
    template<typename FormatContext>
    auto format(const recovery_state& s, FormatContext& ctx) const {
        std::string_view str = "unknown";
        switch (s) {
        case recovery_state::missing:
            str = "missing";
            break;
        case recovery_state::needsrebuild:
            str = "needsrebuild";
            break;
        case recovery_state::recovered:
            str = "recovered";
            break;
        case recovery_state::nonrecovered:
            str = "nonrecovered";
            break;
        }
        return format_to(ctx.out(), "{}", str);
    }
};

namespace storage::internal {
using namespace storage; // NOLINT

inline std::filesystem::path
data_segment_staging_name(const ss::lw_shared_ptr<segment>& s) {
    return std::filesystem::path(
      fmt::format("{}.staging", s->reader().filename()));
}

/// Check if the file is on BTRFS, and disable copy-on-write if so.  COW
/// is not useful for logs and can cause issues.
static ss::future<>
maybe_disable_cow(const std::filesystem::path& path, ss::file& file) {
    if (co_await ss::file_system_at(path.string()) == ss::fs_type::btrfs) {
        try {
            int flags = -1;
            // ss::syscall_result throws on errors, so not checking returns.
            co_await file.ioctl(FS_IOC_GETFLAGS, (void*)&flags);
            if ((flags & FS_NOCOW_FL) == 0) {
                flags |= FS_NOCOW_FL;
                co_await file.ioctl(FS_IOC_SETFLAGS, (void*)&flags);
                vlog(stlog.trace, "Disabled COW on BTRFS segment {}", path);
            }
        } catch (std::system_error& e) {
            // Non-fatal, user will just get degraded behaviour
            // when btrfs tries to COW on a journal.
            vlog(stlog.info, "Error disabling COW on {}: {}", path, e);
        }
    }
}

static inline ss::file wrap_handle(ss::file f, debug_sanitize_files debug) {
    if (debug) {
        return ss::file(ss::make_shared(file_io_sanitizer(std::move(f))));
    }
    return f;
}

ss::future<ss::file> make_handle(
  const std::filesystem::path path,
  ss::open_flags flags,
  ss::file_open_options opt,
  debug_sanitize_files debug) {
    auto file = co_await ss::open_file_dma(path.string(), flags, opt);

    if ((flags & ss::open_flags::create) == ss::open_flags::create) {
        co_await maybe_disable_cow(path, file);
    }

    co_return wrap_handle(std::move(file), debug);
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
  ss::io_priority_class iopc,
  config::binding<size_t> fallocate_size) {
    return internal::make_writer_handle(path, debug)
      .then([number_of_chunks, iopc, path, fallocate_size](ss::file writer) {
          try {
              // NOTE: This try-catch is needed to not uncover the real
              // exception during an OOM condition, since the appender allocates
              // 1MB of memory aligned buffers
              return ss::make_ready_future<segment_appender_ptr>(
                std::make_unique<segment_appender>(
                  writer,
                  segment_appender::options(
                    iopc, number_of_chunks, fallocate_size)));
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
    auto def = segment_appender::write_behind_memory
               / config::shard_local_cfg().append_chunk_size();

    if (!ntpc.has_overrides()) {
        return def;
    }
    auto& o = ntpc.get_overrides();
    if (o.compaction_strategy) {
        return def / 2;
    }
    return def;
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
    vlog(gclog.trace, "compacting segment compaction index:{}", compacted_path);
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
      .then([cfg, s, &pb, h = std::move(h)](
              compacted_offset_list list) mutable {
          const auto tmpname = data_segment_staging_name(s);
          return make_segment_appender(
                   tmpname,
                   cfg.sanitize,
                   segment_appender::write_behind_memory
                     / config::shard_local_cfg().append_chunk_size(),
                   cfg.iopc,
                   config::shard_local_cfg().segment_fallocation_step.bind())
            .then([l = std::move(list), &pb, h = std::move(h), cfg, s, tmpname](
                    segment_appender_ptr w) mutable {
                auto raw = w.get();
                auto red = copy_data_segment_reducer(std::move(l), raw);
                auto r = create_segment_full_reader(s, cfg, pb, std::move(h));
                vlog(
                  gclog.trace,
                  "copying compacted segment data from {} to {}",
                  s->reader().filename(),
                  tmpname);
                return std::move(r)
                  .consume(std::move(red), model::no_timeout)
                  .finally([raw, w = std::move(w)]() mutable {
                      return raw->close()
                        .handle_exception([](std::exception_ptr e) {
                            vlog(
                              gclog.error,
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
    co_await s->reader().close();

    ss::sstring old_name = compacted.string();
    vlog(
      gclog.trace,
      "swapping compacted segment temp file {} with the segment {}",
      old_name,
      s->reader().filename());
    co_await ss::rename_file(old_name, s->reader().filename());

    auto r = segment_reader(
      s->reader().filename(),
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      cfg.sanitize);
    co_await r.load_size();

    // update partition size probe
    pb.delete_segment(*s.get());
    std::swap(s->reader(), r);
    pb.add_initial_segment(*s.get());
}

/**
 * Executes segment compaction, returns size of compacted segment
 */
ss::future<size_t> do_self_compact_segment(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  storage::probe& pb,
  storage::readers_cache& readers_cache) {
    vlog(gclog.trace, "self compacting segment {}", s->reader().filename());
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
      .then([s, &readers_cache](storage::index_state idx) {
          return readers_cache.evict_segment_readers(s).then(
            [s,
             idx = std::move(idx)](readers_cache::range_lock_holder) mutable {
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
                s->release_batch_cache_index();
                return s->index()
                  .flush()
                  .then([s] { return s->size_bytes(); })
                  .finally([l = std::move(lock)] {});
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
                      vlog(gclog.warn, "error closing compacted index:{}", e);
                  })
                  .then([f = std::move(fut), x = std::move(x)]() mutable {
                      return std::move(f);
                  });
            });
      });
}

ss::future<compaction_result> self_compact_segment(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  storage::probe& pb,
  storage::readers_cache& readers_cache) {
    if (s->has_appender()) {
        return ss::make_exception_future<compaction_result>(
          std::runtime_error(fmt::format(
            "Cannot compact an active segment. cfg:{} - segment:{}", cfg, s)));
    }
    if (s->finished_self_compaction()) {
        return ss::make_ready_future<compaction_result>(s->size_bytes());
    }
    auto idx_path = std::filesystem::path(s->reader().filename());
    idx_path.replace_extension(".compaction_index");
    return detect_compaction_index_state(idx_path, cfg)
      .then([idx_path, s, cfg, &pb, &readers_cache](
              compacted_index::recovery_state state) mutable {
          vlog(gclog.trace, "segment {} compaction state: {}", idx_path, state);
          switch (state) {
          case compacted_index::recovery_state::recovered: {
              vlog(gclog.debug, "detected {} is already compacted", idx_path);
              return ss::make_ready_future<compaction_result>(s->size_bytes());
          }
          case compacted_index::recovery_state::nonrecovered:
              return do_self_compact_segment(s, cfg, pb, readers_cache)
                .then([before = s->size_bytes(), &pb](size_t sz_after) {
                    pb.segment_compacted();
                    return compaction_result(before, sz_after);
                });
          case compacted_index::recovery_state::missing:
              [[fallthrough]];
          case compacted_index::recovery_state::needsrebuild: {
              vlog(gclog.info, "Rebuilding index file... ({})", idx_path);
              pb.corrupted_compaction_index();
              return s->read_lock()
                .then([s, cfg, &pb, idx_path](ss::rwlock::holder h) {
                    return rebuild_compaction_index(
                      create_segment_full_reader(s, cfg, pb, std::move(h)),
                      idx_path,
                      cfg);
                })
                .then([s, cfg, &pb, idx_path, &readers_cache] {
                    vlog(
                      gclog.info,
                      "rebuilt index: {}, attempting compaction again",
                      idx_path);
                    return self_compact_segment(s, cfg, pb, readers_cache);
                });
          }
          }
          __builtin_unreachable();
      })
      .then([s](compaction_result r) {
          s->mark_as_finished_self_compaction();
          return r;
      });
}

ss::future<ss::lw_shared_ptr<segment>> make_concatenated_segment(
  std::filesystem::path path,
  std::vector<ss::lw_shared_ptr<segment>> segments,
  compaction_config cfg) {
    // read locks on source segments
    std::vector<ss::rwlock::holder> locks;
    locks.reserve(segments.size());
    for (auto& segment : segments) {
        locks.push_back(co_await segment->read_lock());
    }

    // fast check if we should abandon all the expensive i/o work if we happened
    // to be racing with an operation like truncation or shutdown.
    for (const auto& segment : segments) {
        if (unlikely(segment->is_closed())) {
            throw std::runtime_error(fmt::format(
              "Aborting compaction of closed segment: {}", *segment));
        }
    }

    auto compacted_idx_path = compacted_index_path(path);
    if (co_await ss::file_exists(compacted_idx_path.string())) {
        co_await ss::remove_file(compacted_idx_path.string());
    }
    co_await write_concatenated_compacted_index(
      compacted_idx_path, segments, cfg);

    // concatenation process
    if (co_await ss::file_exists(path.string())) {
        co_await ss::remove_file(path.string());
    }
    auto writer = co_await make_writer_handle(path, cfg.sanitize);
    auto output = co_await ss::make_file_output_stream(std::move(writer));
    for (auto& segment : segments) {
        auto reader_handle = co_await segment->reader().data_stream(
          0, cfg.iopc);
        co_await ss::copy(reader_handle.stream(), output);
        co_await reader_handle.close();
    }
    co_await output.close();

    // offsets span the concatenated range
    segment::offset_tracker offsets(
      segments.front()->offsets().term,
      segments.front()->offsets().base_offset);
    offsets.committed_offset = segments.back()->offsets().committed_offset;
    offsets.dirty_offset = segments.back()->offsets().dirty_offset;
    offsets.stable_offset = segments.back()->offsets().stable_offset;

    // build segment reader over combined data
    segment_reader reader(
      path.string(),
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      cfg.sanitize);
    co_await reader.load_size();

    // build an empty index for the segment
    auto index_name = path;
    index_name.replace_extension("base_index");
    if (co_await ss::file_exists(index_name.string())) {
        co_await ss::remove_file(index_name.string());
    }
    segment_index index(
      index_name.string(),
      offsets.base_offset,
      segment_index::default_data_buffer_step,
      cfg.sanitize);

    co_return ss::make_lw_shared<segment>(
      offsets,
      std::move(reader),
      std::move(index),
      nullptr,
      std::nullopt,
      std::nullopt);
}

ss::future<std::vector<compacted_index_reader>> make_indices_readers(
  std::vector<ss::lw_shared_ptr<segment>>& segments,
  ss::io_priority_class io_pc,
  storage::debug_sanitize_files sanitize) {
    return ssx::async_transform(
      segments.begin(),
      segments.end(),
      [io_pc, sanitize](ss::lw_shared_ptr<segment>& seg) {
          const auto path = compacted_index_path(
            seg->reader().filename().c_str());
          auto f = ss::now();
          if (seg->has_compaction_index()) {
              f = seg->compaction_index().close();
          }
          return f.then([io_pc, sanitize, path]() {
              return make_reader_handle(path, sanitize)
                .then([path, io_pc](auto reader_fd) {
                    return make_file_backed_compacted_reader(
                      path.string(), reader_fd, io_pc, 64_KiB);
                });
          });
      });
}

ss::future<> rewrite_concatenated_indicies(
  compacted_index_writer writer, std::vector<compacted_index_reader>& readers) {
    return ss::do_with(
      std::move(writer), [&readers](compacted_index_writer& writer) {
          return ss::do_with(
            index_copy_reducer{writer},
            [&readers, &writer](index_copy_reducer& reducer) {
                return ss::do_for_each(
                         readers.begin(),
                         readers.end(),
                         [&reducer](compacted_index_reader& rdr) {
                             vlog(
                               gclog.trace,
                               "concatenating index: {}",
                               rdr.filename());
                             return rdr.consume(reducer, model::no_timeout);
                         })
                  .finally([&writer] { return writer.close(); });
            });
      });
}

ss::future<> do_write_concatenated_compacted_index(
  std::filesystem::path target_path,
  std::vector<ss::lw_shared_ptr<segment>>& segments,
  compaction_config cfg) {
    return make_indices_readers(segments, cfg.iopc, cfg.sanitize)
      .then([cfg, target_path = std::move(target_path)](
              std::vector<compacted_index_reader> readers) mutable {
          vlog(gclog.debug, "concatenating {} indicies", readers.size());
          return ss::do_with(
            std::move(readers),
            [cfg, target_path = std::move(target_path)](
              std::vector<compacted_index_reader>& readers) mutable {
                return ss::parallel_for_each(
                         readers.begin(),
                         readers.end(),
                         [](compacted_index_reader& reader) {
                             return reader.verify_integrity();
                         })
                  .then([] { return true; })
                  .handle_exception([](const std::exception_ptr& e) {
                      vlog(
                        gclog.info,
                        "compacted index is corrupted, skipping concatenation "
                        "- {}",
                        e);
                      return false;
                  })
                  .then([cfg, target_path = std::move(target_path), &readers](
                          bool verified_successfully) {
                      if (!verified_successfully) {
                          return ss::now();
                      }

                      return make_compacted_index_writer(
                               target_path, cfg.sanitize, cfg.iopc)
                        .then([&readers](compacted_index_writer writer) {
                            return rewrite_concatenated_indicies(
                              std::move(writer), readers);
                        });
                  })
                  .finally([&readers] {
                      return ss::parallel_for_each(
                        readers,
                        [](compacted_index_reader& r) { return r.close(); });
                  });
            });
      });
}

ss::future<> write_concatenated_compacted_index(
  std::filesystem::path target_path,
  std::vector<ss::lw_shared_ptr<segment>> segments,
  compaction_config cfg) {
    if (segments.empty()) {
        return ss::now();
    }
    std::vector<compacted_index_reader> readers;
    readers.reserve(segments.size());
    return ss::do_with(
      std::move(segments),
      [cfg, target_path = std::move(target_path)](
        std::vector<ss::lw_shared_ptr<segment>>& segments) mutable {
          return do_write_concatenated_compacted_index(
            std::move(target_path), segments, cfg);
      });
}

ss::future<std::vector<ss::rwlock::holder>> transfer_segment(
  ss::lw_shared_ptr<segment> to,
  ss::lw_shared_ptr<segment> from,
  compaction_config cfg,
  probe& probe,
  std::vector<ss::rwlock::holder> locks) {
    co_await from->close();

    co_await to->index().drop_all_data();

    // segment data file
    auto from_path = std::filesystem::path(from->reader().filename());
    co_await do_swap_data_file_handles(from_path, to, cfg, probe);

    // offset index
    to->index().swap_index_state(
      std::move(from->index()).release_index_state());
    to->force_set_commit_offset_from_index();
    co_await to->index().flush();

    // compaction index
    from_path = compacted_index_path(from_path);
    auto to_path = compacted_index_path(
      std::filesystem::path(to->reader().filename()));
    co_await ss::rename_file(from_path.string(), to_path.string());

    // clean up replacement segment
    co_await from->remove_persistent_state();

    co_return std::move(locks);
}

ss::future<std::vector<ss::rwlock::holder>> write_lock_segments(
  std::vector<ss::lw_shared_ptr<segment>>& segments,
  ss::semaphore::clock::duration timeout,
  int retries) {
    vassert(retries >= 0, "Invalid retries value");
    std::vector<ss::rwlock::holder> held;
    held.reserve(segments.size());
    while (true) {
        try {
            std::vector<ss::future<ss::rwlock::holder>> held_f;
            held_f.reserve(segments.size());
            for (auto& segment : segments) {
                held_f.push_back(
                  segment->write_lock(ss::semaphore::clock::now() + timeout));
            }
            held = co_await ss::when_all_succeed(held_f.begin(), held_f.end());
            break;
        } catch (ss::semaphore_timed_out&) {
            held.clear();
        }
        if (retries == 0) {
            throw ss::semaphore_timed_out();
        }
        --retries;
    }
    co_return held;
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
