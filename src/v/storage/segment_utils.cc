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
#include "storage/chunk_cache.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/fs_utils.h"
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
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <roaring/roaring.hh>

#include <optional>

namespace storage::internal {
using namespace storage; // NOLINT

/// Check if the file is on BTRFS, and disable copy-on-write if so.  COW
/// is not useful for logs and can cause issues.
static ss::future<>
maybe_disable_cow(const std::filesystem::path& path, ss::file& file) {
    try {
        if (co_await ss::file_system_at(path.string()) == ss::fs_type::btrfs) {
            int flags = -1;
            // ss::syscall_result throws on errors, so not checking returns.
            co_await file.ioctl(FS_IOC_GETFLAGS, (void*)&flags);
            if ((flags & FS_NOCOW_FL) == 0) {
                flags |= FS_NOCOW_FL;
                co_await file.ioctl(FS_IOC_SETFLAGS, (void*)&flags);
                vlog(stlog.trace, "Disabled COW on BTRFS segment {}", path);
            }
        }
    } catch (std::filesystem::filesystem_error& e) {
        // This can happen if e.g. our file open races with an unlink
        vlog(stlog.info, "Filesystem error disabling COW on {}: {}", path, e);
    } catch (std::system_error& e) {
        // Non-fatal, user will just get degraded behaviour
        // when btrfs tries to COW on a journal.
        vlog(stlog.info, "System error disabling COW on {}: {}", path, e);
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
  const std::filesystem::path& path,
  storage::debug_sanitize_files debug,
  bool truncate) {
    auto flags = ss::open_flags::rw | ss::open_flags::create;
    if (truncate) {
        flags |= ss::open_flags::truncate;
    }
    return make_handle(path, flags, writer_opts(), debug);
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
  ss::io_priority_class iopc,
  storage_resources& resources) {
    return ss::make_ready_future<compacted_index_writer>(
      make_file_backed_compacted_index(
        path.string(), iopc, debug, false, resources));
}

ss::future<segment_appender_ptr> make_segment_appender(
  const segment_full_path& path,
  debug_sanitize_files debug,
  size_t number_of_chunks,
  std::optional<uint64_t> segment_size,
  ss::io_priority_class iopc,
  storage_resources& resources) {
    return internal::make_writer_handle(path, debug)
      .then([number_of_chunks, iopc, path, segment_size, &resources](
              ss::file writer) {
          try {
              // NOTE: This try-catch is needed to not uncover the real
              // exception during an OOM condition, since the appender allocates
              // 1MB of memory aligned buffers
              return ss::make_ready_future<segment_appender_ptr>(
                std::make_unique<segment_appender>(
                  writer,
                  segment_appender::options(
                    iopc, number_of_chunks, segment_size, resources)));
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
               / internal::chunks().chunk_size();

    if (!ntpc.has_overrides()) {
        return def;
    }
    auto& o = ntpc.get_overrides();
    if (o.compaction_strategy) {
        return def / 2;
    }
    return def;
}

uint64_t segment_size_from_config(const ntp_config& ntpc) {
    auto def = config::shard_local_cfg().log_segment_size();

    if (!ntpc.has_overrides()) {
        return def;
    }
    auto& o = ntpc.get_overrides();
    if (o.segment_size) {
        return o.segment_size.value();
    } else {
        return def;
    }
}

ss::future<roaring::Roaring>
natural_index_of_entries_to_keep(compacted_index_reader reader) {
    reader.reset();
    return reader.consume(compaction_key_reducer(), model::no_timeout);
}

ss::future<> copy_filtered_entries(
  compacted_index_reader reader,
  roaring::Roaring to_copy_index,
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
  compacted_index_reader reader,
  compaction_config cfg,
  storage_resources& resources) {
    const auto tmpname = std::filesystem::path(
      fmt::format("{}.staging", reader.path()));
    return natural_index_of_entries_to_keep(reader)
      .then(
        [reader, cfg, tmpname, &resources](
          roaring::Roaring bitmap) -> ss::future<> {
            auto truncating_writer = make_file_backed_compacted_index(
              tmpname.string(), cfg.iopc, cfg.sanitize, true, resources);

            return copy_filtered_entries(
              reader, std::move(bitmap), std::move(truncating_writer));
        })
      .then(
        [old_name = tmpname,
         new_name = ss::sstring(reader.path())]() -> ss::future<> {
            // from glibc: If oldname is not a directory, then any
            // existing file named newname is removed during the
            // renaming operation
            return ss::rename_file(std::string(old_name), new_name);
        });
};

ss::future<> write_clean_compacted_index(
  compacted_index_reader reader,
  compaction_config cfg,
  storage_resources& resources) {
    // integrity verified in `do_detect_compaction_index_state`
    return do_write_clean_compacted_index(reader, cfg, resources)
      .finally([reader]() mutable {
          return reader.close().then_wrapped(
            [reader](ss::future<>) { /*ignore*/ });
      });
}
ss::future<compacted_index::recovery_state>
do_detect_compaction_index_state(segment_full_path p, compaction_config cfg) {
    using flags = compacted_index::footer_flags;
    return make_reader_handle(p, cfg.sanitize)
      .then([cfg, p](ss::file f) {
          return make_file_backed_compacted_reader(
            p, std::move(f), cfg.iopc, 64_KiB);
      })
      .then([](compacted_index_reader reader) {
          return reader.verify_integrity()
            .then([reader]() mutable { return reader.load_footer(); })
            .then([](compacted_index::footer footer) {
                if (bool(footer.flags & flags::self_compaction)) {
                    return compacted_index::recovery_state::already_compacted;
                }
                if (bool(footer.flags & flags::incomplete)) {
                    return compacted_index::recovery_state::index_needs_rebuild;
                }
                return compacted_index::recovery_state::index_recovered;
            })
            .finally([reader]() mutable { return reader.close(); });
      })
      .handle_exception_type([](const compacted_index::needs_rebuild_error& e) {
          vlog(stlog.info, "compacted index needs rebuild: {}", e);
          return compacted_index::recovery_state::index_needs_rebuild;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            stlog.warn,
            "detected error while attempting compacted index recovery, {}. "
            "marking as 'needs rebuild'. Common situation during crashes or "
            "hard shutdowns.",
            e);
          return compacted_index::recovery_state::index_needs_rebuild;
      });
}

ss::future<compacted_index::recovery_state>
detect_compaction_index_state(segment_full_path p, compaction_config cfg) {
    return ss::file_exists(p.string()).then([p, cfg](bool exists) {
        if (exists) {
            return do_detect_compaction_index_state(p, cfg);
        }
        return ss::make_ready_future<compacted_index::recovery_state>(
          compacted_index::recovery_state::index_missing);
    });
}

ss::future<compacted_offset_list>
generate_compacted_list(model::offset o, compacted_index_reader reader) {
    reader.reset();
    return reader.consume(compacted_offset_list_reducer(o), model::no_timeout)
      .finally([reader] {});
}

ss::future<> do_compact_segment_index(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  storage_resources& resources) {
    auto compacted_path = s->reader().path().to_compacted_index();
    vlog(gclog.trace, "compacting segment compaction index:{}", compacted_path);
    return make_reader_handle(compacted_path, cfg.sanitize)
      .then([cfg, compacted_path, s, &resources](ss::file f) {
          auto reader = make_file_backed_compacted_reader(
            compacted_path, std::move(f), cfg.iopc, 64_KiB);
          return write_clean_compacted_index(reader, cfg, resources);
      });
}
ss::future<storage::index_state> do_copy_segment_data(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  storage::probe& pb,
  ss::rwlock::holder h,
  storage_resources& resources,
  offset_delta_time apply_offset) {
    auto idx_path = s->reader().path().to_compacted_index();
    return make_reader_handle(idx_path, cfg.sanitize)
      .then([s, cfg, idx_path](ss::file f) {
          auto reader = make_file_backed_compacted_reader(
            idx_path, std::move(f), cfg.iopc, 64_KiB);
          return generate_compacted_list(s->offsets().base_offset, reader)
            .finally([reader]() mutable {
                return reader.close().then_wrapped([](ss::future<>) {});
            });
      })
      .then([cfg, s, &pb, h = std::move(h), &resources, apply_offset](
              compacted_offset_list list) mutable {
          const auto tmpname = s->reader().path().to_staging();
          return make_segment_appender(
                   tmpname,
                   cfg.sanitize,
                   segment_appender::write_behind_memory
                     / internal::chunks().chunk_size(),
                   std::nullopt,
                   cfg.iopc,
                   resources)
            .then([l = std::move(list),
                   &pb,
                   h = std::move(h),
                   cfg,
                   s,
                   tmpname,
                   apply_offset](segment_appender_ptr w) mutable {
                auto raw = w.get();
                auto red = copy_data_segment_reducer(
                  std::move(l),
                  raw,
                  s->path().is_internal_topic(),
                  apply_offset);
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
      s->reader().path(),
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
 * Executes segment compaction, returns size of compacted segment or an empty
 * optional if segment wasn't compacted
 */
ss::future<std::optional<size_t>> do_self_compact_segment(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  storage::probe& pb,
  storage::readers_cache& readers_cache,
  storage_resources& resources,
  offset_delta_time apply_offset) {
    vlog(gclog.trace, "self compacting segment {}", s->reader().path());
    auto read_holder = co_await s->read_lock();
    auto segment_generation = s->get_generation_id();

    if (s->is_closed()) {
        throw segment_closed_exception();
    }

    co_await do_compact_segment_index(s, cfg, resources);
    // copy the bytes after segment is good - note that we
    // need to do it with the READ-lock, not the write lock
    auto idx = co_await do_copy_segment_data(
      s, cfg, pb, std::move(read_holder), resources, apply_offset);

    auto rdr_holder = co_await readers_cache.evict_segment_readers(s);

    auto write_lock_holder = co_await s->write_lock();
    if (segment_generation != s->get_generation_id()) {
        vlog(
          stlog.debug,
          "segment generation mismatch current generation: {}, previous "
          "generation: {}, skipping compaction",
          s->get_generation_id(),
          segment_generation);
        const ss::sstring staging_file = s->reader().path().to_staging();
        if (co_await ss::file_exists(staging_file)) {
            co_await ss::remove_file(staging_file);
        }
        co_return std::nullopt;
    }

    if (s->is_closed()) {
        throw segment_closed_exception();
    }

    co_await s->index().drop_all_data();

    auto compacted_file = s->reader().path().to_staging();
    co_await do_swap_data_file_handles(compacted_file, s, cfg, pb);

    s->index().swap_index_state(std::move(idx));
    s->force_set_commit_offset_from_index();
    s->release_batch_cache_index();
    co_await s->index().flush();
    s->advance_generation();
    co_return s->size_bytes();
}

ss::future<> rebuild_compaction_index(
  model::record_batch_reader rdr,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  fragmented_vector<model::tx_range>&& aborted_txs,
  segment_full_path p,
  compaction_config cfg,
  storage_resources& resources) {
    return make_compacted_index_writer(p, cfg.sanitize, cfg.iopc, resources)
      .then([r = std::move(rdr), stm_manager, txs = std::move(aborted_txs)](
              compacted_index_writer w) mutable {
          return ss::do_with(
            std::move(w),
            [stm_manager, r = std::move(r), txs = std::move(txs)](
              auto& writer) mutable {
                return std::move(r)
                  .consume(
                    tx_reducer(stm_manager, std::move(txs), &writer),
                    model::no_timeout)
                  .then_wrapped(
                    [&writer](ss::future<tx_reducer::stats> fut) mutable {
                        if (fut.failed()) {
                            vlog(
                              gclog.error,
                              "Error rebuilding index: {}, {}",
                              writer.filename(),
                              fut.get_exception());
                        } else {
                            vlog(
                              gclog.info,
                              "tx reducer path: {} stats {}",
                              writer.filename(),
                              fut.get0());
                        }
                    })
                  .finally([&writer]() {
                      // writer needs to be closed in all cases,
                      // else can trigger a potential assert.
                      return writer.close().handle_exception(
                        [](std::exception_ptr e) {
                            vlog(
                              gclog.warn,
                              "error closing compacted index:{}",
                              e);
                        });
                  });
            });
      });
}

ss::future<compaction_result> self_compact_segment(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  compaction_config cfg,
  storage::probe& pb,
  storage::readers_cache& readers_cache,
  storage_resources& resources,
  offset_delta_time apply_offset) {
    if (s->has_appender()) {
        throw std::runtime_error(fmt::format(
          "Cannot compact an active segment. cfg:{} - segment:{}", cfg, s));
    }

    if (s->finished_self_compaction() || !s->has_compactible_offsets(cfg)) {
        co_return compaction_result{s->size_bytes()};
    }

    segment_full_path idx_path = s->path().to_compacted_index();
    auto state = co_await detect_compaction_index_state(idx_path, cfg);

    vlog(gclog.trace, "segment {} compaction state: {}", idx_path, state);

    switch (state) {
    case compacted_index::recovery_state::already_compacted: {
        vlog(gclog.debug, "detected {} is already compacted", idx_path);
        s->mark_as_finished_self_compaction();
        co_return compaction_result{s->size_bytes()};
    }
    case compacted_index::recovery_state::index_recovered: {
        auto sz_before = s->size_bytes();
        auto sz_after = co_await do_self_compact_segment(
          s, cfg, pb, readers_cache, resources, apply_offset);
        // compaction wasn't executed, return
        if (!sz_after) {
            co_return compaction_result(sz_before);
        }
        pb.segment_compacted();
        s->mark_as_finished_self_compaction();
        co_return compaction_result(sz_before, *sz_after);
    }
    case compacted_index::recovery_state::index_missing:
        [[fallthrough]];
    case compacted_index::recovery_state::index_needs_rebuild: {
        vlog(gclog.info, "Rebuilding index file... ({})", idx_path);
        pb.corrupted_compaction_index();
        auto h = co_await s->read_lock();
        // TODO: Improve memory management here, eg: ton of aborted txs?
        auto aborted_txs = co_await stm_manager->aborted_tx_ranges(
          s->offsets().base_offset, s->offsets().stable_offset);
        co_await rebuild_compaction_index(
          create_segment_full_reader(s, cfg, pb, std::move(h)),
          stm_manager,
          std::move(aborted_txs),
          idx_path,
          cfg,
          resources);

        vlog(
          gclog.info,
          "rebuilt index: {}, attempting compaction again",
          idx_path);
        co_return co_await self_compact_segment(
          s, stm_manager, cfg, pb, readers_cache, resources, apply_offset);
    }
    }
    __builtin_unreachable();
}

ss::future<
  std::tuple<ss::lw_shared_ptr<segment>, std::vector<segment::generation_id>>>
make_concatenated_segment(
  segment_full_path path,
  std::vector<ss::lw_shared_ptr<segment>> segments,
  compaction_config cfg,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table) {
    // read locks on source segments
    std::vector<ss::rwlock::holder> locks;
    locks.reserve(segments.size());
    std::vector<segment::generation_id> generations;
    generations.reserve(segments.size());
    for (auto& segment : segments) {
        locks.push_back(co_await segment->read_lock());
        generations.push_back(segment->get_generation_id());
    }

    // fast check if we should abandon all the expensive i/o work if we
    // happened to be racing with an operation like truncation or shutdown.
    for (const auto& segment : segments) {
        if (unlikely(segment->is_closed())) {
            throw std::runtime_error(fmt::format(
              "Aborting compaction of closed segment: {}", *segment));
        }
    }

    auto compacted_idx_path = path.to_compacted_index();
    if (co_await ss::file_exists(ss::sstring(compacted_idx_path))) {
        co_await ss::remove_file(ss::sstring(compacted_idx_path));
    }
    co_await write_concatenated_compacted_index(
      compacted_idx_path, segments, cfg, resources);

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

    auto& front = segments.front()->offsets();
    auto& back = segments.back()->offsets();

    // offsets span the concatenated range
    segment::offset_tracker offsets(front.term, front.base_offset);
    offsets.committed_offset = std::max(
      front.committed_offset, back.committed_offset);
    offsets.dirty_offset = std::max(front.dirty_offset, back.committed_offset);
    offsets.stable_offset = std::max(front.stable_offset, back.stable_offset);

    // build segment reader over combined data
    segment_reader reader(
      path,
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      cfg.sanitize);
    co_await reader.load_size();

    // build an empty index for the segment
    auto index_name = path.to_index();
    if (co_await ss::file_exists(index_name.string())) {
        co_await ss::remove_file(index_name.string());
    }
    segment_index index(
      index_name,
      offsets.base_offset,
      segment_index::default_data_buffer_step,
      feature_table,
      cfg.sanitize);

    co_return std::make_tuple(
      ss::make_lw_shared<segment>(
        offsets,
        std::move(reader),
        std::move(index),
        nullptr,
        std::nullopt,
        std::nullopt,
        resources,
        segments.front()->get_generation_id() + segment::generation_id(1)),
      std::move(generations));
}

ss::future<std::vector<compacted_index_reader>> make_indices_readers(
  std::vector<ss::lw_shared_ptr<segment>>& segments,
  ss::io_priority_class io_pc,
  storage::debug_sanitize_files sanitize) {
    return ssx::async_transform(
      segments.begin(),
      segments.end(),
      [io_pc, sanitize](ss::lw_shared_ptr<segment>& seg) {
          const auto path = seg->reader().path().to_compacted_index();
          auto f = ss::now();
          if (seg->has_compaction_index()) {
              f = seg->compaction_index().close();
          }
          return f.then([io_pc, sanitize, path]() {
              return make_reader_handle(path, sanitize)
                .then([path, io_pc](auto reader_fd) {
                    return make_file_backed_compacted_reader(
                      path, reader_fd, io_pc, 64_KiB);
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
  compaction_config cfg,
  storage_resources& resources) {
    return make_indices_readers(segments, cfg.iopc, cfg.sanitize)
      .then([cfg, target_path = std::move(target_path), &resources](
              std::vector<compacted_index_reader> readers) mutable {
          vlog(gclog.debug, "concatenating {} indicies", readers.size());
          return ss::do_with(
            std::move(readers),
            [cfg, target_path = std::move(target_path), &resources](
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
                        "compacted index is corrupted, skipping "
                        "concatenation "
                        "- {}",
                        e);
                      return false;
                  })
                  .then([cfg,
                         target_path = std::move(target_path),
                         &readers,
                         &resources](bool verified_successfully) {
                      if (!verified_successfully) {
                          return ss::now();
                      }

                      return make_compacted_index_writer(
                               target_path, cfg.sanitize, cfg.iopc, resources)
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
  compaction_config cfg,
  storage_resources& resources) {
    if (segments.empty()) {
        return ss::now();
    }
    std::vector<compacted_index_reader> readers;
    readers.reserve(segments.size());
    return ss::do_with(
      std::move(segments),
      [cfg, target_path = std::move(target_path), &resources](
        std::vector<ss::lw_shared_ptr<segment>>& segments) mutable {
          return do_write_concatenated_compacted_index(
            std::move(target_path), segments, cfg, resources);
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
    auto from_path = from->reader().path();
    co_await do_swap_data_file_handles(from_path, to, cfg, probe);

    // offset index
    to->index().swap_index_state(
      std::move(from->index()).release_index_state());
    to->force_set_commit_offset_from_index();
    co_await to->index().flush();

    // compaction index
    from_path = from_path.to_compacted_index();
    auto to_path = to->reader().path().to_compacted_index();
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

float random_jitter(jitter_percents jitter_percents) {
    vassert(
      jitter_percents >= 0 && jitter_percents <= 100,
      "jitter percents should be in range [0,100]. Requested {}",
      jitter_percents);
    // multiply by 10 to increase resolution
    auto p = jitter_percents() * 10;
    auto jit = random_generators::get_int<int64_t>(-p, p);
    return jit / 1000.0f;
}

bytes start_offset_key(model::ntp ntp) {
    iobuf buf;
    reflection::serialize(buf, kvstore_key_type::start_offset, std::move(ntp));
    return iobuf_to_bytes(buf);
}

bytes clean_segment_key(model::ntp ntp) {
    iobuf buf;
    reflection::serialize(buf, kvstore_key_type::clean_segment, std::move(ntp));
    return iobuf_to_bytes(buf);
}

offset_delta_time should_apply_delta_time_offset(
  ss::sharded<features::feature_table>& feature_table) {
    return offset_delta_time{
      feature_table.local_is_initialized()
      && feature_table.local().is_active(features::feature::node_isolation)};
}

} // namespace storage::internal
