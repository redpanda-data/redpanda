// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_utils.h"

#include "base/likely.h"
#include "base/units.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "config/configuration.h"
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
#include "storage/file_sanitizer.h"
#include "storage/fs_utils.h"
#include "storage/fwd.h"
#include "storage/index_state.h"
#include "storage/lock_manager.h"
#include "storage/log_reader.h"
#include "storage/logger.h"
#include "storage/ntp_config.h"
#include "storage/parser_utils.h"
#include "storage/scoped_file_tracker.h"
#include "storage/segment.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/as_future.hh>
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
    } catch (const std::filesystem::filesystem_error& e) {
        // This can happen if e.g. our file open races with an unlink
        vlog(stlog.info, "Filesystem error disabling COW on {}: {}", path, e);
    } catch (const std::system_error& e) {
        // Non-fatal, user will just get degraded behaviour
        // when btrfs tries to COW on a journal.
        vlog(stlog.info, "System error disabling COW on {}: {}", path, e);
    }
}

static inline ss::file wrap_handle(
  std::filesystem::path path,
  ss::file f,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    if (ntp_sanitizer_config) {
        return ss::file(ss::make_shared(file_io_sanitizer(
          std::move(f),
          std::move(path),
          std::move(ntp_sanitizer_config.value()))));
    }
    return f;
}

ss::future<ss::file> make_handle(
  std::filesystem::path path,
  ss::open_flags flags,
  ss::file_open_options opt,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    auto file = co_await ss::open_file_dma(path.string(), flags, opt);

    if ((flags & ss::open_flags::create) == ss::open_flags::create) {
        co_await maybe_disable_cow(path, file);
    }

    co_return wrap_handle(
      std::move(path), std::move(file), std::move(ntp_sanitizer_config));
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
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config,
  bool truncate) {
    auto flags = ss::open_flags::rw | ss::open_flags::create;
    if (truncate) {
        flags |= ss::open_flags::truncate;
    }
    return make_handle(
      path, flags, writer_opts(), std::move(ntp_sanitizer_config));
}
/// make file handle with default opts
ss::future<ss::file> make_reader_handle(
  const std::filesystem::path& path,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    return make_handle(
      path,
      ss::open_flags::ro | ss::open_flags::create,
      ss::file_open_options{},
      std::move(ntp_sanitizer_config));
}

ss::future<compacted_index_writer> make_compacted_index_writer(
  const std::filesystem::path& path,
  ss::io_priority_class iopc,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    return ss::make_ready_future<compacted_index_writer>(
      make_file_backed_compacted_index(
        path.string(),
        iopc,
        false,
        resources,
        std::move(ntp_sanitizer_config)));
}

ss::future<segment_appender_ptr> make_segment_appender(
  const segment_full_path& path,
  size_t number_of_chunks,
  std::optional<uint64_t> segment_size,
  ss::io_priority_class iopc,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config) {
    return internal::make_writer_handle(path, std::nullopt)
      .then([number_of_chunks,
             iopc,
             path,
             segment_size,
             &resources,
             ntp_sanitizer_config = std::move(ntp_sanitizer_config)](
              ss::file writer) mutable {
          // file_io_sanitizer requires a pointer to the appender,
          // so we create it inline and give it the pointer after
          // the appender is created.
          ss::shared_ptr<file_io_sanitizer> sanitized_writer{nullptr};
          if (ntp_sanitizer_config) {
              sanitized_writer = ss::make_shared<file_io_sanitizer>(
                std::move(writer),
                path,
                std::move(ntp_sanitizer_config.value()));

              writer = ss::file(sanitized_writer);
          }

          try {
              // NOTE: This try-catch is needed to not uncover the real
              // exception during an OOM condition, since the appender allocates
              // 1MB of memory aligned buffers
              auto appender_ptr = std::make_unique<segment_appender>(
                writer,
                segment_appender::options(
                  iopc, number_of_chunks, segment_size, resources));

              if (sanitized_writer) {
                  sanitized_writer->set_pointer_to_appender(appender_ptr.get());
              }

              return ss::make_ready_future<segment_appender_ptr>(
                std::move(appender_ptr));
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
    auto bitmap = co_await natural_index_of_entries_to_keep(reader);
    auto staging_to_clean = scoped_file_tracker{
      cfg.files_to_cleanup, {tmpname}};
    auto truncating_writer = make_file_backed_compacted_index(
      tmpname.string(), cfg.iopc, true, resources, cfg.sanitizer_config);
    co_await copy_filtered_entries(
      reader, std::move(bitmap), std::move(truncating_writer));
    co_await ss::rename_file(std::string(tmpname), ss::sstring(reader.path()));
    staging_to_clean.clear();
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
    return make_reader_handle(p, cfg.sanitizer_config)
      .then([cfg, p](ss::file f) {
          return make_file_backed_compacted_reader(
            p, std::move(f), cfg.iopc, 64_KiB, cfg.asrc);
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
    return make_reader_handle(compacted_path, cfg.sanitizer_config)
      .then([cfg, compacted_path, s, &resources](ss::file f) {
          auto reader = make_file_backed_compacted_reader(
            compacted_path, std::move(f), cfg.iopc, 64_KiB, cfg.asrc);
          return write_clean_compacted_index(reader, cfg, resources);
      });
}

ss::future<storage::index_state> do_copy_segment_data(
  ss::lw_shared_ptr<segment> seg,
  compaction_config cfg,
  storage::probe& pb,
  ss::rwlock::holder rw_lock_holder,
  storage_resources& resources,
  offset_delta_time apply_offset,
  ss::sharded<features::feature_table>& feature_table) {
    // preserve broker_timestamp and clean_compact_timestamp from the segment's
    // index
    auto old_broker_timestamp = seg->index().broker_timestamp();
    auto old_clean_compact_timestamp = seg->index().clean_compact_timestamp();

    const bool past_tombstone_delete_horizon
      = internal::is_past_tombstone_delete_horizon(seg, cfg);

    // find out which offsets will survive compaction
    auto idx_path = seg->reader().path().to_compacted_index();
    auto compacted_reader = make_file_backed_compacted_reader(
      idx_path,
      co_await make_reader_handle(idx_path, cfg.sanitizer_config),
      cfg.iopc,
      64_KiB,
      cfg.asrc);
    auto compacted_offsets
      = co_await generate_compacted_list(
          seg->offsets().get_base_offset(), compacted_reader)
          .finally([&] {
              return compacted_reader.close().then_wrapped([](ss::future<>) {});
          });

    // prepare a new segment with only the compacted_offsets
    auto tmpname = seg->reader().path().to_staging();

    auto appender = co_await make_segment_appender(
      tmpname,
      segment_appender::write_behind_memory / internal::chunks().chunk_size(),
      std::nullopt,
      cfg.iopc,
      resources,
      cfg.sanitizer_config);

    vlog(
      gclog.trace,
      "copying compacted segment data from {} to {}",
      seg->reader().filename(),
      tmpname);
    bool may_have_tombstone_records = false;
    auto should_keep = [compacted_list = std::move(compacted_offsets),
                        past_tombstone_delete_horizon,
                        &may_have_tombstone_records](
                         const model::record_batch& b,
                         const model::record& r,
                         bool) {
        // Deal with tombstone record removal
        if (r.is_tombstone() && past_tombstone_delete_horizon) {
            return ss::make_ready_future<bool>(false);
        }

        const auto o = b.base_offset() + model::offset_delta(r.offset_delta());
        const auto keep = compacted_list.contains(o);

        if (r.is_tombstone() && keep) {
            may_have_tombstone_records = true;
        }

        return ss::make_ready_future<bool>(keep);
    };

    model::offset segment_last_offset{};
    if (likely(feature_table.local().is_active(
          features::feature::compaction_placeholder_batch))) {
        segment_last_offset = seg->offsets().get_committed_offset();
    }
    auto copy_reducer = copy_data_segment_reducer(
      std::move(should_keep),
      appender.get(),
      seg->path().is_internal_topic(),
      apply_offset,
      segment_last_offset,
      /*cidx=*/nullptr,
      /*inject_failure=*/false,
      cfg.asrc);

    // create the segment, get the in-memory index for the new segment
    auto new_index = co_await create_segment_full_reader(
                       seg, cfg, pb, std::move(rw_lock_holder))
                       .consume(std::move(copy_reducer), model::no_timeout)
                       .finally([&] {
                           return appender->close().handle_exception(
                             [](std::exception_ptr e) {
                                 vlog(
                                   gclog.error,
                                   "Error copying index to new segment:{}",
                                   e);
                             });
                       });

    // restore broker timestamp and clean compact timestamp
    new_index.broker_timestamp = old_broker_timestamp;
    new_index.clean_compact_timestamp = old_clean_compact_timestamp;

    // Set may_have_tombstone_records
    new_index.may_have_tombstone_records = may_have_tombstone_records;

    co_return new_index;
}

model::record_batch_reader create_segment_full_reader(
  ss::lw_shared_ptr<storage::segment> s,
  storage::compaction_config cfg,
  storage::probe& pb,
  ss::rwlock::holder h) {
    auto o = s->offsets();
    auto reader_cfg = log_reader_config(
      o.get_base_offset(), o.get_dirty_offset(), cfg.iopc);
    reader_cfg.skip_batch_cache = true;
    segment_set::underlying_t set;
    set.reserve(1);
    set.push_back(s);
    auto lease = std::make_unique<lock_manager::lease>(
      segment_set(std::move(set)));
    lease->locks.push_back(std::move(h));
    return model::make_record_batch_reader<log_reader>(
      std::move(lease), reader_cfg, pb, nullptr);
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
    // the on disk file is changing so clear the size cache
    s->clear_cached_disk_usage();

    auto r = std::make_unique<segment_reader>(
      s->reader().path(),
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      cfg.sanitizer_config);
    co_await r->load_size();

    // update partition size probe
    pb.delete_segment(*s.get());
    s->swap_reader(std::move(r));
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
  offset_delta_time apply_offset,
  ss::rwlock::holder read_holder,
  ss::sharded<features::feature_table>& feature_table) {
    if (cfg.asrc) {
        cfg.asrc->check();
    }

    vlog(gclog.trace, "self compacting segment {}", s->reader().path());
    auto segment_generation = s->get_generation_id();

    if (s->is_closed()) {
        throw segment_closed_exception();
    }

    // broker_timestamp is used for retention.ms, but it's only in the index,
    // not it in the segment itself. save it to restore it later
    co_await do_compact_segment_index(s, cfg, resources);
    // copy the bytes after segment is good - note that we
    // need to do it with the READ-lock, not the write lock
    auto staging_file = s->reader().path().to_staging();
    auto staging_to_clean = scoped_file_tracker{
      cfg.files_to_cleanup, {staging_file}};
    // check the abort source after compacting segment index
    if (cfg.asrc) {
        cfg.asrc->check();
    }
    auto idx = co_await do_copy_segment_data(
      s,
      cfg,
      pb,
      std::move(read_holder),
      resources,
      apply_offset,
      feature_table);
    vlog(
      gclog.trace, "finished copying segment data for {}", s->reader().path());

    auto rdr_holder = co_await readers_cache.evict_segment_readers(s);

    auto write_lock_holder = co_await s->write_lock();
    if (segment_generation != s->get_generation_id()) {
        vlog(
          gclog.debug,
          "segment generation mismatch current generation: {}, previous "
          "generation: {}, skipping compaction",
          s->get_generation_id(),
          segment_generation);
        co_return std::nullopt;
    }

    if (s->is_closed()) {
        throw segment_closed_exception();
    }

    co_await s->index().drop_all_data();

    auto compacted_file = s->reader().path().to_staging();
    co_await do_swap_data_file_handles(compacted_file, s, cfg, pb);
    staging_to_clean.clear();

    s->index().swap_index_state(std::move(idx));
    s->force_set_commit_offset_from_index();
    s->release_batch_cache_index();
    co_await s->index().flush();
    s->advance_generation();
    co_return s->size_bytes();
}

ss::future<> build_compaction_index(
  model::record_batch_reader rdr,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  fragmented_vector<model::tx_range> aborted_txs,
  segment_full_path p,
  compaction_config cfg,
  storage_resources& resources) {
    auto w = co_await make_compacted_index_writer(
      p, cfg.iopc, resources, cfg.sanitizer_config);
    auto reducer = tx_reducer(stm_manager, std::move(aborted_txs), &w);
    auto index_builder = co_await ss::coroutine::as_future<tx_reducer::stats>(
      std::move(rdr)
        .consume(std::move(reducer), model::no_timeout)
        .finally([&w] { return w.close(); }));
    if (index_builder.failed()) {
        auto exception = index_builder.get_exception();
        vlog(
          gclog.error,
          "Error rebuilding index: {}, {}",
          w.filename(),
          exception);
        std::rethrow_exception(exception);
    }
    vlog(
      gclog.info,
      "tx reducer path: {} stats {}",
      w.filename(),
      index_builder.get());
}

bool compacted_index_needs_rebuild(compacted_index::recovery_state state) {
    switch (state) {
    case compacted_index::recovery_state::index_missing:
    case compacted_index::recovery_state::index_needs_rebuild:
        return true;
    case compacted_index::recovery_state::index_recovered:
    case compacted_index::recovery_state::already_compacted:
        return false;
    }
    __builtin_unreachable();
}

ss::future<> rebuild_compaction_index(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  compaction_config cfg,
  storage::probe& pb,
  storage_resources& resources) {
    segment_full_path idx_path = s->path().to_compacted_index();
    vlog(gclog.info, "Rebuilding index file... ({})", idx_path);
    pb.corrupted_compaction_index();
    auto h = co_await s->read_lock();
    if (s->is_closed()) {
        throw segment_closed_exception();
    }
    // TODO: Improve memory management here, eg: ton of aborted txs?
    auto aborted_txs = co_await stm_manager->aborted_tx_ranges(
      s->offsets().get_base_offset(), s->offsets().get_stable_offset());
    co_await build_compaction_index(
      create_segment_full_reader(s, cfg, pb, std::move(h)),
      stm_manager,
      std::move(aborted_txs),
      idx_path,
      cfg,
      resources);
    vlog(
      gclog.info, "rebuilt index: {}, attempting compaction again", idx_path);
}

ss::future<compacted_index::recovery_state> maybe_rebuild_compaction_index(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  const compaction_config& cfg,
  ss::rwlock::holder& read_holder,
  storage_resources& resources,
  storage::probe& pb) {
    segment_full_path idx_path = s->path().to_compacted_index();

    compacted_index::recovery_state state;
    std::optional<scoped_file_tracker> to_clean;
    while (true) {
        if (s->is_closed()) {
            // Temporary compaction index will be removed by file tracker.
            vlog(
              gclog.debug,
              "Stopping in maybe_rebuild_compaction_index, segment closed: {}",
              s->filename());
            throw segment_closed_exception();
        }
        // Check the index state while the read lock is held, preventing e.g.
        // concurrent truncations, which removes the index.
        state = co_await detect_compaction_index_state(idx_path, cfg);
        if (!compacted_index_needs_rebuild(state)) {
            break;
        }
        // Rebuilding the compaction index will take the read lock again,
        // so release here.
        read_holder.return_all();

        // It's possible that while the lock isn't held, the segment is closed
        // and removed. In case that happens, track the new compacted index for
        // removal until we check under lock that the segment is still alive.
        // Otherwise, we may end up with a stray compacted index.
        if (!to_clean.has_value()) {
            to_clean.emplace(
              scoped_file_tracker{cfg.files_to_cleanup, {idx_path}});
        }

        co_await rebuild_compaction_index(s, stm_manager, cfg, pb, resources);

        // Take the lock again before proceeding.
        read_holder = co_await s->read_lock();
    }

    // Compaction index was successfully built, remove it from files to clean.
    if (to_clean.has_value()) {
        to_clean->clear();
    }

    co_return state;
}

ss::future<compaction_result> self_compact_segment(
  ss::lw_shared_ptr<segment> s,
  ss::lw_shared_ptr<storage::stm_manager> stm_manager,
  const compaction_config& cfg,
  storage::probe& pb,
  storage::readers_cache& readers_cache,
  storage_resources& resources,
  ss::sharded<features::feature_table>& feature_table) {
    if (s->has_appender()) {
        throw std::runtime_error(fmt::format(
          "Cannot compact an active segment. cfg:{} - segment:{}", cfg, s));
    }

    if (
      !s->has_compactible_offsets(cfg)
      || (s->finished_self_compaction() && !may_have_removable_tombstones(s, cfg))) {
        co_return compaction_result{s->size_bytes()};
    }

    auto read_holder = co_await s->read_lock();
    compacted_index::recovery_state state
      = co_await maybe_rebuild_compaction_index(
        s, stm_manager, cfg, read_holder, resources, pb);

    if (state == compacted_index::recovery_state::already_compacted) {
        vlog(
          gclog.debug,
          "detected {} is already compacted",
          s->path().to_compacted_index());
        s->mark_as_finished_self_compaction();
        co_return compaction_result{s->size_bytes()};
    }

    vassert(
      state == compacted_index::recovery_state::index_recovered,
      "Unexpected state {}",
      state);

    auto sz_before = s->size_bytes();
    auto apply_offset = should_apply_delta_time_offset(feature_table);
    auto sz_after = co_await do_self_compact_segment(
      s,
      cfg,
      pb,
      readers_cache,
      resources,
      apply_offset,
      std::move(read_holder),
      feature_table);

    // compaction wasn't executed, return
    if (!sz_after) {
        co_return compaction_result(sz_before);
    }

    pb.segment_compacted();
    pb.add_compaction_removed_bytes(ssize_t(sz_before) - ssize_t(*sz_after));
    s->mark_as_finished_self_compaction();
    co_return compaction_result(sz_before, *sz_after);
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
    auto writer = co_await make_writer_handle(path, cfg.sanitizer_config);
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
    segment::offset_tracker offsets(front.get_term(), front.get_base_offset());
    const auto committed_offset = std::max(
      front.get_committed_offset(), back.get_committed_offset());
    const auto stable_offset = std::max(
      front.get_stable_offset(), back.get_stable_offset());
    const auto dirty_offset = std::max(
      front.get_dirty_offset(), back.get_committed_offset());

    offsets.set_offsets(
      segment::offset_tracker::committed_offset_t{committed_offset},
      segment::offset_tracker::stable_offset_t{stable_offset},
      segment::offset_tracker::dirty_offset_t{dirty_offset});

    // build segment reader over combined data
    auto reader = std::make_unique<segment_reader>(
      path,
      config::shard_local_cfg().storage_read_buffer_size(),
      config::shard_local_cfg().storage_read_readahead_count(),
      cfg.sanitizer_config);
    co_await reader->load_size();

    // build an empty index for the segment
    auto index_name = path.to_index();
    if (co_await ss::file_exists(index_name.string())) {
        co_await ss::remove_file(index_name.string());
    }
    // start the new index with the newest of the broker_timestamps from the
    // segments
    auto new_broker_timestamp = [&]() -> std::optional<model::timestamp> {
        // invariants: segments is not empty, but for completeness handle the
        // empty case
        if (unlikely(segments.empty())) {
            return std::nullopt;
        }
        auto seg_it = *std::ranges::max_element(
          segments, std::less<>{}, [](auto& seg) {
              return seg->index().broker_timestamp();
          });
        return seg_it->index().broker_timestamp();
    }();

    // If both of the segments have a clean_compact_timestamp set, then the new
    // index should use the maximum timestamp. If at least one segment doesn't
    // have a clean_compact_timestamp, then the new index should not either.
    auto new_clean_compact_timestamp =
      [&]() -> std::optional<model::timestamp> {
        // invariants: segments is not empty, but for completeness handle the
        // empty case
        if (unlikely(segments.empty())) {
            return std::nullopt;
        }
        std::optional<model::timestamp> new_ts;
        for (const auto& seg : segments) {
            // If even one segment in the set does not have a
            // clean_compact_timestamp, we must not mark the new index as having
            // one.
            if (!seg->index().has_clean_compact_timestamp()) {
                return std::nullopt;
            }

            auto clean_ts = seg->index().clean_compact_timestamp();
            if (!new_ts.has_value()) {
                new_ts = clean_ts;
            } else {
                new_ts = std::max(new_ts.value(), clean_ts.value());
            }
        }
        return new_ts;
    }();

    // If any of the segments contain a tombstone record, then the new index
    // should reflect that.
    auto new_may_have_tombstone_records = std::ranges::any_of(
      segments,
      [](const auto& s) { return s->index().may_have_tombstone_records(); });

    segment_index index(
      index_name,
      offsets.get_base_offset(),
      segment_index::default_data_buffer_step,
      feature_table,
      cfg.sanitizer_config,
      new_broker_timestamp,
      new_clean_compact_timestamp,
      new_may_have_tombstone_records);

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
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config,
  ss::abort_source* as) {
    return ssx::async_transform(
      segments.begin(),
      segments.end(),
      [io_pc, san_cfg = ntp_sanitizer_config, as](
        ss::lw_shared_ptr<segment>& seg) {
          const auto path = seg->reader().path().to_compacted_index();
          auto f = ss::now();
          if (seg->has_compaction_index()) {
              f = seg->compaction_index().close();
          }
          return f.then([io_pc, san_cfg, path, as]() mutable {
              return make_reader_handle(path, std::move(san_cfg))
                .then([path, io_pc, as](auto reader_fd) {
                    return make_file_backed_compacted_reader(
                      path, reader_fd, io_pc, 64_KiB, as);
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
    return make_indices_readers(
             segments, cfg.iopc, cfg.sanitizer_config, cfg.asrc)
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
                               target_path,
                               cfg.iopc,
                               resources,
                               cfg.sanitizer_config)
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
        } catch (const ss::semaphore_timed_out&) {
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

void mark_segment_as_finished_window_compaction(
  ss::lw_shared_ptr<segment> seg, bool set_clean_compact_timestamp) {
    seg->mark_as_finished_windowed_compaction();
    if (set_clean_compact_timestamp) {
        seg->index().maybe_set_clean_compact_timestamp(model::timestamp::now());
    }
}

bool is_past_tombstone_delete_horizon(
  ss::lw_shared_ptr<segment> seg, const compaction_config& cfg) {
    if (
      seg->index().has_clean_compact_timestamp()
      && cfg.tombstone_retention_ms.has_value()) {
        auto tombstone_delete_horizon = model::timestamp(
          seg->index().clean_compact_timestamp()->value()
          + cfg.tombstone_retention_ms->count());
        return (model::timestamp::now() > tombstone_delete_horizon);
    }

    return false;
}

bool may_have_removable_tombstones(
  ss::lw_shared_ptr<segment> seg, const compaction_config& cfg) {
    return seg->index().may_have_tombstone_records()
           && is_past_tombstone_delete_horizon(seg, cfg);
}

} // namespace storage::internal
