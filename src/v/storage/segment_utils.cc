#include "storage/segment_utils.h"

#include "model/timeout_clock.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/lock_manager.h"
#include "storage/log_reader.h"
#include "storage/logger.h"
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
    return reader.consume(truncation_offset_reducer{}, model::no_timeout)
      .then([reader](Roaring to_keep) mutable {
          reader.reset();
          return reader.consume(
            compaction_key_reducer(std::move(to_keep)), model::no_timeout);
      });
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
    using flags = compacted_index::footer_flags;

    return reader.load_footer()
      .then([cfg, reader](compacted_index::footer footer) mutable {
          if (
            (footer.flags & flags::self_compaction) == flags::self_compaction) {
              return ss::now();
          }
          return reader.verify_integrity().then([reader, cfg] {
              return do_write_clean_compacted_index(reader, cfg);
          });
      })
      .finally([reader]() mutable {
          return reader.close().then_wrapped(
            [reader](ss::future<>) { /*ignore*/ });
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
    // TODO: add exception safety to this method
    // TODO: regenerate the index if missing
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
ss::future<> do_copy_segment_data(
  ss::lw_shared_ptr<segment> s,
  compaction_config cfg,
  model::record_batch_reader reader) {
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
      .then([cfg, s](compacted_offset_list list) {
          const auto tmpname = data_segment_staging_name(s);
          return make_segment_appender(
                   tmpname,
                   cfg.sanitize,
                   segment_appender::chunks_no_buffer,
                   cfg.iopc)
            .then([l = std::move(list)](segment_appender_ptr w) mutable {
                return copy_data_segment_reducer(std::move(l), std::move(w));
            });
      })
      .then([r = std::move(reader)](copy_data_segment_reducer red) mutable {
          return std::move(r).consume(std::move(red), model::no_timeout);
      });
}

model::record_batch_reader create_segment_full_reader(
  ss::lw_shared_ptr<storage::segment> s,
  storage::compaction_config cfg,
  storage::probe& pb,
  ss::rwlock::holder h) {
    auto o = s->offsets();
    auto reader_cfg = log_reader_config(
      o.base_offset,
      o.dirty_offset,
      0,
      s->size_bytes(),
      cfg.iopc,
      std::nullopt,
      std::nullopt,
      std::nullopt);
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
  storage::compaction_config cfg) {
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
      .then([s](ss::file f) mutable {
          return f.stat()
            .then([f](struct stat s) {
                return ss::make_ready_future<uint64_t, ss::file>(s.st_size, f);
            })
            .then([s](uint64_t size, ss::file fd) {
                auto r = segment_reader(
                  s->reader().filename(),
                  std::move(fd),
                  size,
                  default_segment_readahead_size);
                std::swap(s->reader(), r);
            });
      });
}

ss::future<> self_compact_segment(
  ss::lw_shared_ptr<segment> s, compaction_config cfg, storage::probe& pb) {
    if (s->has_appender()) {
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "Cannot compact an active segment. cfg:{} - segment:{}", cfg, s)));
    }
    return s->read_lock()
      .then([cfg, s, &pb](ss::rwlock::holder h) {
          if (s->is_closed()) {
              return ss::make_exception_future<>(segment_closed_exception());
          }
          auto reader = create_segment_full_reader(s, cfg, pb, std::move(h));
          return do_compact_segment_index(s, cfg)
            // copy the bytes after segment is good - note that we
            // need to do it with the READ-lock, not the write lock
            .then([reader = std::move(reader), cfg, s]() mutable {
                return do_copy_segment_data(s, cfg, std::move(reader));
            });
      })
      .then([s] { return s->write_lock(); })
      .then([cfg, s](ss::rwlock::holder h) {
          if (s->is_closed()) {
              return ss::make_exception_future<>(segment_closed_exception());
          }
          auto compacted_file = data_segment_staging_name(s);
          return do_swap_data_file_handles(compacted_file, s, cfg)
            // Nothing can happen in between this lock
            .finally([h = std::move(h)] {});
      });
}

} // namespace storage::internal
