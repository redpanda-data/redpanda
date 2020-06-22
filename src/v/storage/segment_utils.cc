#include "storage/segment_utils.h"

#include "model/timeout_clock.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/logger.h"
#include "units.h"
#include "utils/file_sanitizer.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <fmt/core.h>
#include <roaring/roaring.hh>

namespace storage::internal {
using namespace storage; // NOLINT

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

ss::future<Roaring> index_of_index_of_entries(compacted_index_reader reader) {
    reader.reset();
    return reader.load_footer().then([reader](compacted_index::footer) mutable {
        return reader.consume(
          compaction_key_reducer(std::nullopt), model::no_timeout);
    });
}

class compaction_second_pass_functor {
public:
    compaction_second_pass_functor(Roaring b, compacted_index_writer& w)
      : _bm(std::move(b))
      , _writer(&w) {}

    ss::future<ss::stop_iteration> operator()(compacted_index::entry&& e) {
        using stop_t = ss::stop_iteration;
        const bool should_add = _bm.contains(_i);
        ++_i;
        if (should_add) {
            bytes_view bv = e.key;
            return _writer->index(bv, e.offset, e.delta)
              .then([k = std::move(e.key)] {
                  return ss::make_ready_future<stop_t>(stop_t::no);
              });
        }
        return ss::make_ready_future<stop_t>(stop_t::no);
    }
    void end_of_stream() {}

private:
    uint32_t _i = 0;
    Roaring _bm;
    compacted_index_writer* _writer;
};

ss::future<> copy_filtered_entries(
  compacted_index_reader reader,
  Roaring to_copy_index,
  compacted_index_writer writer) {
    reader.reset();
    return ss::do_with(
      std::move(writer),
      [bm = std::move(to_copy_index),
       reader](compacted_index_writer& writer) mutable {
          return reader.load_footer()
            .then([](compacted_index::footer) {})
            .then([reader, bm = std::move(bm), &writer]() mutable {
                return reader.consume(
                  compaction_second_pass_functor(std::move(bm), writer),
                  model::no_timeout);
            })
            // must be last
            .finally([&writer] {
                writer.set_flag(compacted_index::footer_flags::self_compaction);
                // do not handle exception on the close
                return writer.close();
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
          return reader
            .consume(compaction_key_reducer(std::nullopt), model::no_timeout)
            .then([reader, cfg](Roaring bitmap) {
                const auto tmpname = std::filesystem::path(
                  fmt::format("{}.staging", reader.filename()));
                return make_handle(
                         tmpname,
                         ss::open_flags::rw | ss::open_flags::truncate,
                         writer_opts(),
                         cfg.sanitize)
                  .then([tmpname, cfg, reader, bm = std::move(bitmap)](
                          ss::file f) mutable {
                      auto writer = make_file_backed_compacted_index(
                        tmpname.string(),
                        std::move(f),
                        cfg.iopc,
                        // TODO: pass this memory from the cfg
                        segment_appender::write_behind_memory / 2);
                      return copy_filtered_entries(
                        reader, std::move(bm), std::move(writer));
                  })
                  .then([old_name = tmpname.string(),
                         new_name = reader.filename()] {
                      // from glibc: If oldname is not a directory, then any
                      // existing file named newname is removed during the
                      // renaming operation
                      return ss::rename_file(old_name, new_name);
                  });
            });
      })
      .finally([reader]() mutable {
          return reader.close().then_wrapped(
            [reader](ss::future<>) { /*ignore*/ });
      });
}

ss::future<offset_compaction_list>
generate_compacted_list(compacted_index_reader) {
    return ss::make_ready_future<offset_compaction_list>(
      offset_compaction_list(model::offset{}));
}

ss::future<> write_compacted_segment(
  ss::lw_shared_ptr<segment>, segment_appender_ptr, offset_compaction_list) {
    return ss::now();
}

ss::future<>
self_compact_segment(ss::lw_shared_ptr<segment> s, compaction_config cfg) {
    if (s->has_appender()) {
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "Cannot compact an active segment. cfg:{} - segment:{}", cfg, s)));
    }

    auto compacted_path = std::filesystem::path(s->reader().filename());
    compacted_path.replace_extension(".compaction_index");
    vlog(stlog.trace, "compacting index:{}", compacted_path);
    return make_reader_handle(compacted_path, cfg.sanitize)
      .then([cfg, compacted_path, s](ss::file f) {
          auto reader = make_file_backed_compacted_reader(
            compacted_path.string(), std::move(f), cfg.iopc, 64_KiB);
          return write_clean_compacted_index(reader, cfg);
      })
      .then([compacted_path, cfg] {
          /// re-open the index *after* we have written it clean above
          return make_reader_handle(compacted_path, cfg.sanitize)
            .then([cfg, compacted_path](ss::file f) {
                auto reader = make_file_backed_compacted_reader(
                  compacted_path.string(), std::move(f), cfg.iopc, 64_KiB);
                return generate_compacted_list(reader).finally(
                  [reader]() mutable {
                      return reader.close().then_wrapped([](ss::future<>) {});
                  });
            });
      })
      .then([cfg, s](offset_compaction_list list) {
          const auto tmpname = std::filesystem::path(
            fmt::format("{}.staging", s->reader().filename()));
          return make_segment_appender(
                   tmpname,
                   cfg.sanitize,
                   segment_appender::chunks_no_buffer,
                   cfg.iopc)
            .then([s, list = std::move(list)](segment_appender_ptr w) mutable {
                return write_compacted_segment(
                  s, std::move(w), std::move(list));
            });
      });
}
} // namespace storage::internal
