#include "storage/segment_utils.h"

#include "utils/file_sanitizer.h"

#include <seastar/core/reactor.hh>

namespace storage::internal {
using namespace storage; // NOLINT

static inline ss::future<ss::file> make_handle(
  const std::filesystem::path& path,
  ss::open_flags flags,
  ss::file_open_options opt,
  debug_sanitize_files debug) {
    return ss::file_exists(path.string())
      .then([opt, path, debug, flags](bool exists) {
          const auto opf = exists ? flags : flags | ss::open_flags::create;
          return ss::open_file_dma(path.string(), opf, opt)
            .then([debug](ss::file writer) {
                if (debug) {
                    return ss::file(
                      ss::make_shared(file_io_sanitizer(std::move(writer))));
                }
                return writer;
            });
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
    return make_handle(path, ss::open_flags::rw, writer_opts(), debug);
}
/// make file handle with default opts
ss::future<ss::file> make_reader_handle(
  const std::filesystem::path& path, storage::debug_sanitize_files debug) {
    return make_handle(
      path, ss::open_flags::ro, ss::file_open_options{}, debug);
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

} // namespace storage::internal
