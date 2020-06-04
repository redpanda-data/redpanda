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

/// make file handle with default opts
ss::future<ss::file> make_writer_handle(
  const std::filesystem::path& path, storage::debug_sanitize_files debug) {
    ss::file_open_options opt{
      /// We fallocate the full file segment
      .extent_allocation_size_hint = 0,
      /// don't allow truncate calls
      .sloppy_size = false,
    };
    return make_handle(path, ss::open_flags::rw, opt, debug);
}
/// make file handle with default opts
ss::future<ss::file> make_reader_handle(
  const std::filesystem::path& path, storage::debug_sanitize_files debug) {
    return make_handle(
      path, ss::open_flags::ro, ss::file_open_options{}, debug);
}
} // namespace storage::internal
