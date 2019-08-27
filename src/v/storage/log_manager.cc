#include "storage/log_manager.h"

#include "storage/logger.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/file.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>

#include <fmt/format.h>

#include <exception>
#include <filesystem>
#include <regex>

namespace storage {

log_manager::log_manager(log_config config) noexcept
  : _config(std::move(config)) {
}

future<> log_manager::start() {
    return make_ready_future<>();
}

future<> log_manager::stop() {
    return make_ready_future<>();
}

static std::filesystem::path make_filename(
  const sstring& base,
  const model::namespaced_topic_partition& ntp,
  model::offset base_offset,
  int64_t term,
  record_version_type version) {
    return format(
             "{}/{}/{}/{}/{}-{}-{}.log",
             base,
             ntp.ns,
             ntp.tp.topic.name,
             ntp.tp.partition,
             base_offset.value(),
             term,
             to_string(version))
      .c_str();
}

future<log_segment_ptr> log_manager::make_log_segment(
  const model::namespaced_topic_partition& ntp,
  model::offset base_offset,
  int64_t term,
  record_version_type version,
  size_t buffer_size) {
    auto flags = open_flags::create | open_flags::rw;
    file_open_options opts;
    opts.extent_allocation_size_hint = 32 << 20;
    opts.sloppy_size = true;
    auto filename = make_filename(
      _config.base_dir, ntp, base_offset, term, version);
    // FIXME: Should be done by the controller component.
    return recursive_touch_directory(filename.parent_path().c_str())
      .then([this,
             filename = sstring(filename.c_str()),
             base_offset,
             flags,
             opts = std::move(opts),
             term,
             buffer_size] {
          return open_file_dma(filename, flags, std::move(opts))
            .then_wrapped([this,
                           filename = std::move(filename),
                           base_offset,
                           term,
                           buffer_size](future<file> f) {
                file fd;
                try {
                    fd = f.get0();
                } catch (...) {
                    auto ep = std::current_exception();
                    stlog.error(
                      "Could not create log segment {}. Found exception: {}",
                      filename,
                      ep);
                    return make_exception_future<log_segment_ptr>(ep);
                }
                if (_config.should_sanitize) {
                    fd = file(make_shared(file_io_sanitizer(std::move(fd))));
                }
                auto ls = make_lw_shared<log_segment>(
                  filename, fd, term, base_offset, buffer_size);
                return make_ready_future<log_segment_ptr>(std::move(ls));
            });
      });
}

} // namespace storage
