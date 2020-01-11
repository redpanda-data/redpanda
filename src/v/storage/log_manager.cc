#include "storage/log_manager.h"

#include "model/fundamental.h"
#include "storage/fs_utils.h"
#include "storage/log_replayer.h"
#include "storage/logger.h"
#include "utils/directory_walker.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/file.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <fmt/format.h>

#include <exception>
#include <filesystem>

namespace storage {

log_manager::log_manager(log_config config) noexcept
  : _config(std::move(config)) {}

ss::future<> log_manager::stop() {
    return ss::parallel_for_each(_logs, [](logs_type::value_type& entry) {
        return entry.second->close();
    });
}

ss::future<log_manager::log_handles> log_manager::make_log_segment(
  const model::ntp& ntp,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size) {
    auto path = segment_path::make_segment_path(
      _config.base_dir, ntp, base_offset, term, version);
    stlog.trace("Creating new segment {}", path.string());
    return ss::do_with(
      log_handles{},
      [this, pc, buf_size, path = std::move(path)](log_handles& h) {
          ss::file_open_options opt{
            .extent_allocation_size_hint = 32 << 20,
            .sloppy_size = true,
          };
          const auto flags = ss::open_flags::create | ss::open_flags::rw;
          return ss::open_file_dma(path.string(), flags, std::move(opt))
            .then([&h, pc, this](ss::file writer) {
                if (_config.should_sanitize) {
                    writer = ss::file(
                      ss::make_shared(file_io_sanitizer(std::move(writer))));
                }
                h.appender = std::make_unique<log_segment_appender>(
                  writer, log_segment_appender::options(pc));
            })
            .then(
              [this, buf_size, path] { return open_segment(path, buf_size); })
            .then([&h](segment_reader_ptr p) {
                if (!p) {
                    return ss::make_exception_future<log_handles>(
                      std::runtime_error("Failed to open segment"));
                }
                p->set_last_visible_byte_offset(0);
                h.reader = std::move(p);
                return ss::make_ready_future<log_handles>(std::move(h));
            })
            .handle_exception([&h](auto e) {
                return h.appender->close().then([e = std::move(e)]() mutable {
                    return ss::make_exception_future<log_handles>(std::move(e));
                });
            });
      });
}

// Recover the last segment. Whenever we close a segment, we will likely
// open a new one to which we will direct new writes. That new segment
// might be empty. To optimize log replay, implement #140.
static ss::future<log_set> do_recover(log_set&& seg_set) {
    return ss::async([seg_set = std::move(seg_set)]() mutable {
        if (seg_set.empty()) {
            return seg_set;
        }
        auto last = seg_set.last();
        auto stat = last->stat().get0();
        auto replayer = log_replayer(last);
        auto recovered = replayer.recover_in_thread(
          ss::default_priority_class());
        if (recovered) {
            // Max offset is inclusive
            last->set_last_written_offset(*recovered.last_valid_offset());
        } else {
            if (stat.st_size == 0) {
                seg_set.pop_last();
                last->close().get();
                remove_file(last->get_filename()).get();
            } else {
                seg_set.pop_last();
                ss::engine()
                  .rename_file(
                    last->get_filename(),
                    last->get_filename() + ".cannotrecover")
                  .get();
            }
        }
        return seg_set;
    });
}

static void set_max_offsets(log_set& seg_set) {
    for (auto it = seg_set.begin(); it != seg_set.end(); ++it) {
        auto next = std::next(it);
        if (next != seg_set.end()) {
            (*it)->set_last_written_offset(
              (*next)->base_offset() - model::offset(1));
        }
    }
}

ss::future<segment_reader_ptr>
log_manager::open_segment(const std::filesystem::path& path, size_t buf_size) {
    std::optional<segment_path::metadata> meta;
    try {
        meta = segment_path::parse_segment_filename(path.filename().string());
    } catch (...) {
        stlog.error("An error occurred parsing filename: {}", path);
        return ss::make_exception_future<segment_reader_ptr>(
          std::current_exception());
    }

    if (!meta) {
        stlog.trace("Cannot open non-segment file: {}", path);
        return ss::make_ready_future<segment_reader_ptr>(nullptr);
    }

    if (meta->version != record_version_type::v1) {
        stlog.error(
          "Segment has invalid version {} != {} path {}",
          meta->version,
          record_version_type::v1,
          path);
        return ss::make_exception_future<segment_reader_ptr>(
          std::runtime_error("Invalid segment format"));
    }

    return ss::open_file_dma(path.string(), ss::open_flags::ro)
      .then([](ss::file f) {
          return f.stat().then([f](struct stat s) {
              return ss::make_ready_future<uint64_t, ss::file>(s.st_size, f);
          });
      })
      .then([this, buf_size, path = std::move(path), meta](
              uint64_t size, ss::file fd) {
          if (_config.should_sanitize) {
              fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
          }
          return ss::make_lw_shared<log_segment_reader>(
            path.string(),
            std::move(fd),
            model::term_id(meta->term),
            meta->base_offset,
            size,
            buf_size);
      });
}

ss::future<std::vector<segment_reader_ptr>>
log_manager::open_segments(ss::sstring dir) {
    using segs_type = std::vector<segment_reader_ptr>;
    return ss::do_with(
      segs_type{}, [this, dir = std::move(dir)](segs_type& segs) {
          auto f = directory_walker::walk(
            dir, [this, dir, &segs](ss::directory_entry seg) {
                /*
                 * Skip non-regular files (including links)
                 */
                if (
                  !seg.type || *seg.type != ss::directory_entry_type::regular) {
                    return ss::make_ready_future<>();
                }
                auto path = std::filesystem::path(
                  fmt::format("{}/{}", dir, seg.name));
                return open_segment(std::move(path))
                  .then([&segs](segment_reader_ptr p) {
                      if (p) { // skip non-segment files
                          segs.push_back(std::move(p));
                      }
                  });
            });
          /*
           * if the directory walker returns an exceptional future then all the
           * segment readers that were created are cleaned up by ss::do_with.
           */
          return f.then([&segs]() mutable {
              return ss::make_ready_future<segs_type>(std::move(segs));
          });
      });
}

ss::future<log_ptr> log_manager::manage(model::ntp ntp) {
    ss::sstring path = fmt::format("{}/{}", _config.base_dir, ntp.path());
    return recursive_touch_directory(path)
      .then([this, path] { return open_segments(path); })
      .then([this, ntp](std::vector<segment_reader_ptr> segs) {
          auto seg_set = log_set(std::move(segs));
          set_max_offsets(seg_set);
          return do_recover(std::move(seg_set))
            .then([this, ntp](log_set seg_set) {
                auto ptr = ss::make_lw_shared<storage::log>(
                  ntp, *this, std::move(seg_set));
                _logs.emplace(std::move(ntp), ptr);
                return ptr;
            });
      });
}

} // namespace storage
