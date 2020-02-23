#include "storage/log_manager.h"

#include "model/fundamental.h"
#include "storage/fs_utils.h"
#include "storage/log.h"
#include "storage/log_replayer.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/log_set.h"
#include "storage/logger.h"
#include "storage/segment.h"
#include "storage/segment_index.h"
#include "utils/directory_walker.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
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
    return _open_gate.close().then([this] {
        return ss::parallel_for_each(_logs, [](logs_type::value_type& entry) {
            return entry.second.close();
        });
    });
}

ss::future<std::unique_ptr<segment>> log_manager::make_log_segment(
  const model::ntp& ntp,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size) {
    return ss::with_gate(
      _open_gate, [this, &ntp, base_offset, term, pc, version, buf_size] {
          return do_make_log_segment(
            ntp, base_offset, term, pc, version, buf_size);
      });
}
ss::future<std::unique_ptr<segment>> log_manager::do_make_log_segment(
  const model::ntp& ntp,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size) {
    auto path = segment_path::make_segment_path(
      _config.base_dir, ntp, base_offset, term, version);
    stlog.trace("Creating new segment {}", path.string());
    ss::file_open_options opt{
      .extent_allocation_size_hint = 32 << 20,
      .sloppy_size = true,
    };
    const auto flags = ss::open_flags::create | ss::open_flags::rw;
    return ss::open_file_dma(path.string(), flags, std::move(opt))
      .then([pc, this](ss::file writer) {
          if (_config.should_sanitize) {
              writer = ss::file(
                ss::make_shared(file_io_sanitizer(std::move(writer))));
          }
          return std::make_unique<log_segment_appender>(
            writer, log_segment_appender::options(pc));
      })
      .then([this, buf_size, path](segment_appender_ptr a) {
          return open_segment(path, buf_size)
            .then_wrapped([this, a = std::move(a)](
                            ss::future<std::unique_ptr<segment>> s) mutable {
                try {
                    auto seg = s.get0();
                    seg->reader()->set_last_visible_byte_offset(0);
                    auto cache = std::make_unique<batch_cache_index>(
                      _batch_cache);
                    return ss::make_ready_future<std::unique_ptr<segment>>(
                      std::make_unique<segment>(
                        seg->reader(),
                        std::move(seg->index()),
                        std::move(a),
                        std::move(cache)));
                } catch (...) {
                    auto raw = a.get();
                    return raw->close().then(
                      [a = std::move(a), e = std::current_exception()] {
                          return ss::make_exception_future<
                            std::unique_ptr<segment>>(e);
                      });
                }
            });
      });
}

// Recover the last segment. Whenever we close a segment, we will likely
// open a new one to which we will direct new writes. That new segment
// might be empty. To optimize log replay, implement #140.
static ss::future<log_set> do_recover(log_set&& segments) {
    return ss::async([segments = std::move(segments)]() mutable {
        if (segments.empty()) {
            return std::move(segments);
        }
        log_set::underlying_t good = std::move(segments).release();
        log_set::underlying_t to_recover;
        to_recover.push_back(std::move(good.back()));
        good.pop_back(); // always recover last segment
        auto good_end = std::partition(
          good.begin(), good.end(), [](std::unique_ptr<segment>& ss) {
              auto& s = *ss;
              try {
                  return s.index()->materialize_index().get0();
              } catch (...) {
                  stlog.info(
                    "Error materializing index:{}. Recovering parent "
                    "segment:{}. Details:{}",
                    s.index()->filename(),
                    s.reader()->filename(),
                    std::current_exception());
              }
              return false;
          });
        std::move(
          std::move_iterator(good_end),
          std::move_iterator(good.end()),
          std::back_inserter(to_recover));
        good.erase(
          good_end, good.end()); // remove all the ones we copied into recover

        for (auto& s : to_recover) {
            auto stat = s->reader()->stat().get0();
            if (stat.st_size == 0) {
                stlog.info("Removing empty segment: {}", s);
                s->close().get();
                ss::remove_file(s->reader()->filename()).get();
                ss::remove_file(s->index()->filename()).get();
                continue;
            }
            auto replayer = log_replayer(*s);
            auto recovered = replayer.recover_in_thread(
              ss::default_priority_class());
            if (!recovered) {
                stlog.info("Unable to recover segment: {}", s);
                s->close().get();
                ss::rename_file(
                  s->reader()->filename(),
                  s->reader()->filename() + ".cannotrecover")
                  .get();
                continue;
            }
            s->truncate(
               recovered.last_offset.value(),
               recovered.truncate_file_pos.value())
              .get();
            // persist index
            s->index()->flush().get();
            stlog.info("Recovered: {}", s);
            good.emplace_back(std::move(s));
        }
        return log_set(std::move(good));
    });
}

static void set_max_offsets(log_set& segments) {
    for (auto it = segments.begin(); it != segments.end(); ++it) {
        auto next = std::next(it);
        if (next != segments.end()) {
            (*it)->reader()->set_last_written_offset(
              (*next)->reader()->base_offset() - model::offset(1));
        }
    }
}

ss::future<std::unique_ptr<segment>>
log_manager::open_segment(const std::filesystem::path& path, size_t buf_size) {
    auto const meta = segment_path::parse_segment_filename(
      path.filename().string());
    if (meta->version != record_version_type::v1) {
        return ss::make_exception_future<std::unique_ptr<segment>>(
          std::runtime_error(fmt::format(
            "Segment has invalid version {} != {} path {}",
            meta->version,
            record_version_type::v1,
            path)));
    }
    // note: this file _must_ be open in `ro` mode only. Seastar uses dma files
    // with no shared buffer cache around them. When we use a writer w/ dma
    // at the same time as the reader, we need a way to synchronize filesytem
    // metadata. In order to prevent expensive synchronization primitives
    // fsyncing both *reads* and *writes* we open this file in ro mode and if
    // raft requires truncation, we open yet-another handle w/ rw mode just for
    // the truncation which gives us all the benefits of preventing x-file
    // synchronization This is fine, because truncation to sealed segments are
    // supposed to be very rare events. The hotpath of truncating the appender,
    // is optimized.
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
      })
      .then([this](segment_reader_ptr ptr) {
          auto index_name = ptr->filename() + ".offset_index";
          return ss::open_file_dma(
                   index_name, ss::open_flags::create | ss::open_flags::rw)
            .then_wrapped([ptr, index_name](ss::future<ss::file> f) {
                try {
                    auto fd = f.get0();
                    return ss::make_ready_future<ss::file>(fd);
                } catch (...) {
                    return ptr->close().then(
                      [ptr, e = std::current_exception()] {
                          return ss::make_exception_future<ss::file>(e);
                      });
                }
            })
            .then([this, ptr, index_name](ss::file f) {
                if (_config.should_sanitize) {
                    f = ss::file(
                      ss::make_shared(file_io_sanitizer(std::move(f))));
                }
                segment_index_ptr idx = std::make_unique<segment_index>(
                  index_name,
                  f,
                  ptr->base_offset(),
                  segment_index::default_data_buffer_step);
                auto cache = std::make_unique<batch_cache_index>(_batch_cache);
                return ss::make_ready_future<std::unique_ptr<segment>>(
                  std::make_unique<segment>(
                    ptr, std::move(idx), nullptr, std::move(cache)));
            });
      });
}

ss::future<std::vector<std::unique_ptr<segment>>>
log_manager::open_segments(ss::sstring dir) {
    using segs_type = std::vector<std::unique_ptr<segment>>;
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
                try {
                    auto is_valid = segment_path::parse_segment_filename(
                      path.filename().string());
                    if (!is_valid) {
                        return ss::make_ready_future<>();
                    }
                } catch (...) {
                    // not a reader filename
                    return ss::make_ready_future<>();
                }
                return open_segment(std::move(path))
                  .then([&segs](std::unique_ptr<segment> p) {
                      segs.push_back(std::move(p));
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

ss::future<log>
log_manager::manage(model::ntp ntp, log_manager::storage_type type) {
    return ss::with_gate(
      _open_gate, [this, ntp = std::move(ntp), type]() mutable {
          return do_manage(std::move(ntp), type);
      });
}

ss::future<log>
log_manager::do_manage(model::ntp ntp, log_manager::storage_type type) {
    if (_config.base_dir.empty()) {
        return ss::make_exception_future<log>(std::runtime_error(
          "log_manager:: cannot have empty config.base_dir"));
    }
    ss::sstring path = fmt::format("{}/{}", _config.base_dir, ntp.path());
    vassert(_logs.find(ntp) == _logs.end(), "cannot double register same ntp");
    if (type == storage_type::memory) {
        auto l = storage::make_memory_backed_log(ntp, path);
        _logs.emplace(std::move(ntp), l);
        // raft uses the directory so it have to be created for in mem
        // implementation
        return recursive_touch_directory(path).then([l] { return l; });
    }
    return recursive_touch_directory(path)
      .then([this, path] { return open_segments(path); })
      .then([this, ntp](std::vector<std::unique_ptr<segment>> segs) {
          auto segments = log_set(std::move(segs));
          set_max_offsets(segments);
          return do_recover(std::move(segments))
            .then([this, ntp](log_set segments) mutable {
                auto ptr = storage::make_disk_backed_log(
                  ntp, *this, std::move(segments));
                _logs.emplace(std::move(ntp), ptr);
                return ptr;
            });
      });
}
std::ostream& operator<<(std::ostream& o, log_manager::storage_type t) {
    switch (t) {
    case log_manager::storage_type::memory:
        return o << "{memory}";
    case log_manager::storage_type::disk:
        return o << "{disk}";
    }
    return o << "{unknown-storage-type}";
}
} // namespace storage
