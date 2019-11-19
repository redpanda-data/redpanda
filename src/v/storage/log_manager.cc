#include "storage/log_manager.h"

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
#include <regex>

namespace storage {

log_manager::log_manager(log_config config) noexcept
  : _config(std::move(config)) {
}

future<> log_manager::stop() {
    return parallel_for_each(_logs, [](logs_type::value_type& entry) {
        return entry.second->close();
    });
}

static sstring make_filename(
  const sstring& base,
  const model::ntp& ntp,
  const model::offset& base_offset,
  const model::term_id& term,
  const record_version_type& version) {
    return format(
      "{}/{}/{}-{}-{}.log",
      base,
      ntp.path(),
      base_offset(),
      term(),
      to_string(version));
}

future<log_segment_ptr> log_manager::make_log_segment(
  const model::ntp& ntp,
  model::offset base_offset,
  model::term_id term,
  record_version_type version,
  size_t buffer_size) {
    auto flags = open_flags::create | open_flags::rw;
    file_open_options opts;
    opts.extent_allocation_size_hint = 32 << 20;
    opts.sloppy_size = true;
    auto filename = make_filename(
      _config.base_dir, ntp, base_offset, term, version);
    return open_file_dma(filename, flags, std::move(opts))
      .then_wrapped(
        [this, filename, base_offset, term, buffer_size](future<file> f) {
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
}

static std::optional<std::tuple<model::offset, int64_t, record_version_type>>
extract_segment_metadata(const sstring& seg) {
    const std::regex re("^(\\d+)-(\\d+)-([\\x00-\\x7F]+).log$");
    std::cmatch match;
    if (!std::regex_match(seg.c_str(), match, re)) {
        return {};
    }
    return std::make_tuple(
      model::offset(boost::lexical_cast<uint64_t>(match[1].str())),
      boost::lexical_cast<int64_t>(match[2].str()),
      from_string(match[3].str()));
}

// Recover the last segment. Whenever we close a segment, we will likely
// open a new one to which we will direct new writes. That new segment
// might be empty. To optimize log replay, implement #140.
// Called from a seastar::thread.
static void do_recover(log_set& seg_set) {
    if (seg_set.begin() == seg_set.end()) {
        return;
    }
    auto last = seg_set.last();
    auto stat = last->stat().get0();
    auto replayer = log_replayer(last);
    auto recovered = replayer.recover_in_thread(default_priority_class());
    if (recovered) {
        // Max offset is exclusive.
        last->set_last_written_offset(
          *recovered.last_valid_offset() + model::offset(1));
    } else {
        if (stat.st_size == 0) {
            seg_set.pop_last();
            last->close().get();
            remove_file(last->get_filename()).get();
        } else {
            seg_set.pop_last();
            engine()
              .rename_file(
                last->get_filename(), last->get_filename() + ".cannotrecover")
              .get();
        }
    }
}

void set_max_offsets(log_set& seg_set) {
    for (auto it = seg_set.begin(); it != seg_set.end(); ++it) {
        auto next = std::next(it);
        if (next != seg_set.end()) {
            (*it)->set_last_written_offset((*next)->base_offset());
        }
    }
}
future<log_ptr> log_manager::manage(model::ntp ntp) {
    return async([this, ntp = std::move(ntp)]() mutable {
        sstring path = fmt::format("{}/{}", _config.base_dir, ntp.path());
        recursive_touch_directory(path).get();
        std::vector<log_segment_ptr> segs;
        directory_walker::walk(path, [this, path, &segs](directory_entry seg) {
            if (!seg.type || *seg.type != directory_entry_type::regular) {
                return make_ready_future<>();
            }

            auto seg_metadata = extract_segment_metadata(seg.name);
            if (!seg_metadata) {
                stlog.error(
                  "Could not extract name for segment: {}", seg.name);
                return make_ready_future<>();
            }

            auto&& [offset, term, version] = std::move(seg_metadata.value());
            if (version != record_version_type::v1) {
                stlog.error(
                  "Found sement with invalid version: {}", seg.name);
                return make_ready_future<>();
            }

            auto seg_name = path + "/" + seg.name;
            return open_file_dma(seg_name, open_flags::ro)
              .then([this, &segs, seg_name, offset = offset, term = term](
                      file fd) mutable {
                  segs.push_back(make_lw_shared<log_segment>(
                    std::move(seg_name),
                    std::move(fd),
                    model::term_id(term),
                    offset,
                    default_read_buffer_size));
              });
        }).get();

        auto seg_set = log_set(std::move(segs));
        set_max_offsets(seg_set);
        do_recover(seg_set);
        auto ptr = make_lw_shared<log>(ntp, *this, std::move(seg_set));
        _logs.emplace(std::move(ntp), ptr);
        return ptr;
    });
}

} // namespace storage
