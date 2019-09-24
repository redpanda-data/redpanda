#include "storage/log_manager.h"

#include "storage/log_replayer.h"
#include "storage/logger.h"
#include "storage/shard_assignment.h"
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
    return parallel_for_each(
      _logs,
      [this](
        std::pair<const model::namespaced_topic_partition, log_ptr>& entry) {
          return entry.second->close();
      });
}

static std::filesystem::path make_filename(
  const sstring& base,
  const model::namespaced_topic_partition& ntp,
  model::offset base_offset,
  model::term_id term,
  record_version_type version) {
    return format(
             "{}/{}/{}-{}-{}.log",
             base,
             ntp.path(),
             base_offset.value(),
             term(),
             to_string(version))
      .c_str();
}

future<log_segment_ptr> log_manager::make_log_segment(
  const model::namespaced_topic_partition& ntp,
  model::offset base_offset,
  model::term_id term,
  record_version_type version,
  size_t buffer_size) {
    auto flags = open_flags::create | open_flags::rw;
    file_open_options opts;
    opts.extent_allocation_size_hint = 32 << 20;
    opts.sloppy_size = true;
    auto filename = make_filename(
      _config.base_dir, ntp, base_offset, term(), version);
    // FIXME: Should be done by the controller component.
    return recursive_touch_directory(filename.parent_path().c_str())
      .then([this,
             filename = sstring(filename.c_str()),
             base_offset,
             flags,
             opts = std::move(opts),
             term = term(),
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
                    stlog().error(
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

static bool is_valid_name(const sstring& filename) {
    // cannot include '.' in the regex to simplify life
    const std::regex re("^[A-Za-z0-9|-|_]+$");
    return std::regex_match(filename.data(), re);
}

future<> log_manager::load_logs() {
    return do_load_logs(_config.base_dir, {}, 0);
}

future<> log_manager::do_load_logs(
  sstring path, std::array<sstring, 3> components, unsigned level) {
    if (level == components.size()) {
        try {
            return load_segments(
              std::move(path),
              model::namespaced_topic_partition{
                model::ns{std::move(components[0])},
                model::topic_partition{
                  model::topic{std::move(components[1])},
                  model::partition_id{
                    boost::lexical_cast<model::partition_id::type>(
                      components[2])}}});
        } catch (const boost::bad_lexical_cast& not_a_log_file) {
        }
        return make_ready_future<>();
    }
    return directory_walker::walk(
      path,
      [this, path, components = std::move(components), level](
        directory_entry de) {
          if (!de.type || *de.type != directory_entry_type::directory) {
              return make_ready_future<>();
          }
          if (!is_valid_name(de.name)) {
              return make_ready_future<>();
          }
          auto c = components;
          c[level] = de.name;
          return do_load_logs(path + "/" + de.name, std::move(c), level + 1);
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
        last->set_last_written_offset(
          *recovered.last_valid_offset() + 1); // Max offset is exclusive.
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

future<> log_manager::load_segments(
  sstring path, model::namespaced_topic_partition ntp) {
    if (engine().cpu_id() != shard_of(ntp)) {
        return make_ready_future<>();
    }

    return async([this,
                  path = std::move(path),
                  ntp = std::move(ntp)]() mutable {
        std::vector<log_segment_ptr> segs;
        directory_walker::walk(path, [this, path, &segs](directory_entry seg) {
            if (!seg.type || *seg.type != directory_entry_type::regular) {
                return make_ready_future<>();
            }

            auto seg_metadata = extract_segment_metadata(seg.name);
            if (!seg_metadata) {
                return make_ready_future<>();
            }

            auto [offset, term, version] = std::move(*seg_metadata);
            if (version != record_version_type::v1) {
                return make_ready_future<>();
            }

            auto seg_name = path + "/" + seg.name;
            return open_file_dma(seg_name, open_flags::ro)
              .then([this, &segs, seg_name, offset, term](file fd) mutable {
                  segs.push_back(make_lw_shared<log_segment>(
                    std::move(seg_name),
                    std::move(fd),
                    term,
                    offset,
                    default_read_buffer_size));
              });
        }).get();

        auto seg_set = log_set(std::move(segs));
        set_max_offsets(seg_set);
        do_recover(seg_set);
        _logs.emplace(ntp, make_lw_shared<log>(ntp, *this, std::move(seg_set)));
    });
}

} // namespace storage
