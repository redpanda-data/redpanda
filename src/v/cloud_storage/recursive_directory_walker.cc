/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recursive_directory_walker.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/logger.h"
#include "ssx/watchdog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <chrono>
#include <deque>
#include <string_view>
#include <vector>

namespace cloud_storage {

struct walk_accumulator {
    walk_accumulator(
      ss::sstring start_dir,
      const access_time_tracker& t,
      std::optional<recursive_directory_walker::filter_type> collect_filter)
      : tracker(t)
      , dirlist({std::move(start_dir)})
      , filter(std::move(collect_filter)) {}

    ss::future<> visit(const ss::sstring& target, ss::directory_entry entry) {
        auto entry_path = fmt::format("{}/{}", target, entry.name);
        if (entry.type && entry.type == ss::directory_entry_type::regular) {
            size_t file_size{0};
            std::chrono::system_clock::time_point atime;
            if (const auto tracker_entry = tracker.get(entry_path);
                tracker_entry.has_value()) {
                file_size = tracker_entry->size;
                atime = tracker_entry->time_point();
            } else {
                auto file_stats = co_await ss::file_stat(entry_path);
                file_size = file_stats.size;
                atime = file_stats.time_accessed;
            }

            vlog(
              cst_log.debug,
              "Regular file found {} ({})",
              entry_path,
              file_size);

            current_cache_size += static_cast<uint64_t>(file_size);
            if (entry_path.ends_with(cache_tmp_file_extension)) {
                tmp_files_size += file_size;
            }

            if (!filter || filter.value()(entry_path)) {
                files.push_back(
                  {atime,
                   (std::filesystem::path(target) / entry.name.data()).native(),
                   static_cast<uint64_t>(file_size)});
            } else if (filter) {
                ++filtered_out_files;
            }
        } else if (
          entry.type && entry.type == ss::directory_entry_type::directory) {
            vlog(cst_log.debug, "Dir found {}", entry_path);
            dirlist.emplace_front(entry_path);
        }
    }

    bool empty() const { return dirlist.empty(); }

    ss::sstring pop() {
        auto r = std::move(dirlist.back());
        dirlist.pop_back();
        return r;
    }

    const access_time_tracker& tracker;
    std::deque<ss::sstring> dirlist;
    std::optional<recursive_directory_walker::filter_type> filter;
    fragmented_vector<file_list_item> files;
    uint64_t current_cache_size{0};
    size_t filtered_out_files{0};
    size_t tmp_files_size{0};
};
} // namespace cloud_storage

namespace {
ss::future<> walker_process_directory(
  const ss::sstring& start_dir,
  ss::sstring target,
  cloud_storage::walk_accumulator& state,
  fragmented_vector<ss::sstring>& empty_dirs) {
    try {
        ss::file target_dir = co_await open_directory(target);

        bool seen_dentries = false;
        co_await target_dir
          .list_directory(
            [&state, &target, &seen_dentries](ss::directory_entry entry) {
                seen_dentries = true;
                return state.visit(target, std::move(entry));
            })
          .done()
          .finally([target_dir]() mutable { return target_dir.close(); });

        if (unlikely(!seen_dentries) && target != start_dir) {
            empty_dirs.push_back(target);
        }
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            // skip this directory, move to the next one
        } else {
            throw;
        }
    }
}
} // namespace
namespace cloud_storage {

ss::future<walk_result> recursive_directory_walker::walk(
  ss::sstring start_dir,
  const access_time_tracker& tracker,
  uint16_t max_concurrency,
  std::optional<filter_type> collect_filter) {
    vassert(max_concurrency > 0, "Max concurrency must be greater than 0");

    auto guard = _gate.hold();

    watchdog wd1m(std::chrono::seconds(60), [] {
        vlog(cst_log.info, "Directory walk is taking more than 1 min");
    });
    watchdog wd10m(std::chrono::seconds(600), [] {
        vlog(cst_log.warn, "Directory walk is taking more than 10 min");
    });

    // Object to accumulate data as we walk directories
    walk_accumulator state(start_dir, tracker, std::move(collect_filter));

    fragmented_vector<ss::sstring> empty_dirs;

    // Listing directories involves blocking I/O which in Seastar is serviced by
    // a dedicated thread pool and workqueue. When listing directories with
    // large number of sub-directories this leads to a large queue of work items
    // for each of which we have to incur the cost of task switching. Even for a
    // lightly loaded reactor, it can take up to 1ms for a full round-trip. The
    // round-trip time is ~100x the syscall duration. With 2 level directory
    // structure, 100K files, 2 getdents64 calls per directory this adds up to
    // 400K syscalls and 6m of wall time.
    //
    // By running the directory listing in parallel we also parallelize the
    // round-trip overhead. This leads to a significant speedup in the
    // directory listing phase.
    //
    // Limit the number of concurrent directory reads to avoid running out of
    // file descriptors.
    //
    // Empirical testing shows that this value is a good balance between
    // performance and resource usage.
    std::vector<ss::sstring> targets;
    targets.reserve(max_concurrency);

    while (!state.empty()) {
        targets.clear();

        auto concurrency = std::min(
          state.dirlist.size(), size_t(max_concurrency));

        for (size_t i = 0; i < concurrency; ++i) {
            auto target = state.pop();
            vassert(
              std::string_view(target).starts_with(start_dir),
              "Looking at directory {}, which is outside of initial dir "
              "{}.",
              target,
              start_dir);
            targets.push_back(std::move(target));
        }

        co_await ss::parallel_for_each(
          targets, [&start_dir, &state, &empty_dirs](ss::sstring target) {
              return walker_process_directory(
                start_dir, std::move(target), state, empty_dirs);
          });
    }

    co_return walk_result{
      .cache_size = state.current_cache_size,
      .filtered_out_files = state.filtered_out_files,
      .regular_files = std::move(state.files),
      .empty_dirs = std::move(empty_dirs),
      .tmp_files_size = state.tmp_files_size};
}

ss::future<> recursive_directory_walker::stop() {
    vlog(cst_log.debug, "Stopping recursive directory walker");
    return _gate.close();
}

} // namespace cloud_storage
