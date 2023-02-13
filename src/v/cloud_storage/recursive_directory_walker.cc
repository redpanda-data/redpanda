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

#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/logger.h"
#include "utils/gate_guard.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <chrono>
#include <deque>
#include <string_view>
#include <vector>

namespace cloud_storage {

struct walk_accumulator {
    walk_accumulator(ss::sstring start_dir, const access_time_tracker& t)
      : tracker(t)
      , dirlist({start_dir}) {}

    ss::future<> visit(ss::sstring const& target, ss::directory_entry entry) {
        seen_dentries = true;
        auto entry_path = std::filesystem::path(target)
                          / std::filesystem::path(entry.name);
        if (entry.type && entry.type == ss::directory_entry_type::regular) {
            vlog(cst_log.debug, "Regular file found {}", entry_path);

            auto file_stats = co_await ss::file_stat(entry_path.string());

            auto last_access_timepoint
              = tracker.estimate_timestamp(entry_path.native())
                  .value_or(file_stats.time_accessed);

            current_cache_size += static_cast<uint64_t>(file_stats.size);
            files.push_back(
              {last_access_timepoint,
               (std::filesystem::path(target) / entry.name.data()).native(),
               static_cast<uint64_t>(file_stats.size)});
        } else if (
          entry.type && entry.type == ss::directory_entry_type::directory) {
            vlog(cst_log.debug, "Dir found {}", entry_path);
            dirlist.push_front(entry_path.string());
        }
    }

    bool empty() const { return dirlist.empty(); }

    ss::sstring pop() {
        auto r = dirlist.back();
        dirlist.pop_back();
        return r;
    }

    void reset_seen_dentries() { seen_dentries = false; }

    const access_time_tracker& tracker;
    bool seen_dentries{false};
    std::deque<ss::sstring> dirlist;
    fragmented_vector<file_list_item> files;
    uint64_t current_cache_size{0};
};

ss::future<walk_result> recursive_directory_walker::walk(
  ss::sstring start_dir, const access_time_tracker& tracker) {
    gate_guard guard{_gate};

    // Object to accumulate data as we walk directories
    walk_accumulator state(start_dir, tracker);

    std::vector<ss::sstring> empty_dirs;

    while (!state.empty()) {
        auto target = state.pop();
        vassert(
          std::string_view(target).starts_with(start_dir),
          "Looking at directory {}, which is outside of initial dir {}.",
          target,
          start_dir);

        try {
            ss::file target_dir = co_await open_directory(target);

            state.reset_seen_dentries();
            co_await target_dir
              .list_directory([&state, &target](ss::directory_entry entry) {
                  return state.visit(target, std::move(entry));
              })
              .done()
              .finally([target_dir]() mutable { return target_dir.close(); });

            if (unlikely(!state.seen_dentries) && target != start_dir) {
                empty_dirs.push_back(target);
            }
        } catch (std::filesystem::filesystem_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                // skip this directory, move to the ext one
            } else {
                throw;
            }
        }
    }

    co_return walk_result{
      .cache_size = state.current_cache_size,
      .regular_files = std::move(state.files),
      .empty_dirs = std::move(empty_dirs)};
}

ss::future<> recursive_directory_walker::stop() {
    vlog(cst_log.debug, "Stopping recursive directory walker");
    return _gate.close();
}

} // namespace cloud_storage
