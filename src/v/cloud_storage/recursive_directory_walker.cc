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

ss::future<std::pair<uint64_t, std::vector<file_list_item>>>
recursive_directory_walker::walk(ss::sstring start_dir) {
    gate_guard guard{_gate};

    std::vector<file_list_item> files;
    uint64_t current_cache_size(0);

    std::deque<ss::sstring> dirlist = {start_dir};

    while (!dirlist.empty()) {
        auto target = dirlist.back();
        dirlist.pop_back();
        vassert(
          std::string_view(target).starts_with(start_dir),
          "Looking at directory {}, which is outside of initial dir {}.",
          target,
          start_dir);

        try {
            ss::file target_dir = co_await open_directory(target);
            auto sub = target_dir.list_directory(
              [&files, &current_cache_size, &dirlist, _target{target}](
                ss::directory_entry entry) -> ss::future<> {
                  auto target{_target};
                  vlog(cst_log.debug, "Looking at directory {}", target);

                  auto entry_path = std::filesystem::path(target)
                                    / std::filesystem::path(entry.name);
                  if (
                    entry.type
                    && entry.type == ss::directory_entry_type::regular) {
                      vlog(cst_log.debug, "Regular file found {}", entry_path);

                      auto file_stats = co_await ss::file_stat(
                        entry_path.string());

                      auto last_access_timepoint = file_stats.time_accessed;

                      current_cache_size += static_cast<uint64_t>(
                        file_stats.size);
                      files.push_back(
                        {last_access_timepoint,
                         (std::filesystem::path(target) / entry.name.data())
                           .native(),
                         static_cast<uint64_t>(file_stats.size)});
                  } else if (
                    entry.type
                    && entry.type == ss::directory_entry_type::directory) {
                      vlog(cst_log.debug, "Dir found {}", entry_path);
                      dirlist.push_front(entry_path.string());
                  }
                  co_return;
              });
            co_await sub.done().finally(
              [target_dir]() mutable { return target_dir.close(); });
        } catch (std::filesystem::filesystem_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                // skip this directory, move to the ext one
            } else {
                throw;
            }
        }
    }
    std::sort(files.begin(), files.end(), [](auto& a, auto& b) {
        return a.access_time < b.access_time;
    });

    co_return std::make_pair(current_cache_size, files);
}

ss::future<> recursive_directory_walker::stop() {
    vlog(cst_log.debug, "Stopping recursive directory walker");
    return _gate.close();
}

} // namespace cloud_storage
