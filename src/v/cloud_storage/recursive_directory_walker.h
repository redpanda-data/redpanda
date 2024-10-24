/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "container/fragmented_vector.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <chrono>

namespace cloud_storage {

inline constexpr auto cache_tmp_file_extension{".part"};

class access_time_tracker;

struct file_list_item {
    std::chrono::system_clock::time_point access_time;
    ss::sstring path;
    uint64_t size;
};

struct walk_result {
    uint64_t cache_size{0};
    size_t filtered_out_files{0};
    fragmented_vector<file_list_item> regular_files;
    fragmented_vector<ss::sstring> empty_dirs;
    size_t tmp_files_size{0};
};

class recursive_directory_walker {
public:
    using filter_type = std::function<bool(std::string_view)>;

    ss::future<> stop();

    // recursively walks start_dir, returns the total size of files in this
    // directory and a list of files sorted by access time from oldest to newest
    ss::future<walk_result> walk(
      ss::sstring start_dir,
      const access_time_tracker& tracker,
      uint16_t max_concurrency,
      std::optional<filter_type> collect_filter = std::nullopt);

private:
    ss::gate _gate;
};

} // namespace cloud_storage
