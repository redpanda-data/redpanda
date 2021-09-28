/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <chrono>

namespace cloud_storage {
struct file_list_item {
    std::chrono::system_clock::time_point access_time;
    ss::sstring path;
    uint64_t size;
};
class recursive_directory_walker {
public:
    ss::future<> stop();

    // recursively walks start_dir, returns the total size of files in this
    // directory and a list of files sorted by access time from oldest to newest
    ss::future<std::pair<uint64_t, std::vector<file_list_item>>>
    walk(ss::sstring start_dir);

private:
    ss::gate _gate;
};

} // namespace cloud_storage
