/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crash_tracker/prepared_writer.h"

#include "crash_tracker/logger.h"
#include "crash_tracker/types.h"
#include "hashing/xx.h"
#include "model/timestamp.h"
#include "utils/arch.h"
#include "version/version.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/print_safe.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <fcntl.h>
#include <system_error>
#include <unistd.h>

using namespace std::chrono_literals;

namespace crash_tracker {

std::ostream& operator<<(std::ostream& os, prepared_writer::state s) {
    switch (s) {
    case prepared_writer::state::uninitialized:
        return os << "uninitialized";
    case prepared_writer::state::initialized:
        return os << "initialized";
    case prepared_writer::state::filled:
        return os << "filled";
    case prepared_writer::state::written:
        return os << "written";
    case prepared_writer::state::released:
        return os << "released";
    }
}

ss::future<>
prepared_writer::initialize(std::filesystem::path crash_file_path) {
    vassert(_state == state::uninitialized, "Unexpected state: {}", _state);
    _crash_report_file_name = std::move(crash_file_path);

    _serde_output.reserve_memory(crash_description::serde_size_overestimate);

    // Create the crash recorder file
    auto f = co_await ss::open_file_dma(
      _crash_report_file_name->c_str(),
      ss::open_flags::create | ss::open_flags::rw | ss::open_flags::truncate
        | ss::open_flags::exclusive);
    co_await f.close();

    // Open the crash recorder file using ::open().
    // We need to use the low level open() function here instead of the seastar
    // API or higher-level C++ primitives because we need to be able to
    // manipulate the file using async-signal-safe, allocation-free functions
    // inside signal handlers.
    _fd = ::open(_crash_report_file_name->c_str(), O_WRONLY);
    if (_fd == -1) {
        throw std::system_error(
          errno,
          std::system_category(),
          fmt::format(
            "Failed to open {} to record crash reason",
            *_crash_report_file_name));
    }

    _prepared_cd.app_version = ss::sstring{redpanda_version()};
    _prepared_cd.arch = ss::sstring{util::cpu_arch::current().name};

    // Update _state as the last step in initialize() to make earlier changes
    // visible across threads.
    _state = state::initialized;
}

crash_description* prepared_writer::fill() {
    // Note: this CAS serves two purposes:
    // 1. Ensures the visibility of the changes made in initialize()
    // 2. Ensures only a single owner can win the race for the prepared_writer
    //    with competing calls of fill() and release() across threads.
    auto before = state::initialized;
    auto success = _state.compare_exchange_strong(before, state::filled);
    if (!success) {
        // The old value of _state could have been anything but uninitialized
        // because of competing calls of fill(), write() or release() on other
        // threads. These competing calls are unlikely but possible. Example: a
        // sigabrt signal handler calls fill() on thread T3 while a segfault
        // signal handler calls fill() on thread T2.
        vassert(
          before != state::uninitialized,
          "fill() must be called after initialize(). Unexpected state: {}",
          before);
        return nullptr;
    }
    _prepared_cd.crash_time = model::timestamp::now();

    return &_prepared_cd;
}

bool prepared_writer::write() {
    auto before = state::filled;
    auto success = _state.compare_exchange_strong(before, state::written);
    vassert(
      success,
      "write() must be called after a fill() that returned a non-null value. "
      "Unexpected state: {}",
      before);

    return try_write_crash();
}

bool prepared_writer::try_write_crash() {
    bool success = true;

    static_assert(
      sizeof(crash_description) < 200,
      "Sanity check to prevent overflowing the stack. Even though "
      "crash_description may contain a large amount of pre-allocated data, it "
      "should remain relatively small on the stack.");
    serde::write(_serde_output, std::move(_prepared_cd));

    for (const auto& frag : _serde_output) {
        size_t written = 0;
        while (written < frag.size()) {
            auto res = ::write(
              _fd, frag.get() + written, frag.size() - written);
            if (res == -1 && errno == EINTR) {
                // EINTR is retriable
                continue;
            } else if (res == -1) {
                // Return that writing the crash failed but try to continue to
                // write later fragments as much information as possible
                success = false;
                break;
            } else {
                written += res;
            }
        }
    }

    ::fsync(_fd);

    return success;
}

ss::future<> prepared_writer::release() {
    // Note: this CAS ensures only a single owner can win the race for the
    // prepared_writer with competing calls of fill() and release() across
    // threads.
    auto before = state::initialized;
    auto success = _state.compare_exchange_strong(before, state::released);
    if (!success) {
        // Sanity check that the CAS was unsuccessful because of a race with
        // fill() and not because the prepared_writer was not initialized.
        vassert(
          before != state::uninitialized,
          "release() must be called after initialize(). Unexpected state: {}",
          before);

        // If another call to fill() won the race, release() is a noop
        co_return;
    }

    ::close(_fd);
    _fd = -1;
    co_await ss::remove_file(_crash_report_file_name->c_str());
    vlog(
      ctlog.debug, "Deleted crash report file: {}", *_crash_report_file_name);
    _crash_report_file_name.reset();

    _state = state::released;

    co_return;
}

void prepared_writer::reset() {
    auto cur_state = _state.load();
    if (cur_state != state::released) {
        ::close(_fd);
        _fd = -1;
    }
    _state = state::uninitialized;
    _serde_output.clear();
    _crash_report_file_name.reset();
    _prepared_cd = crash_description{};
}

} // namespace crash_tracker
