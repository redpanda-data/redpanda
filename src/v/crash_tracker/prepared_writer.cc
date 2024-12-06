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

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/print_safe.hh>

#include <fmt/chrono.h>

#include <chrono>
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
    _crash_report_file_name = std::move(crash_file_path);
    _prepared_cd = crash_description{
      ._type = crash_type::unknown,
      ._crash_time = model::timestamp{},
      ._crash_message = ss::sstring{},
      ._stacktrace = ss::sstring{},
      ._addition_info = ss::sstring{},
    };

    _prepared_cd._crash_message.resize(4096, '\0');
    _prepared_cd._stacktrace.resize(4096, '\0');
    _prepared_cd._addition_info.resize(4094, '\0');

    // Overestimate the size of a serialized _prepared_cd
    const size_t serde_reserve = 1024 + 3 * 4096;
    _serde_output.reserve_memory(serde_reserve);

    // Create the crash recorder file
    auto f = co_await ss::open_file_dma(
      _crash_report_file_name.c_str(),
      ss::open_flags::create | ss::open_flags::rw);
    co_await f.close();
    co_await ss::sync_directory(_crash_report_file_name.parent_path().string());

    // Open the crash recorder file using ::open().
    // We need to use the low level open() function here instead of the seastar
    // API or higher-level C++ primitives because we need to be able to
    // manipulate the file using async-signal-safe, allocation-free functions
    // inside signal handlers.
    _fd = ::open(_crash_report_file_name.c_str(), O_WRONLY);
    if (_fd == -1) {
        throw std::system_error(
          errno,
          std::system_category(),
          fmt::format(
            "Failed to open {} to record crash reason",
            _crash_report_file_name));
    }

    _state = state::initialized;
}

bool prepared_writer::update_crash_md(const crash_description& updated) {
    serde::write(_serde_output, updated);

    auto res = ::ftruncate(_fd, 0);
    if (res != 0) {
        return false;
    }

    for (const auto& frag : _serde_output) {
        auto written = ::write(_fd, frag.get(), frag.size());
        if (written == -1) {
            return false;
        }
    }

    ::fsync(_fd);

    return true;
}

crash_description& prepared_writer::fill() {
    vassert(_state == state::initialized, "Unexpected state: {}", _state);
    _state = state::filled;
    return _prepared_cd;
}

void prepared_writer::write() {
    vassert(_state == state::filled, "Unexpected state: {}", _state);
    _state = state::written;

    _prepared_cd._crash_time = model::timestamp::now();

    if (update_crash_md(_prepared_cd)) {
        constexpr static std::string_view success
          = "Recorded crash reason to crash file.\n";
        ss::print_safe(success.data(), success.size());
    } else {
        constexpr static std::string_view failure
          = "Failed to record crash reason to crash file.\n";
        ss::print_safe(failure.data(), failure.size());
    }
}

ss::future<> prepared_writer::release() {
    vassert(_state != state::released, "Unexpected state: {}", _state);

    if (_state != state::uninitialized) {
        ::close(_fd);
        co_await ss::remove_file(_crash_report_file_name.c_str());
        co_await ss::sync_directory(
          _crash_report_file_name.parent_path().string());
        vlog(
          ctlog.debug,
          "Deleted crash report file: {}",
          _crash_report_file_name);
    }

    _state = state::released;

    co_return;
}

} // namespace crash_tracker
