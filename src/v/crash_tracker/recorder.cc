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

#include "crash_tracker/recorder.h"

#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "crash_tracker/types.h"
#include "hashing/xx.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "utils/file_io.h"

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

static constexpr std::string_view crash_report_suffix = ".crash";

recorder& get_recorder() {
    static recorder inst;
    return inst;
}

ss::future<> recorder::start() {
    // Ensure that the crash report directory exists
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        vlog(
          ctlog.info,
          "Creating crash report directory {}",
          crash_report_dir.string());
        co_await ss::recursive_touch_directory(crash_report_dir.string());
        vlog(
          ctlog.debug,
          "Successfully created crash report directory {}",
          crash_report_dir.string());
    }

    // Loop a few times to avoid (very unlikely) collisions in the filename
    std::optional<std::filesystem::path> crash_file_name{};
    for (int i = 0; i < 10; ++i) {
        auto time_now = model::timestamp::now().value();
        auto random_int = random_generators::get_int(0, 10000);
        auto try_name = crash_report_dir
                        / fmt::format(
                          "{}_{}{}", time_now, random_int, crash_report_suffix);
        if (co_await ss::file_exists(try_name.string())) {
            // Try again in the rare case of a collision
            co_await ss::sleep(1ms);
            continue;
        }

        crash_file_name = try_name;
        break;
    }
    if (!crash_file_name) {
        // The anti-collision above should ensure that we never reach this
        throw std::runtime_error(
          "Failed to create a unique crash recorder file");
    }

    std::lock_guard<ss::util::spinlock> g(_writer_lock);
    co_await _writer.initialize(*crash_file_name);
}

namespace {

void record_backtrace(crash_description& cd) {
    size_t pos = 0;
    ss::backtrace([&cd, &pos](ss::frame f) {
        if (pos >= cd._stacktrace.size()) {
            return; // Prevent buffer overflow
        }

        const bool first = pos == 0;
        auto result = fmt::format_to_n(
          cd._stacktrace.begin() + pos,
          cd._stacktrace.size() - pos,
          "{}{:#x}",
          first ? "" : " ",
          f.addr);

        pos += result.size;
    });
}

void print_skipping() {
    constexpr static std::string_view skipping
      = "Skipping recording crash reason to crash file.\n";
    ss::print_safe(skipping.data(), skipping.size());
}

} // namespace

/// Async-signal safe
void recorder::record_crash_sighandler(int signo) {
    // If the recorder is already locked, give up to prevent possible deadlocks.
    // For example, an assertion failure while recording a segfault may lead to
    // a deadlock if we unconditionally waited for the lock here.
    std::unique_lock<ss::util::spinlock> g(_writer_lock, std::try_to_lock);
    if (!g.owns_lock() || !_writer.initialized()) {
        print_skipping();
        return;
    }

    auto& cd = _writer.fill();
    record_backtrace(cd);

    auto& format_buf = cd._crash_message;

    switch (signo) {
    case SIGSEGV: {
        cd._type = crash_type::segfault;
        fmt::format_to_n(
          format_buf.begin(),
          format_buf.size(),
          "Segmentation fault on shard {}.",
          ss::this_shard_id());

        break;
    }
    case SIGABRT: {
        cd._type = crash_type::abort;
        fmt::format_to_n(
          format_buf.begin(),
          format_buf.size(),
          "Aborting on shard {}.",
          ss::this_shard_id());
        break;
    }
    case SIGILL: {
        cd._type = crash_type::illegal_instruction;
        fmt::format_to_n(
          format_buf.begin(),
          format_buf.size(),
          "Illegal instruction on shard {}.",
          ss::this_shard_id());
        break;
    }
    default:
        // Other signal handlers are not handled yet
        __builtin_unreachable();
    }

    _writer.write();
}

void recorder::record_crash_exception(std::exception_ptr eptr) {
    if (is_crash_loop_limit_reached(eptr)) {
        // We specifically do not want to record crash_loop_limit_reached errors
        // as crashes because they are not informative and would build up
        // garbage on disk and would force to expire earlier useful crash logs.
        return;
    }

    // If the recorder is already locked, give up to prevent possible deadlocks.
    // For example, an assertion failure while recording a segfault may lead to
    // a deadlock if we unconditionally waited for the lock here.
    std::unique_lock<ss::util::spinlock> g(_writer_lock, std::try_to_lock);
    if (!g.owns_lock() || !_writer.initialized()) {
        print_skipping();
        return;
    }

    auto& cd = _writer.fill();

    record_backtrace(cd);
    cd._type = crash_type::startup_exception;

    auto& format_buf = cd._crash_message;
    fmt::format_to_n(
      format_buf.begin(),
      format_buf.size(),
      "Failure during startup: {}",
      eptr);

    _writer.write();
}

ss::future<std::vector<recorder::recorded_crash>>
recorder::get_recorded_crashes() const {
    auto result = std::vector<recorded_crash>{};
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        co_return result;
    }

    for (const auto& entry :
         std::filesystem::directory_iterator(crash_report_dir)) {
        if (entry.path().string().ends_with(crash_report_suffix)) {
            auto buf = co_await read_fully(entry.path());
            try {
                auto crash_desc = serde::from_iobuf<crash_description>(
                  std::move(buf));
                result.emplace_back(std::move(crash_desc));
            } catch (const serde::serde_exception&) {
                vlog(
                  ctlog.warn,
                  "Ignoring malformed crash report file {}",
                  entry.path());
            }
        }
    }

    co_return result;
}

bool recorder::recorded_crash::is_uploaded() const {
    vassert(false, "Unimplemented");
}

void recorder::recorded_crash::mark_uploaded() const {
    vassert(false, "Unimplemented");
}

ss::future<> recorder::stop() {
    std::lock_guard<ss::util::spinlock> g(_writer_lock);
    co_await _writer.release();
}

} // namespace crash_tracker
