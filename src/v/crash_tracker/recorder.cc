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

#include "base/vassert.h"
#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "crash_tracker/types.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "utils/directory_walker.h"
#include "utils/file_io.h"

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/print_safe.hh>

#include <chrono>
#include <filesystem>
#include <string_view>

using namespace std::chrono_literals;

namespace crash_tracker {

static constexpr std::string_view crash_report_suffix = ".crash";

namespace {

std::filesystem::path
to_upload_marker_path(const std::filesystem::path& crash_report_path) {
    return crash_report_path.string() + recorder::upload_marker_suffix;
}

std::filesystem::path
to_crash_report_path(const std::filesystem::path& upload_marker_path) {
    auto full_path = upload_marker_path.string();
    return full_path.substr(
      0, full_path.size() - recorder::upload_marker_suffix.size());
}

} // namespace

recorder& get_recorder() {
    static recorder inst;
    return inst;
}

recorder get_test_recorder() { return recorder{}; }

ss::future<> recorder::ensure_crashdir_exists() const {
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
}

ss::future<std::filesystem::path> recorder::generate_crashfile_name() const {
    auto crash_report_dir = config::node().crash_report_dir_path();

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
            continue;
        }

        co_return try_name;
    }

    // The anti-collision above should ensure that we never reach this
    throw std::runtime_error("Failed to create a unique crash recorder file");
}

ss::future<> recorder::remove_old_crashfiles() const {
    auto crash_files = co_await get_recorded_crashes(
      recorder::include_malformed_files::yes);

    if (crash_files.size() > crash_files_to_keep) {
        // Delete the oldest crash reports to avoid filling up the disc with
        // crash report files
        for (size_t i = 0; i < crash_files.size() - crash_files_to_keep; ++i) {
            const auto& fpath = crash_files[i].file_path.string();
            vlog(ctlog.debug, "Deleting old crash report file {}", fpath);
            co_await ss::remove_file(fpath);
        }
    }

    co_return;
}

namespace {
ss::future<> upload_marker_walker_fn(
  std::filesystem::path basedir, ss::directory_entry entry) {
    const auto path = (basedir / std::string_view{entry.name}).string();
    if (!path.ends_with(recorder::upload_marker_suffix)) {
        // Not an upload marker
        co_return;
    }

    auto dangling = !co_await ss::file_exists(
      to_crash_report_path(std::filesystem::path{path}).string());
    if (dangling) {
        vlog(
          ctlog.trace, "Removing dangling crash report upload marker {}", path);
        co_await ss::remove_file(path);
    }
}
} // namespace

ss::future<> recorder::remove_dangling_upload_markers() const {
    auto basedir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(basedir.string())) {
        co_return;
    }

    co_await directory_walker::walk(
      basedir.string(), [basedir](ss::directory_entry entry) -> ss::future<> {
          return upload_marker_walker_fn(basedir, std::move(entry));
      });
}

ss::future<> recorder::start() {
    co_await ensure_crashdir_exists();
    co_await remove_old_crashfiles();
    co_await remove_dangling_upload_markers();
    co_await _writer.initialize(co_await generate_crashfile_name());
}

namespace {

void record_backtrace(crash_description& cd) {
    size_t pos = 0;
    auto printer = [&pos, &cd](auto fmt, auto&&... args) {
        if (pos >= cd.stacktrace.capacity()) {
            return; // Prevent buffer overflow
        }

        auto result = fmt::format_to_n(
          cd.stacktrace.begin() + pos,
          cd.stacktrace.capacity() - pos,
          fmt,
          std::forward<decltype(args)...>(args)...);
        pos = std::min(pos + result.size, cd.stacktrace.capacity());
    };

    ss::backtrace([&pos, &printer](ss::frame f) {
        const bool first = pos == 0;
        if (!first) {
            printer(FMT_STRING(" "), " ");
        }

        if (!f.so->name.empty()) {
            printer(FMT_STRING("{}+"), f.so->name.c_str());
        }

        printer(FMT_STRING("{:#x}"), f.addr);
    });
}

void print_skipping() {
    constexpr static std::string_view skipping
      = "Skipping recording crash reason to crash file.\n";
    ss::print_safe(skipping.data(), skipping.size());
}

void record_message(crash_description& cd, std::string_view msg) {
    auto& format_buf = cd.crash_message;
    fmt::format_to_n(
      format_buf.begin(),
      format_buf.capacity(),
      "{} on shard {}.",
      msg,
      ss::this_shard_id());
}

} // namespace

/// Async-signal safe
void recorder::record_crash_sighandler(recorded_signo signo) {
    auto* cd_opt = _writer.fill();
    if (!cd_opt) {
        // The writer has already been consumed by another crash
        print_skipping();
        return;
    }
    auto& cd = *cd_opt;

    record_backtrace(cd);

    switch (signo) {
    case recorded_signo::sigsegv: {
        cd.type = crash_type::segfault;
        record_message(cd, "Segmentation fault");
        break;
    }
    case recorded_signo::sigabrt: {
        cd.type = crash_type::abort;
        record_message(cd, "Aborting");
        break;
    }
    case recorded_signo::sigill: {
        cd.type = crash_type::illegal_instruction;
        record_message(cd, "Illegal instruction");
        break;
    }
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

    auto* cd_opt = _writer.fill();
    if (!cd_opt) {
        // The writer has already been consumed by another crash
        print_skipping();
        return;
    }
    auto& cd = *cd_opt;

    record_backtrace(cd);
    cd.type = crash_type::startup_exception;

    auto& format_buf = cd.crash_message;
    fmt::format_to_n(
      format_buf.begin(),
      format_buf.capacity(),
      "Failure during startup: {}",
      eptr);

    _writer.write();
}

void recorder::record_crash_vassert(std::string_view msg) {
    if (_writer.uninitialized()) {
        // The vassert callback may be called before the recorder is
        // initialized. If there is a vassert crash before the recorder is
        // initialized, we cannot record the crash and so we print and return
        // here.
        print_skipping();
        return;
    }

    auto* cd_opt = _writer.fill();
    if (!cd_opt) {
        // The writer has already been consumed by another crash
        print_skipping();
        return;
    }
    auto& cd = *cd_opt;

    record_backtrace(cd);
    cd.type = crash_type::assertion;
    record_message(cd, msg);

    _writer.write();
}

ss::future<bool> recorder::recorded_crash::is_uploaded() const {
    co_return co_await ss::file_exists(
      to_upload_marker_path(file_path).string());
}

ss::future<> recorder::recorded_crash::mark_uploaded() const {
    // Create an empty upload marker
    const auto marker_path = to_upload_marker_path(file_path).string();
    auto f = co_await ss::open_file_dma(marker_path, ss::open_flags::create);
    co_await f.close();
    co_return;
};

std::chrono::system_clock::time_point
recorder::recorded_crash::timestamp() const {
    // Prefer the recorded timestamp, fall back to the last write time
    return crash ? model::to_time_point(crash->crash_time) : last_write_time;
}

namespace {
ss::future<> recorded_crashes_walker_fn(
  std::filesystem::path basedir,
  ss::directory_entry entry,
  std::vector<recorder::recorded_crash>& result,
  recorder::include_malformed_files incl_malformed) {
    const auto path = (basedir / std::string_view{entry.name}).string();
    if (!path.ends_with(crash_report_suffix)) {
        // Filter only for crash files
        co_return;
    }

    auto file_stats = co_await ss::file_stat(path);

    auto buf = co_await read_fully(path);
    try {
        auto crash_desc = serde::from_iobuf<crash_description>(std::move(buf));
        result.emplace_back(
          path, std::move(crash_desc), file_stats.time_changed);
    } catch (const serde::serde_exception& e) {
        vlog(
          ctlog.debug,
          "Exception while deserializing a crash report file {}: {}",
          path,
          e);
        if (incl_malformed) {
            result.emplace_back(path, std::nullopt, file_stats.time_changed);
        } else {
            vlog(ctlog.warn, "Ignoring malformed crash report file {}", path);
        }
    }
}
} // namespace

ss::future<std::vector<recorder::recorded_crash>>
recorder::get_recorded_crashes(
  recorder::include_malformed_files incl_malformed) const {
    auto result = std::vector<recorded_crash>{};
    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        co_return result;
    }

    co_await directory_walker::walk(
      crash_report_dir.string(),
      [crash_report_dir, &result, incl_malformed](ss::directory_entry entry) {
          return recorded_crashes_walker_fn(
            crash_report_dir, std::move(entry), result, incl_malformed);
      });

    std::sort(
      result.begin(),
      result.end(),
      [](const recorded_crash& a, const recorded_crash& b) {
          return a.timestamp() < b.timestamp();
      });

    co_return result;
}

ss::future<> recorder::stop() { co_await _writer.release(); }

} // namespace crash_tracker
