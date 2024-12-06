/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crash_tracker/service.h"

#include "base/vassert.h"
#include "config/node_config.h"
#include "crash_tracker/logger.h"
#include "crash_tracker/service.h"
#include "crash_tracker/types.h"
#include "hashing/xx.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "serde/rw/envelope.h"
#include "utils/file_io.h"

#include <seastar/core/seastar.hh>
#include <seastar/util/print_safe.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <system_error>
#include <unistd.h>

using namespace std::chrono_literals;

namespace crash_tracker {

// Crash tracking resets every 1h.
static constexpr model::timestamp_clock::duration crash_reset_duration{1h};
static constexpr std::string_view crash_report_suffix = ".crash";

ss::future<> service::start() {
    if (config::node().developer_mode()) {
        // crash loop tracking has value only in long running clusters
        // that can potentially accumulate state across restarts.
        co_return;
    }

    co_await check_for_crash_loop();
    co_await initialize_state();
}

ss::future<> service::check_for_crash_loop() const {
    auto file_path = config::node().crash_loop_tracker_path();
    std::optional<crash_tracker_metadata> maybe_crash_md;
    if (
      // Tracking is reset every time the broker boots in recovery mode.
      !config::node().recovery_mode_enabled()
      && co_await ss::file_exists(file_path.string())) {
        // Ok to read the entire file, it contains a serialized uint32_t.
        auto buf = co_await read_fully(file_path);
        try {
            maybe_crash_md = serde::from_iobuf<crash_tracker_metadata>(
              std::move(buf));
        } catch (const serde::serde_exception&) {
            // A malformed log file, ignore and reset it later.
            // We truncate it below.
            vlog(ctlog.warn, "Ignorning malformed tracker file {}", file_path);
        }
    }

    // Compute the checksum of the current node configuration.
    auto current_config = co_await read_fully_to_string(
      config::node().get_cfg_file_path());
    auto checksum = xxhash_64(current_config.c_str(), current_config.length());

    if (maybe_crash_md) {
        auto& crash_md = maybe_crash_md.value();
        auto& limit = config::node().crash_loop_limit.value();

        // Check if it has been atleast 1h since last unsuccessful restart.
        // Tracking resets every 1h.
        auto time_since_last_start
          = model::duration_since_epoch(model::timestamp::now())
            - model::duration_since_epoch(crash_md._last_start_ts);

        auto crash_limit_ok = !limit || crash_md._crash_count <= limit.value();
        auto node_config_changed = crash_md._config_checksum != checksum;
        auto tracking_reset = time_since_last_start > crash_reset_duration;

        auto ok_to_proceed = crash_limit_ok || node_config_changed
                             || tracking_reset;

        if (!ok_to_proceed) {
            auto crashes_msg = co_await describe_recorded_crashes();
            vlog(
              ctlog.error,
              "Crash loop detected. Too many consecutive crashes {}, exceeded "
              "{} configured value {}. To recover Redpanda from this state, "
              "manually remove file at path {}. Crash loop automatically "
              "resets 1h after last crash or with node configuration changes. "
              "{}",
              crash_md._crash_count,
              config::node().crash_loop_limit.name(),
              limit.value(),
              file_path,
              crashes_msg);
            throw std::runtime_error("Crash loop detected, aborting startup.");
        }

        vlog(
          ctlog.debug,
          "Consecutive crashes detected: {} node config changed: {} "
          "time based tracking reset: {}",
          crash_md._crash_count,
          node_config_changed,
          tracking_reset);

        if (node_config_changed || tracking_reset) {
            crash_md._crash_count = 0;
        }
    }

    // Truncate and bump the crash count. We consider a run to be unclean by
    // default unless the scheduled cleanup (that runs very late in shutdown)
    // resets the file. See schedule_crash_tracker_file_cleanup().
    auto new_crash_count = maybe_crash_md
                             ? maybe_crash_md.value()._crash_count + 1
                             : 1;
    crash_tracker_metadata updated{
      ._crash_count = new_crash_count,
      ._config_checksum = checksum,
      ._last_start_ts = model::timestamp::now()};
    co_await write_fully(file_path, serde::to_iobuf(updated));
    co_await ss::sync_directory(
      config::node().data_directory.value().as_sstring());
}

ss::future<> service::initialize_state() {
    vassert(!initialized.load(), "Initialize should only be called once");

    prepared_cd = crash_description{
      ._crash_time = model::timestamp{},
      ._crash_reason = ss::sstring{},
      ._stacktrace = std::vector<uint64_t>{},
    };

    prepared_cd._crash_reason.resize(4096, '\0');
    prepared_cd._stacktrace.reserve(30);
    serde_output.reserve_memory(10240);

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

    // Add a random number to the path to make duplicates very unlikely
    auto time_now = model::timestamp::now().value();
    auto random_int = random_generators::get_int(0, 10000);
    crash_report_file_name
      = crash_report_dir
        / fmt::format("{}_{}{}", time_now, random_int, crash_report_suffix);
    fd = ::open(crash_report_file_name.c_str(), O_CREAT | O_WRONLY);
    if (fd == -1) {
        throw std::system_error(
          errno,
          std::system_category(),
          fmt::format(
            "Failed to open {} to record crash reason",
            crash_report_file_name));
    }

    initialized.store(true);
}

ss::future<ss::sstring> service::describe_recorded_crashes() const {
    constexpr std::string_view no_crashes_msg
      = "(No crash files have been recorded.)";

    auto crash_report_dir = config::node().crash_report_dir_path();
    if (!co_await ss::file_exists(crash_report_dir.string())) {
        co_return no_crashes_msg;
    }

    std::vector<crash_description> crash_descriptions;
    for (const auto& entry :
         std::filesystem::directory_iterator(crash_report_dir)) {
        if (entry.path().string().ends_with(crash_report_suffix)) {
            auto buf = co_await read_fully(entry.path());
            try {
                auto crash_desc = serde::from_iobuf<crash_description>(
                  std::move(buf));
                crash_descriptions.push_back(std::move(crash_desc));
            } catch (const serde::serde_exception&) {
                vlog(
                  ctlog.warn,
                  "Ignoring malformed crash report file {}",
                  entry.path());
            }
        }
    }

    if (crash_descriptions.empty()) {
        co_return no_crashes_msg;
    }

    constexpr auto format_time = [](model::timestamp ts_model) {
        auto ts_chrono = model::to_time_point(ts_model);
        return fmt::format("{:%Y-%m-%d %T}", fmt::localtime(ts_chrono));
    };

    std::stringstream ss;
    ss << "The following crashes have been recorded:";
    for (size_t i = 0; i < crash_descriptions.size(); ++i) {
        // Show up to 5 oldest and newest crashes with ... in between
        constexpr size_t crashes_to_show_each_side = 5;
        auto maybe_jump_to = crash_descriptions.size()
                             - crashes_to_show_each_side;
        if (i == crashes_to_show_each_side && i < maybe_jump_to) {
            ss << "\n    ...";
            i = maybe_jump_to;
        }

        fmt::print(
          ss,
          "\nCrash #{} at {} - {}",
          i + 1,
          format_time(crash_descriptions[i]._crash_time),
          crash_descriptions[i]._crash_reason);
    }

    co_return ss.str();
}

bool service::update_crash_md(const crash_description& updated) {
    serde::write(serde_output, updated);

    auto res = ::ftruncate(fd, 0);
    if (res != 0) {
        return false;
    }

    for (const auto& frag : serde_output) {
        auto written = ::write(fd, frag.get(), frag.size());
        if (written == -1) {
            return false;
        }
    }

    ::fsync(fd);

    return true;
}

void service::record_crash(crash_recorder_fn recorder) {
    if (!initialized.load()) {
        constexpr static std::string_view skipping
          = "Skipping recording crash reason to crash file.\n";
        ss::print_safe(skipping.data(), skipping.size());
    }

    prepared_cd._crash_time = model::timestamp::now();
    recorder(prepared_cd);

    if (update_crash_md(prepared_cd)) {
        constexpr static std::string_view success
          = "Recorded crash reason to crash file.\n";
        ss::print_safe(success.data(), success.size());
    } else {
        constexpr static std::string_view failure
          = "Failed to record crash reason to crash file.\n";
        ss::print_safe(failure.data(), failure.size());
    }
}

ss::future<> service::stop() {
    auto file = config::node().crash_loop_tracker_path().string();
    if (co_await ss::file_exists(file)) {
        co_await ss::remove_file(file);
        co_await ss::sync_directory(
          config::node().data_directory().as_sstring());
        vlog(ctlog.debug, "Deleted crash loop tracker file: {}", file);
    }

    if (initialized.load()) {
        ::close(fd);
        co_await ss::remove_file(crash_report_file_name.c_str());
        co_await ss::sync_directory(
          config::node().crash_report_dir_path().string());
        vlog(
          ctlog.debug, "Deleted crash report file: {}", crash_report_file_name);
    }

    co_return;
}

} // namespace crash_tracker
