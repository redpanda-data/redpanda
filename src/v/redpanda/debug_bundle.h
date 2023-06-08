/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "utils/gate_guard.h"
#include "utils/mutex.h"
#include "utils/request_auth.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/process.hh>

#include <chrono>
#include <concepts>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <vector>

namespace debug_bundle {

enum class errc : int16_t {
    success = 0, // must be 0
    already_running,
    nothing_running,
    bundle_not_ready,
    bundle_not_on_disk,
    invalid_filename,
    process_fail,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "debug_bundle::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::already_running:
            return "Bundle creation is already running";
        case errc::nothing_running:
            return "Bundle creation is not running";
        case errc::bundle_not_ready:
            return "Bundle creation is running but the file is not yet ready";
        case errc::bundle_not_on_disk:
            return "Bundle creation is not running or the file is not on disk";
        case errc::invalid_filename:
            return "Filename does not refer to a debug bundle";
        case errc::process_fail:
            return "Spawned process failed to run";
        }
        return "debug_bundle::errc::unknown";
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}

static constexpr ss::shard_id debug_bundle_shard_id = 0;

// metric_t represents the type for metrics_interval flag to rpk. That flag
// could have any time units such as ms or h. Therefore, this struct is meant to
// capture and represent any time unit.
struct metric_interval_t {
    std::chrono::milliseconds interval_ms;
    ss::sstring units;

    metric_interval_t(std::chrono::milliseconds interval, ss::sstring u)
      : interval_ms{interval}
      , units{u} {}
};

ss::sstring to_journalctl_fmt(std::chrono::year_month_day ymd);
ss::sstring to_time_fmt(metric_interval_t metric);

struct debug_bundle_params {
    std::optional<std::chrono::year_month_day> logs_since;
    std::optional<std::chrono::year_month_day> logs_until;
    std::optional<ss::sstring> logs_size_limit;
    std::optional<metric_interval_t> metrics_interval;

    debug_bundle_params()
      : logs_since{std::nullopt}
      , logs_until{std::nullopt}
      , logs_size_limit{std::nullopt}
      , metrics_interval{std::nullopt} {}
};

struct debug_bundle_credentials {
    ss::sstring username;
    ss::sstring password;
    ss::sstring mechanism;
};

struct rpk_debug_bundle {
    std::unique_ptr<ss::experimental::process> process;
    mutex process_mu;
    ss::future<> control;
    ss::sstring host_path;
    ss::sstring host_home;
    const std::filesystem::path write_dir;
    const std::filesystem::path rpk_path;
    const debug_bundle_params params;
    std::optional<debug_bundle_credentials> creds;

    rpk_debug_bundle();
    rpk_debug_bundle(
      ss::sstring host_path,
      ss::sstring host_home,
      const std::filesystem::path write_dir,
      const std::filesystem::path rpk_path,
      const debug_bundle_params params,
      std::optional<debug_bundle_credentials> creds);

    ss::future<> start_process();
    ss::future<> handle_process();

    std::vector<ss::sstring> make_rpk_args();
    errc handle_wait_status(ss::experimental::process::wait_status wstatus);
    ss::future<std::error_code> do_remove(ss::sstring filename);
    errc do_abort(bool use_sigkill);
};

ss::sstring make_bundle_filename(
  const std::filesystem::path write_dir, const ss::sstring filename);

errc make_rpk_err_value(std::error_code err);

constexpr std::string_view default_bundle_name = "debug-bundle.zip";

class debug_bundle : public ss::peering_sharded_service<debug_bundle> {
public:
    debug_bundle(
      const std::filesystem::path write_dir,
      const std::filesystem::path rpk_path);

    ss::future<> start();
    ss::future<std::error_code> start_creating_bundle(
      const debug_bundle_params params,
      std::optional<debug_bundle_credentials> creds);
    ss::future<> stop();
    ss::future<std::error_code> remove_bundle(ss::sstring filename);
    ss::future<std::error_code> abort_bundle();

    const std::filesystem::path& get_write_dir() const { return _write_dir; }
    ss::future<std::error_code> is_ready(ss::sstring filename);

    template<
      std::invocable<> Func,
      typename Futurator = ss::futurize<std::invoke_result_t<Func>>>
    typename Futurator::type with_lock(Func&& func) {
        gate_guard guard{_rpk_gate};
        auto units = co_await _rpk_mu.get_units();
        co_return co_await Futurator::invoke(std::forward<Func>(func));
    }

private:
    friend ss::future<std::error_code> force_stop(debug_bundle& b);
    friend ss::future<std::error_code>
    maybe_remove_aborted_bundle(debug_bundle& b);

    const std::filesystem::path _write_dir;
    ss::sstring _host_path;
    ss::sstring _host_home;
    const std::filesystem::path _rpk_cmd;
    ss::gate _rpk_gate;
    mutex _rpk_mu;
    std::unique_ptr<rpk_debug_bundle> _rpk;

    ss::sstring get_host_path_env();
    ss::sstring get_home_dir_env();
    ss::future<bool> is_rpk_running();
    ss::future<bool> is_core0_running();

    // do_nonblocking_abort will throw, from seastar level, if there is no
    // process running
    ss::future<std::error_code> do_abort(bool use_sigkill);
};

} // namespace debug_bundle

namespace std {
template<>
struct is_error_code_enum<debug_bundle::errc> : true_type {};
} // namespace std
