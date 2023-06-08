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

#include "redpanda/debug_bundle.h"

#include "ssx/future-util.h"
#include "vlog.h"

#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>
#include <seastar/util/process.hh>

#include <fmt/format.h>

#include <filesystem>
#include <optional>
#include <stdexcept>
#include <system_error>

using namespace std::chrono_literals;

static ss::logger bundle_log{"debug_bundle"};

static constexpr auto rpk_abort_sleep_ms = 10ms;

namespace debug_bundle {

ss::sstring make_bundle_filename(
  const std::filesystem::path write_dir, const ss::sstring filename) {
    auto bundle_name = write_dir / std::string{filename};
    return bundle_name.string();
}

errc make_rpk_err_value(std::error_code err) { return errc(err.value()); }

ss::sstring to_journalctl_fmt(std::chrono::year_month_day ymd) {
    return fmt::format(
      "{}-{}-{}",
      static_cast<int>(ymd.year()),
      static_cast<unsigned>(ymd.month()),
      static_cast<unsigned>(ymd.day()));
}

ss::sstring to_time_fmt(metric_interval_t metric) {
    if (metric.units == "ns") {
        return fmt::format(
          "{}ns", std::chrono::nanoseconds{metric.interval_ms}.count());
    } else if (metric.units == "us") {
        return fmt::format(
          "{}us", std::chrono::microseconds{metric.interval_ms}.count());
    } else if (metric.units == "ms") {
        return fmt::format("{}ms", metric.interval_ms.count());
    } else if (metric.units == "s") {
        return fmt::format(
          "{}s",
          std::chrono::duration_cast<std::chrono::seconds>(metric.interval_ms)
            .count());
    } else if (metric.units == "m") {
        return fmt::format(
          "{}m",
          std::chrono::duration_cast<std::chrono::minutes>(metric.interval_ms)
            .count());
    } else if (metric.units == "h") {
        return fmt::format(
          "{}h",
          std::chrono::duration_cast<std::chrono::hours>(metric.interval_ms)
            .count());
    } else if (metric.units == "d") {
        return fmt::format(
          "{}d",
          std::chrono::duration_cast<std::chrono::days>(metric.interval_ms)
            .count());
    } else if (metric.units == "w") {
        return fmt::format(
          "{}w",
          std::chrono::duration_cast<std::chrono::weeks>(metric.interval_ms)
            .count());
    } else if (metric.units == "y") {
        return fmt::format(
          "{}y",
          std::chrono::duration_cast<std::chrono::years>(metric.interval_ms)
            .count());
    } else {
        vlog(
          bundle_log.warn,
          "Unknown units {} for metrics-interval, defaulting to ms");
        return fmt::format("{}ms", metric.interval_ms.count());
    }
}

rpk_debug_bundle::rpk_debug_bundle()
  : process{nullptr}
  , control{ss::make_ready_future<>()}
  , creds{std::nullopt} {}

rpk_debug_bundle::rpk_debug_bundle(
  ss::sstring host_path,
  ss::sstring host_home,
  const std::filesystem::path write_dir,
  const std::filesystem::path rpk_path,
  const debug_bundle_params params,
  std::optional<debug_bundle_credentials> creds)
  : process{nullptr}
  , control{ss::make_ready_future<>()}
  , host_path{host_path}
  , host_home{host_home}
  , write_dir{write_dir}
  , rpk_path{rpk_path}
  , params{std::move(params)}
  , creds{std::move(creds)} {}

std::vector<ss::sstring> rpk_debug_bundle::make_rpk_args() {
    auto bundle_name = make_bundle_filename(
      write_dir, ss::sstring{default_bundle_name});
    std::vector<ss::sstring> rpk_argv{
      rpk_path.string(), "debug", "bundle", "--output", bundle_name};

    if (creds.has_value()) {
        rpk_argv.push_back("--user");
        rpk_argv.push_back(creds->username);
        rpk_argv.push_back("--password");
        rpk_argv.push_back(creds->password);
        rpk_argv.push_back("--sasl-mechanism");
        rpk_argv.push_back(creds->mechanism);
    }

    if (params.logs_since.has_value()) {
        rpk_argv.push_back("--logs-since");
        rpk_argv.push_back(to_journalctl_fmt(params.logs_since.value()));
    }

    if (params.logs_until.has_value()) {
        rpk_argv.push_back("--logs-until");
        rpk_argv.push_back(to_journalctl_fmt(params.logs_until.value()));
    }

    if (params.logs_size_limit.has_value()) {
        rpk_argv.push_back("--logs-size-limit");
        rpk_argv.push_back(params.logs_size_limit.value());
    }

    if (params.metrics_interval.has_value()) {
        rpk_argv.push_back("--metrics-interval");
        rpk_argv.push_back(to_time_fmt(params.metrics_interval.value()));
    }

    return rpk_argv;
}

template<class>
inline constexpr bool always_false_v = false;

errc rpk_debug_bundle::handle_wait_status(
  ss::experimental::process::wait_status wstatus) {
    return ss::visit(
      wstatus,
      [](ss::experimental::process::wait_exited exit_status) {
          if (exit_status.exit_code != 0) {
              vlog(
                bundle_log.error,
                "Bundle creation exited with code {}",
                exit_status.exit_code);
              return errc::process_fail;
          } else {
              vlog(
                bundle_log.debug,
                "Bundle creation successful, debug bundle {}",
                default_bundle_name);
              return errc::success;
          }
      },
      [](ss::experimental::process::wait_signaled exit_signal) {
          vlog(
            bundle_log.error,
            "Bundle creation terminated with signal {}",
            exit_signal.terminating_signal);
          return errc::process_fail;
      });
}

ss::future<>
consume_input_stream(ss::input_stream<char> stream, bool is_stdout) {
    ss::sstring stream_name{is_stdout ? "stdout" : "stderr"};

    while (!stream.eof()) {
        auto buf = co_await stream.read();
        if (buf.empty()) {
            continue;
        }

        std::string str_buf{buf.begin(), buf.end()}, line;
        std::stringstream ss(str_buf);
        while (!ss.eof()) {
            std::getline(ss, line);
            vlog(bundle_log.trace, "{} line {}", stream_name, line);
        }
    }
}

ss::future<> rpk_debug_bundle::handle_process() {
    if (process == nullptr) {
        vlog(
          bundle_log.error,
          "Failed to run rpk debug bunde: process is undefined");
        co_return;
    }

    co_await consume_input_stream(process->stdout(), true);
    auto wstatus = co_await process->wait();
    auto err = handle_wait_status(wstatus);
    if (err != errc::success) {
        co_await consume_input_stream(process->stderr(), false);
    }
    // A null process indicates that the fork is not running
    process = nullptr;
}

ss::future<> rpk_debug_bundle::start_process() {
    auto units = co_await process_mu.get_units();
    auto rpk_argv = make_rpk_args();
    try {
        auto p = co_await ss::experimental::spawn_process(
          rpk_path, {.argv = rpk_argv, .env = {host_path, host_home}});
        process = std::make_unique<ss::experimental::process>(std::move(p));
    } catch (const std::system_error& ec) {
        vlog(
          bundle_log.error,
          "Process spawn failed: {} - {}",
          ec.code(),
          ec.what());
        co_return;
    }

    control = handle_process();
}

ss::future<std::error_code> rpk_debug_bundle::do_remove(ss::sstring filename) {
    auto units = co_await process_mu.get_units();
    co_await ss::remove_file(make_bundle_filename(write_dir, filename));
    co_await ss::sync_directory(write_dir.c_str());
    co_return errc::success;
}

errc rpk_debug_bundle::do_abort(bool use_sigkill) {
    try {
        if (use_sigkill) {
            process->kill();
        } else {
            process->terminate();
        }
    } catch (const std::system_error& ec) {
        // A system error is thrown when SIGTERM/SIGKILL are called on an
        // already dead process, handle that case here.
        vlog(
          bundle_log.trace,
          "{} failed, process already dead: ec {}",
          use_sigkill ? "SIGKILL" : "SIGTERM",
          ec);
        return errc::nothing_running;
    }
    return errc::success;
}

debug_bundle::debug_bundle(
  const std::filesystem::path write_dir, const std::filesystem::path rpk_path)
  : _write_dir{write_dir}
  , _rpk_cmd{rpk_path}
  , _rpk{nullptr} {}

ss::sstring debug_bundle::get_host_path_env() {
    auto host_env = std::getenv("PATH");
    if (!host_env) {
        vlog(
          bundle_log.warn,
          "Failed to get 'PATH' environmental variable, the debug bundle may "
          "be incomplete due to missing dependencies");
        return "PATH=/usr/bin:/usr/local/bin";
    } else {
        return fmt::format("PATH={}", host_env);
    }
}

ss::sstring debug_bundle::get_home_dir_env() {
    auto home_dir = std::getenv("HOME");
    if (!home_dir) {
        vlog(
          bundle_log.warn,
          "Failed to get 'HOME' environmental variable, the debug bundle may "
          "fail to run");
        return "";
    } else {
        return fmt::format("HOME={}", home_dir);
    }
}

ss::future<> debug_bundle::start() {
    vlog(bundle_log.info, "Starting debug bundle ...");
    _host_path = get_host_path_env();
    _host_home = get_home_dir_env();
    co_return;
}

ss::future<> debug_bundle::stop() {
    vlog(bundle_log.info, "Stopping debug bundle ...");
    auto core0_running = co_await is_core0_running();
    if (core0_running) {
        co_await container().invoke_on(
          debug_bundle_shard_id,
          [](debug_bundle& b) { return b.do_abort(true); });
    }

    co_await _rpk_gate.close();
}

ss::future<bool> debug_bundle::is_rpk_running() {
    co_return (_rpk != nullptr && _rpk->process != nullptr);
}

ss::future<bool> debug_bundle::is_core0_running() {
    co_return co_await container().invoke_on(
      debug_bundle_shard_id,
      [](debug_bundle& b) { return b.is_rpk_running(); });
}

ss::future<std::error_code> debug_bundle::start_creating_bundle(
  const debug_bundle_params params,
  std::optional<debug_bundle_credentials> creds) {
    gate_guard guard{_rpk_gate};
    auto units = co_await _rpk_mu.get_units();

    auto core0_running = co_await is_core0_running();
    if (core0_running) {
        co_return errc::already_running;
    }

    co_await container().invoke_on(
      debug_bundle_shard_id,
      [this, params{std::move(params)}, creds{std::move(creds)}](
        debug_bundle& b) mutable {
          _rpk.reset(new rpk_debug_bundle{
            b._host_path,
            b._host_home,
            b.get_write_dir(),
            b._rpk_cmd,
            std::move(params),
            std::move(creds)});
          ssx::spawn_with_gate(
            _rpk_gate, [this] { return _rpk->start_process(); });
      });

    co_return errc::success;
}

ss::future<std::error_code> debug_bundle::is_ready(ss::sstring filename) {
    // First check if the requested filename is actually a bundle
    // TODO(@NyaliaLui): In the future when we support more than 1 bundle on
    // disk, this condition will evolve to check if the filename follows our
    // naming scheme.
    if (filename != default_bundle_name) {
        co_return errc::invalid_filename;
    }

    // Then check if the requested filename is on disk
    auto exists = co_await ss::file_exists(
      make_bundle_filename(get_write_dir(), filename));
    auto core0_running = co_await is_core0_running();
    if (!exists) {
        if (core0_running) {
            co_return errc::bundle_not_ready;
        } else {
            co_return errc::bundle_not_on_disk;
        }
    } else {
        // It is possible for the status check to occur in a small window where
        // the spawned process is still running but RPK has written to disk
        // already.
        if (core0_running) {
            co_return errc::bundle_not_ready;
        }
    }

    co_return errc::success;
}

ss::future<std::error_code> debug_bundle::remove_bundle(ss::sstring filename) {
    gate_guard guard{_rpk_gate};
    auto units = co_await _rpk_mu.get_units();

    auto err = co_await is_ready(filename);
    if (make_rpk_err_value(err) != errc::success) {
        co_return err;
    }

    co_return co_await container().invoke_on(
      debug_bundle_shard_id, [&filename](debug_bundle& b) {
          // rpk.do_remove will acquire the mutex that protects spawned
          // processes. This is to protect against situations where a new
          // process is spawned before remove is called.
          return b._rpk->do_remove(filename);
      });
}

ss::future<std::error_code> debug_bundle::do_abort(bool use_sigkill) {
    if (_rpk == nullptr || _rpk->process == nullptr) {
        co_return errc::nothing_running;
    }

    co_return _rpk->do_abort(use_sigkill);
}

ss::future<std::error_code> force_stop(debug_bundle& b) {
    // If process is running
    // do SIGTERM
    // WAIT
    // If process is still running (process is stopped at the end of
    // rpk_debug_bundle::handle_process()) otherwise, do SIGKILL

    auto rpk_running = co_await b.is_rpk_running();
    if (!rpk_running) {
        // First check returns nothing_running because the abort is unecessary
        co_return errc::nothing_running;
    }

    co_await b.do_abort(false);

    co_await ss::sleep(rpk_abort_sleep_ms);

    rpk_running = co_await b.is_rpk_running();
    if (!rpk_running) {
        // Second check returns success because the abort worked
        co_return errc::success;
    }

    co_await b.do_abort(true);

    // Do not reset _rpk here, only debug_bundle::start_creating_bundle() can do
    // that
    // TODO(@NyaliaLui): make a Singleton class for _rpk with
    // start_creating_bundle() as the only friend
    co_return errc::success;
}

ss::future<std::error_code> maybe_remove_aborted_bundle(debug_bundle& b) {
    auto units = co_await b._rpk_mu.get_units();

    auto rpk_running = co_await b.is_rpk_running();
    if (rpk_running) {
        co_return errc::nothing_running;
    }

    auto bundle_name = make_bundle_filename(
      b.get_write_dir(), ss::sstring{default_bundle_name});
    bool exists = co_await ss::file_exists(bundle_name);

    if (exists) {
        vlog(
          bundle_log.debug, "Removing aborted bundle {}", default_bundle_name);
        // rpk_debug_bundle::do_remove() will acquire the mutex that protects
        // spawned processes.
        co_return co_await b._rpk->do_remove(bundle_name);
    }

    co_return errc::success;
}

ss::future<std::error_code> debug_bundle::abort_bundle() {
    gate_guard guard{_rpk_gate};

    auto core0_running = co_await is_core0_running();
    if (!core0_running) {
        co_return errc::nothing_running;
    }

    auto err = co_await container().invoke_on(
      debug_bundle_shard_id, [](debug_bundle& b) { return force_stop(b); });

    if (make_rpk_err_value(err) != errc::success) {
        co_return err;
    }

    // Remove the bundle after abort is successful.
    co_return co_await container().invoke_on(
      debug_bundle_shard_id, [](debug_bundle& b) {
          // maybe_remove_aborted_bundle will acquire the rpk mutex
          return maybe_remove_aborted_bundle(b);
      });
}

} // namespace debug_bundle
