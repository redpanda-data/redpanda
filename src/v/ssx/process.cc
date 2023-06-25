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

#include "ssx/process.h"

#include "ssx/future-util.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/log.hh>

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <fmt/format.h>

#include <sstream>
#include <string>

namespace ssx {

namespace {
ss::logger proclog{"process"};

ss::future<> consume_input_stream(
  ss::input_stream<char> stream, bool is_stdout, ss::abort_source& as) {
    std::string_view stream_name{is_stdout ? "STDOUT" : "STDERR"};

    std::string line;
    while (!stream.eof()) {
        if (as.abort_requested()) {
            co_return;
        }

        auto buf = co_await stream.read();
        if (buf.empty()) {
            continue;
        }

        boost::iostreams::stream<boost::iostreams::basic_array_source<char>> ss{
          buf.begin(), buf.end()};
        while (!ss.eof()) {
            std::getline(ss, line);
            vlog(proclog.trace, "{}: {}", stream_name, line);
        }
    }
}

// Unwraps the result of a Seastar process
process::exit_status_t
unwrap_wait_status(ss::experimental::process::wait_status& result) {
    return ss::visit(
      result,
      [](ss::experimental::process::wait_exited exited) {
          return process::exit_status_t{
            .exit_int = exited.exit_code, .exit_reason = "exit code"};
      },
      [](ss::experimental::process::wait_signaled signaled) {
          return process::exit_status_t{
            .exit_int = signaled.terminating_signal,
            .exit_reason = "exit signal"};
      });
}
} // namespace

process::~process() {
    vassert(
      !is_running(),
      "Processes must exit before destruction: cmd {}",
      _cmd_str);
}

ss::future<std::error_code> process::spawn(
  const std::filesystem::path& cmd, ss::experimental::spawn_parameters params) {
    gate_guard g{_gate};
    if (is_running()) {
        vlog(proclog.error, "A process is already running: cmd {}", _cmd_str);
        co_return process_errc::running;
    }

    // Set command string for logging purposes
    _cmd_str = cmd.string();

    auto p = co_await ss::experimental::spawn_process(cmd, std::move(params));
    _process.emplace(std::move(p));

    // Capture output async in the background so the broker is not blocked.
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
        return consume_input_stream(_process->stdout(), true, _as)
          .then([this] {
              return consume_input_stream(_process->stderr(), false, _as);
          })
          // According to seastar docs, a process that is not wait'd may leave a
          // zombie behind. So we do that here.
          .then([this] { return handle_wait(); })
          .finally([this] { _process.reset(); });
    });

    co_return process_errc::success;
}

ss::future<> process::stop() {
    _as.request_abort();
    co_await _gate.close();
}

process::exit_status_t process::exit_status() {
    return unwrap_wait_status(_wait_status);
}

ss::future<> process::handle_wait() {
    vassert(is_running(), "_process not instantiated");

    _wait_status = co_await _process->wait();
    auto status = exit_status();

    // There is no signal=0, so an exit_int=0 is the success exit code.
    if (status.exit_int != 0) {
        vlog(
          proclog.error,
          "Process stop fail: cmd {}, {}={}",
          _cmd_str,
          status.exit_reason,
          status.exit_int);
    } else {
        vlog(proclog.trace, "Process stop success: cmd {}", _cmd_str);
    }
}

ss::future<std::error_code>
process::terminate(std::chrono::milliseconds timeout) {
    gate_guard g{_gate};
    if (!is_running()) {
        co_return process_errc::does_not_exist;
    }

    try {
        _process->terminate();
    } catch (const std::system_error&) {
        // The process is already dead
        co_return process_errc::does_not_exist;
    }

    auto deadline = ss::lowres_clock::now() + timeout;

    co_return co_await ss::with_timeout(
      deadline,
      ss::do_until(
        [this, deadline] {
            // The callback (do_until loop) is not auto canceled on timeout,
            // so exit when the deadline is exceeded.
            if (ss::lowres_clock::now() > deadline) {
                return true;
            }

            return !is_running();
        },
        [] { return ss::sleep(std::chrono::milliseconds{5}); }))
      .then([] {
          return ss::make_ready_future<process_errc>(process_errc::success);
      })
      .handle_exception_type([this](const ss::timed_out_error&) {
          if (_process.has_value()) {
              _process->kill();
          }
          return ss::make_ready_future<process_errc>(process_errc::success);
      })
      .handle_exception_type([](const std::system_error&) {
          // The process is already dead
          return ss::make_ready_future<process_errc>(
            process_errc::does_not_exist);
      });
}

} // namespace ssx
