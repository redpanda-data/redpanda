// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/process.hh>

#include <chrono>
#include <filesystem>
#include <system_error>
#include <utility>
#include <vector>

namespace ssx {

enum class process_errc : int16_t {
    success = 0, // must be 0
    does_not_exist,
    running,
    non_zero_exit,
    signaled
};

struct process_errc_category final : public std::error_category {
    const char* name() const noexcept final { return "process::errc"; }

    std::string message(int c) const final {
        switch (static_cast<process_errc>(c)) {
        case process_errc::success:
            return "Success";
        case process_errc::does_not_exist:
            return "Process does not exist";
        case process_errc::running:
            return "Process is already running";
        case process_errc::non_zero_exit:
            return "Process exited with non-zero status";
        case process_errc::signaled:
            return "Process exited on signal";
        }
        return "process::process_errc::unknown";
    }
};
inline const std::error_category& error_category() noexcept {
    static process_errc_category e;
    return e;
}
inline std::error_code make_error_code(process_errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}

/*
 * \brief Manages a POSIX fork that runs on one core
 *
 *  Seastar supports running external processes via a POSIX fork with
 * ss::experimental::spawn_process. This class does not spawn two or more forks
 * within the same instance. The process runs on one core and it is easy to
 * misuse. Therefore, this wrapper is responsible for:
 *      1. Starting a process
 *      2. Killing a process
 *      3. Waiting for the process to finish
 *
 *   Example:
 *      process p
 *      p.spawn()
 *      while p.is_running():
 *          do_other_work()
 *      status = p.exit_status()
 *      log(status.exit_int, status.exit_reason)
 */
class process {
public:
    // A struct to represent the result of a process
    struct exit_status_t {
        int exit_int;
        ss::sstring exit_reason;
    };

    process()
      : _process{std::nullopt}
      , _wait_status{ss::experimental::process::wait_exited{-1}} {}
    process(const process&) = delete;
    ~process();

    // Spawns a posix fork and starts running the command.
    //
    // Output from stdout and stderr are logged at TRACE level.
    ss::future<std::error_code> spawn(
      const std::filesystem::path& cmd,
      ss::experimental::spawn_parameters params);

    ss::future<> stop();

    // Terminate a running process
    //
    // /param: timeout: Amount of time to wait for the process to respond to
    // SIGTERM. SIGKILL is sent after timeout. Default 1s.
    ss::future<std::error_code>
    terminate(std::chrono::milliseconds timeout = std::chrono::seconds(1));

    // Unwraps the result of a process into an integer for the exit code/signal
    // and a string describing what caused the exit.
    exit_status_t exit_status();
    bool is_running() const { return _process.has_value(); }

private:
    ss::sstring _cmd_str;
    std::optional<ss::experimental::process> _process;
    ss::experimental::process::wait_status _wait_status;
    ss::gate _gate;
    ss::abort_source _as;

    // Wait for the running process to finish
    ss::future<> handle_wait();
};

} // namespace ssx

namespace std {
template<>
struct is_error_code_enum<ssx::process_errc> : true_type {};
} // namespace std
