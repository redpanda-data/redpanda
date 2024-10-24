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

#include "external_process.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/later.hh>
#include <seastar/util/process.hh>

#include <functional>
#include <optional>

namespace external_process {
ss::future<std::unique_ptr<external_process>>
external_process::create_external_process(
  std::vector<ss::sstring> command,
  std::optional<std::vector<ss::sstring>> env) {
    vassert(
      !command.empty(),
      "Should not be passed an empty command list to "
      "create_external_process");

    auto path = std::filesystem::path(command[0]);
    ss::experimental::spawn_parameters params{
      .argv = std::move(command),
      .env = env.has_value() ? std::move(env).value()
                             : std::vector<ss::sstring>{}};

    auto p = co_await ss::experimental::spawn_process(path, params);

    co_return std::unique_ptr<external_process>(
      new external_process(std::move(p), std::move(path), std::move(params)));
}

external_process::external_process(
  ss::experimental::process p,
  std::filesystem::path path,
  ss::experimental::spawn_parameters params)
  : _process(std::move(p))
  , _path(std::move(path))
  , _params(std::move(params))
  , _terminate_mutex("external_process::terminate")
  , _wait_mutex("external_process::wait") {}

external_process::~external_process() {
    vassert(_completed, "Process has not yet completed");
    vassert(!_stdout_consumer.has_value(), "stdout consumer not cleaned up");
    vassert(!_stderr_consumer.has_value(), "stderr consumer not cleaned up");
}

ss::future<ss::experimental::process::wait_status> external_process::wait() {
    auto guard = _gate.hold();
    if (_completed) {
        throw std::system_error(
          make_error_code(error_code::process_already_completed));
    }

    auto units = _wait_mutex.try_get_units();
    if (!units) {
        throw std::system_error(
          make_error_code(error_code::process_already_being_waited_on));
    }

    auto resp = co_await _process.wait();

    if (_stdout_consumer.has_value()) {
        try {
            co_await std::move(_stdout_consumer).value();
        } catch (const std::exception& e) {
            std::ignore = e;
        }

        _stdout_consumer.reset();
    }

    if (_stderr_consumer.has_value()) {
        try {
            co_await std::move(_stderr_consumer).value();
        } catch (const std::exception& e) {
            std::ignore = e;
        }

        _stderr_consumer.reset();
    }

    _completed = true;
    term_as.request_abort();

    co_return resp;
}

ss::future<> external_process::terminate(std::chrono::milliseconds timeout) {
    auto guard = _gate.hold();

    if (_completed) {
        throw std::system_error(
          make_error_code(error_code::process_already_completed));
    }

    auto units = _terminate_mutex.try_get_units();
    if (!units) {
        throw std::system_error(
          make_error_code(error_code::process_already_being_terminated));
    }

    if (_wait_mutex.ready()) {
        throw std::system_error(
          make_error_code(error_code::terminate_called_before_wait));
    }

    try {
        _process.terminate();
    } catch (const std::exception& e) {
        throw std::system_error(
          make_error_code(error_code::failed_to_terminate_process), e.what());
    }

    try {
        co_await ss::sleep_abortable(timeout, term_as);
    } catch (const ss::sleep_aborted& e) {
        std::ignore = e;
    }

    if (!_completed) {
        try {
            _process.kill();
        } catch (const std::exception& e) {
            throw std::system_error(
              make_error_code(error_code::failed_to_terminate_process),
              e.what());
        }
    }
}
} // namespace external_process
