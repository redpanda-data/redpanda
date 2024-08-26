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

#include <seastar/core/seastar.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/later.hh>

namespace external_process {
ss::future<external_process> external_process::create_external_process(
  std::vector<ss::sstring> command,
  std::optional<std::vector<ss::sstring>> env) {
    vassert(
      !command.empty(),
      "Should not be passed an empty command list to "
      "create_external_process");

    ss::experimental::spawn_parameters params;
    auto path = std::filesystem::path(command[0]);
    params.argv = std::move(command);
    if (env) {
        params.env = std::move(env).value();
    }

    auto p = co_await ss::experimental::spawn_process(path, params);

    co_return external_process(
      std::move(p), std::move(path), std::move(params));
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
        co_await std::move(_stdout_consumer).value();
        _stdout_consumer.reset();
    }

    if (_stderr_consumer.has_value()) {
        co_await std::move(_stderr_consumer).value();
        _stderr_consumer.reset();
    }

    _completed = true;

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
    } catch (std::exception& e) {
        throw std::system_error(
          make_error_code(error_code::failed_to_terminate_process), e.what());
    }

    bool cancel = false;
    return ss::do_with(cancel, timeout, [this](bool& cancel, auto& timeout) {
        return ss::with_timeout(
                 ss::lowres_clock::now() + timeout,
                 ss::do_until(
                   [this, &cancel] { return _completed || cancel; },
                   [] { return ss::yield(); }))
          .then([] { return ss::now(); })
          .handle_exception_type([this, &cancel](const ss::timed_out_error&) {
              cancel = true;
              try {
                  _process.kill();
                  return ss::now();
              } catch (std::exception& e) {
                  throw std::system_error(
                    make_error_code(error_code::failed_to_terminate_process),
                    e.what());
              }
          });
    });
}
} // namespace external_process
