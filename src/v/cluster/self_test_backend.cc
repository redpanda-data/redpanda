/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/self_test_backend.h"

#include "cluster/logger.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>

namespace cluster {

ss::future<> self_test_backend::start() { return ss::now(); }

ss::future<> self_test_backend::stop() {
    co_await _gate.close();
    co_await _disk_test.stop();
    co_await _network_test.stop();
    co_await _lock.get_units(); /// Ensure outstanding work is completed
}

ss::future<std::vector<self_test_result>> self_test_backend::do_start_test(
  std::optional<diskcheck_opts> dto, std::optional<netcheck_opts> nto) {
    auto gate_holder = _gate.hold();
    std::vector<self_test_result> results;
    if (dto) {
        /// Disk
        try {
            vlog(clusterlog.info, "Starting disk self test...");
            results.push_back(
              co_await _disk_test.run(*dto).then([](auto result) {
                  vlog(clusterlog.info, "Disk self test finished with success");
                  return result;
              }));
        } catch (const std::exception& ex) {
            vlog(clusterlog.warn, "Disk self test finished with error");
            results.push_back(self_test_result{.error = ex.what()});
        }
    }
    if (nto) {
        /// Network
        try {
            vlog(clusterlog.info, "Starting network self test...");
            auto ntr = co_await _network_test.run(*nto).then([](auto results) {
                vlog(
                  clusterlog.info, "Network self test finished with success");
                return results;
            });
            std::copy(ntr.begin(), ntr.end(), std::back_inserter(results));
        } catch (const std::exception& ex) {
            vlog(clusterlog.warn, "Network self test finished with error");
            results.push_back(self_test_result{.error = ex.what()});
        }
    }
    co_return results;
}

get_status_response self_test_backend::start_test(start_test_request req) {
    auto gate_holder = _gate.hold();
    auto units = _lock.try_get_units();
    if (units) {
        _id = req.id;
        vlog(
          clusterlog.info, "Starting redpanda self-tests with id: {}", req.id);
        (void)do_start_test(req.dto, req.nto)
          .then([this, id = req.id](auto results) {
              _prev_run = get_status_response{
                .id = id,
                .status = self_test_status::idle,
                .results = std::move(results)};
          })
          .finally([this, units = std::move(units)] {
              /// TODO: No need for this when disk + network tests are written
              _disk_test.reset();
              _network_test.reset();
          });
    } else {
        vlog(
          clusterlog.info,
          "Request to start already in-progress test receieved, updating test "
          "UUID from: {} to: {}",
          _id,
          req.id);
        _id = req.id;
    }
    return get_status();
}

ss::future<get_status_response> self_test_backend::stop_test() {
    auto gate_holder = _gate.hold();
    _disk_test.cancel();
    _network_test.cancel();
    try {
        /// When lock is released, the 'then' block above will set the _prev_run
        /// var with the finalized test results from the cancelled run.
        static const auto stop_test_timeout = 5s;
        co_return co_await _lock.with(
          stop_test_timeout, [this] { return _prev_run; });
    } catch (const ss::semaphore_timed_out&) {
        vlog(clusterlog.warn, "Failed to stop self tests within 5s timeout");
    }
    co_return get_status_response{
      .id = _id, .status = self_test_status::running};
}

get_status_response self_test_backend::get_status() const {
    if (!_lock.ready()) {
        return get_status_response{
          .id = _id, .status = self_test_status::running};
    }
    return _prev_run;
}

} // namespace cluster
