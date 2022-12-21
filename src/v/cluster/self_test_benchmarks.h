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

#pragma once

#include "cluster/self_test_rpc_types.h"
#include "seastarx.h"
#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster {

/// TODO: This entire source file to be replaced by actual self tests
/// Two self tests are planned for follow up publishing, one to disk the
/// performance of the disk and another for network
class self_test_stub {
public:
    self_test_stub() = default;

    self_test_stub(const self_test_stub&) = delete;
    self_test_stub& operator=(const self_test_stub&) = delete;

    self_test_stub(self_test_stub&&) = delete;
    self_test_stub& operator=(self_test_stub&&) = delete;

    virtual ~self_test_stub() = default;

    virtual ss::future<> stop() {
        _as.request_abort();
        return _gate.close();
    }

    /// Cancel outstanding jobs, can reschedule new ones
    void cancel() { _cancel = true; };
    void reset() { _cancel = false; };

protected:
    virtual ss::future<bool>
    gated_sleep(ss::lowres_clock::duration stop) final {
        gate_guard g{_gate};
        const auto stoptime = ss::lowres_clock::now() + stop;
        try {
            while (!_cancel && stoptime > ss::lowres_clock::now()) {
                co_await ss::sleep_abortable(100ms, _as);
            }
        } catch (const ss::sleep_aborted&) {
            /// Graceful exit
        }
        co_return _cancel;
    }

private:
    bool _cancel{false};
    ss::gate _gate;
    ss::abort_source _as;
};

class diskcheck final : public self_test_stub {
public:
    ss::future<self_test_result> run(diskcheck_opts opts) {
        auto start = ss::lowres_clock::now();
        auto was_cancelled = co_await gated_sleep(opts.duration);
        auto result = self_test_result{
          .name = "Stub disk test",
          .duration = ss::lowres_clock::now() - start};
        if (was_cancelled) {
            result.warning = "Test aborted during run";
        }
        co_return result;
    }
};

class networkcheck final : public self_test_stub {
public:
    ss::future<std::vector<self_test_result>> run(netcheck_opts opts) {
        auto start = ss::lowres_clock::now();
        auto was_cancelled = co_await gated_sleep(opts.duration);
        auto result = self_test_result{
          .name = "Stub network test",
          .duration = ss::lowres_clock::now() - start};
        if (was_cancelled) {
            result.warning = "Test aborted during run";
        }
        co_return std::vector<self_test_result>{result};
    }
};

} // namespace cluster
