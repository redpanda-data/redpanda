/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "test_utils/seastar_fuzz.h"

#include "base/seastarx.h"
#include "base/vassert.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <array>
#include <cstdlib>
#include <mutex>
#include <string>

namespace seastar_fuzz {

namespace {

void ensure_test_runner_started() {
    static std::once_flag once;
    std::call_once(once, [] {
        static std::array<std::string, 3> args{
          "seastar_fuzz", "-c1", "--overprovisioned"};
        static std::array<char*, 3> argv{
          args[0].data(), args[1].data(), args[2].data()};
        const bool ok = ss::testing::global_test_runner().start(
          argv.size(), argv.data());
        vassert(ok, "failed to start the test reactor");
        const int rc = std::atexit(
          [] { std::ignore = ss::testing::global_test_runner().finalize(); });
        vassert(rc == 0, "failed to register std::atexit() reactor shutdown");
    });
}

} // namespace

void test_one_input(std::function<void()> fn) {
    ensure_test_runner_started();
    ss::testing::global_test_runner().run_sync(
      [fn = std::move(fn)]() mutable -> ss::future<> {
          return ss::async([fn = std::move(fn)] { fn(); });
      });
}

} // namespace seastar_fuzz
