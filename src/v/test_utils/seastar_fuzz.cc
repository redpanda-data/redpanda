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
#include <csignal>
#include <cstdlib>
#include <mutex>
#include <string>

namespace seastar_fuzz {

namespace {

struct handler_slot {
    int sig;
    struct sigaction action{};
};

/// What a vassert, an abort or a memory fault arrives as. Seastar takes only
/// SIGSEGV, SIGABRT and SIGILL at boot; the other two are carried in case
/// that changes.
std::array<handler_slot, 5>& crash_handlers() {
    static std::array<handler_slot, 5> handlers{
      handler_slot{.sig = SIGILL},
      handler_slot{.sig = SIGABRT},
      handler_slot{.sig = SIGSEGV},
      handler_slot{.sig = SIGBUS},
      handler_slot{.sig = SIGFPE}};
    return handlers;
}

void save_crash_handlers() {
    for (auto& slot : crash_handlers()) {
        std::ignore = ::sigaction(slot.sig, nullptr, &slot.action);
    }
}

/// libFuzzer claims only a signal that has no handler, and seastar takes some
/// at boot. Restoring the pre-boot state lets a crash reach libFuzzer and
/// land in an artifact.
void restore_crash_handlers() {
    for (const auto& slot : crash_handlers()) {
        std::ignore = ::sigaction(slot.sig, &slot.action, nullptr);
    }
}

/// Seastar also blocks most signals on the reactor thread. A synchronous
/// SIGBUS or SIGTRAP raised while blocked kills the process with no artifact,
/// and libFuzzer's control signals (SIGUSR1/2, SIGXFSZ) would never arrive.
/// Only these are unblocked: the rest of the mask, SIGPIPE included, is
/// seastar's business.
void unblock_fuzzer_signals() {
    sigset_t sigs;
    sigemptyset(&sigs);
    for (const auto& slot : crash_handlers()) {
        sigaddset(&sigs, slot.sig);
    }
    for (const int sig : {SIGTRAP, SIGXFSZ, SIGUSR1, SIGUSR2}) {
        sigaddset(&sigs, sig);
    }
    vassert(
      ::pthread_sigmask(SIG_UNBLOCK, &sigs, nullptr) == 0,
      "failed to unblock libFuzzer's signals");
}

void ensure_test_runner_started() {
    static std::once_flag once;
    std::call_once(once, [] {
        // global_test_runner starts the reactor lazily on the first run_sync(),
        // so save libFuzzer's handlers before giving it a chance to boot.
        save_crash_handlers();
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
          // run_sync() has now booted the reactor and installed Seastar's
          // handlers. Restore libFuzzer's once, before running the first body.
          static std::once_flag once;
          std::call_once(once, restore_crash_handlers);
          // The body runs on the reactor thread, where seastar blocks crash
          // signals. Unblock so a fault during the body is delivered.
          unblock_fuzzer_signals();
          return ss::async([fn = std::move(fn)] { fn(); });
      });
}

} // namespace seastar_fuzz
