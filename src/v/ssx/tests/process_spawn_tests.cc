// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/node_config.h"
#include "ssx/process.h"
#include "test_utils/async.h"
#include "vassert.h"

#include <seastar/testing/thread_test_case.hh>

#include <fmt/format.h>

#include <chrono>

inline ss::logger test_log("test");

std::filesystem::path get_trap_script() {
    auto trap_script = std::getenv("HANDLE_SIGTERM_SCRIPT");
    vassert(trap_script, "$HANDLE_SIGTERM_SCRIPT does not exist");
    BOOST_REQUIRE_MESSAGE(
      ss::file_exists(trap_script).get0(),
      std::string(trap_script) + " does not exist");
    return std::filesystem::path{trap_script};
}

std::filesystem::path get_timed_loop_script() {
    auto timed_script = std::getenv("TIMED_LOOP_SCRIPT");
    vassert(timed_script, "$TIMED_LOOP_SCRIPT does not exist");
    BOOST_REQUIRE_MESSAGE(
      ss::file_exists(timed_script).get0(),
      std::string(timed_script) + " does not exist");
    return std::filesystem::path{timed_script};
}

SEASTAR_THREAD_TEST_CASE(process_start) {
    auto cmd = get_timed_loop_script();
    int script_runtime_s = 10;
    std::vector<ss::sstring> argv{
      cmd.string(), fmt::format("{}", script_runtime_s)};
    ss::experimental::spawn_parameters params{.argv = argv};

    ssx::process p;
    BOOST_REQUIRE(!p.is_running());

    auto status = p.exit_status();
    BOOST_REQUIRE_EQUAL(status.exit_int, -1);
    BOOST_REQUIRE_EQUAL(status.exit_reason, ss::sstring{"exit code"});

    std::error_code err;

    // Start first run
    err = p.spawn(cmd, params).get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::success);

    // Try dup run
    err = p.spawn(cmd, params).get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::running);

    // Wait until process stops
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds{script_runtime_s}, [&p] { return !p.is_running(); })
      .get();

    // The reason for stopping should be success (exit code 0)
    status = p.exit_status();
    BOOST_REQUIRE_EQUAL(status.exit_int, 0);
    BOOST_REQUIRE_EQUAL(status.exit_reason, ss::sstring{"exit code"});

    p.stop().get();
}

SEASTAR_THREAD_TEST_CASE(process_stop_sigterm) {
    auto cmd = get_timed_loop_script();
    int script_runtime_s = 10;
    std::vector<ss::sstring> argv{
      cmd.string(), fmt::format("{}", script_runtime_s)};
    ss::experimental::spawn_parameters params{.argv = argv};

    ssx::process p;
    std::error_code err;

    // Try terminate before run
    test_log.info("Terminating -- expect fail");
    err = p.terminate().get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::does_not_exist);

    // Run the process
    test_log.info("Running -- expect success");
    err = p.spawn(cmd, params).get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::success);

    // Let the script run for a bit
    using namespace std::chrono_literals;
    ss::sleep(1s).get();

    // Try terminate
    test_log.info("Terminating ...");
    auto fut = p.terminate();

    // Spawn should still report running process
    err = p.spawn(cmd, params).get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::running);

    // Wait until the process to stops
    tests::cooperative_spin_wait_with_timeout(3s, [&p] {
        return !p.is_running();
    }).get();

    // Terminate should succeed
    err = fut.get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::success);

    // The reason for stopping should be SIGTERM (signal 15)
    auto status = p.exit_status();
    BOOST_REQUIRE_EQUAL(status.exit_int, SIGTERM);
    BOOST_REQUIRE_EQUAL(status.exit_reason, ss::sstring{"exit signal"});

    p.stop().get();
}

SEASTAR_THREAD_TEST_CASE(process_stop_sigkill) {
    auto cmd = get_trap_script();
    std::vector<ss::sstring> argv{cmd.string()};
    ss::experimental::spawn_parameters params{.argv = argv};

    ssx::process p;
    std::error_code err;

    // Run the process
    test_log.info("Running -- expect success");
    err = p.spawn(cmd, params).get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::success);

    // Let the script run for a bit
    using namespace std::chrono_literals;
    ss::sleep(1s).get();

    // Try terminate
    test_log.info("Terminating ...");
    auto fut = p.terminate();

    // Spawn should still report running process
    err = p.spawn(cmd, params).get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::running);

    // Wait until the process stops
    tests::cooperative_spin_wait_with_timeout(3s, [&p] {
        return !p.is_running();
    }).get();

    // Terminate should succeed
    err = fut.get();
    BOOST_REQUIRE_EQUAL(err, ssx::process_errc::success);

    // The reason for stopping should be SIGKILL (signal 9)
    auto status = p.exit_status();
    BOOST_REQUIRE_EQUAL(status.exit_int, SIGKILL);
    BOOST_REQUIRE_EQUAL(status.exit_reason, ss::sstring{"exit signal"});

    p.stop().get();
}
