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

#include "test_utils/test.h"
#include "utils/external_process.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <system_error>

TEST_CORO(external_process, bin_true) {
    auto proc
      = co_await external_process::external_process::create_external_process(
        {"/bin/true"});
    auto res = co_await proc->wait();
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    EXPECT_EQ(wait_exited.exit_code, 0);
}

TEST_CORO(external_process, bin_false) {
    auto proc
      = co_await external_process::external_process::create_external_process(
        {"/bin/false"});
    auto res = co_await proc->wait();
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    EXPECT_EQ(wait_exited.exit_code, 1);
}

using consumption_result_type =
  typename ss::input_stream<char>::consumption_result_type;
using stop_consuming_type =
  typename consumption_result_type::stop_consuming_type;
using tmp_buf = stop_consuming_type::tmp_buf;

struct consumer {
    consumer(std::string_view expected, bool& matched)
      : _expected(expected)
      , _matched(matched) {}

    ss::future<consumption_result_type> operator()(tmp_buf buf) {
        _matched = std::equal(buf.begin(), buf.end(), _expected.begin());
        if (_matched) {
            return ss::make_ready_future<consumption_result_type>(
              stop_consuming_type({}));
        }
        _expected.remove_prefix(buf.size());
        return ss::make_ready_future<consumption_result_type>(
          ss::continue_consuming{});
    }

    std::string_view _expected;
    bool& _matched;
};

TEST_CORO(external_process, echo_hi) {
    bool matched = false;
    auto proc
      = co_await external_process::external_process::create_external_process(
        {"/bin/echo", "-n", "hi"});
    proc->set_stdout_consumer(consumer{"hi", matched});
    auto res = co_await proc->wait();

    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    EXPECT_EQ(wait_exited.exit_code, 0);

    EXPECT_TRUE(matched);
}

TEST_CORO(external_process, run_long_process) {
    using namespace std::chrono_literals;

    auto start_time = std::chrono::high_resolution_clock::now();
    auto proc
      = co_await external_process::external_process::create_external_process(
        {"/bin/sleep", "10"});
    auto res = co_await proc->wait();
    auto end_time = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    EXPECT_EQ(wait_exited.exit_code, 0);
    EXPECT_GE(
      std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time)
        .count(),
      10);
}

TEST_CORO(external_process, run_long_process_and_terminate) {
    using namespace std::chrono_literals;

    auto proc
      = co_await external_process::external_process::create_external_process(
        {"/bin/sleep", "10"});
    auto wait_fut = proc->wait();
    co_await ss::sleep(1s);
    EXPECT_TRUE(proc->is_running());
    co_await proc->terminate(1s);
    EXPECT_TRUE(!proc->is_running());
    auto res = co_await std::move(wait_fut);
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_signaled>(res));
    auto wait_signaled = std::get<ss::experimental::process::wait_signaled>(
      res);
    EXPECT_EQ(wait_signaled.terminating_signal, SIGTERM);
}

TEST_CORO(external_process, test_sigterm_ignored) {
    using namespace std::chrono_literals;
    const char* script_path = std::getenv("HANDLE_SIGTERM_SCRIPT");
    ASSERT_TRUE_CORO(script_path != nullptr)
      << "Missing 'HANDLE_SIGTERM_SCRIPT' env variable";
    auto script_exists = co_await ss::file_exists(script_path);
    ASSERT_TRUE_CORO(script_exists) << script_path << " does not exist";
    bool matched = false;

    auto proc
      = co_await external_process::external_process::create_external_process(
        {script_path});
    proc->set_stdout_consumer(consumer{"sigterm called", matched});
    co_await ss::sleep(100ms);
    ASSERT_TRUE_CORO(proc->is_running());
    auto wait_fut = proc->wait();
    auto term_fut = proc->terminate(5s);
    co_await ss::sleep(1s);
    // SIGTERM should be handled at this point and we're waiting 5 seconds
    // until sending SIGKILL so the process should still be running
    EXPECT_TRUE(proc->is_running());
    EXPECT_TRUE(!term_fut.available());
    EXPECT_TRUE(!wait_fut.available());
    EXPECT_TRUE(!wait_fut.failed());
    EXPECT_TRUE(matched);
    co_await ss::sleep(5s);
    EXPECT_TRUE(!proc->is_running());
    EXPECT_TRUE(term_fut.available());
    EXPECT_TRUE(wait_fut.available());
    EXPECT_TRUE(!wait_fut.failed());
    co_await std::move(term_fut);
    auto res = co_await std::move(wait_fut);
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_signaled>(res));
    auto wait_signaled = std::get<ss::experimental::process::wait_signaled>(
      res);
    EXPECT_EQ(wait_signaled.terminating_signal, SIGKILL);
}

TEST_CORO(external_process, no_such_bin) {
    try {
        co_await external_process::external_process::create_external_process(
          {"/no/such/proc"});
        ASSERT_TRUE_CORO(false) << "Should not have succeeded";
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code(), std::error_code(ENOENT, std::system_category()));
    } catch (...) {
        ASSERT_TRUE_CORO(false) << "Unexpected exception thrown";
    }
}

TEST_CORO(external_process, test_bad_args) {
    auto proc
      = co_await external_process::external_process::create_external_process(
        {"/usr/bin/ls", "--nope"});
    auto res = co_await proc->wait();
    ASSERT_TRUE_CORO(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    EXPECT_EQ(wait_exited.exit_code, 2);
}
