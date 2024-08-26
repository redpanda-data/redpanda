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

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/process.hh>

#include <boost/test/tools/old/interface.hpp>
#include <utils/external_process.h>

#include <chrono>
#include <optional>
#include <system_error>
#include <variant>

SEASTAR_THREAD_TEST_CASE(external_process_bin_true) {
    auto proc = external_process::external_process::create_external_process(
                  {"/bin/true"})
                  .get();
    auto res = proc.wait().get();
    BOOST_REQUIRE(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    BOOST_CHECK_EQUAL(wait_exited.exit_code, 0);
}

SEASTAR_THREAD_TEST_CASE(external_process_bin_false) {
    auto proc = external_process::external_process::create_external_process(
                  {"/bin/false"})
                  .get();
    auto res = proc.wait().get();
    BOOST_REQUIRE(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    BOOST_CHECK_EQUAL(wait_exited.exit_code, 1);
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

SEASTAR_THREAD_TEST_CASE(external_process_echo_hi) {
    ss::do_with(bool(false), [](auto& matched) {
        auto proc = external_process::external_process::create_external_process(
                      {"/bin/echo", "-n", "hi"})
                      .get();

        proc.set_stdout_consumer(consumer{"hi", matched});
        auto res = proc.wait().get();

        BOOST_REQUIRE(
          std::holds_alternative<ss::experimental::process::wait_exited>(res));
        auto wait_exited = std::get<ss::experimental::process::wait_exited>(
          res);
        BOOST_CHECK_EQUAL(wait_exited.exit_code, 0);

        BOOST_CHECK(matched);
        return ss::now();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(external_process_run_long_process) {
    using namespace std::chrono_literals;

    auto proc = external_process::external_process::create_external_process(
                  {"/bin/sleep", "10"})
                  .get();
    auto start_time = std::chrono::high_resolution_clock::now();
    auto res = proc.wait().get();
    auto end_time = std::chrono::high_resolution_clock::now();
    BOOST_REQUIRE(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    BOOST_CHECK_EQUAL(wait_exited.exit_code, 0);
    BOOST_CHECK(
      std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time)
        .count()
      >= 0);
}

SEASTAR_THREAD_TEST_CASE(external_process_run_long_process_and_terminate) {
    using namespace std::chrono_literals;

    auto proc = external_process::external_process::create_external_process(
                  {"/bin/sleep", "10"})
                  .get();
    auto wait_fut = proc.wait();
    ss::sleep(1s).get();
    BOOST_CHECK(proc.is_running());
    proc.terminate(1s).get();
    BOOST_CHECK(!proc.is_running());
    auto res = wait_fut.get();
    BOOST_REQUIRE(
      std::holds_alternative<ss::experimental::process::wait_signaled>(res));
    auto wait_signaled = std::get<ss::experimental::process::wait_signaled>(
      res);
    BOOST_CHECK_EQUAL(wait_signaled.terminating_signal, SIGTERM);
}

SEASTAR_THREAD_TEST_CASE(external_process_test_sigterm_ignored) {
    using namespace std::chrono_literals;
    const char* script_path = std::getenv("HANDLE_SIGTERM_SCRIPT");
    vassert(script_path, "Missing 'HANDLE_SIGTERM_SCRIPT' env variable");

    BOOST_REQUIRE_MESSAGE(
      ss::file_exists(script_path).get(),
      std::string(script_path) + " does not exist");

    ss::do_with(
      bool(false),
      std::string(script_path),
      [](auto& matched, auto& script_path) {
          auto proc
            = external_process::external_process::create_external_process(
                {script_path.c_str()})
                .get();
          proc.set_stdout_consumer(consumer{"sigterm called", matched});
          ss::sleep(100ms).get();
          BOOST_REQUIRE(proc.is_running());
          auto wait_fut = proc.wait();
          auto term_fut = proc.terminate(5s);
          ss::sleep(1s).get();
          // SIGTERM should be handled at this point and we're waiting 5 seconds
          // until sending SIGKILL so the process should still be running
          BOOST_CHECK(proc.is_running());
          BOOST_CHECK(!term_fut.available());
          BOOST_CHECK(!wait_fut.available());
          BOOST_CHECK(!wait_fut.failed());
          BOOST_CHECK(matched);
          ss::sleep(5s).get();
          BOOST_CHECK(!proc.is_running());
          BOOST_CHECK(term_fut.available());
          BOOST_CHECK(wait_fut.available());
          BOOST_CHECK(!wait_fut.failed());
          term_fut.get();
          auto res = wait_fut.get();
          BOOST_REQUIRE(
            std::holds_alternative<ss::experimental::process::wait_signaled>(
              res));
          auto wait_signaled
            = std::get<ss::experimental::process::wait_signaled>(res);
          BOOST_CHECK_EQUAL(wait_signaled.terminating_signal, SIGKILL);
          return ss::now();
      })
      .get();
}

SEASTAR_THREAD_TEST_CASE(external_process_no_such_bin) {
    BOOST_CHECK_EXCEPTION(
      external_process::external_process::create_external_process(
        {"/no/such/proc"})
        .get(),
      std::system_error,
      [](std::system_error const& e) {
          return e.code() == std::error_code(ENOENT, std::system_category());
      });
}

SEASTAR_THREAD_TEST_CASE(external_process_test_bad_args) {
    auto proc = external_process::external_process::create_external_process(
                  {"/usr/bin/ls", "--nope"})
                  .get();
    auto res = proc.wait().get();
    BOOST_REQUIRE(
      std::holds_alternative<ss::experimental::process::wait_exited>(res));
    auto wait_exited = std::get<ss::experimental::process::wait_exited>(res);
    BOOST_CHECK_EQUAL(wait_exited.exit_code, 2);
}
