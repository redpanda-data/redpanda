/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "ssx/rwlock.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <boost/test/tools/old/interface.hpp>

#include <iostream>

using namespace ssx;
using namespace std::chrono_literals;

namespace {
ss::logger test_log("test_log");

// Returns true if the presence of 'str' in 'os' matches what is expected, per
// 'should_contain'.
bool stream_contains(
  bool should_contain, const std::ostringstream& os, const ss::sstring& str) {
    auto out = os.str();
    bool contains = out.find(str) != out.npos;
    if (should_contain != contains) {
        test_log.set_ostream(std::cerr);
        vlog(test_log.error, "Output: '{}' vs Searched: '{}'", out, str);
    }
    return should_contain == contains;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(rwlock_test) {
    ss::logger captured_log("captured_log");
    seastar::global_logger_registry().set_logger_level(
      "captured_log", ss::log_level::trace);
    auto revert_stream = ss::defer(
      [&] { captured_log.set_ostream(std::cerr); });
    logging_rwlock lock(captured_log, [] { return "c-lock"; });

    std::ostringstream outer_os;
    captured_log.set_ostream(outer_os);
    auto write_holder = std::make_unique<ssx::logging_rwlock::holder>(
      std::move(lock.hold_write_lock().get()));
    auto out = outer_os.str();
    BOOST_CHECK(stream_contains(
      true, outer_os, "Write lock on c-lock (0) acquired after"));
    {
        for (int i = 1; i < 3; i++) {
            std::ostringstream os;
            captured_log.set_ostream(os);
            // Set a logging timeout shorter than the take timeout so we log
            // about slow acquisition.
            BOOST_REQUIRE_EXCEPTION(
              lock
                .hold_write_lock(
                  ss::semaphore::time_point::clock::now() + 100ms, 50ms)
                .get(),
              ss::semaphore_timed_out,
              [](const auto&) { return true; });
            auto out = os.str();
            BOOST_CHECK(stream_contains(
              true, os, fmt::format("Write lock on c-lock ({}) has taken", i)));
            BOOST_CHECK(stream_contains(
              false,
              os,
              fmt::format("Write lock on c-lock ({}) acquired after", i)));
        }
        for (int i = 3; i < 5; i++) {
            std::ostringstream os;
            captured_log.set_ostream(os);
            BOOST_REQUIRE_EXCEPTION(
              lock
                .hold_read_lock(
                  ss::semaphore::time_point::clock::now() + 100ms, 50ms)
                .get(),
              ss::semaphore_timed_out,
              [](const auto&) { return true; });
            BOOST_CHECK(stream_contains(
              true, os, fmt::format("Read lock on c-lock ({}) has taken", i)));
            BOOST_CHECK(stream_contains(
              false,
              os,
              fmt::format("Read lock on c-lock ({}) acquired after", i)));
        }
        captured_log.set_ostream(outer_os);
    }
    BOOST_CHECK(
      stream_contains(false, outer_os, "Dropping lock on c-lock (0)"));
    write_holder.reset();
    BOOST_CHECK(stream_contains(true, outer_os, "Dropping lock on c-lock (0)"));
}

SEASTAR_THREAD_TEST_CASE(rwlock_test_no_log_after_move) {
    ss::logger captured_log("captured_log");
    seastar::global_logger_registry().set_logger_level(
      "captured_log", ss::log_level::trace);
    auto revert_stream = ss::defer(
      [&] { captured_log.set_ostream(std::cerr); });
    std::ostringstream os;
    captured_log.set_ostream(os);
    logging_rwlock lock(captured_log, [] { return "c-lock"; });
    auto write_holder = std::make_unique<ssx::logging_rwlock::holder>(
      std::move(lock.hold_write_lock().get()));
    stream_contains(false, os, "Dropping lock");
    {
        auto new_holder = std::move(*write_holder);
        write_holder.reset();
        stream_contains(false, os, "Dropping lock");
    }
    stream_contains(true, os, "Dropping lock");
}
