// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/source_location.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;
namespace ss = seastar;

TEST(CheckpointMutex, checkpoint_mutex_lock) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    EXPECT_TRUE(mutex.has_units() == true);
    EXPECT_TRUE(mutex.get_blocking_checkpoint() == std::nullopt);

    // Acquire the mutex normally.
    auto line = vlog::file_line::current();
    auto units = mutex.get_units().get();
    EXPECT_TRUE(mutex.has_units() == false);
    EXPECT_EQ(
      mutex.get_blocking_checkpoint().value().line.filename,
      ss::sstring("checkpoint_mutex_test.cc"));
    EXPECT_EQ(mutex.get_blocking_checkpoint().value().line.line, line.line + 1);

    line = vlog::file_line::current();
    auto fut = mutex.get_units();
    EXPECT_TRUE(fut.available() == false);

    // We have another waiter so the mutex should not be available.
    units.release();
    EXPECT_TRUE(mutex.has_units() == false);

    // Another waiter should be able to acquire the mutex.
    auto units2 = std::move(fut).get();
    EXPECT_EQ(
      mutex.get_blocking_checkpoint().value().line.filename,
      ss::sstring("checkpoint_mutex_test.cc"));
    EXPECT_EQ(mutex.get_blocking_checkpoint().value().line.line, line.line + 1);

    units2.release();
    EXPECT_TRUE(mutex.has_units() == true);
}

TEST(CheckpointMutex, checkpoint_mutex_broken) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    EXPECT_TRUE(mutex.has_units() == true);
    EXPECT_TRUE(mutex.get_blocking_checkpoint() == std::nullopt);

    // Acquire the mutex normally.
    auto line = vlog::file_line::current();
    auto units = mutex.get_units().get();
    EXPECT_TRUE(mutex.has_units() == false);
    EXPECT_EQ(
      mutex.get_blocking_checkpoint().value().line.filename,
      ss::sstring("checkpoint_mutex_test.cc"));
    EXPECT_EQ(mutex.get_blocking_checkpoint().value().line.line, line.line + 1);

    auto units2 = mutex.get_units(); // async

    // Break the mutex.
    mutex.broken();
    EXPECT_TRUE(mutex.has_units() == false);

    bool thrown = false;
    try {
        std::move(units2).get();
    } catch (const ss::broken_semaphore& e) {
        ss::sstring msg = e.what();
        // Message should contain the name of the checkpoint that held
        // the units when the semaphore was broken.
        EXPECT_TRUE(msg.find("checkpoint_mutex_test.cc") != ss::sstring::npos);
        thrown = true;
    }
    EXPECT_TRUE(thrown);
}

TEST(CheckpointMutex, checkpoint_mutex_aborted) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    EXPECT_TRUE(mutex.has_units() == true);
    EXPECT_TRUE(mutex.get_blocking_checkpoint() == std::nullopt);

    // Acquire the mutex normally.
    auto line = vlog::file_line::current();
    auto units = mutex.get_units().get();
    EXPECT_TRUE(mutex.has_units() == false);
    EXPECT_EQ(
      mutex.get_blocking_checkpoint().value().line.filename,
      ss::sstring("checkpoint_mutex_test.cc"));
    EXPECT_EQ(mutex.get_blocking_checkpoint().value().line.line, line.line + 1);

    ss::abort_source as;
    auto units2 = mutex.get_units(as); // async

    // Invoke the abort source to simulate an aborted operation.
    as.request_abort();
    EXPECT_TRUE(mutex.has_units() == false);

    bool thrown = false;
    try {
        std::move(units2).get();
    } catch (const ss::semaphore_aborted& e) {
        ss::sstring msg = e.what();
        // Message should contain the name of the checkpoint that held
        // the units
        EXPECT_TRUE(msg.find("checkpoint_mutex_test") != ss::sstring::npos);
        thrown = true;
    }
    EXPECT_TRUE(thrown);

    thrown = false;

    // Check that this works even if we're called with the source which is
    // already aborted.
    auto units3 = mutex.get_units(as); // async

    EXPECT_TRUE(mutex.has_units() == false);

    try {
        std::move(units3).get();
    } catch (const ss::semaphore_aborted& e) {
        ss::sstring msg = e.what();
        EXPECT_TRUE(msg.find("checkpoint_mutex_test") != ss::sstring::npos);
        EXPECT_TRUE(
          msg.find(ssx::sformat("{}", line.line + 1)) != ss::sstring::npos);
        thrown = true;
    }
    EXPECT_TRUE(thrown);
}

TEST(CheckpointMutex, checkpoint_mutex_timed_out) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    EXPECT_TRUE(mutex.has_units() == true);
    EXPECT_TRUE(mutex.get_blocking_checkpoint() == std::nullopt);

    // Acquire the mutex normally.
    auto line = vlog::file_line::current();
    auto units = mutex.get_units().get();
    EXPECT_TRUE(mutex.has_units() == false);
    EXPECT_EQ(
      mutex.get_blocking_checkpoint().value().line.filename,
      ss::sstring("checkpoint_mutex_test.cc"));
    EXPECT_EQ(mutex.get_blocking_checkpoint().value().line.line, line.line + 1);

    // Check with the deadline
    auto deadline = std::chrono::steady_clock::now() + 1ms;
    auto units2 = mutex.get_units(deadline); // async

    // Sleep long enough to ensure that the timeout occurs.
    ss::sleep(100ms).get();
    EXPECT_TRUE(mutex.has_units() == false);

    bool thrown = false;
    try {
        std::move(units2).get();
    } catch (const ss::semaphore_timed_out& e) {
        ss::sstring msg = e.what();
        EXPECT_TRUE(msg.find("checkpoint_mutex_test.cc") != ss::sstring::npos);
        EXPECT_TRUE(
          msg.find(ssx::sformat("{}", line.line + 1)) != ss::sstring::npos);
        thrown = true;
    }
    EXPECT_TRUE(thrown);

    // Check with the timeout
    thrown = false;
    auto units3 = mutex.get_units(10ms); // async

    // Sleep long enough to ensure that the timeout occurs.
    ss::sleep(100ms).get();
    EXPECT_TRUE(mutex.has_units() == false);

    try {
        std::move(units3).get();
    } catch (const ss::semaphore_timed_out& e) {
        ss::sstring msg = e.what();
        EXPECT_TRUE(msg.find("checkpoint_mutex_test.cc") != ss::sstring::npos);
        EXPECT_TRUE(
          msg.find(ssx::sformat("{}", line.line + 1)) != ss::sstring::npos);
        thrown = true;
    }
    EXPECT_TRUE(thrown);
}

TEST(CheckpointMutex, checkpoint_mutex_lock_with) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");

    // Acquire the mutex normally.
    auto res = mutex
                 .with([&] {
                     return ss::sleep(500ms).then([&] {
                         EXPECT_FALSE(mutex.has_units());
                         return 1;
                     });
                 })
                 .get();
    EXPECT_TRUE(mutex.has_units());
    EXPECT_EQ(res, 1);

    res = mutex
            .with(
              1s,
              [&] {
                  return ss::sleep(500ms).then([&] {
                      EXPECT_FALSE(mutex.has_units());
                      return 2;
                  });
              })
            .get();
    EXPECT_TRUE(mutex.has_units());
    EXPECT_EQ(res, 2);

    res = mutex
            .with(
              ss::timer<>::clock::now() + 1s,
              [&] {
                  return ss::sleep(500ms).then([&] {
                      EXPECT_FALSE(mutex.has_units());
                      return 3;
                  });
              })
            .get();
    EXPECT_TRUE(mutex.has_units());
    EXPECT_EQ(res, 3);

    ss::abort_source as;
    res = mutex
            .with(
              as,
              [&] {
                  return ss::sleep(500ms).then([&] {
                      EXPECT_FALSE(mutex.has_units());
                      return 4;
                  });
              })
            .get();
    EXPECT_TRUE(mutex.has_units());
    EXPECT_EQ(res, 4);
}
