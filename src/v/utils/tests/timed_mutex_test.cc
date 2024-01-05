#include "base/seastarx.h"
#include "ssx/semaphore.h"
#include "utils/timed_mutex.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

/*
 * The test spawns several fibers incrementing a shared variable, the increment
 * reads a value, sleep(1s) and writes value+1. The long pause between the &
 * the write "guarantees" that the fibers interleave causing a lost update
 * anomaly.
 *
 * Some of the interleaving fibers overwrite a result of another leading to
 * the final value of the variable be less than expected:
 *
 *   v1 = read();
 *   v2 = read(); <- interleaving
 *   write(v1+1);
 *   write(v2+1); <- overwrites the first fiber
 */

seastar::future<> slow(timed_mutex& mutex, int& counter) {
    return mutex.lock("slow").then([&counter](locked_token t) {
        int read = counter;
        return seastar::sleep(std::chrono::seconds(1))
          .then([&counter, read] { counter = read + 1; })
          .then([t = std::move(t)] {});
    });
}

seastar::future<>
concurrent_increment(int num_fibers, int& variable, timed_mutex& mutex) {
    return seastar::do_with(
      ssx::semaphore(0, "test/concurrent-incr"),
      [&mutex, &variable, num_fibers](auto& sem) {
          for (int i = 0; i < num_fibers; i++) {
              (void)slow(mutex, variable).then([&sem] { sem.signal(1); });
          }
          return sem.wait(num_fibers);
      });
}

SEASTAR_THREAD_TEST_CASE(test_timed_mutex_counts_locks) {
    int num_fibers = 5;
    int variable = 0;
    timed_mutex mutex(true);
    concurrent_increment(num_fibers, variable, mutex).get();
    BOOST_REQUIRE_EQUAL(mutex.get_lock_counter(), num_fibers);
    BOOST_REQUIRE_EQUAL(variable, num_fibers);
}

SEASTAR_THREAD_TEST_CASE(test_timed_mutex_doesnt_count_locks) {
    int num_fibers = 5;
    int variable = 0;
    timed_mutex mutex(false);
    concurrent_increment(num_fibers, variable, mutex).get();
    BOOST_REQUIRE_EQUAL(mutex.get_lock_counter(), 0);
    BOOST_REQUIRE_EQUAL(variable, num_fibers);
}
