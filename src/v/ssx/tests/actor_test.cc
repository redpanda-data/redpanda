// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/actor.h"
#include "test_utils/async.h"

#include <seastar/core/future.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

namespace ssx {

using ::testing::ElementsAre;

namespace {

// Test actor that records messages it processes
template<size_t MaxSize, overflow_policy Policy = overflow_policy::block>
class test_actor : public actor<int, MaxSize, Policy> {
public:
    ss::future<> process(int msg) override {
        processed.push_back(msg);
        co_return;
    }

    void on_error(std::exception_ptr) noexcept override {}

    std::vector<int> processed;
};

// Test actor that can throw errors and tracks them
template<size_t MaxSize>
class throwing_actor : public actor<int, MaxSize> {
public:
    ss::future<> process(int msg) override {
        processed.push_back(msg);
        if (msg < 0) {
            throw std::runtime_error("negative value");
        }
        co_return;
    }

    void on_error(std::exception_ptr ex) noexcept override {
        errors.push_back(ex);
    }

    std::vector<int> processed;
    std::vector<std::exception_ptr> errors;
};

} // namespace

TEST(Actor, ProcessesMessagesInOrder) {
    test_actor<10> actor;
    actor.start().get();

    actor.tell(1).get();
    actor.tell(2).get();
    actor.tell(3).get();

    tests::drain_task_queue().get();

    EXPECT_THAT(actor.processed, ElementsAre(1, 2, 3));
    actor.stop().get();
}

TEST(Actor, ErrorsDoNotStopProcessing) {
    throwing_actor<10> actor;
    actor.start().get();

    actor.tell(1).get();
    actor.tell(-1).get(); // This will throw
    actor.tell(2).get();

    tests::drain_task_queue().get();

    EXPECT_THAT(actor.processed, ElementsAre(1, -1, 2));
    EXPECT_EQ(actor.errors.size(), 1);
    actor.stop().get();
}

TEST(Actor, DropOldestPolicy) {
    test_actor<2, overflow_policy::drop_oldest> actor;
    actor.start().get();

    // With drop_oldest, tell() never blocks, so we can fill the queue
    // and subsequent tells will drop the oldest messages
    actor.tell(1);
    actor.tell(2);
    // Queue is now full, next tell should drop oldest
    actor.tell(3); // Drops 1, adds 3
    actor.tell(4); // Drops 2, adds 4

    tests::drain_task_queue().get();

    // Should have processed 3 and 4 (1 and 2 were dropped)
    EXPECT_THAT(actor.processed, ElementsAre(3, 4));
    actor.stop().get();
}

// Test actor with slow processing to create contention scenarios
template<size_t MaxSize>
class slow_actor : public actor<int, MaxSize> {
public:
    explicit slow_actor(ss::semaphore& sem)
      : _processing_sem(sem) {}

    ss::future<> process(int msg) override {
        processed.push_back(msg);
        // Wait on semaphore to simulate slow processing
        co_await _processing_sem.wait();
        co_return;
    }

    void on_error(std::exception_ptr) noexcept override {}

    std::vector<int> processed;

private:
    ss::semaphore& _processing_sem;
};

TEST(Actor, ConcurrentTellsWithFullMailbox) {
    // Test that multiple concurrent tell() calls waiting on a full mailbox
    // all successfully send when space becomes available. This tests that
    // broadcast() is used instead of signal() to wake all waiters.
    ss::semaphore processing_sem{0};
    slow_actor<2> actor{processing_sem};
    actor.start().get();

    // Fill the mailbox
    actor.tell(1).get();
    actor.tell(2).get();

    // Start multiple concurrent tell() calls that will block
    auto tell_fut1 = actor.tell(3);
    auto tell_fut2 = actor.tell(4);
    auto tell_fut3 = actor.tell(5);

    // Yield to let the tell() calls reach the wait point
    ss::sleep(std::chrono::milliseconds(10)).get();

    // Allow processing to drain the queue
    processing_sem.signal(5);
    tests::drain_task_queue().get();

    // All tell() futures should complete
    tell_fut1.get();
    tell_fut2.get();
    tell_fut3.get();

    // All messages should be processed
    EXPECT_THAT(actor.processed, ElementsAre(1, 2, 3, 4, 5));
    actor.stop().get();
}

TEST(Actor, NoMessageLossUnderContention) {
    // Test that no messages are lost when many concurrent tell() calls
    // are made while the mailbox is under contention. This tests both
    // the broadcast() change and the while-loop predicate check.
    ss::semaphore processing_sem{0};
    slow_actor<3> actor{processing_sem};
    actor.start().get();

    constexpr int num_messages = 20;
    std::vector<ss::future<>> tell_futures;

    // Send messages concurrently
    for (int i = 0; i < num_messages; ++i) {
        tell_futures.push_back(actor.tell(i));
        // Add some yield points to create contention
        if (i % 5 == 0) {
            ss::sleep(std::chrono::milliseconds(1)).get();
        }
    }

    // Allow all messages to be processed
    processing_sem.signal(num_messages);

    // Wait for all tell() calls to complete
    ss::when_all_succeed(tell_futures.begin(), tell_futures.end()).get();
    tests::drain_task_queue().get();

    // Verify all messages were processed (order may vary for concurrent sends)
    EXPECT_THAT(
      actor.processed,
      ::testing::UnorderedElementsAre(
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
    actor.stop().get();
}

} // namespace ssx
