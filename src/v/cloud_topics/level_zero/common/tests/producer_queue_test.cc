/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/common/producer_queue.h"
#include "model/record.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <map>

using namespace cloud_topics::l0;

TEST_CORO(producer_queue_test, basic_reserve_redeem_release) {
    producer_queue queue;
    model::producer_id pid{1};

    auto ticket = queue.reserve(pid);

    // First ticket should redeem immediately
    co_await ticket.redeem();

    // Release the ticket
    ticket.release();
}

TEST_CORO(producer_queue_test, ordering_enforced) {
    producer_queue queue;
    model::producer_id pid{1};

    // Reserve two tickets for the same producer
    auto ticket1 = queue.reserve(pid);
    auto ticket2 = queue.reserve(pid);

    // First ticket redeems immediately
    co_await ticket1.redeem();

    // Second ticket should not redeem until first is released
    bool ticket2_redeemed = false;
    auto ticket2_fut = ticket2.redeem().then(
      [&ticket2_redeemed] { ticket2_redeemed = true; });

    // Give it a moment to potentially redeem (it shouldn't)
    co_await ss::sleep(std::chrono::milliseconds(10));
    ASSERT_FALSE_CORO(ticket2_redeemed);

    // Release ticket1, which should allow ticket2 to redeem
    ticket1.release();

    co_await std::move(ticket2_fut);
    ASSERT_TRUE_CORO(ticket2_redeemed);

    ticket2.release();
}

TEST_CORO(producer_queue_test, no_producer_id_is_noop) {
    producer_queue queue;

    // Reserve multiple tickets with no_producer_id
    auto ticket1 = queue.reserve(model::no_producer_id);
    auto ticket2 = queue.reserve(model::no_producer_id);
    auto ticket3 = queue.reserve(model::no_producer_id);

    // All should redeem immediately without waiting
    co_await ticket1.redeem();
    co_await ticket2.redeem();
    co_await ticket3.redeem();

    // Release is also a noop
    ticket1.release();
    ticket2.release();
    ticket3.release();
}

TEST_CORO(producer_queue_test, multiple_producers_no_cross_blocking) {
    producer_queue queue;
    model::producer_id pid1{1};
    model::producer_id pid2{2};

    // Reserve tickets for two different producers
    auto ticket1_p1 = queue.reserve(pid1);
    auto ticket1_p2 = queue.reserve(pid2);

    // Both should redeem immediately (different producers)
    co_await ticket1_p1.redeem();
    co_await ticket1_p2.redeem();

    // Reserve second tickets while first are still held
    auto ticket2_p1 = queue.reserve(pid1);
    auto ticket2_p2 = queue.reserve(pid2);

    // Neither second ticket should be redeemable yet
    bool ticket2_p1_redeemed = false;
    bool ticket2_p2_redeemed = false;

    auto fut_p1 = ticket2_p1.redeem().then(
      [&ticket2_p1_redeemed] { ticket2_p1_redeemed = true; });
    auto fut_p2 = ticket2_p2.redeem().then(
      [&ticket2_p2_redeemed] { ticket2_p2_redeemed = true; });

    co_await ss::sleep(std::chrono::milliseconds(10));
    ASSERT_FALSE_CORO(ticket2_p1_redeemed);
    ASSERT_FALSE_CORO(ticket2_p2_redeemed);

    // Release producer 1's first ticket
    ticket1_p1.release();
    co_await std::move(fut_p1);
    ASSERT_TRUE_CORO(ticket2_p1_redeemed);

    // Producer 2's second ticket should still be blocked
    ASSERT_FALSE_CORO(ticket2_p2_redeemed);

    // Release producer 2's first ticket
    ticket1_p2.release();
    co_await std::move(fut_p2);
    ASSERT_TRUE_CORO(ticket2_p2_redeemed);

    ticket2_p1.release();
    ticket2_p2.release();
}

TEST_CORO(producer_queue_test, auto_release_on_destruction) {
    producer_queue queue;
    model::producer_id pid{1};

    auto ticket1 = queue.reserve(pid);
    auto ticket2 = queue.reserve(pid);
    auto ticket3 = queue.reserve(pid);

    {
        // an exception or some error happends and ticket2 is released early
        auto temp = std::move(ticket2);
    }

    // but that should not make ticket3 available because ticket1 is still
    // not yet redeemed.
    auto ticket3_redeem = ticket3.redeem();
    co_await tests::drain_task_queue();
    ASSERT_FALSE_CORO(ticket3_redeem.available());

    co_await ticket1.redeem();
    ticket1.release();
    co_await std::move(ticket3_redeem);
    ticket3.release();
}

TEST_CORO(producer_queue_test, continuation_cleanup) {
    producer_queue queue;
    model::producer_id pid{1};
    auto ticket1 = queue.reserve(pid);
    auto ticket2 = queue.reserve(pid);
    {
        auto temp = std::move(ticket2);
    }
    ticket1.release();
    co_return;
}

TEST_CORO(producer_queue_test, redeem_with_abort_source) {
    producer_queue queue;
    model::producer_id pid{1};

    // Test 1: Abort while waiting for previous ticket
    auto ticket1 = queue.reserve(pid);
    auto ticket2 = queue.reserve(pid);

    co_await ticket1.redeem();

    ss::abort_source as;
    auto redeem_fut = ticket2.redeem(as);

    // Give it a moment to start waiting
    co_await ss::sleep(std::chrono::milliseconds(10));

    // Trigger abort while ticket2 is waiting
    as.request_abort();

    // Should get an exception
    ASSERT_THROW_CORO(
      co_await std::move(redeem_fut), ss::abort_requested_exception);

    // ticket2 should have auto-released due to abort
    ticket1.release();

    // Test 2: Successful redeem followed by abort (should be no-op)
    auto ticket3 = queue.reserve(pid);
    ss::abort_source as2;
    co_await ticket3.redeem(as2);

    // Abort after successful redeem - should not affect anything
    as2.request_abort();
    ticket3.release();

    ASSERT_EQ_CORO(queue.size(), 0);
}

namespace {

ss::future<> random_sleep() {
    auto sleep_ms = std::chrono::milliseconds(random_generators::get_int(5));
    return ss::sleep(sleep_ms);
}

} // namespace

TEST_CORO(producer_queue_test, concurrent_operations) {
    producer_queue queue;
    model::producer_id pid1{1};
    model::producer_id pid2{2};
    model::producer_id pid3{3};

    // Track completion order per producer
    using completion_order_t = std::map<model::producer_id, std::vector<int>>;
    auto completion_order = ss::make_lw_shared<completion_order_t>();

    std::vector<ss::future<>> operations;

    // Spawn concurrent operations for multiple producers
    constexpr int requests_per_producer = 50;
    for (int i = 0; i < requests_per_producer; ++i) {
        for (auto pid : {pid1, pid2, pid3}) {
            operations.push_back(
              ss::do_with(
                queue.reserve(pid),
                i,
                completion_order,
                [pid](auto& ticket, int request_id, auto& order) {
                    return ticket.redeem().then(
                      [&ticket, request_id, &order, pid] {
                          return random_sleep().then(
                            [&ticket, request_id, &order, pid] {
                                (*order)[pid].push_back(request_id);
                                ticket.release();
                            });
                      });
                }));
        }
    }

    co_await ss::when_all_succeed(operations.begin(), operations.end());

    // Verify each producer saw monotonically increasing request IDs
    for (const auto& [pid, requests] : *completion_order) {
        ASSERT_EQ_CORO(requests.size(), requests_per_producer);
        for (size_t i = 0; i < requests.size(); ++i) {
            ASSERT_EQ_CORO(requests[i], static_cast<int>(i));
        }
    }
    ASSERT_EQ_CORO(queue.size(), 0);
}

TEST_CORO(producer_queue_test, concurrent_operations_with_drops) {
    producer_queue queue;
    model::producer_id pid1{1};
    model::producer_id pid2{2};
    model::producer_id pid3{3};

    // Track completion order per producer
    using completion_order_t = std::map<model::producer_id, std::vector<int>>;
    auto completion_order = ss::make_lw_shared<completion_order_t>();

    std::vector<ss::future<>> operations;

    // Spawn concurrent operations for multiple producers
    constexpr int requests_per_producer = 500;
    for (int i = 0; i < requests_per_producer; ++i) {
        for (auto pid : {pid1, pid2, pid3}) {
            operations.push_back(
              ss::do_with(
                queue.reserve(pid),
                i,
                completion_order,
                ss::abort_source{},
                [pid](auto& ticket, int request_id, auto& order, auto& as) {
                    auto n = random_generators::get_int(0, 5);
                    if (n == 0) {
                        // Explicit release
                        return random_sleep().then(
                          [&ticket] { ticket.release(); });
                    } else if (n == 1) {
                        // No release, just destructor
                        return random_sleep();
                    } else if (n == 2) {
                        // Abort before redeem completes
                        auto redeem_fut = ticket.redeem(as);
                        as.request_abort();
                        return std::move(redeem_fut)
                          .then([&ticket, request_id, &order, pid] {
                              (*order)[pid].push_back(request_id);
                              ticket.release();
                          })
                          .handle_exception([](auto) {
                              // Abort exception is expected, ignore
                          });
                    } else if (n == 3) {
                        // Random abort during operation
                        auto redeem_fut = ticket.redeem(as);
                        return random_sleep().then(
                          [&as,
                           redeem_fut = std::move(redeem_fut),
                           &ticket,
                           request_id,
                           &order,
                           pid]() mutable {
                              // Maybe abort, maybe not
                              if (random_generators::get_int(0, 1) == 0) {
                                  as.request_abort();
                              }
                              return std::move(redeem_fut)
                                .then([&ticket, request_id, &order, pid] {
                                    return random_sleep().then(
                                      [&ticket, request_id, &order, pid] {
                                          (*order)[pid].push_back(request_id);
                                          ticket.release();
                                      });
                                })
                                .handle_exception([](auto) {});
                          });
                    } else {
                        // Happy path with abort source (but no abort)
                        return ticket.redeem(as).then(
                          [&ticket, request_id, &order, pid] {
                              // Add random sleep to encourage interleaving
                              return random_sleep().then(
                                [&ticket, request_id, &order, pid] {
                                    (*order)[pid].push_back(request_id);
                                    ticket.release();
                                });
                          });
                    }
                }));
        }
    }

    co_await ss::when_all_succeed(operations.begin(), operations.end());

    // Verify each producer saw monotonically increasing request IDs
    for (const auto& [pid, requests] : *completion_order) {
        ASSERT_TRUE_CORO(std::ranges::is_sorted(requests))
          << fmt::format("pid={}, requests={}", pid, requests);
    }
    ASSERT_EQ_CORO(queue.size(), 0);
}
