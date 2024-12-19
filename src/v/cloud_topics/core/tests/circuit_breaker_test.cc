/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/core/circuit_breaker.h"
#include "test_utils/test.h"

#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

using namespace experimental::cloud_topics;
using namespace std::chrono_literals;

TEST(CircuitBreaker, open_closed_half_open_open) {
    // Start with open CB.
    // Register errors and transition to closed CB.
    // Wait cooldown timeout and transition to halfopen state.
    // Wait another cooldown timeout and transition to open state.
    const size_t error_capacity = 10;
    const auto cooldown = 10s;
    core::circuit_breaker<ss::manual_clock> cb(error_capacity, cooldown);

    // Check that state is 'open' initially
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);

    // Check that state is 'open' if errors are below the threshold
    cb.register_error();
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);

    // Trigger transition to the closed state
    for (size_t i = 0; i < error_capacity; i++) {
        cb.register_error();
    }
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::closed);

    // Check that the state goes to half open after cooldown
    ss::manual_clock::advance(cooldown);
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::half_open);

    // Check that the state transitions back to open
    ss::manual_clock::advance(cooldown);
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);
}

TEST(CircuitBreaker, open_closed_half_open_closed) {
    // Start with open CB.
    // Register errors and transition to closed CB.
    // Wait cooldown timeout and transition to halfopen state.
    // Register error and transition to closed CB state.
    const size_t error_capacity = 10;
    const auto cooldown = 10s;
    core::circuit_breaker<ss::manual_clock> cb(error_capacity, cooldown);

    // Trigger transition to the closed state
    for (size_t i = 0; i < error_capacity + 1; i++) {
        cb.register_error();
    }
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::closed);

    // Transition to half open state
    ss::manual_clock::advance(cooldown);
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::half_open);

    cb.register_error();
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::closed);

    // Check that the state transitions back to open
    ss::manual_clock::advance(cooldown);
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::half_open);
}

TEST(CircuitBreaker, open_closed_open) {
    // Start with open CB.
    // Register errors and transition to closed CB.
    // Wait two cooldown intervals and transition to open state.
    const size_t error_capacity = 10;
    const auto cooldown = 10s;
    core::circuit_breaker<ss::manual_clock> cb(error_capacity, cooldown);

    // Check that state is 'open' initially
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);

    // Check that state is 'open' if errors are below the threshold
    cb.register_error();
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);

    // Trigger transition to the closed state
    for (size_t i = 0; i < error_capacity; i++) {
        cb.register_error();
    }
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::closed);

    // Check that the state goes to open after long cooldown
    ss::manual_clock::advance(2 * cooldown);
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);
}

TEST(CircuitBreaker, error_ttl) {
    // Start with open CB.
    // Register error and wait for cooldown period.
    // Register more errors and make sure that the CB is not closed.
    const size_t error_capacity = 10;
    const auto cooldown = 10s;
    core::circuit_breaker<ss::manual_clock> cb(error_capacity, cooldown);

    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);

    // This shouldn't trigger state transition because we need +1
    // error for this.
    for (size_t i = 0; i < error_capacity; i++) {
        cb.register_error();
    }
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);

    // Expire all registered errors
    ss::manual_clock::advance(cooldown);
    for (size_t i = 0; i < error_capacity; i++) {
        cb.register_error();
    }
    ASSERT_EQ(cb.state(), core::circuit_breaker_state::open);
}
