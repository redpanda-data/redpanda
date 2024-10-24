// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/move_canary.h"

#include <boost/test/unit_test.hpp>

#include <tuple>

struct canary_as_member {
    bool is_moved_from() const { return mc.is_moved_from(); }
    move_canary mc;
};

struct canary_as_base : move_canary {};

using all_canary_types
  = std::tuple<move_canary, canary_as_member, canary_as_base>;

BOOST_AUTO_TEST_CASE_TEMPLATE(move_canary_default_ctor, T, all_canary_types) {
    BOOST_CHECK_EQUAL(T{}.is_moved_from(), false);
    T c;
    BOOST_CHECK_EQUAL(c.is_moved_from(), false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(move_canary_move_ctor, T, all_canary_types) {
    T c0;
    T c1(std::move(c0));

    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK_EQUAL(c0.is_moved_from(), true);
    BOOST_CHECK_EQUAL(c1.is_moved_from(), false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  move_canary_move_assignment, T, all_canary_types) {
    T c0;
    T c1;
    c1 = std::move(c0);

    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK_EQUAL(c0.is_moved_from(), true);
    BOOST_CHECK_EQUAL(c1.is_moved_from(), false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  move_canary_copy_assignment, T, all_canary_types) {
    T c0;
    T c1;
    c1 = c0;

    BOOST_CHECK_EQUAL(c0.is_moved_from(), false);
    BOOST_CHECK_EQUAL(c1.is_moved_from(), false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  move_canary_assignment_is_transitive, T, all_canary_types) {
    T c0, c1, c2;
    c1 = std::move(c0);
    c2 = c0; // NOLINT(bugprone-use-after-move)

    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK_EQUAL(c2.is_moved_from(), true);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  move_canary_moved_to_not_moved, T, all_canary_types) {
    T c0, c1, c2;
    c1 = std::move(c0);

    // NOLINTNEXTLINE(bugprone-use-after-move)
    BOOST_CHECK_EQUAL(c0.is_moved_from(), true); // c0 is moved-from here

    // but assignment from a not moved-from object clears
    // that state
    c0 = c2;
    BOOST_CHECK_EQUAL(c0.is_moved_from(), false);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(move_canary_copy_ctor, T, all_canary_types) {
    T c0, c1;
    c1 = std::move(c0);
    // c0 is moved-from here, c1 is not

    // NOLINTNEXTLINE(bugprone-use-after-move)
    T c2(c0);
    BOOST_CHECK_EQUAL(c2.is_moved_from(), true);

    T c3(c1);
    BOOST_CHECK_EQUAL(c3.is_moved_from(), false);
}

#ifdef NDEBUG
// size tests for release mode

struct canary_4_bytes_base : debug_move_canary {
    int32_t x;
};

static_assert(
  sizeof(canary_4_bytes_base) == sizeof(int32_t), "no size impact as base");

struct canary_4_bytes_noua_member {
    int32_t x;
    [[no_unique_address]] debug_move_canary c;
};

static_assert(
  sizeof(canary_4_bytes_noua_member) == sizeof(int32_t),
  "no size impact as no_unique_address member");
#endif
