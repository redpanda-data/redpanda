/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/sloth_mail/tests/kinds.h"

#include <gtest/gtest.h>

#include <variant>

using namespace cluster::sloth_mail;
using namespace cluster::sloth_mail::tests;

struct serde_test_fixture : public ::testing::Test {
    // test compatibility if a new mail kind is added
    using old_kinds = std::variant<kinds::test_kind_0>;
    using new_kinds = std::variant<kinds::test_kind_0, kinds::test_kind_1>;

    template<typename Kind>
    using old_pairs_vector_of_kind
      = impl::types<old_kinds>::template pairs_vector_of_kind<Kind>;

    template<typename Kind>
    using new_pairs_vector_of_kind
      = impl::types<new_kinds>::template pairs_vector_of_kind<Kind>;

    static_assert(std::is_same_v<
                  old_pairs_vector_of_kind<kinds::test_kind_0>,
                  new_pairs_vector_of_kind<kinds::test_kind_0>>);

    using old_mail_request = impl::types<old_kinds>::mail_request;
    using new_mail_request = impl::types<new_kinds>::mail_request;

    void assert_old_eq_new(
      const old_mail_request& old_req, const new_mail_request& new_req) {
        // only test_kind_0 supported
        GTEST_ASSERT_EQ(old_req.data.size(), 1);
        GTEST_ASSERT_EQ(new_req.data.size(), 1);
        const auto& old_entry = *old_req.data.begin();
        const auto& new_entry = *new_req.data.begin();
        GTEST_ASSERT_EQ(old_entry.first, kinds::test_kind_0::id);
        GTEST_ASSERT_EQ(new_entry.first, kinds::test_kind_0::id);
        const auto& old_data = old_entry.second;
        const auto& new_data = new_entry.second;
        GTEST_ASSERT_TRUE(
          std::holds_alternative<old_pairs_vector_of_kind<kinds::test_kind_0>>(
            old_data));
        GTEST_ASSERT_TRUE(
          std::holds_alternative<new_pairs_vector_of_kind<kinds::test_kind_0>>(
            new_data));
        GTEST_ASSERT_EQ(
          std::get<old_pairs_vector_of_kind<kinds::test_kind_0>>(old_data),
          std::get<new_pairs_vector_of_kind<kinds::test_kind_0>>(new_data));
    }

    old_mail_request make_old_request() {
        old_mail_request req;
        req.data.emplace(
          kinds::test_kind_0::id,
          old_pairs_vector_of_kind<kinds::test_kind_0>{
            {.key = {.key = 1}, .value = {.value = "v1"}},
            {.key = {.key = 2}, .value = {.value = "v2"}}});
        return req;
    }

    new_mail_request make_new_request_wo_new_kind() {
        new_mail_request req;
        req.data.emplace(
          kinds::test_kind_0::id,
          new_pairs_vector_of_kind<kinds::test_kind_0>{
            {.key = {.key = 1}, .value = {.value = "v1"}},
            {.key = {.key = 2}, .value = {.value = "v2"}}});
        return req;
    }

    new_mail_request make_new_request_w_new_kind() {
        new_mail_request req;
        req.data.emplace(
          kinds::test_kind_0::id,
          new_pairs_vector_of_kind<kinds::test_kind_0>{
            {.key = {.key = 1}, .value = {.value = "v1"}},
            {.key = {.key = 2}, .value = {.value = "v2"}}});
        req.data.emplace(
          kinds::test_kind_1::id,
          new_pairs_vector_of_kind<kinds::test_kind_1>{
            {.key = {.key = "k1"}, .value = {.value = 1}},
            {.key = {.key = "k2"}, .value = {.value = 2}}});
        return req;
    }
};

TEST_F(serde_test_fixture, old_to_new) {
    auto roundtripped = serde::from_iobuf<new_mail_request>(
      serde::to_iobuf(make_old_request()));
    assert_old_eq_new(make_old_request(), roundtripped);
}

TEST_F(serde_test_fixture, new_to_old) {
    auto roundtripped = serde::from_iobuf<old_mail_request>(
      serde::to_iobuf(make_new_request_wo_new_kind()));
    assert_old_eq_new(roundtripped, make_new_request_wo_new_kind());
}

TEST_F(serde_test_fixture, new_both_kinds) {
    auto roundtripped = serde::from_iobuf<new_mail_request>(
      serde::to_iobuf(make_new_request_w_new_kind()));
    GTEST_ASSERT_TRUE(roundtripped == make_new_request_w_new_kind());
}
