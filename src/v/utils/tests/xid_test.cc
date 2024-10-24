// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "serde/rw/rw.h"
#include "test_utils/test.h"
#include "utils/xid.h"

#include <absl/container/node_hash_map.h>
#include <gtest/gtest.h>

xid random_xid() {
    static std::uniform_int_distribution<int> rand_fill('@', '~');
    xid::data_t array;
    memset(
      array.data(), rand_fill(random_generators::internal::gen), array.size());

    return xid(array);
}

TEST(xid, string_formatting_test) {
    for (int i = 0; i < 1000; ++i) {
        auto test_xid = random_xid();
        ASSERT_EQ(
          boost::lexical_cast<xid>(fmt::format("{}", test_xid)), test_xid);
    }
}

TEST(xid, hashing_test) {
    absl::node_hash_map<xid, int> map;
    for (int i = 0; i < 100; ++i) {
        auto r_xid = random_xid();
        map[r_xid] = i;
        ASSERT_EQ(map[r_xid], i);
    }
}

TEST(xid, string_round_trip_test) {
    for (int i = 0; i < 100; ++i) {
        auto test_xid = random_xid();
        ASSERT_EQ(xid::from_string(fmt::format("{}", test_xid)), test_xid);
    }
}

TEST(xid, serde_round_trip_test) {
    for (int i = 0; i < 1000; ++i) {
        auto test_xid = random_xid();
        ASSERT_EQ(serde::from_iobuf<xid>(serde::to_iobuf(test_xid)), test_xid);
    }
}

TEST(xid, test_xid_string_validation) {
    // invalid length 21 characters
    ASSERT_THROW(xid::from_string("ccc0a2mn6i1e6brmdbip0"), invalid_xid);
    // invalid length 19 characters
    ASSERT_THROW(xid::from_string("c0a2mn6i1e6brmdbip0"), invalid_xid);
    // invalid value of last character
    ASSERT_THROW(xid::from_string("cc0a2mn6i1e6brmdbipa"), invalid_xid);
}
