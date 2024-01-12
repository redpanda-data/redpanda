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
#include "utils/xid.h"

#include <absl/container/node_hash_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

xid random_xid() {
    static std::uniform_int_distribution<int> rand_fill('@', '~');
    xid::data_t array;
    memset(
      array.data(), rand_fill(random_generators::internal::gen), array.size());

    return xid(array);
}

BOOST_AUTO_TEST_CASE(string_formatting_test) {
    for (int i = 0; i < 1000; ++i) {
        auto test_xid = random_xid();
        BOOST_REQUIRE_EQUAL(
          boost::lexical_cast<xid>(fmt::format("{}", test_xid)), test_xid);
    }
}

BOOST_AUTO_TEST_CASE(serde_rt_test) {
    for (int i = 0; i < 1000; ++i) {
        auto test_xid = random_xid();
        BOOST_REQUIRE_EQUAL(
          serde::from_iobuf<xid>(serde::to_iobuf(test_xid)), test_xid);
    }
}

BOOST_AUTO_TEST_CASE(hasing_test) { absl::node_hash_map<xid, int> map; }
