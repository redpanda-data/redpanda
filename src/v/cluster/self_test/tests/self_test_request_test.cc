// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "cluster/self_test_rpc_types.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_make_netcheck_request) {
    static const auto check_frags =
      [](iobuf::const_iterator begin, iobuf::const_iterator end, size_t sz) {
          vassert(
            std::distance(begin, end) >= 2,
            "check frags verifies constant fragment legnth up until final "
            "fragment in iobuf");
          /// Its expected for the final fragment to have a size < 'sz'
          const auto final_equiv_frag = --(--end);
          for (; begin != final_equiv_frag; ++begin) {
              if (begin->size() != sz) {
                  return false;
              }
          }
          return true;
      };

    auto req = cluster::make_netcheck_request(model::node_id{0}, 52).get0().buf;
    BOOST_CHECK_EQUAL(req.size_bytes(), 52);
    BOOST_CHECK_EQUAL(std::distance(req.cbegin(), req.cend()), 1);
    req = cluster::make_netcheck_request(model::node_id{0}, 8192).get0().buf;
    BOOST_CHECK_EQUAL(req.size_bytes(), 8192);
    BOOST_CHECK_EQUAL(std::distance(req.cbegin(), req.cend()), 1);
    req = cluster::make_netcheck_request(model::node_id{0}, 8193).get0().buf;
    BOOST_CHECK_EQUAL(req.size_bytes(), 8193);
    BOOST_CHECK_EQUAL(std::distance(req.cbegin(), req.cend()), 2);
    /// Verify the first of 2 fragments has the expected size of 8192 while
    /// remainder leftover size must be in second fragment
    BOOST_CHECK_EQUAL(req.cbegin()->size(), 8192);
    BOOST_CHECK_EQUAL((++req.cbegin())->size(), 1);
    const auto size = ((8192 * 52) + 100);
    req = cluster::make_netcheck_request(model::node_id{0}, size).get0().buf;
    BOOST_CHECK_EQUAL(req.size_bytes(), size);
    BOOST_CHECK_EQUAL(std::distance(req.cbegin(), req.cend()), 53);
    BOOST_CHECK(check_frags(req.cbegin(), req.cend(), 8192));
    BOOST_CHECK_EQUAL((--req.cend())->size(), 100);
}
