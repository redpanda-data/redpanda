/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/quota_manager.h"
#include <boost/test/unit_test.hpp>


BOOST_AUTO_TEST_CASE(test_inoutpair) {
  using namespace kafka;
  inoutpair<int> a {{ 10, 12 }};
  const inoutpair<int> b {{ 20, 21 }};

  auto r = to_each(std::plus{}, a, b);
  BOOST_CHECK_EQUAL(r.in(), 30);
  BOOST_CHECK_EQUAL(r.out(), 33);

  inoutpair<std::string> c {{ "30ms", "too long" }};
  inoutpair<bool> done {{ false, false }};

  auto q = to_each( [](int x, int y, std::string z, bool& done) {
    done = true;
    return fmt::format("From {} to {} it takes {}", x, y, z);
  }, a, b, c, done);
  BOOST_CHECK_EQUAL(q.in(), "From 10 to 20 it takes 30ms");
  BOOST_CHECK_EQUAL(q.out(), "From 12 to 21 it takes too long");
  BOOST_CHECK(done.in());
  BOOST_CHECK(done.out());

  struct S {
     double d; 
     struct Sin { 
        std::string s; 
        Sin(const char* sc):s(sc){}
        Sin() = delete;
        Sin(const Sin&) = delete;
        Sin& operator=(const Sin&) = delete;
        Sin(Sin&&) = default;
        Sin& operator=(Sin&&) = default;
     };
     Sin sin;
  };
  const inoutpair<S> s {{ S{3.5, "three and a half"}, S{3.142, "π"} }};
  auto sprime = to_each ( [](const S& s) { return s.d > 3.3; }, s );
  BOOST_CHECK_EQUAL(sprime.in(),true);
  BOOST_CHECK_EQUAL(sprime.out(),false);

  const std::vector<inoutpair<int>> vecp { {{3,4}}, {{18,19}}, {{33,32}} };
  const auto pvec = to_inside_out(vecp);
  BOOST_CHECK_EQUAL(pvec.in(), (std::vector<int>{3,18,33}) );
  BOOST_CHECK_EQUAL(pvec.out(), (std::vector<int>{4,19,32}) );
}
