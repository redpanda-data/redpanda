// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/snc_quota_manager.h"

#include <boost/test/unit_test.hpp>

#include <random>

BOOST_AUTO_TEST_CASE(cap_to_ceiling_test) {
    using namespace kafka::detail;
    int64_t v;
    BOOST_CHECK_EQUAL(cap_to_ceiling(v = 0, 0), 0);
    BOOST_CHECK_EQUAL(v, 0);
    BOOST_CHECK_EQUAL(cap_to_ceiling(v = 100, 0), 0);
    BOOST_CHECK_EQUAL(v, 100);
    BOOST_CHECK_EQUAL(cap_to_ceiling(v = 0, 110), 110);
    BOOST_CHECK_EQUAL(v, 110);
    BOOST_CHECK_EQUAL(cap_to_ceiling(v = -53, 0), 53);
    BOOST_CHECK_EQUAL(v, 0);
    BOOST_CHECK_EQUAL(
      cap_to_ceiling(v = -4'110'014, -(-10'725'196)), 10'725'196 + 4'110'014);
    BOOST_CHECK_EQUAL(v, 10'725'196);
}

BOOST_AUTO_TEST_CASE(dispense_negative_deltas_test) {
    using namespace kafka::detail;
    using quota_vec = std::vector<kafka::snc_quota_manager::quota_t>;
    {
        quota_vec schedule{0, 0};
        dispense_negative_deltas(
          schedule, -7'436'386'678, quota_vec{8'000'000'000, 8'000'000'000});
        BOOST_CHECK_EQUAL(
          schedule, (quota_vec{-3'718'193'339, -3'718'193'339}));
    }
    {
        quota_vec schedule{0, 0};
        dispense_negative_deltas(
          schedule, -247'128'089, quota_vec{122'474'653, 124'653'436});
        BOOST_CHECK_EQUAL(schedule, (quota_vec{-122'474'653, -124'653'436}));
    }
}

BOOST_AUTO_TEST_CASE(dispense_equally_test) {
    using namespace kafka::detail;
    using quota_t = kafka::snc_quota_manager::quota_t;
    using quota_vec = std::vector<quota_t>;

    // TBD: add random seed
    std::default_random_engine reng(1);
    std::uniform_int_distribution<quota_t> dist(
      bottomless_token_bucket::min_quota, bottomless_token_bucket::max_quota);

    for (size_t shards_count = 1; shards_count != 65; ++shards_count)
        for (size_t k = 0; k != 1000; ++k) {
            quota_vec schedule(shards_count, quota_t{0});
            quota_t delta;
            if (k == 0) {
                delta = 1070810392239;
            } else {
                delta = dist(reng);
            }
            dispense_equally(schedule, delta);
            const auto schedule_total = std::reduce(
              schedule.cbegin(), schedule.cend(), quota_t{0}, std::plus{});
            BOOST_CHECK_EQUAL(schedule_total, delta);
        }
}
