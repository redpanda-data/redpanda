// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/boost_hist.h"
#include "utils/hdr_hist.h"

#include <seastar/testing/perf_tests.hh>

template<typename value_type>
class simple_hist final {
public:
    struct bucket {
        value_type le;
        uint64_t count;
    };

    simple_hist(size_t buckets, value_type min) {
        value_type le = min;
        _buckets.reserve(buckets + 1);
        for (int i = 0; i < buckets; ++i) {
            _buckets.push_back(bucket{le, 0});
            le *= 2;
        }
        // Overflow bucket
        _buckets.push_back(bucket{std::numeric_limits<value_type>::max(), 0});
    }

    inline void record(value_type val) noexcept {
        value_type dval = (value_type)val;
        for (size_t i = 0; i < _buckets.size(); ++i) {
            auto& bucket = _buckets[i];
            if (dval <= bucket.le) {
                bucket.count += 1;
                break;
            }
        }
        _sum += val;
        _count += 1;
    }

private:
    std::vector<bucket> _buckets;
    value_type _sum;
    uint64_t _count;
};

PERF_TEST(boost_hist, insert_few) {
    boost_hist h(26, 10, 100);
    perf_tests::start_measuring_time();
    for (int64_t i = 0; i < 100; ++i) {
        h.record(i);
    }

    perf_tests::do_not_optimize(h);
    perf_tests::stop_measuring_time();
}

PERF_TEST(boost_hist, insert_many) {
    boost_hist h(26, 10, 100);
    perf_tests::start_measuring_time();

    // So many inserts we'll cross 8 bit and 16 bit limits
    // for per-bucket counts
    for (int i = 0; i < 17000; ++i) {
        for (int64_t j = 0; j < 100; ++j) {
            h.record(j);
        }
    }

    perf_tests::stop_measuring_time();
}

PERF_TEST(hdr_hist, insert_few) {
    hdr_hist h;
    perf_tests::start_measuring_time();
    for (int64_t i = 0; i < 100; ++i) {
        h.record(i);
    }

    perf_tests::do_not_optimize(h);
    perf_tests::stop_measuring_time();
}

PERF_TEST(hdr_hist, insert_many) {
    hdr_hist h;
    perf_tests::start_measuring_time();

    // So many inserts we'll cross 8 bit and 16 bit limits
    // for per-bucket counts
    for (int i = 0; i < 17000; ++i) {
        for (int64_t j = 0; j < 100; ++j) {
            h.record(j);
        }
    }

    perf_tests::stop_measuring_time();
}

PERF_TEST(simple_hist, insert_few) {
    simple_hist<double> h(26, 10);
    perf_tests::start_measuring_time();
    for (int64_t i = 0; i < 100; ++i) {
        h.record(i);
    }

    perf_tests::do_not_optimize(h);
    perf_tests::stop_measuring_time();
}

PERF_TEST(simple_hist, insert_many) {
    simple_hist<double> h(26, 10);
    perf_tests::start_measuring_time();

    // So many inserts we'll cross 8 bit and 16 bit limits
    // for per-bucket counts
    for (int i = 0; i < 17000; ++i) {
        for (int64_t j = 0; j < 100; ++j) {
            h.record(j);
        }
    }

    perf_tests::do_not_optimize(h);
    perf_tests::stop_measuring_time();
}
