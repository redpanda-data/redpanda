// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/boost_hist.h"

#include <boost/test/unit_test.hpp>

namespace bh = boost::histogram;

BOOST_AUTO_TEST_CASE(insert_and_read) {
    boost_hist h;
    h.record(1);

    auto ih = h.hist();
    std::cerr << ih.size() << std::endl;

    ss::metrics::histogram sshist;
    for (bh::axis::index_type i = -1; i < (int)ih.size() - 1; ++i) {
        std::cerr << "i = " << i << std::endl;
        auto bin = ih.axis().bin(i);
        // auto& bucket = sshist.buckets[i + 1];
        // bucket.count = ih.at(i);
        std::cerr << "count = " << ih.at(i) << std::endl;
        std::cerr << "upper = " << bin.upper() << std::endl;
        // bucket.upper_bound = bin.upper();
    }

    auto sshist2 = h.to_seastar();
}
