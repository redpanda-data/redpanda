// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "boost_hist.h"

namespace bh = boost::histogram;
namespace tr = bh::axis::transform;

// Typical bucket ranges:
//  - Queue depth (0-512)
//  - Disk/net latency (10us-100ms)
//  - Logical RPC latency (10us-60s)
//  - Queuing time latency (10us-60s)

boost_hist::boost_hist()
  : boost_hist(26, 10, 60000000) {}

boost_hist::boost_hist(size_t nbuckets, double lower, double upper) {
    _hist = bh::make_histogram_with(
      std::array<uint64_t, 30>(),
      bh::axis::regular<double, tr::log>(nbuckets, lower, upper));
}

ss::metrics::histogram boost_hist::to_seastar() {
    ss::metrics::histogram sshist;
    sshist.buckets.resize(_hist.size());
    sshist.sample_sum = _sum;

    int64_t cumulative = 0;
    for (bh::axis::index_type i = -1; i < (int)_hist.size() - 1; ++i) {
        auto bin = _hist.axis().bin(i);
        auto& bucket = sshist.buckets[i + 1];
        cumulative += _hist.at(i);
        bucket.count = cumulative;
        bucket.upper_bound = bin.upper();

        sshist.sample_count += _hist.at(i);
    }

    return sshist;
}

// void boost_hist::record(int64_t value)

std::unique_ptr<boost_hist::measurement> boost_hist::auto_measure() {
    return std::make_unique<measurement>(*this);
}
