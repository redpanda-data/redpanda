// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "random/generators.h"
#include "test_utils/test.h"

#include <seastar/core/loop.hh>

#include <boost/range/irange.hpp>

TEST_CORO(iobuf_mt, test_cross_shard_shares) {
    constexpr size_t parallel_shares = 10000;
    constexpr size_t iterations = 100;
    const auto a = random_generators::gen_alphanum_string(128);
    const auto b = random_generators::gen_alphanum_string(128);
    iobuf buf;
    buf.append(a.data(), a.size());
    buf.append(b.data(), b.size());

    // If iobuf::share(..) isn't thread-safe then the code below is likely to
    // generate a segfault.
    for (size_t i = 0; i < iterations; i++) {
        co_await ss::parallel_for_each(
          boost::irange(0ul, parallel_shares),
          [b = buf.share(0, buf.size_bytes())](auto s) mutable {
              return ss::smp::submit_to(
                s % ss::smp::count, [b = b.share(0, b.size_bytes())]() mutable {
                    for (size_t i = 0; i < iterations; i++) {
                        b.share(0, b.size_bytes());
                    }
                });
          });
    }
}
