// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/this_fiber_id.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/perf_tests.hh>

PERF_TEST(fiber_local, test_this_fiber_id_x100) {
    return ss::yield().then([] {
        return ss::yield().then([] {
            return ss::yield().then([] {
                return ss::yield().then([] {
                    // Run actual bench
                    uint64_t id = 0;
                    perf_tests::start_measuring_time();
                    // The actual operation is very fast and
                    // measuring it separately gives big error. To get the
                    // realistic result we need to time the sequence of 100
                    // operations.
                    for (int i = 0; i < 100; i++) {
                        id += ssx::this_fiber_id();
                    }
                    perf_tests::stop_measuring_time();
                    perf_tests::do_not_optimize(id);
                    return ss::now();
                });
            });
        });
    });
}

PERF_TEST(fiber_local, test_this_fiber_id_deep_x100) {
    return ss::yield().then([] {
        return ss::yield().then([] {
            return ss::yield().then([] {
                return ss::yield().then([] {
                    return ss::yield().then([] {
                        return ss::yield().then([] {
                            return ss::yield().then([] {
                                return ss::yield().then([] {
                                    return ss::yield().then([] {
                                        return ss::yield().then([] {
                                            // Run actual bench
                                            uint64_t id = 0;
                                            perf_tests::start_measuring_time();
                                            for (int i = 0; i < 100; i++) {
                                                id += ssx::this_fiber_id();
                                            }
                                            perf_tests::stop_measuring_time();
                                            perf_tests::do_not_optimize(id);
                                            return ss::now();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
}
