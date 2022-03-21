/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/types.h"

/// This harness brings up an entire redpanda fixture + the c++ implementation
/// of the wasm engine. Use this fixture for when a complete end-to-end
/// infrastructure is needed to perform some tests
class coproc_bench_fixture : public coproc_test_fixture {
public:
    struct router_test_plan {
        using plan_t = absl::flat_hash_map<model::ntp, std::size_t>;
        plan_t inputs;
        plan_t outputs;
    };

    using result_t = router_test_plan::plan_t;

    /// \brief Start the actual test, ensure that startup() has been called,
    /// it initializes the storage layer and registers the coprocessors
    ss::future<result_t> start_benchmark(router_test_plan);

    /// \brief Builder for the structures needed to build a test plan
    static router_test_plan::plan_t
      build_simple_opts(log_layout_map, std::size_t);

private:
    ss::future<> push_all(router_test_plan::plan_t);
    ss::future<result_t> consume_all(router_test_plan::plan_t);
};
