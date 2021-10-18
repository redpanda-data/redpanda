/*
 * Copyright 2020 Vectorized, Inc.
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

#include <absl/container/flat_hash_map.h>

/// This harness brings up an entire redpanda fixture + the c++ implementation
/// of the wasm engine. Use this fixture for when a complete end-to-end
/// infrastructure is needed to perform some tests
class coproc_bench_fixture : public coproc_test_fixture {
public:
    struct router_test_plan {
        struct options {
            std::size_t batch_size = 10;
            std::size_t number_of_batches = 10;
        };
        using input_type = absl::flat_hash_map<model::ntp, options>;
        input_type inputs;
        absl::flat_hash_set<model::ntp> outputs;
    };

    /// \brief Start the actual test, ensure that startup() has been called,
    /// it initializes the storage layer and registers the coprocessors
    ss::future<absl::flat_hash_map<model::ntp, std::size_t>>
      start_benchmark(router_test_plan);

    /// \brief Builder for the structures needed to build a test plan
    static router_test_plan::input_type
      build_simple_opts(log_layout_map, std::size_t, std::size_t);

    static absl::flat_hash_set<model::ntp> expand(log_layout_map);

private:
    ss::future<> push_all(router_test_plan::input_type);

    ss::future<absl::flat_hash_map<model::ntp, std::size_t>>
      consume_all(absl::flat_hash_set<model::ntp>);
};
