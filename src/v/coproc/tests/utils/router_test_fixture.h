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

#include "coproc/logger.h"
#include "coproc/script_manager.h"
#include "coproc/tests/utils/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/supervisor_test_fixture.h"
#include "coproc/types.h"

// Non-sharded rpc_service to emmulate the javascript engine
class router_test_fixture
  : public coproc_test_fixture
  , public supervisor_test_fixture {
private:
    struct push_action_tag;
    struct drain_action_tag;

public:
    struct router_test_plan {
        struct options {
            std::size_t number_of_batches = 100;
            std::size_t number_of_pushes = 1;
        };
        using all_opts = absl::flat_hash_map<model::ntp, options>;
        all_opts input;
        all_opts output;
    };

    using copro_map = coproc::supervisor::copro_map;
    using enable_reqs_data = std::vector<coproc::enable_copros_request::data>;

    using push_results = absl::flat_hash_map<model::ntp, model::offset>;
    using drain_results
      = absl::flat_hash_map<model::ntp, std::pair<model::offset, std::size_t>>;

    /// \brief Initialize the storage layer, then submit all coprocessors to
    /// v/coproc/service , this doesn't start the actual test
    ss::future<> startup(log_layout_map) override;

    /// \brief Start the actual test, ensure that startup() has been called, it
    /// initializes the storage layer and registers the coprocessors
    ss::future<std::tuple<push_results, drain_results>>
      start_benchmark(router_test_plan);

    /// \brief Builder for the structures needed to build a test plan
    router_test_plan::all_opts build_simple_opts(log_layout_map, std::size_t);

private:
    template<typename T>
    using action_results = std::conditional_t<
      std::is_same_v<push_action_tag, T>,
      push_results,
      drain_results>;

    template<typename T>
    using results_mapped_t = typename action_results<T>::mapped_type;

    template<typename ActionTag>
    ss::future<action_results<ActionTag>> send_all(router_test_plan::all_opts);

    template<
      typename ActionTag,
      typename ResultType = results_mapped_t<ActionTag>>
    ss::future<ResultType>
    send_n_times(const model::ntp&, router_test_plan::options);

    template<
      typename ActionTag,
      typename ResultType = results_mapped_t<ActionTag>>
    ss::future<>
    do_action(const model::ntp&, std::size_t, std::size_t, ResultType&);

    /// Sanity checks, throws if the service fails to register a
    /// coprocessor, helpful when debugging possible issues within the test
    /// setup process
    void validate_result(
      const enable_reqs_data&,
      result<rpc::client_context<coproc::enable_copros_reply>>);

    ss::future<> enable_coprocessors(enable_reqs_data&);

    void to_ecr_data(enable_reqs_data&, const copro_map&);
};
