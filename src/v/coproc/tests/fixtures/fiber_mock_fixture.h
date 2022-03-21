/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/script_context_backend.h"
#include "coproc/script_context_frontend.h"
#include "coproc/script_context_router.h"
#include "coproc/types.h"
#include "redpanda/tests/fixture.h"

#include <absl/container/flat_hash_map.h>

struct enable_coproc {
    enable_coproc();
};

struct basic_copro_base {
    using batch_t = model::record_batch_reader::data_t;
    using result_t = absl::btree_map<model::topic, batch_t>;

    virtual ~basic_copro_base() = default;

    virtual result_t operator()(model::ntp, batch_t) = 0;
};

/// Fixture that tests coproc at a lower level
///
/// Even though a redpanda_thread_fixture must be stood up to use this fixture,
/// no coprocessors are ever deployed. This fixture reaches into the application
/// to grab the partition manager and to create normal topics. After that it
/// emmulates a single sharded fiber. The main advantage of this is that one can
/// look into the internal data structure \ref router_t to ensure the retry
/// mechanism is working as expected
class fiber_mock_fixture
  : public enable_coproc
  , public redpanda_thread_fixture {
private:
    struct state {
        ss::abort_source as;
        coproc::routes_t routes;
        std::unique_ptr<basic_copro_base> copro;
        absl::flat_hash_map<model::ntp, model::offset> high_input;
    };

public:
    struct test_parameters {
        model::topic_namespace tn;
        int partitions{1};
        int records_per_input{100};
        /// If latest is set, a re-hydrate must occur after first run
        coproc::topic_ingestion_policy policy{
          coproc::topic_ingestion_policy::earliest};
    };

    using expected_t = absl::flat_hash_map<model::ntp, std::size_t>;

    virtual ~fiber_mock_fixture() { _state.stop().get(); }

    /// \brief Bring up the input topics initialized with number of records
    ///
    /// Also ensures the core \ref state structure and the corresponding router
    /// is initialized properly
    ///
    /// Asserts if the test env isn't initialized as it expected it should be
    ss::future<> init_test(test_parameters);

    /// \brief Starts the pseudo-fiber
    ///
    /// Emmulates a single sharded script_context. The main advantage of this
    /// being its now possible to know when a fiber is 'done'. This is when all
    /// outputs have cought up with their respective inputs.
    ///
    /// Throws ss::timed_out_error() if the described predicate isn't reached
    /// before \ref timeout expires
    template<typename copro>
    ss::future<> run(
      model::timeout_clock::time_point timeout = model::timeout_clock::now()
                                                 + std::chrono::seconds(10)) {
        return _state.invoke_on_all([this, timeout](state& s) {
            /// Store copro
            s.copro = std::make_unique<copro>();
            /// Start pseudo-fiber
            return do_run(s, timeout);
        });
    }

    /// \brief Ensures the correct results on output topics
    ///
    /// Just because the fiber finished without error doesn't mean the fiber
    /// completed all tasks without failure. A bug in \ref write_materialized
    /// could produce incorrect data even if the retry mechanism is complete.
    /// Use this method to consume expected results from materialized logs, will
    /// call BOOST_CHECK() against all ntps in the expected set
    ss::future<> verify_results(expected_t);

private:
    ss::future<> do_run(
      fiber_mock_fixture::state& s, model::timeout_clock::time_point timeout);

    ss::future<coproc::output_write_inputs> transform(
      coproc::script_id id,
      coproc::input_read_results irr,
      const std::unique_ptr<basic_copro_base>& cp);

    coproc::input_read_args make_input_read_args(state& s);
    coproc::output_write_args make_output_write_args(state& s);

    ss::future<ss::lw_shared_ptr<coproc::source>>
    make_source(model::ntp input, state& s, test_parameters);

private:
    /// Static script id, never changes as only emmulates 1 fiber at once
    coproc::script_id _id{1};

    /// Includes a router + coprocessor per shard
    ss::sharded<state> _state;
};
