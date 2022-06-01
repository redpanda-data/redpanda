/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "test_utils/fixture.h"

#include <seastar/core/when_all.hh>

FIXTURE_TEST(test_wasm_engine_restart, coproc_test_fixture) {
    model::topic single_input("plain_topic");
    setup({{single_input, 5}}).get();
    enable_coprocessors(
      {{.id = 599872,
        .data{
          .tid = coproc::registry::type_identifier::identity_coprocessor,
          .topics = {std::make_pair<>(
            single_input, coproc::topic_ingestion_policy::stored)}}}})
      .get();
    std::vector<model::ntp> inputs;
    std::vector<model::ntp> outputs;
    for (auto i = 0; i < 5; ++i) {
        inputs.emplace_back(
          model::kafka_namespace, single_input, model::partition_id(i));
        outputs.emplace_back(
          model::kafka_namespace,
          to_materialized_topic(
            single_input, identity_coprocessor::identity_topic),
          model::partition_id(i));
    }

    auto push_inputs =
      [this](const std::vector<model::ntp>& ntps) -> ss::future<> {
        std::vector<ss::future<>> fs;
        for (const auto& ntp : ntps) {
            fs.emplace_back(produce(ntp, make_random_batch(500)));
        }
        return ss::when_all_succeed(fs.begin(), fs.end());
    };
    /// Push some data...
    push_inputs(inputs).get();

    /// Cause an implied crash, redpanda should begin its recovery phase
    set_delay_heartbeat(true).get();
    ss::sleep(2s).get();
    set_delay_heartbeat(false).get();

    /// Push some more data...
    push_inputs(inputs).get();

    /// Ensure at-least 10 * 2 * N-Ntps batches were read. Why at least? Because
    /// due to the offset commit interval within coproc, coprocessor scripts
    /// themselves have an at-least-once durability guarantee, so it would be
    /// more likely that a script processed a duplicate record. The wider the
    /// commit interval, the more likely this is.
    std::vector<ss::future<model::record_batch_reader::data_t>> fs;
    for (const auto& ntp : outputs) {
        fs.emplace_back(consume(ntp, (500 * 2)));
    }
    auto results = ss::when_all_succeed(fs.begin(), fs.end()).get0();
    for (const auto& data : results) {
        BOOST_CHECK_GE(num_records(data), (500 * 2));
    }
}
