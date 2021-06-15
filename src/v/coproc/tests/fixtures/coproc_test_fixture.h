/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/tests/fixtures/coproc_fixture_iface.h"
#include "coproc/tests/utils/event_publisher.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "redpanda/tests/fixture.h"

class coproc_test_fixture
  : public redpanda_thread_fixture
  , public coproc_fixture_iface {
public:
    ss::future<> enable_coprocessors(std::vector<deploy>) override;

    ss::future<> disable_coprocessors(std::vector<uint64_t>) override;

    ss::future<> setup(log_layout_map) override;

    /// TODO: Support restarting of this type of test fixture
    /// This will aid in failure scenario testing
    ss::future<> restart() override { return ss::now(); }

    ss::future<model::offset>
    push(const model::ntp&, model::record_batch_reader) override;

    ss::future<std::optional<model::record_batch_reader::data_t>> drain(
      const model::ntp&,
      std::size_t,
      model::offset = model::offset(0),
      model::timeout_clock::time_point = model::timeout_clock::now()
                                         + std::chrono::seconds(5)) override;

    coproc::wasm::event_publisher& get_publisher() { return _publisher; };

private:
    coproc::wasm::event_publisher _publisher;
};
