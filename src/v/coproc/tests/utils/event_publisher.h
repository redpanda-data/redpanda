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
#include "config/configuration.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/wasm_event_generator.h"
#include "coproc/types.h"
#include "kafka/client/client.h"
#include "kafka/protocol/produce.h"
#include "model/record_batch_reader.h"

#include <seastar/core/sstring.hh>

#include <vector>

namespace coproc::wasm {
/// Adds the ability to publish coprocessor event to the
/// coprocessor_internal_topic to any fixture
class event_publisher {
public:
    event_publisher();
    ~event_publisher() { _client.stop().get(); }

    /// Starts up the kafka client and sends out a create_topics request to
    /// create the 'coprocessor_internal_topic'
    ss::future<> start();

    /// Publishes valid wasm::events to the 'coprocessor_internal_topic'
    ///
    /// Raw interface to the internal topic, you can publish any event however
    /// messages are checked for validity before being written to the topic,
    /// result of this is arg0 of the result tuple.
    ss::future<std::vector<kafka::produce_response::partition>>
      publish_events(model::record_batch_reader);

private:
    ss::future<> create_coproc_internal_topic();

private:
    kafka::client::client _client;
};
} // namespace coproc::wasm
