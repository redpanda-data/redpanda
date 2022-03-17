/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/types.h"
#include "kafka/client/client.h"
#include "model/record_batch_reader.h"

#include <vector>

namespace coproc::wasm {
/// Publishes valid wasm::events to the 'coprocessor_internal_topic'
///
/// Raw interface to the internal topic, you can publish any event however
/// messages are checked for validity before being written to the topic,
/// result of this is arg0 of the result tuple.
ss::future<std::vector<kafka::produce_response::partition>>
publish_events(kafka::client::client&, model::record_batch_reader);

/// Creates the 'coproc_internal_topic' like rpk wasm publish would
///
/// Performs additional validation ensuring that the topic is created with the
/// expected parameters, if validation fails, this will assert
ss::future<> create_coproc_internal_topic(kafka::client::client&);

/// Resolves when \ref id is fully registered within the fixtures pacemaker
ss::future<> wait_for_copro(ss::sharded<coproc::pacemaker>&, coproc::script_id);

} // namespace coproc::wasm
