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

#include "coproc/event_handler.h"
#include "coproc/script_dispatcher.h"
#include "coproc/types.h"
#include "coproc/wasm_event.h"
#include "kafka/client/client.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <filesystem>

namespace coproc::wasm {

/// The wasm::event_listener listens on an internal topic called the
/// "coprocessor_internal_topic" for events of type coproc::wasm::event.
/// When new events arrive they are parsed, reconciled, then it is forwarded to
/// the handler for current corpoc_type
class event_listener {
public:
    /// class constructor
    explicit event_listener(ss::abort_source&);

    /// To be invoked once on redpanda::application startup
    ///
    /// \brief Connects the kafka::client to this broker, starts all registred
    /// handler and starts the loop
    ss::future<> start();

    /// To be invoked once on redpanda::applications call to svc->stop()
    ///
    /// \brief Shuts down the poll loop, initialized by the start() method
    ss::future<> stop();

    /// Register event_handel for coproc type
    void register_handler(event_type, event_handler*);

    ss::abort_source& get_abort_source();

private:
    ss::future<> do_start();
    ss::future<> do_ingest();

    ss::future<ss::stop_iteration>
    poll_topic(model::record_batch_reader::data_t&);

    ss::future<> process_events(event_batch events, model::offset last_offset);

private:
    /// Kafka client used to poll the internal topic
    kafka::client::client _client;

    /// Primitives used to manage the poll loop
    ss::gate _gate;
    ss::abort_source& _abort_source;

    /// Current offset into the 'coprocessor_internal_topic'
    model::offset _offset{0};

    absl::flat_hash_map<event_type, event_handler*>
      _handlers; // size of this map will be 3
};

} // namespace coproc::wasm
