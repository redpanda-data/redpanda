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

#include "coproc/pacemaker.h"
#include "coproc/script_dispatcher.h"
#include "coproc/types.h"
#include "coproc/wasm_event.h"
#include "kafka/client/client.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/btree_set.h>

#include <chrono>
#include <filesystem>

namespace coproc::wasm {

/// The wasm::event_listener listens on an internal topic called the
/// "coprocessor_internal_topic" for events of type coproc::wasm::event.
/// When new events arrive they are parsed, reconciled, then the appropriate RPC
/// command is forwarded to the wasm engine
class event_listener {
public:
    /// class constructor
    ///
    /// \brief Takes the pacemaker as a reference, to call add / remove source()
    explicit event_listener(ss::sharded<pacemaker>&);

    /// To be invoked once on redpanda::application startup
    ///
    /// \brief Connects the kafka::client to this broker, and starts the loop
    ss::future<> start();

    /// To be invoked once on redpanda::applications call to svc->stop()
    ///
    /// \brief Shuts down the poll loop, initialized by the start() method
    ss::future<> stop();

private:
    ss::future<> do_start();
    ss::future<> do_ingest();

    ss::future<ss::stop_iteration>
    poll_topic(model::record_batch_reader::data_t&);

    ss::future<>
      persist_actions(absl::btree_map<script_id, log_event>, model::offset);

    ss::future<> boostrap_status_topic();
    ss::future<> advertise_state_update(std::size_t n = 3);

private:
    /// Kafka client used to poll the internal topic
    kafka::client::client _client;

    /// Primitives used to manage the poll loop
    ss::gate _gate;
    ss::abort_source _abort_source;

    /// Current offset into the 'coprocessor_internal_topic'
    model::offset _offset{0};

    /// Set of known script ids to be active
    absl::btree_map<script_id, deploy_attributes> _active_ids;

    /// Managing instance of coprocessor infra
    ss::sharded<coproc::pacemaker>& _pacemaker;

    /// Used to make requests to the wasm engine
    script_dispatcher _dispatcher;
};

} // namespace coproc::wasm
