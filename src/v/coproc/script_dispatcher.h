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
#include "coproc/supervisor.h"
#include "coproc/types.h"

#include <seastar/core/sharded.hh>

#include <optional>

namespace coproc::wasm {

/// Main interface between redpanda and the wasm engine.
///
/// Registers / deregisters scripts with the wasm engine over TCP, and upon
/// retrival of the reply, invokes the appropriate action within the pacemaker.
class script_dispatcher {
public:
    explicit script_dispatcher(ss::sharded<pacemaker>&, ss::abort_source&);

    /// Called when new coprocessors arrive on the coproc_internal_topic
    ///
    /// The wasm engine will be sent the list of coprocessors to enable
    /// Upon retrival of each successful ack, the script will be registered with
    /// the pacemaker.
    ss::future<> enable_coprocessors(enable_copros_request);

    /// Called when removal commands arrive on the coproc_internal_topic
    ///
    /// The wasm engine will be send the list of coprocessor ids to remove from
    /// its internal map. Upon retrival of each successful ack, the script will
    /// be deregistered from the pacemaker.
    ss::future<> disable_coprocessors(disable_copros_request);

    /// Invoke this after fatal error has occurred and its desired to clear all
    /// state from the wasm engine.
    ss::future<> disable_all_coprocessors();

private:
    /// The following methods are introduced to sidestep an issue detected when
    /// using .map/invoke_on_all within the context of a coroutine
    ss::future<std::vector<std::vector<coproc::errc>>>
      add_sources(script_id, std::vector<topic_namespace_policy>);
    ss::future<std::vector<coproc::errc>> remove_sources(script_id);

    /// Return std::nullopt only when the abort source is triggered,
    /// otherwise will forever loop attempting to re-connect to the wasm
    /// engine.
    ss::future<std::optional<supervisor_client_protocol>> get_client();

private:
    /// Interface to the coproc subsystem. This class calls add_source &
    /// remove_source to add and remove coprocessors. It must do them however
    /// across all shards
    ss::sharded<pacemaker>& _pacemaker;

    /// Reference to the script_listeners abort source. This class uses this
    /// abort source to know when to abort the transports retry loop in order to
    /// properly shutdown
    ss::abort_source& _abort_source;

    /// Underlying transport handle to the wasm engine. Although there are one
    /// of these per shard, this class is not sharded and only borrows a
    /// reference to this from shard 0
    rpc::reconnect_transport& _transport;
};

} // namespace coproc::wasm
