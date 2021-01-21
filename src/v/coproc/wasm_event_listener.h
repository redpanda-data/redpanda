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

#include "coproc/types.h"
#include "coproc/wasm_event.h"
#include "kafka/requests/batch_consumer.h"
#include "pandaproxy/client/client.h"
#include "utils/unresolved_address.h"

#include <seastar/core/abort_source.hh>

#include <chrono>
#include <filesystem>

namespace coproc {

struct wasm_script_action {
    ss::sstring name;
    iobuf source_code;
    wasm_event_action action() const {
        /// If this came from a remove event, the validator would
        /// have failed if the value() field of the record wasn't
        /// empty. Therefore checking if this iobuf is empty is a
        /// certain way to know if the intended request was to
        /// deploy or remove
        return source_code.empty() ? wasm_event_action::remove
                                   : wasm_event_action::deploy;
    }
};

class wasm_event_listener {
public:
    explicit wasm_event_listener(std::filesystem::path);

    /// \brief Initializes the listener, ensuring the 'wasm_root' directory is
    /// created establishes a connection to the broker, and begins the poll loop
    void start();

    /// \brief Shuts down the poll loop, started by the start() method
    ss::future<> stop() {
        _abort_source.request_abort();
        return _gate.close().then([this] { return _client.stop(); });
    }

    /// \brief Absolute paths to local wasm resource directories
    const std::filesystem::path& wasm_root() const { return _wasm_root; }
    const std::filesystem::path& submit_dir() const { return _submit_dir; }
    const std::filesystem::path& active_dir() const { return _active_dir; }
    const std::filesystem::path& inactive_dir() const { return _inactive_dir; }

private:
    ss::future<> do_start();

    ss::future<ss::stop_iteration>
    poll_topic(model::record_batch_reader::data_t&);

    ss::future<> resolve_wasm_scripts(std::vector<wasm_script_action>);

    std::vector<wasm_script_action>
    process_wasm_events(model::record_batch_reader::data_t&);

    ss::future<> resolve_wasm_script(wasm_script_action);

private:
    /// This may never change as it is not a configurable value
    static const inline model::topic_partition _coproc_internal_tp{
      model::topic_partition(
        model::topic("coproc_internal_topic"), model::partition_id(0))};

    /// Kafka client used to poll the internal topic
    pandaproxy::client::client _client;

    /// Primitives used to manage the poll loop
    ss::gate _gate;
    ss::abort_source _abort_source;

    /// Root directory where the wasm engine will store working set of files
    std::filesystem::path _wasm_root;
    std::filesystem::path _active_dir;
    std::filesystem::path _submit_dir;
    std::filesystem::path _inactive_dir;

    /// Current offset into the '_coproc_internal_topic'
    model::offset _offset{0};
};

} // namespace coproc
