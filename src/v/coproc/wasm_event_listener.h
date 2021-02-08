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
#include "kafka/client/client.h"
#include "utils/unresolved_address.h"

#include <seastar/core/abort_source.hh>

#include <chrono>
#include <filesystem>

namespace coproc {

class wasm_event_listener {
public:
    explicit wasm_event_listener(std::filesystem::path);

    /// \brief Initializes the listener, ensuring the 'wasm_root' directory is
    /// created establishes a connection to the broker, and begins the poll loop
    ss::future<> start();

    /// \brief Shuts down the poll loop, started by the start() method
    ss::future<> stop();

    /// \brief Absolute paths to local wasm resource directories
    const std::filesystem::path& wasm_root() const { return _wasm_root; }
    const std::filesystem::path& submit_dir() const { return _submit_dir; }
    const std::filesystem::path& active_dir() const { return _active_dir; }
    const std::filesystem::path& inactive_dir() const { return _inactive_dir; }

private:
    ss::future<> do_start();

    ss::future<> poll_topic(model::record_batch_reader::data_t&);

    ss::future<> resolve_wasm_script(ss::sstring, iobuf);

private:
    /// This may never change as it is not a configurable value
    static const inline model::topic_partition _coproc_internal_tp{
      model::topic_partition(
        model::topic("coprocessor_internal_topic"), model::partition_id(0))};

    /// Kafka client used to poll the internal topic
    kafka::client::client _client;

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
