/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/btree_map.h"
#include "coproc/tests/fixtures/supervisor_test_fixture.h"
#include "coproc/tests/utils/wasm_event_generator.h"
#include "coproc/types.h"

#include <seastar/core/future.hh>

using log_layout_map = absl::btree_map<model::topic, size_t>;

/// Interface for how all coproc fixtures should operate
class coproc_fixture_iface : public supervisor_test_fixture {
public:
    static const auto inline tp_stored = coproc::topic_ingestion_policy::stored;
    static const auto inline tp_earliest
      = coproc::topic_ingestion_policy::earliest;
    static const auto inline tp_latest = coproc::topic_ingestion_policy::latest;

    struct deploy {
        uint64_t id;
        coproc::wasm::cpp_enable_payload data;
    };

    /// Higher level abstraction of 'publish_events'
    ///
    /// Maps tuple to proper encoded wasm record and calls 'publish_events'
    virtual ss::future<> enable_coprocessors(std::vector<deploy>) = 0;

    /// Higher level abstraction of 'publish_events'
    ///
    /// Maps the list of ids to the proper type and calls 'publish_events'
    virtual ss::future<> disable_coprocessors(std::vector<uint64_t>) = 0;

    /// Call to define the state-of-the-world
    ///
    /// This is the current ntps registered within the log manger. Since
    /// theres no shard_table, a trivial hashing scheme is internally used to
    /// map ntps to shards.
    virtual ss::future<> setup(log_layout_map) = 0;

    /// Simulates a redpanda restart from failure
    ///
    /// All internal state is wiped and setup() is called again. Application is
    /// forced to read from the existing _data_dir to bootstrap
    virtual ss::future<> restart() = 0;

    /// \brief Write records to storage::api
    virtual ss::future<model::offset>
    push(const model::ntp&, model::record_batch_reader) = 0;

    /// \brief Read records from storage::api up until 'limit' or 'time'
    /// starting at 'offset'
    virtual ss::future<std::optional<model::record_batch_reader::data_t>> drain(
      const model::ntp&,
      std::size_t,
      model::offset = model::offset(0),
      model::timeout_clock::time_point = model::timeout_clock::now()
                                         + std::chrono::seconds(5))
      = 0;
};
