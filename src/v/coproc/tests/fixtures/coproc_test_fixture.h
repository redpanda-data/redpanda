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
#include "coproc/tests/fixtures/supervisor_test_fixture.h"
#include "coproc/tests/utils/event_publisher.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "redpanda/tests/fixture.h"

#include <absl/container/btree_map.h>

using log_layout_map = absl::btree_map<model::topic, size_t>;

class coproc_test_fixture : public supervisor_test_fixture {
public:
    static const auto inline tp_stored = coproc::topic_ingestion_policy::stored;
    static const auto inline tp_earliest
      = coproc::topic_ingestion_policy::earliest;
    static const auto inline tp_latest = coproc::topic_ingestion_policy::latest;

    struct deploy {
        uint64_t id;
        coproc::wasm::cpp_enable_payload data;
    };

    /// Class constructor
    ///
    /// Initializes the '_root_fixture' member variable
    /// Using encapsulation over inheritance so that the root fixture can be
    /// destroyed and re-initialized in the middle of a test
    coproc_test_fixture();

    /// Higher level abstraction of 'publish_events'
    ///
    /// Maps tuple to proper encoded wasm record and calls 'publish_events'
    ss::future<> enable_coprocessors(std::vector<deploy>);

    /// Higher level abstraction of 'publish_events'
    ///
    /// Maps the list of ids to the proper type and calls 'publish_events'
    ss::future<> disable_coprocessors(std::vector<uint64_t>);

    /// Call to define the state-of-the-world
    ///
    /// This is the current ntps registered within the log manger. Since
    /// theres no shard_table, a trivial hashing scheme is internally used to
    /// map ntps to shards.
    ss::future<> setup(log_layout_map);

    /// Simulates a redpanda restart from failure
    ///
    /// All internal state is wiped and setup() is called again. Application is
    /// forced to read from the existing _data_dir to bootstrap
    ss::future<> restart();

    /// \brief Write records to storage::api
    ss::future<model::offset>
    push(const model::ntp&, model::record_batch_reader);

    /// \brief Read records from storage::api up until 'limit' or 'time'
    /// starting at 'offset'
    ss::future<std::optional<model::record_batch_reader::data_t>> drain(
      const model::ntp&,
      std::size_t,
      model::offset = model::offset(0),
      model::timeout_clock::time_point = model::timeout_clock::now()
                                         + std::chrono::seconds(5));

    coproc::wasm::event_publisher& get_publisher() { return _publisher; };

protected:
    redpanda_thread_fixture* root_fixture() {
        vassert(_root_fixture != nullptr, "Access root_fixture when null");
        return _root_fixture.get();
    }

private:
    coproc::wasm::event_publisher _publisher;

    std::unique_ptr<redpanda_thread_fixture> _root_fixture;
};
