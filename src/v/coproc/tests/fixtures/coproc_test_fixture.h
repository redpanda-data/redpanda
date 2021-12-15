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
#include "kafka/client/fwd.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "redpanda/tests/fixture.h"

#include <absl/container/btree_map.h>

using log_layout_map = absl::btree_map<model::topic, size_t>;

class coproc_api_fixture {
public:
    static const auto inline tp_stored = coproc::topic_ingestion_policy::stored;
    static const auto inline tp_earliest
      = coproc::topic_ingestion_policy::earliest;
    static const auto inline tp_latest = coproc::topic_ingestion_policy::latest;

    struct deploy {
        uint64_t id;
        coproc::wasm::cpp_enable_payload data;
    };

    coproc_api_fixture();
    ~coproc_api_fixture();

    /// Starts the client and creates the internal copro topic
    ss::future<> start();

    /// Higher level abstraction of 'publish_events'
    ///
    /// Maps tuple to proper encoded wasm record and calls 'publish_events'
    ss::future<> enable_coprocessors(std::vector<deploy>);

    /// Higher level abstraction of 'publish_events'
    ///
    /// Maps the list of ids to the proper type and calls 'publish_events'
    ss::future<> disable_coprocessors(std::vector<uint64_t>);

    /// \brief Write records to storage::api
    ss::future<> produce(model::ntp, model::record_batch_reader);

    /// \brief Read records from storage::api up until 'limit' or 'time'
    /// starting at 'offset'
    ss::future<model::record_batch_reader::data_t> consume(
      model::ntp ntp,
      std::size_t limit,
      model::offset start_offset = model::offset(0),
      model::timeout_clock::time_point timeout = model::timeout_clock::now()
                                                 + std::chrono::seconds(10));

    kafka::client::client& get_client() { return *_client; }

protected:
    ss::future<model::record_batch_reader::data_t> do_consume(
      model::topic_partition,
      model::offset,
      std::size_t,
      model::timeout_clock::time_point);

    ss::future<> push_wasm_events(std::vector<coproc::wasm::event>);

private:
    absl::flat_hash_set<coproc::script_id> _active_ids;
    std::unique_ptr<kafka::client::client> _client;
};

class coproc_test_fixture
  : public coproc_api_fixture
  , public supervisor_test_fixture {
public:
    /// Calls the superclass method with the same name
    ///
    /// But additionally waits until the copros are registered with rp
    ss::future<> enable_coprocessors(std::vector<deploy>);

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

protected:
    redpanda_thread_fixture* root_fixture() {
        vassert(_root_fixture != nullptr, "Access root_fixture when null");
        return _root_fixture.get();
    }

private:
    std::unique_ptr<redpanda_thread_fixture> _root_fixture{
      std::make_unique<redpanda_thread_fixture>()};
};
