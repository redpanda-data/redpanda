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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "storage/api.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

using log_layout_map = absl::flat_hash_map<model::topic_namespace, size_t>;

class coproc_test_fixture : public redpanda_thread_fixture {
public:
    using script_manager_client
      = rpc::client<coproc::script_manager_client_protocol>;
    using opt_reader_data_t = std::optional<model::record_batch_reader::data_t>;
    using erc = coproc::enable_response_code;
    using drc = coproc::disable_response_code;

    static const inline auto e = coproc::topic_ingestion_policy::earliest;
    static const inline auto s = coproc::topic_ingestion_policy::stored;
    static const inline auto l = coproc::topic_ingestion_policy::latest;

    coproc_test_fixture() = default;
    coproc_test_fixture(const coproc_test_fixture&) = delete;
    coproc_test_fixture(const coproc_test_fixture&&) = delete;
    coproc_test_fixture& operator=(const coproc_test_fixture&) = delete;
    coproc_test_fixture& operator=(const coproc_test_fixture&&) = delete;

    ~coproc_test_fixture();

    /// \brief Grab a convienent handle to a connected script_manager_client
    script_manager_client& sm_client() { return _client; }

    /// \brief Write records to storage::api
    ss::future<model::offset>
    push(const model::ntp&, model::record_batch_reader);

    /// \brief Read records from storage::api up until 'limit' or 'time'
    /// starting at 'offset'
    ss::future<opt_reader_data_t> drain(
      const model::ntp&,
      std::size_t,
      model::offset = model::offset(0),
      model::timeout_clock::time_point = model::timeout_clock::now() + 5s);

protected:
    /// \brief Populate 'app.storage' with the user defined test layout
    virtual ss::future<> startup(log_layout_map data);

    const log_layout_map& get_layout() const { return _llm; }

private:
    ss::future<model::record_batch_reader::data_t> do_drain(
      kafka::partition_wrapper,
      model::offset,
      std::size_t,
      model::timeout_clock::time_point);

    /// \brief Discover for which shard an ntp exists on, like the shard table
    /// only works by querying v/storage instead
    ss::future<std::optional<ss::shard_id>> shard_for_ntp(const model::ntp&);

    script_manager_client make_client();

private:
    log_layout_map _llm;

    script_manager_client _client{make_client()};
};
