/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/tests/cluster_test_fixture.h"
#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/fixtures/supervisor_test_fixture.h"

#include <absl/container/flat_hash_map.h>

/// A cluster_test_fixture with a kafka::client and convienence methods from
/// coproc_api_fixture useful for multi-node coproc unit tests
class coproc_cluster_fixture
  : public cluster_test_fixture
  , public coproc_api_fixture {
public:
    using wasm_ptr = std::unique_ptr<supervisor_test_fixture>;

    /// Class constructor
    ///
    /// Calls superclass constructors and ensures 'enable_coproc' is true
    coproc_cluster_fixture() noexcept;

    /// Calls coproc_api_fixture superclass method with same name
    ///
    /// Additionally this method waits until the coprocessors are deployed
    /// within the coproc::pacemaker on all instances of rp that belong to the
    /// cluster_test_fixture
    ss::future<> enable_coprocessors(std::vector<deploy>);

    /// Calls cluster_test_fixture superclass method with same name
    ///
    /// Additionally instantiates an instance of the wasm engine used for
    /// testing and places it in the \ref instances map
    application* create_node_application(
      model::node_id node_id,
      int kafka_port = 9092,
      int rpc_port = 11000,
      int proxy_port = 8082,
      int schema_reg_port = 8081,
      int coproc_supervisor_port = 43189);

private:
    std::vector<model::node_id> get_node_ids();

private:
    absl::flat_hash_map<model::node_id, wasm_ptr> _instances;
};
