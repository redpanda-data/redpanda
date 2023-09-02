// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "cluster/rm_stm.h"
#include "config/property.h"
#include "raft/tests/simple_raft_fixture.h"

static ss::logger logger{"rm_stm-test"};

struct rm_stm_test_fixture : simple_raft_fixture {
    void create_stm_and_start_raft(
      storage::ntp_config::default_overrides overrides = {}) {
        create_raft(overrides);
        raft::state_machine_manager_builder stm_m_builder;

        _stm = stm_m_builder.create_stm<cluster::rm_stm>(
          logger,
          _raft.get(),
          tx_gateway_frontend,
          _feature_table,
          _producer_state_manager,
          config::mock_binding(std::numeric_limits<uint64_t>::max()));

        _raft->start(std::move(stm_m_builder)).get();
        _started = true;
    }

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::shared_ptr<cluster::rm_stm> _stm;
};
