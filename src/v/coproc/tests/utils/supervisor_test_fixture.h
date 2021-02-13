/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "coproc/tests/utils/supervisor.h"
#include "rpc/test/rpc_integration_fixture.h"

/// Use this test fixture when you want to start a coproc::supervisor up.
/// Optionally query the internal state of the c++ 'wasm' engine with the
/// provided methods
class supervisor_test_fixture : public rpc_sharded_integration_fixture {
public:
    supervisor_test_fixture()
      : rpc_sharded_integration_fixture(43189) {
        configure_server();
        _coprocessors.start().get();
        register_service<coproc::supervisor>(std::ref(_coprocessors));
        start_server();
    }

    ~supervisor_test_fixture() override {
        stop_server();
        _coprocessors.stop().get();
    }

    /// The script_map_t is identical across all shards
    std::size_t total_registered() const {
        return _coprocessors.local().size();
    }

private:
    ss::sharded<coproc::script_map_t> _coprocessors;
};
