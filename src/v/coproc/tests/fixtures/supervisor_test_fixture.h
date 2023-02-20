/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "vassert.h"

/// Use this test fixture when you want to start a coproc::supervisor up.
/// Optionally query the internal state of the c++ 'wasm' engine with the
/// provided methods
class supervisor_test_fixture : public rpc_sharded_integration_fixture {
public:
    explicit supervisor_test_fixture(uint32_t port = 43189)
      : rpc_sharded_integration_fixture(port) {
        configure_server();
        _delay_heartbeat.start().get();
        _delay_heartbeat
          .invoke_on_all([](ss::lw_shared_ptr<bool>& ptr) {
              ptr = ss::make_lw_shared<bool>(false);
          })
          .get();
        _coprocessors.start().get();
        try {
            server()
              .invoke_on_all([&](rpc::rpc_server& s) {
                  std::vector<std::unique_ptr<rpc::service>> service;
                  service.emplace_back(std::make_unique<coproc::supervisor>(
                    _sg,
                    _ssg,
                    std::ref(_coprocessors),
                    std::ref(_delay_heartbeat)));
                  s.add_services(std::move(service));
              })
              .get();
            start_server();
        } catch (const std::exception& ex) {
            vassert(
              false,
              "Exception in supervisor_test_fixture constructor: {}",
              ex);
        }
    }

    ~supervisor_test_fixture() override {
        stop_server();
        _coprocessors.stop().get();
        _delay_heartbeat.stop().get();
    }

    /// The script_map_t is identical across all shards
    std::size_t total_registered() const {
        return _coprocessors.local().size();
    }

    ss::future<> set_delay_heartbeat(bool value) {
        return _delay_heartbeat.invoke_on_all(
          [value](ss::lw_shared_ptr<bool>& delay) { *delay = value; });
    }

private:
    ss::sharded<ss::lw_shared_ptr<bool>> _delay_heartbeat;
    ss::sharded<coproc::script_map_t> _coprocessors;
};
