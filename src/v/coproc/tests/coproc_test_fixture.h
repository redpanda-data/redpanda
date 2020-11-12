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
#include "coproc/logger.h"
#include "coproc/tests/coproc_fixture_base.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "vlog.h"

#include <type_traits>

struct rpc_sharded_service_tag;
struct rpc_non_sharded_service_tag;

template<typename rpc_should_shard_tag>
class coproc_test_fixture : public coproc_fixture_base {
public:
    using erc = coproc::enable_response_code;
    using drc = coproc::disable_response_code;

    static const inline auto e = coproc::topic_ingestion_policy::earliest;
    static const inline auto s = coproc::topic_ingestion_policy::stored;
    static const inline auto l = coproc::topic_ingestion_policy::latest;

    static_assert(
      std::is_same_v<
        rpc_should_shard_tag,
        rpc_sharded_service_tag> || std::is_same_v<rpc_should_shard_tag, rpc_non_sharded_service_tag>,
      "Template type must be one of two defined tags in coproc_test_fixture.h");

    explicit coproc_test_fixture(uint16_t port, bool start_router = true)
      : coproc_fixture_base(start_router)
      , _svc(port) {
        _svc.configure_server();
    }

    ~coproc_test_fixture() override {
        // TODO(rob) Hack for observed issue where quick shutdown of server
        // when using sharded rpc client will crash. Clubhouse ID: 1289
        ss::sleep(std::chrono::seconds(2)).get();
        _svc.stop_server();
    }

    /// data -> What ntps should exist in api::storage
    /// cpros -> Initial state of the coproc::router
    void startup(log_layout_map&& data, active_copros&& cpros = {}) {
        vlog(coproc::coproclog.info, "Starting RPC server...");
        _svc.start_server();
        coproc_fixture_base::startup(std::move(data));
        expand_copros(std::move(cpros)).get();
    }

    rpc::transport_configuration client_config() const {
        return _svc.client_config();
    }

protected:
    template<typename T, typename... Args>
    void register_service(Args&&... args) {
        _svc.template register_service<T>(std::forward<Args>(args)...);
    }

private:
    using rpc_fixture = std::conditional_t<
      std::is_same_v<rpc_should_shard_tag, rpc_sharded_service_tag>,
      rpc_sharded_integration_fixture,
      rpc_simple_integration_fixture>;

    rpc_fixture _svc;
};
