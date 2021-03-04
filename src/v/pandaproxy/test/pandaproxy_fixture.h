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

#include "http/client.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/metadata.h"
#include "pandaproxy/application.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/proxy.h"
#include "redpanda/tests/fixture.h"

class pandaproxy_test_fixture : public redpanda_thread_fixture {
public:
    pandaproxy_test_fixture()
      : redpanda_thread_fixture()
      , proxy() {
        configure_proxy();
        start_proxy();
    }

    pandaproxy_test_fixture(pandaproxy_test_fixture const&) = delete;
    pandaproxy_test_fixture(pandaproxy_test_fixture&&) = delete;
    pandaproxy_test_fixture operator=(pandaproxy_test_fixture const&) = delete;
    pandaproxy_test_fixture operator=(pandaproxy_test_fixture&&) = delete;

    ~pandaproxy_test_fixture() { proxy.shutdown(); }

    http::client make_client() {
        rpc::base_transport::configuration transport_cfg;
        transport_cfg.server_addr
          = rpc::resolve_dns(pandaproxy::shard_local_cfg().pandaproxy_api())
              .get();
        return http::client(transport_cfg);
    }

    void set_client_config(ss::sstring name, std::any val) {
        proxy.set_client_config(std::move(name), std::move(val)).get();
    }

private:
    void configure_proxy() {
        pandaproxy::shard_local_cfg().developer_mode.set_value(true);
    }

    void start_proxy() {
        kafka::client::configuration client_config;
        client_config.brokers.set_value(std::vector<unresolved_address>{
          config::shard_local_cfg().advertised_kafka_api()[0].address});
        proxy.initialize(client_config);
        proxy.check_environment();
        proxy.configure_admin_server();
        proxy.wire_up_services();
        proxy.start();
    }

    pandaproxy::application proxy;
};
