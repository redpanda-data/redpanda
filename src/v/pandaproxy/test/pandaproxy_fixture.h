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

#include "config/configuration.h"
#include "http/client.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/metadata.h"
#include "pandaproxy/rest/configuration.h"
#include "redpanda/tests/fixture.h"

class pandaproxy_test_fixture : public redpanda_thread_fixture {
public:
    pandaproxy_test_fixture()
      : redpanda_thread_fixture() {}

    pandaproxy_test_fixture(const pandaproxy_test_fixture&) = delete;
    pandaproxy_test_fixture(pandaproxy_test_fixture&&) = delete;
    pandaproxy_test_fixture operator=(const pandaproxy_test_fixture&) = delete;
    pandaproxy_test_fixture operator=(pandaproxy_test_fixture&&) = delete;
    ~pandaproxy_test_fixture() = default;

    http::client make_proxy_client() {
        net::base_transport::configuration transport_cfg;
        transport_cfg.server_addr = net::unresolved_address{
          "localhost", proxy_port};
        return http::client(transport_cfg);
    }

    http::client make_schema_reg_client() {
        net::base_transport::configuration transport_cfg;
        transport_cfg.server_addr = net::unresolved_address{
          "localhost", schema_reg_port};
        return http::client(transport_cfg);
    }

    void set_config(ss::sstring name, std::any val) {
        app.set_proxy_config(std::move(name), std::move(val)).get();
    }

    void set_client_config(ss::sstring name, std::any val) {
        app.set_proxy_client_config(std::move(name), std::move(val)).get();
    }
};
