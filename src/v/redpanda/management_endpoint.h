/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/endpoint_tls_config.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>

struct mgmt_endpoint_cfg {
    std::vector<model::broker_endpoint> endpoints;
    std::vector<config::endpoint_tls_config> endpoints_tls;
    ss::scheduling_group sg;
};

class management_endpoint {
public:
    explicit management_endpoint(mgmt_endpoint_cfg cfg)
      : _server("admin")
      , _cfg(std::move(cfg)) {
        _server.set_content_streaming(true);
    }

    ss::future<> start();
    ss::future<> stop();

    ss::httpd::http_server& server() { return _server; }

    ss::httpd::http_server const& server() const { return _server; }

private:
    ss::httpd::http_server _server;
    mgmt_endpoint_cfg _cfg;

    ss::future<> configure_listeners();
    void configure_metrics_route();
};
