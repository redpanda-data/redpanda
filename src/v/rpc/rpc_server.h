// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "config/configuration.h"
#include "net/server.h"
#include "rpc/service.h"
#include "rpc/simple_protocol.h"
#include "vassert.h"

namespace rpc {

struct service;

// Wrapper around net::server that serves the internal RPC protocol. It allows
// new services to be registered while the server is running.
class rpc_server {
public:
    explicit rpc_server(net::server_configuration s)
      : _server(std::move(s)) {
        _server.set_protocol(std::make_unique<rpc::simple_protocol>());
    }

    explicit rpc_server(ss::sharded<net::server_configuration>* s)
      : _server(s) {
        _server.set_protocol(std::make_unique<rpc::simple_protocol>());
    }

    rpc_server(rpc_server&&) noexcept = default;
    ~rpc_server() = default;

    rpc_server& operator=(rpc_server&&) noexcept = delete;
    rpc_server(const rpc_server&) = delete;
    rpc_server& operator=(const rpc_server&) = delete;

    // Adds the given services to the protocol.
    // May be called whether or not the server has already been started.
    void add_services(std::vector<std::unique_ptr<service>> services) {
        if (!config::shard_local_cfg().disable_metrics()) {
            for (auto& s : services) {
                s->setup_metrics();
            }
        }
        auto* simple_proto = dynamic_cast<rpc::simple_protocol*>(
          _server.get_protocol());
        vassert(
          simple_proto != nullptr,
          "protocol must be of type rpc::simple_protocol");
        simple_proto->add_services(std::move(services));
    }

    void start() { _server.start(); }
    void shutdown_input() { _server.shutdown_input(); }
    ss::future<> wait_for_shutdown() { return _server.wait_for_shutdown(); }
    ss::future<> stop() { return _server.stop(); }

private:
    net::server _server;
};

} // namespace rpc
