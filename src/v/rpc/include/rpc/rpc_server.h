// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/vassert.h"
#include "config/configuration.h"
#include "net/server.h"
#include "rpc/service.h"

namespace rpc {

struct service;
struct server_context_impl;

// Wrapper around net::server that serves the internal RPC protocol. It allows
// new services to be registered while the server is running.
class rpc_server : public net::server {
public:
    explicit rpc_server(net::server_configuration s)
      : net::server(std::move(s), rpclog) {}

    explicit rpc_server(ss::sharded<net::server_configuration>* s)
      : net::server(s, rpclog) {}

    ~rpc_server() override = default;

    rpc_server(rpc_server&&) noexcept = delete;
    rpc_server& operator=(rpc_server&&) noexcept = delete;
    rpc_server(const rpc_server&) = delete;
    rpc_server& operator=(const rpc_server&) = delete;

    void set_all_services_added() { _all_services_added = true; }

    void set_use_service_unavailable() { _service_unavailable_allowed = true; }

    // Adds the given services to the protocol.
    // May be called whether or not the server has already been started.
    void add_services(std::vector<std::unique_ptr<service>> services) {
        vassert(
          !_all_services_added,
          "Adding service after all services already added");
        if (!config::shard_local_cfg().disable_metrics()) {
            for (auto& s : services) {
                s->setup_metrics();
            }
        }
        std::move(
          services.begin(), services.end(), std::back_inserter(_services));
    }

    std::string_view name() const final {
        return "vectorized internal rpc protocol";
    }

    ss::future<> apply(ss::lw_shared_ptr<net::connection>) final;

    template<std::derived_from<service> T, typename... Args>
    void register_service(Args&&... args) {
        _services.push_back(std::make_unique<T>(std::forward<Args>(args)...));
    }

private:
    ss::future<>
      dispatch_method_once(header, ss::lw_shared_ptr<net::connection>);
    ss::future<> send_reply(ss::lw_shared_ptr<server_context_impl>, netbuf);
    ss::future<>
      send_reply_skip_payload(ss::lw_shared_ptr<server_context_impl>, netbuf);

    bool _all_services_added{false};
    bool _service_unavailable_allowed{false};
    std::vector<std::unique_ptr<service>> _services;
};

} // namespace rpc
