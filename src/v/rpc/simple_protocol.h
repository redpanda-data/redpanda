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
#include "net/server.h"
#include "rpc/service.h"
namespace rpc {
class simple_protocol final : public net::server::protocol {
public:
    template<typename T, typename... Args>
    void register_service(Args&&... args) {
        static_assert(std::is_base_of_v<service, T>, "must extend service.h");
        _services.push_back(std::make_unique<T>(std::forward<Args>(args)...));
    }

    const char* name() const final {
        return "vectorized internal rpc protocol";
    };
    ss::future<> apply(net::server::resources) final;

    void setup_metrics() {
        for (auto& s : _services) {
            s->setup_metrics();
        }
    }

private:
    ss::future<> dispatch_method_once(header, net::server::resources);

    std::vector<std::unique_ptr<service>> _services;
};

} // namespace rpc
