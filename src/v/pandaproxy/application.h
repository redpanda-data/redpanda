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

#include "kafka/client/configuration.h"
#include "pandaproxy/admin/api-doc/config.json.h"
#include "pandaproxy/proxy.h"
#include "seastarx.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/defer.hh>

namespace pandaproxy {

/// \brief The Pandaproxy application.
///
class application {
public:
    int run(int, char**);

    void initialize(const kafka::client::configuration& cfg);
    void check_environment();
    void configure_admin_server();
    void wire_up_services();
    void start();

    void shutdown() {
        while (!_deferred.empty()) {
            _deferred.pop_back();
        }
    }

    ss::future<> set_client_config(ss::sstring name, std::any val);

private:
    using deferred_actions
      = std::vector<ss::deferred_action<std::function<void()>>>;

    // All methods are called from a Seastar thread
    void init_env();
    ss::app_template setup_app_template();
    void validate_arguments(const boost::program_options::variables_map&);
    void hydrate_config(const boost::program_options::variables_map&);

    template<typename Service, typename... Args>
    ss::future<> construct_service(ss::sharded<Service>& s, Args&&... args) {
        auto f = s.start(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] { s.stop().get(); });
        return f;
    }

    std::unique_ptr<ss::app_template> _app;
    ss::logger _log{"pandaproxy::main"};

    ss::sharded<ss::http_server> _admin;
    ss::sharded<pandaproxy::proxy> _proxy;
    // run these first on destruction
    deferred_actions _deferred;
    kafka::client::configuration _client_config;
};

} // namespace pandaproxy