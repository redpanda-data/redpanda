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

#include "archival/service.h"
#include "cluster/controller.h"
#include "cluster/fwd.h"
#include "coproc/event_listener.h"
#include "coproc/pacemaker.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/fwd.h"
#include "raft/group_manager.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/smp_groups.h"
#include "rpc/server.h"
#include "seastarx.h"
#include "security/credential_store.h"
#include "storage/fwd.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/defer.hh>

namespace po = boost::program_options; // NOLINT

class application {
public:
    int run(int, char**);

    void initialize(
      std::optional<YAML::Node> proxy_cfg = std::nullopt,
      std::optional<YAML::Node> proxy_client_cfg = std::nullopt,
      std::optional<scheduling_groups> = std::nullopt);
    void check_environment();
    void configure_admin_server();
    void wire_up_services();
    void wire_up_redpanda_services();
    void start();
    void start_redpanda();

    explicit application(ss::sstring = "redpanda::main");

    void shutdown() {
        while (!_deferred.empty()) {
            _deferred.pop_back();
        }
    }

    ss::future<> set_proxy_config(ss::sstring name, std::any val);
    ss::future<> set_proxy_client_config(ss::sstring name, std::any val);

    ss::sharded<cluster::metadata_cache> metadata_cache;
    ss::sharded<kafka::group_router> group_router;
    ss::sharded<cluster::shard_table> shard_table;
    ss::sharded<storage::api> storage;
    ss::sharded<coproc::pacemaker> pacemaker;
    ss::sharded<cluster::partition_manager> partition_manager;
    ss::sharded<raft::group_manager> raft_group_manager;
    ss::sharded<cluster::metadata_dissemination_service>
      md_dissemination_service;
    ss::sharded<kafka::coordinator_ntp_mapper> coordinator_ntp_mapper;
    std::unique_ptr<cluster::controller> controller;
    ss::sharded<kafka::fetch_session_cache> fetch_session_cache;
    smp_groups smp_service_groups;
    ss::sharded<kafka::quota_manager> quota_mgr;
    ss::sharded<cluster::id_allocator_frontend> id_allocator_frontend;
    ss::sharded<archival::scheduler_service> archival_scheduler;

private:
    using deferred_actions
      = std::vector<ss::deferred_action<std::function<void()>>>;

    // All methods are calleds from Seastar thread
    void init_env();
    ss::app_template setup_app_template();
    void validate_arguments(const po::variables_map&);
    void hydrate_config(const po::variables_map&);

    void admin_register_raft_routes(ss::http_server& server);
    void admin_register_kafka_routes(ss::http_server& server);
    void admin_register_security_routes(ss::http_server& server);

    bool coproc_enabled() {
        const auto& cfg = config::shard_local_cfg();
        return cfg.developer_mode() && cfg.enable_coproc();
    }

    bool archival_storage_enabled();

    template<typename Service, typename... Args>
    ss::future<> construct_service(ss::sharded<Service>& s, Args&&... args) {
        auto f = s.start(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] { s.stop().get(); });
        return f;
    }

    template<typename Service, typename... Args>
    void construct_single_service(std::unique_ptr<Service>& s, Args&&... args) {
        s = std::make_unique<Service>(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] { s->stop().get(); });
    }
    void setup_metrics();
    std::unique_ptr<ss::app_template> _app;
    bool _redpanda_enabled{true};
    std::optional<pandaproxy::configuration> _proxy_config;
    std::optional<kafka::client::configuration> _proxy_client_config;
    scheduling_groups _scheduling_groups;
    ss::logger _log;

    std::unique_ptr<coproc::wasm::event_listener> _wasm_event_listener;
    ss::sharded<rpc::connection_cache> _raft_connection_cache;
    ss::sharded<kafka::group_manager> _group_manager;
    ss::sharded<rpc::server> _rpc;
    ss::sharded<ss::http_server> _admin;
    ss::sharded<rpc::server> _kafka_server;
    ss::sharded<pandaproxy::proxy> _proxy;
    ss::metrics::metric_groups _metrics;
    // run these first on destruction
    deferred_actions _deferred;
};

namespace debug {
extern application* app;
}
