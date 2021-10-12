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

#include "archival/fwd.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/partition_recovery_manager.h"
#include "cluster/fwd.h"
#include "coproc/fwd.h"
#include "kafka/client/configuration.h"
#include "kafka/client/fwd.h"
#include "kafka/server/fwd.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/fwd.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "raft/fwd.h"
#include "redpanda/admin_server.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/smp_groups.h"
#include "rpc/fwd.h"
#include "seastarx.h"
#include "storage/fwd.h"
#include "v8_engine/fwd.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace po = boost::program_options; // NOLINT

class application {
public:
    int run(int, char**);

    void initialize(
      std::optional<YAML::Node> proxy_cfg = std::nullopt,
      std::optional<YAML::Node> proxy_client_cfg = std::nullopt,
      std::optional<YAML::Node> schema_reg_cfg = std::nullopt,
      std::optional<YAML::Node> schema_reg_client_cfg = std::nullopt,
      std::optional<scheduling_groups> = std::nullopt);
    void check_environment();
    void configure_admin_server();
    void wire_up_services();
    void wire_up_redpanda_services();
    void start();
    void start_redpanda();

    explicit application(ss::sstring = "redpanda::main");
    ~application();

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
    std::unique_ptr<coproc::api> coprocessing;
    ss::sharded<cluster::partition_manager> partition_manager;
    ss::sharded<raft::recovery_throttle> recovery_throttle;
    ss::sharded<raft::group_manager> raft_group_manager;
    ss::sharded<cluster::metadata_dissemination_service>
      md_dissemination_service;
    ss::sharded<kafka::coordinator_ntp_mapper> coordinator_ntp_mapper;
    std::unique_ptr<cluster::controller> controller;
    ss::sharded<kafka::fetch_session_cache> fetch_session_cache;
    smp_groups smp_service_groups;
    ss::sharded<kafka::quota_manager> quota_mgr;
    ss::sharded<cluster::id_allocator_frontend> id_allocator_frontend;
    ss::sharded<cloud_storage::remote> cloud_storage_api;
    ss::sharded<cloud_storage::partition_recovery_manager>
      partition_recovery_manager;
    ss::sharded<archival::scheduler_service> archival_scheduler;
    ss::sharded<kafka::rm_group_frontend> rm_group_frontend;
    ss::sharded<cluster::rm_partition_frontend> rm_partition_frontend;
    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<v8_engine::data_policy_table> data_policies;
    ss::sharded<cloud_storage::cache> shadow_index_cache;

private:
    using deferred_actions
      = std::vector<ss::deferred_action<std::function<void()>>>;

    // All methods are calleds from Seastar thread
    void init_env();
    ss::app_template::config setup_app_config();
    void validate_arguments(const po::variables_map&);
    void hydrate_config(const po::variables_map&);

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
    std::optional<pandaproxy::rest::configuration> _proxy_config;
    std::optional<kafka::client::configuration> _proxy_client_config;
    std::optional<pandaproxy::schema_registry::configuration>
      _schema_reg_config;
    std::optional<kafka::client::configuration> _schema_reg_client_config;
    scheduling_groups _scheduling_groups;
    ss::logger _log;

    ss::sharded<rpc::connection_cache> _raft_connection_cache;
    ss::sharded<kafka::group_manager> _group_manager;
    ss::sharded<rpc::server> _rpc;
    ss::sharded<admin_server> _admin;
    ss::sharded<rpc::server> _kafka_server;
    ss::sharded<kafka::client::client> _proxy_client;
    ss::sharded<pandaproxy::rest::proxy> _proxy;
    std::unique_ptr<pandaproxy::schema_registry::api> _schema_registry;
    ss::sharded<storage::compaction_controller> _compaction_controller;

    ss::metrics::metric_groups _metrics;
    std::unique_ptr<kafka::rm_group_proxy_impl> _rm_group_proxy;
    // run these first on destruction
    deferred_actions _deferred;
};

namespace debug {
extern application* app;
}
