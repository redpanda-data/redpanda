#pragma once

#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "coproc/service.h"
#include "kafka/groups/coordinator_ntp_mapper.h"
#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/quota_manager.h"
#include "raft/group_manager.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/smp_groups.h"
#include "rpc/server.h"
#include "seastarx.h"
#include "storage/api.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>
#include <seastar/util/defer.hh>

namespace po = boost::program_options; // NOLINT
using group_router_type = kafka::group_router<kafka::group_manager>;

class application {
public:
    int run(int, char**);

    void initialize();
    void check_environment();
    void configure_admin_server();
    void wire_up_services();
    void start();

    void shutdown() {
        while (!_deferred.empty()) {
            _deferred.pop_back();
        }
    }

    ss::sharded<cluster::metadata_cache> metadata_cache;
    ss::sharded<group_router_type> group_router;
    ss::sharded<cluster::shard_table> shard_table;
    ss::sharded<storage::api> storage;
    ss::sharded<coproc::active_mappings> mappings;
    ss::sharded<cluster::partition_manager> partition_manager;
    ss::sharded<raft::group_manager> raft_group_manager;
    ss::sharded<cluster::metadata_dissemination_service>
      md_dissemination_service;
    ss::sharded<kafka::coordinator_ntp_mapper> coordinator_ntp_mapper;
    std::unique_ptr<cluster::controller> controller;

private:
    using deferred_actions
      = std::vector<ss::deferred_action<std::function<void()>>>;

    // All methods are calleds from Seastar thread
    void init_env();
    ss::app_template setup_app_template();
    void validate_arguments(const po::variables_map&);
    void hydrate_config(const po::variables_map&);

    void admin_register_raft_routes(ss::http_server& server);

    bool coproc_enabled() {
        const auto& cfg = config::shard_local_cfg();
        return cfg.developer_mode() && cfg.enable_coproc();
    }

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
    scheduling_groups _scheduling_groups;
    smp_groups _smp_groups;
    ss::logger _log{"redpanda::main"};

    ss::sharded<rpc::server> _coproc_rpc;
    ss::sharded<rpc::connection_cache> _raft_connection_cache;
    ss::sharded<kafka::group_manager> _group_manager;
    ss::sharded<rpc::server> _rpc;
    ss::sharded<ss::http_server> _admin;
    ss::sharded<kafka::quota_manager> _quota_mgr;
    ss::sharded<rpc::server> _kafka_server;
    ss::metrics::metric_groups _metrics;
    // run these first on destruction
    deferred_actions _deferred;
};
