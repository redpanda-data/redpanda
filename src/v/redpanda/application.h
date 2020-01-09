#pragma once

#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "kafka/controller_dispatcher.h"
#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/groups/group_shard_mapper.h"
#include "kafka/server.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/smp_groups.h"
#include "rpc/server.h"
#include "seastarx.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>

namespace po = boost::program_options; // NOLINT
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
    ss::sharded<kafka::group_router_type> group_router;
    ss::sharded<kafka::controller_dispatcher> cntrl_dispatcher;
    ss::sharded<cluster::shard_table> shard_table;
    ss::sharded<cluster::partition_manager> partition_manager;
    std::unique_ptr<cluster::controller> controller;

private:
    using deferred_actions
      = std::vector<ss::deferred_action<std::function<void()>>>;

    // All methods are calleds from Seastar thread
    void init_env();
    ss::app_template setup_app_template();
    void validate_arguments(const po::variables_map&);
    void hydrate_config(const po::variables_map&);

    template<typename Service, typename... Args>
    ss::future<> construct_service(ss::sharded<Service>& s, Args&&... args) {
        auto f = s.start(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] { s.stop().get(); });
        return f;
    }

    std::unique_ptr<ss::app_template> _app;
    scheduling_groups _scheduling_groups;
    memory_groups _memory_groups;
    smp_groups _smp_groups;
    ss::logger _log{"redpanda::main"};

    // ss::sharded services
    ss::sharded<rpc::connection_cache> _raft_connection_cache;
    ss::sharded<kafka::group_manager> _group_manager;
    ss::sharded<kafka::group_shard_mapper<cluster::shard_table>>
      _group_shard_mapper;
    ss::sharded<rpc::server> _rpc;
    ss::sharded<ss::http_server> _admin;
    ss::sharded<kafka::quota_manager> _quota_mgr;
    ss::sharded<kafka::kafka_server> _kafka_server;

    // run these first on destruction
    deferred_actions _deferred;
};
