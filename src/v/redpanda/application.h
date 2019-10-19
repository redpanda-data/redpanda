#pragma once

#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/config/configuration.h"
#include "redpanda/kafka/controller_dispatcher.h"
#include "redpanda/kafka/groups/group_manager.h"
#include "redpanda/kafka/groups/group_router.h"
#include "redpanda/kafka/groups/group_shard_mapper.h"
#include "redpanda/kafka/transport/server.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/smp_groups.h"
#include "rpc/server.h"
#include "seastarx.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/httpd.hh>

class stop_signal {
    void signaled() {
        _caught = true;
        _cond.broadcast();
    }

public:
    stop_signal() {
        engine().handle_signal(SIGINT, [this] { signaled(); });
        engine().handle_signal(SIGTERM, [this] { signaled(); });
    }

    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op
        // handler instead.
        engine().handle_signal(SIGINT, [] {});
        engine().handle_signal(SIGTERM, [] {});
    }

    future<> wait() {
        return _cond.wait([this] { return _caught; });
    }

    bool stopping() const {
        return _caught;
    }

private:
    bool _caught = false;
    condition_variable _cond;
};

namespace po = boost::program_options; // NOLINT
class application {
public:
    int run(int, char**);

private:
    using deferred_actions
      = std::vector<deferred_action<std::function<void()>>>;

    // All methods are calleds from Seastar thread
    void init_env();
    app_template setup_app_template();
    void create_groups();
    void validate_arguments(const po::variables_map&);
    void hydrate_config(const po::variables_map&);
    void check_environment();
    void wire_up_services();
    void configure_admin_server();
    template<typename Service, typename... Args>
    future<> construct_service(sharded<Service>& s, Args&&... args) {
        auto f = s.start(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] { s.stop().get(); });
        return f;
    }

    std::unique_ptr<app_template> _app;
    scheduling_groups _scheduling_groups;
    memory_groups _memory_groups;
    smp_groups _smp_groups;
    deferred_actions _deferred;
    logger _log{"redpanda::main"};

    // sharded services
    sharded<config::configuration> _conf;
    sharded<raft::client_cache> _raft_client_cache;
    sharded<cluster::shard_table> _shard_table;
    sharded<cluster::partition_manager> _partition_manager;
    sharded<kafka::groups::group_manager> _group_manager;
    sharded<kafka::groups::group_shard_mapper<cluster::shard_table>>
      _group_shard_mapper;
    sharded<kafka::group_router_type> _group_router;
    sharded<rpc::server> _rpc;
    sharded<http_server> _admin;
    sharded<cluster::metadata_cache> _metadata_cache;
    sharded<kafka::transport::quota_manager> _quota_mgr;
    sharded<kafka::controller_dispatcher> _cntrl_dispatcher;
    sharded<kafka::transport::kafka_server> _kafka_server;
    std::unique_ptr<cluster::controller> _controller;
};
