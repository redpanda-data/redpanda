/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/server/queue_depth_monitor_config.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace cluster {
class metadata_cache;
class topics_frontend;
class config_frontend;
class shard_table;
class partition_manager;
class id_allocator_frontend;
class security_frontend;
class controller_api;
class tx_gateway_frontend;
namespace client_quota {
class frontend;
class store;
} // namespace client_quota
} // namespace cluster

namespace security {
class credential_store;
class authorizer;
namespace audit {
class audit_log_manager;
}
namespace oidc {
class service;
}
} // namespace security

namespace pandaproxy::schema_registry {
class api;
}

namespace ssx {
class singleton_thread_worker;
}

namespace net {
struct server_configuration;
}

namespace features {
class feature_table;
}

namespace kafka {

class server;
class quota_manager;
class snc_quota_manager;
class group_router;
class usage_manager;

class server_app {
public:
    server_app() = default;
    server_app(const server_app&) = delete;
    server_app& operator=(const server_app&) = delete;
    server_app(server_app&&) noexcept = delete;
    server_app& operator=(server_app&&) noexcept = delete;
    ~server_app();

    seastar::future<> init(
      seastar::sharded<net::server_configuration>*,
      seastar::smp_service_group,
      seastar::scheduling_group,
      seastar::sharded<cluster::metadata_cache>&,
      seastar::sharded<cluster::topics_frontend>&,
      seastar::sharded<cluster::config_frontend>&,
      seastar::sharded<features::feature_table>&,
      seastar::sharded<cluster::client_quota::frontend>&,
      seastar::sharded<cluster::client_quota::store>&,
      seastar::sharded<quota_manager>&,
      seastar::sharded<snc_quota_manager>&,
      seastar::sharded<kafka::group_router>&,
      seastar::sharded<kafka::usage_manager>&,
      seastar::sharded<cluster::shard_table>&,
      seastar::sharded<cluster::partition_manager>&,
      seastar::sharded<cluster::id_allocator_frontend>&,
      seastar::sharded<security::credential_store>&,
      seastar::sharded<security::authorizer>&,
      seastar::sharded<security::audit::audit_log_manager>&,
      seastar::sharded<security::oidc::service>&,
      seastar::sharded<cluster::security_frontend>&,
      seastar::sharded<cluster::controller_api>&,
      seastar::sharded<cluster::tx_gateway_frontend>&,
      std::optional<qdc_monitor_config>,
      ssx::singleton_thread_worker&,
      const std::unique_ptr<pandaproxy::schema_registry::api>&);

    seastar::future<> start();
    seastar::future<> shutdown_input();
    seastar::future<> wait_for_shutdown();
    seastar::future<> stop();

    seastar::sharded<server>& ref() { return _server; }
    server& local() { return _server.local(); }

private:
    seastar::sharded<server> _server;
};

} // namespace kafka
