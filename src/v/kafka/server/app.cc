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
#include "kafka/server/app.h"

#include "kafka/server/server.h"

#include <memory>

namespace kafka {

seastar::future<> server_app::init(
  seastar::sharded<net::server_configuration>* conf,
  seastar::smp_service_group smp,
  seastar::scheduling_group sched,
  seastar::sharded<cluster::metadata_cache>& mdc,
  seastar::sharded<cluster::topics_frontend>& tf,
  seastar::sharded<cluster::config_frontend>& cf,
  seastar::sharded<features::feature_table>& ft,
  seastar::sharded<cluster::client_quota::frontend>& cqf,
  seastar::sharded<cluster::client_quota::store>& cqs,
  seastar::sharded<quota_manager>& qm,
  seastar::sharded<snc_quota_manager>& snc_mgr,
  seastar::sharded<kafka::group_router>& gr,
  seastar::sharded<kafka::usage_manager>& um,
  seastar::sharded<cluster::shard_table>& st,
  seastar::sharded<cluster::partition_manager>& pm,
  seastar::sharded<cluster::id_allocator_frontend>& idaf,
  seastar::sharded<security::credential_store>& cs,
  seastar::sharded<security::authorizer>& auth,
  seastar::sharded<security::audit::audit_log_manager>& audit,
  seastar::sharded<security::oidc::service>& oidc,
  seastar::sharded<cluster::security_frontend>& sec,
  seastar::sharded<cluster::controller_api>& ctrl,
  seastar::sharded<cluster::tx_gateway_frontend>& tx,
  std::optional<qdc_monitor_config> qdc,
  ssx::singleton_thread_worker& worker,
  const std::unique_ptr<pandaproxy::schema_registry::api>& pp) {
    return _server.start(
      conf,
      smp,
      sched,
      std::ref(mdc),
      std::ref(tf),
      std::ref(cf),
      std::ref(ft),
      std::ref(cqf),
      std::ref(cqs),
      std::ref(qm),
      std::ref(snc_mgr),
      std::ref(gr),
      std::ref(um),
      std::ref(st),
      std::ref(pm),
      std::ref(idaf),
      std::ref(cs),
      std::ref(auth),
      std::ref(audit),
      std::ref(oidc),
      std::ref(sec),
      std::ref(ctrl),
      std::ref(tx),
      qdc,
      std::ref(worker),
      std::ref(pp));
}

server_app::~server_app() = default;

seastar::future<> server_app::start() {
    return _server.invoke_on_all(&net::server::start);
}

seastar::future<> server_app::shutdown_input() {
    return _server.invoke_on_all(&net::server::shutdown_input);
}

seastar::future<> server_app::wait_for_shutdown() {
    return _server.invoke_on_all(&net::server::wait_for_shutdown);
}

seastar::future<> server_app::stop() { return _server.stop(); }

} // namespace kafka
