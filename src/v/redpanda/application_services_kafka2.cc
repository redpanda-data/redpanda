// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/server/logger.h"
#include "kafka/server/queue_depth_monitor_config.h"
#include "net/conn_quota.h"
#include "net/dns.h"
#include "net/server.h"
#include "net/tls_certificate_probe.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "rpc/rpc_utils.h"
#include "storage/compaction_controller.h"
#include "syschecks/syschecks.h"

#include <seastar/core/thread.hh>
#include <seastar/net/tls.hh>

// Forward declaration (defined in application_config.cc)
storage::backlog_controller_config
compaction_controller_config(ss::scheduling_group sg, uint64_t fs_avail);

void application::wire_up_services_kafka_server(uint64_t fs_avail) {
    _kafka_conn_quotas
      .start(
        []() {
            return net::conn_quota_config{
              .max_connections
              = config::shard_local_cfg().kafka_connections_max.bind(),
              .max_connections_per_ip
              = config::shard_local_cfg().kafka_connections_max_per_ip.bind(),
              .max_connections_overrides
              = config::shard_local_cfg()
                  .kafka_connections_max_overrides.bind(),
            };
        },
        &kafka::klog)
      .get();

    ss::sharded<net::server_configuration> kafka_cfg;
    kafka_cfg.start(ss::sstring("kafka_rpc")).get();
    auto kafka_cfg_cleanup = ss::defer(
      [&kafka_cfg]() { kafka_cfg.stop().get(); });
    kafka_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              c.conn_quotas = std::ref(_kafka_conn_quotas);
              c.max_service_memory_per_core = int64_t(
                memory_groups().kafka_total_memory());
              c.listen_backlog
                = config::shard_local_cfg().rpc_server_listen_backlog;
              if (config::shard_local_cfg().kafka_rpc_server_tcp_recv_buf()) {
                  c.tcp_recv_buf
                    = config::shard_local_cfg().kafka_rpc_server_tcp_recv_buf;
              } else {
                  // Backward compat: prior to Redpanda 22.2, rpc_server_*
                  // settings applied to both Kafka and Internal RPC listeners.
                  c.tcp_recv_buf
                    = config::shard_local_cfg().rpc_server_tcp_recv_buf;
              };
              if (config::shard_local_cfg().kafka_rpc_server_tcp_send_buf()) {
                  c.tcp_send_buf
                    = config::shard_local_cfg().kafka_rpc_server_tcp_send_buf;
              } else {
                  // Backward compat: prior to Redpanda 22.2, rpc_server_*
                  // settings applied to both Kafka and Internal RPC listeners.
                  c.tcp_send_buf
                    = config::shard_local_cfg().rpc_server_tcp_send_buf;
              }

              c.stream_recv_buf
                = config::shard_local_cfg().kafka_rpc_server_stream_recv_buf;
              auto& tls_config = config::node().kafka_api_tls.value();
              for (const auto& ep : config::node().kafka_api()) {
                  ss::shared_ptr<ss::tls::server_credentials> credentials
                    = nullptr;
                  // find credentials for this endpoint
                  auto it = find_if(
                    tls_config.begin(),
                    tls_config.end(),
                    [&ep](const config::endpoint_tls_config& cfg) {
                        return cfg.name == ep.name;
                    });
                  // if tls is configured for this endpoint build reloadable
                  // credentials
                  if (it != tls_config.end()) {
                      syschecks::systemd_message(
                        "Building TLS credentials for kafka")
                        .get();
                      credentials
                        = net::build_reloadable_server_credentials_with_probe(
                            it->config,
                            "kafka",
                            it->name,
                            [this](
                              const std::unordered_set<ss::sstring>& updated,
                              const std::exception_ptr& eptr) {
                                rpc::log_certificate_reload_event(
                                  _log, "Kafka RPC TLS", updated, eptr);
                            })
                            .get();
                  }

                  c.addrs.emplace_back(
                    ep.name, net::resolve_dns(ep.address).get(), credentials);
              }

              c.disable_metrics = net::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
              c.disable_public_metrics = net::public_metrics_disabled(
                config::shard_local_cfg().disable_public_metrics());

              net::config_connection_rate_bindings bindings{
                .config_general_rate
                = config::shard_local_cfg().kafka_connection_rate_limit.bind(),
                .config_overrides_rate
                = config::shard_local_cfg()
                    .kafka_connection_rate_limit_overrides.bind(),
              };

              c.connection_rate_bindings.emplace(std::move(bindings));

              c.tcp_keepalive_bindings.emplace(
                net::tcp_keepalive_bindings{
                  .keepalive_idle_time
                  = config::shard_local_cfg()
                      .kafka_tcp_keepalive_idle_timeout_seconds.bind(),
                  .keepalive_interval
                  = config::shard_local_cfg()
                      .kafka_tcp_keepalive_probe_interval_seconds.bind(),
                  .keepalive_probes
                  = config::shard_local_cfg().kafka_tcp_keepalive_probes.bind(),
                });
          });
      })
      .get();
    std::optional<kafka::qdc_monitor_config> qdc_config;
    if (config::shard_local_cfg().kafka_qdc_enable()) {
        qdc_config = kafka::qdc_monitor_config{
          .latency_alpha = config::shard_local_cfg().kafka_qdc_latency_alpha(),
          .max_latency = config::shard_local_cfg().kafka_qdc_max_latency_ms(),
          .window_count = config::shard_local_cfg().kafka_qdc_window_count(),
          .window_size = config::shard_local_cfg().kafka_qdc_window_size_ms(),
          .depth_alpha = config::shard_local_cfg().kafka_qdc_depth_alpha(),
          .idle_depth = config::shard_local_cfg().kafka_qdc_idle_depth(),
          .min_depth = config::shard_local_cfg().kafka_qdc_min_depth(),
          .max_depth = config::shard_local_cfg().kafka_qdc_max_depth(),
          .depth_update_freq
          = config::shard_local_cfg().kafka_qdc_depth_update_ms(),
        };
    }
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg.local())
      .get();
    _kafka_server
      .init(
        &kafka_cfg,
        smp_service_groups.kafka_smp_sg(),
        scheduling_groups::instance().fetch_sg(),
        scheduling_groups::instance().produce_sg(),
        scheduling_groups::instance().kafka_sg(),
        std::ref(metadata_cache),
        std::ref(controller->get_topics_frontend()),
        std::ref(controller->get_config_frontend()),
        std::ref(controller->get_feature_table()),
        std::ref(controller->get_quota_frontend()),
        std::ref(controller->get_quota_store()),
        std::ref(quota_mgr),
        std::ref(snc_quota_mgr),
        std::ref(group_router),
        std::ref(usage_manager),
        std::ref(shard_table),
        std::ref(partition_manager),
        std::ref(id_allocator_frontend),
        std::ref(controller->get_credential_store()),
        std::ref(controller->get_authorizer()),
        std::ref(audit_mgr),
        std::ref(controller->get_oidc_service()),
        std::ref(controller->get_security_frontend()),
        std::ref(controller->get_api()),
        std::ref(tx_gateway_frontend),
        std::ref(datalake_throttle_manager),
        std::ref(controller->get_cluster_link_frontend()),
        qdc_config,
        std::ref(*thread_worker),
        std::ref(_schema_registry))
      .get();
    construct_service(
      _compaction_controller,
      std::ref(storage),
      ss::sharded_parameter(
        [sg = scheduling_groups::instance().compaction_sg(), fs_avail] {
            return compaction_controller_config(sg, fs_avail);
        }))
      .get();
}
