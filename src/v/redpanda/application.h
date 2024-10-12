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

#include "base/seastarx.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cluster/archival/fwd.h"
#include "cluster/config_manager.h"
#include "cluster/fwd.h"
#include "cluster/inventory_service.h"
#include "cluster/migrations/tx_manager_migrator.h"
#include "cluster/node/local_monitor.h"
#include "cluster/node_status_backend.h"
#include "cluster/node_status_table.h"
#include "cluster/self_test_backend.h"
#include "cluster/self_test_frontend.h"
#include "cluster/tx_coordinator_mapper.h"
#include "config/node_config.h"
#include "crypto/ossl_context_service.h"
#include "datalake/fwd.h"
#include "debug_bundle/fwd.h"
#include "features/fwd.h"
#include "finjector/stress_fiber.h"
#include "kafka/client/configuration.h"
#include "kafka/client/fwd.h"
#include "kafka/server/fwd.h"
#include "kafka/server/snc_quota_manager.h"
#include "metrics/aggregate_metrics_watcher.h"
#include "metrics/metrics.h"
#include "net/conn_quota.h"
#include "net/fwd.h"
#include "pandaproxy/fwd.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/fwd.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "raft/fwd.h"
#include "redpanda/monitor_unsafe.h"
#include "resource_mgmt/cpu_profiler.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/memory_sampling.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "resource_mgmt/smp_groups.h"
#include "resource_mgmt/storage.h"
#include "rpc/fwd.h"
#include "rpc/rpc_server.h"
#include "security/fwd.h"
#include "storage/api.h"
#include "storage/fwd.h"
#include "transform/fwd.h"
#include "utils/stop_signal.h"
#include "wasm/fwd.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

namespace po = boost::program_options; // NOLINT

class admin_server;

namespace cluster {
class cluster_discovery;
} // namespace cluster

inline const auto redpanda_start_time{
  std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch())};

class application {
public:
    int run(int, char**);

    void initialize(
      std::optional<YAML::Node> proxy_cfg = std::nullopt,
      std::optional<YAML::Node> proxy_client_cfg = std::nullopt,
      std::optional<YAML::Node> schema_reg_cfg = std::nullopt,
      std::optional<YAML::Node> schema_reg_client_cfg = std::nullopt,
      std::optional<YAML::Node> audit_log_client_cfg = std::nullopt,
      std::optional<scheduling_groups> = std::nullopt);
    void check_environment();
    void wire_up_and_start(::stop_signal&, bool test_mode = false);
    void post_start_tasks();

    void check_for_crash_loop();
    void schedule_crash_tracker_file_cleanup();

    explicit application(ss::sstring = "main");
    ~application();

    void shutdown();

    ss::future<> set_proxy_config(ss::sstring name, std::any val);
    ss::future<> set_proxy_client_config(ss::sstring name, std::any val);

    smp_groups smp_service_groups;
    scheduling_groups sched_groups;
    ss::sharded<stress_fiber_manager> stress_fiber_manager;

    // Sorted list of services (public members)
    ss::sharded<cloud_storage::cache> shadow_index_cache;
    ss::sharded<cloud_storage::partition_recovery_manager>
      partition_recovery_manager;
    ss::sharded<cloud_storage_clients::client_pool> cloud_storage_clients;
    ss::sharded<cloud_io::remote> cloud_io;
    ss::sharded<cloud_storage::remote> cloud_storage_api;
    ss::sharded<archival::upload_housekeeping_service>
      archival_upload_housekeeping;
    ss::sharded<archival::archiver_manager> archiver_manager;
    ss::sharded<cluster::topic_recovery_status_frontend>
      topic_recovery_status_frontend;
    ss::sharded<cloud_storage::topic_recovery_service> topic_recovery_service;
    ss::sharded<cluster::inventory_service> inventory_service;

    ss::sharded<cluster::tx_coordinator_mapper> tx_coordinator_ntp_mapper;
    ss::sharded<cluster::id_allocator_frontend> id_allocator_frontend;
    ss::sharded<cluster::metadata_cache> metadata_cache;
    ss::sharded<cluster::metadata_dissemination_service>
      md_dissemination_service;
    ss::sharded<cluster::node_status_backend> node_status_backend;
    ss::sharded<cluster::node_status_table> node_status_table;
    ss::sharded<cluster::partition_manager> partition_manager;
    ss::sharded<cluster::tx::producer_state_manager> producer_manager;
    ss::sharded<cluster::rm_partition_frontend> rm_partition_frontend;
    ss::sharded<cluster::self_test_backend> self_test_backend;
    ss::sharded<cluster::self_test_frontend> self_test_frontend;
    ss::sharded<cluster::shard_table> shard_table;
    // only one instance on core 0
    ss::sharded<cluster::tx_topic_manager> tx_topic_manager;
    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;

    ss::sharded<features::feature_table> feature_table;

    // Services required for consumer offsets trimming and recovery.
    ss::sharded<cluster::cloud_metadata::offsets_lookup> offsets_lookup;
    ss::sharded<cluster::cloud_metadata::offsets_recoverer> offsets_recoverer;
    ss::sharded<cluster::cloud_metadata::offsets_recovery_router>
      offsets_recovery_router;

    ss::shared_ptr<cluster::cloud_metadata::offsets_recovery_manager>
      offsets_recovery_manager;

    // Services required for consumer offsets snapshotting.
    ss::sharded<cluster::cloud_metadata::offsets_uploader> offsets_uploader;
    ss::sharded<cluster::cloud_metadata::offsets_upload_router>
      offsets_upload_router;

    ss::shared_ptr<cluster::cloud_metadata::producer_id_recovery_manager>
      producer_id_recovery_manager;

    ss::sharded<kafka::coordinator_ntp_mapper> coordinator_ntp_mapper;
    ss::sharded<kafka::group_router> group_router;
    ss::sharded<kafka::quota_manager> quota_mgr;
    kafka::snc_quota_manager::buckets_t snc_node_quota;
    ss::sharded<kafka::snc_quota_manager> snc_quota_mgr;
    ss::sharded<kafka::rm_group_frontend> rm_group_frontend;
    ss::sharded<kafka::usage_manager> usage_manager;

    ss::sharded<security::audit::audit_log_manager> audit_mgr;

    ss::sharded<raft::group_manager> raft_group_manager;
    ss::sharded<raft::coordinated_recovery_throttle> recovery_throttle;

    ss::sharded<storage::api> storage;
    ss::sharded<storage::node> storage_node;
    ss::sharded<cluster::node::local_monitor> local_monitor;
    std::unique_ptr<storage::disk_space_manager> space_manager;

    std::unique_ptr<cluster::controller> controller;

    std::unique_ptr<ssx::singleton_thread_worker> thread_worker;

    ss::sharded<crypto::ossl_context_service> ossl_context_service;
    ss::sharded<kafka::server> _kafka_server;
    ss::sharded<rpc::connection_cache> _connection_cache;
    ss::sharded<kafka::group_manager> _group_manager;
    ss::sharded<experimental::cloud_topics::reconciler::reconciler> _reconciler;

    const std::unique_ptr<pandaproxy::schema_registry::api>& schema_registry() {
        return _schema_registry;
    }

    ss::sharded<transform::rpc::client>& transforms_client() {
        return _transform_rpc_client;
    }

private:
    using deferred_actions
      = std::deque<ss::deferred_action<std::function<void()>>>;

    struct crash_tracker_metadata
      : serde::envelope<
          crash_tracker_metadata,
          serde::version<0>,
          serde::compat_version<0>> {
        uint32_t _crash_count{0};
        uint64_t _config_checksum{0};
        model::timestamp _last_start_ts;

        auto serde_fields() {
            return std::tie(_crash_count, _config_checksum, _last_start_ts);
        }
    };

    // Constructs and starts the services required to provide cryptographic
    // algorithm support to Redpanda
    void wire_up_and_start_crypto_services();

    // Constructs services across shards required to get bootstrap metadata.
    void wire_up_bootstrap_services();

    // Starts services across shards required to get bootstrap metadata.
    void start_bootstrap_services();

    // Constructs services across shards meant for Redpanda runtime.
    void
    wire_up_runtime_services(model::node_id node_id, ::stop_signal& app_signal);
    void configure_admin_server();
    void wire_up_redpanda_services(model::node_id, ::stop_signal& app_signal);

    void load_feature_table_snapshot();

    void trigger_abort_source();

    // Starts the services meant for Redpanda runtime. Must be called after
    // having constructed the subsystems via the corresponding `wire_up` calls.
    void start_runtime_services(cluster::cluster_discovery&, ::stop_signal&);
    void start_kafka(const model::node_id&, ::stop_signal&);

    // All methods are calleds from Seastar thread
    ss::app_template::config setup_app_config();
    void validate_arguments(const po::variables_map&);
    void hydrate_config(const po::variables_map&);

    bool archival_storage_enabled();

    bool wasm_data_transforms_enabled();

    bool datalake_enabled();

    /**
     * @brief Construct service boilerplate.
     *
     * Construct the given service s, calling start with the given arguments
     * and set up a shutdown callback to stop it.
     *
     * Returns the future from start(), typically you'll call get() on it
     * immediately to wait for creation to complete.
     *
     * @return the future returned by start()
     */
    template<typename Service, typename... Args>
    ss::future<> construct_service(ss::sharded<Service>& s, Args&&... args) {
        _deferred.emplace_back([&s] { s.stop().get(); });
        return s.start(std::forward<Args>(args)...);
    }

    template<typename Service, typename... Args>
    void construct_single_service(std::unique_ptr<Service>& s, Args&&... args) {
        s = std::make_unique<Service>(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] {
            s->stop().get();
            s.reset();
        });
    }

    template<typename Service, typename... Args>
    ss::future<>
    construct_single_service_sharded(ss::sharded<Service>& s, Args&&... args) {
        auto f = s.start_single(std::forward<Args>(args)...);
        _deferred.emplace_back([&s] { s.stop().get(); });
        return f;
    }

    void setup_metrics();
    void setup_public_metrics();
    void setup_internal_metrics();
    std::unique_ptr<ss::app_template> _app;

    // Early in startup, we load config from disk or from the response to
    // a cluster join request: this is used to prime config_manager's state
    // so that the config doesn't walk through all intermediate states
    // in the log during startup.
    cluster::config_manager::preload_result _config_preload;

    // When joining a cluster, we are tipped off as to the last applied offset
    // of the controller stm from another node.  We will wait for this offset
    // to be replicated to our controller log before listening for Kafka
    // requests.
    std::optional<model::offset> _await_controller_last_applied;

    std::optional<pandaproxy::rest::configuration> _proxy_config;
    std::optional<kafka::client::configuration> _proxy_client_config;
    std::optional<pandaproxy::schema_registry::configuration>
      _schema_reg_config;
    std::optional<kafka::client::configuration> _schema_reg_client_config;
    std::optional<kafka::client::configuration> _audit_log_client_config;
    ss::sharded<scheduling_groups_probe> _scheduling_groups_probe;
    ss::logger _log;

    std::optional<config::binding<bool>> _abort_on_oom;

    ss::sharded<memory_sampling> _memory_sampling;
    ss::sharded<rpc::rpc_server> _rpc;
    ss::sharded<admin_server> _admin;
    ss::sharded<net::conn_quota> _kafka_conn_quotas;
    std::unique_ptr<pandaproxy::rest::api> _proxy;
    std::unique_ptr<pandaproxy::schema_registry::api> _schema_registry;
    ss::sharded<storage::compaction_controller> _compaction_controller;
    ss::sharded<archival::upload_controller> _archival_upload_controller;
    std::unique_ptr<monitor_unsafe> _monitor_unsafe;
    ss::sharded<archival::purger> _archival_purger;

    std::unique_ptr<wasm::caching_runtime> _wasm_runtime;
    ss::sharded<transform::service> _transform_service;
    ss::sharded<transform::rpc::local_service> _transform_rpc_service;
    ss::sharded<transform::rpc::client> _transform_rpc_client;

    metrics::internal_metric_groups _metrics;
    ss::sharded<metrics::public_metrics_group_service> _public_metrics;
    std::unique_ptr<kafka::rm_group_proxy_impl> _rm_group_proxy;

    ss::sharded<resources::cpu_profiler> _cpu_profiler;
    ss::sharded<debug_bundle::service> _debug_bundle_service;

    std::unique_ptr<cluster::node_isolation_watcher> _node_isolation_watcher;

    // Small helpers to execute one-time upgrade actions
    std::vector<std::unique_ptr<features::feature_migrator>> _migrators;

    ss::sharded<datalake::coordinator::coordinator_manager>
      _datalake_coordinator_mgr;
    ss::sharded<datalake::coordinator::frontend> _datalake_coordinator_fe;
    ss::sharded<datalake::datalake_manager> _datalake_manager;

    // run these first on destruction
    deferred_actions _deferred;

    ss::sharded<aggregate_metrics_watcher> _aggregate_metrics_watcher;

    // instantiated only in recovery mode
    std::unique_ptr<cluster::tx_manager_migrator> _tx_manager_migrator;

    config::node_override_store _node_overrides{};

    ss::sharded<ss::abort_source> _as;
};

namespace debug {
extern application* app;
}
