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
#include "cloud_topics/app.h"
#include "cloud_topics/test_fixture_cfg.h"
#include "cluster/archival/fwd.h"
#include "cluster/cluster_discovery.h"
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
#include "cluster_link/fwd.h"
#include "config/node_config.h"
#include "crash_tracker/service.h"
#include "crypto/ossl_context_service.h"
#include "datalake/credential_manager.h"
#include "datalake/fwd.h"
#include "debug_bundle/fwd.h"
#include "features/feature_table_snapshot.h"
#include "features/fwd.h"
#include "finjector/stress_fiber.h"
#include "kafka/client/configuration.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/service.h"
#include "kafka/server/app.h"
#include "kafka/server/data_migration_group_proxy_impl.h"
#include "kafka/server/snc_quota_manager.h"
#include "metrics/aggregate_metrics_watcher.h"
#include "metrics/host_metrics_watcher.h"
#include "metrics/instance_metrics.h"
#include "metrics/metrics.h"
#include "net/conn_quota.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/fwd.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/monitor_unsafe.h"
#include "resource_mgmt/cpu_profiler.h"
#include "resource_mgmt/memory_sampling.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "resource_mgmt/smp_groups.h"
#include "resource_mgmt/storage.h"
#include "rpc/rpc_server.h"
#include "ssx/sharded_service_container.h"
#include "storage/api.h"
#include "transform/fwd.h"
#include "utils/stop_signal.h"
#include "wasm/fwd.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace po = boost::program_options; // NOLINT

class admin_server;

namespace cluster {
class cluster_discovery;
} // namespace cluster

namespace cloud_storage_clients {
class client_pool;
class upstream_registry;
} // namespace cloud_storage_clients

inline const auto redpanda_start_time{
  std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch())};

// Knobs used to tweak start-up behavior, primarily for tests. In production
// the defaults apply.
struct test_cfg {
    // Cloud-topics test-fixture overrides (flush loop, level-zero GC, etc.).
    cloud_topics::test_fixture_cfg ct_test_cfg{};
    // Whether to eagerly pre-allocate the per-shard chunk cache pool on
    // storage start. Multi-node fixture tests that run several brokers in one
    // reactor may want to disable it to avoid N-way memory allocations for each
    // chunk cache.
    bool chunk_cache_prealloc{true};
};

class application : public ssx::sharded_service_container {
public:
    int run(int, char**);

    void initialize(
      std::optional<YAML::Node> proxy_cfg = std::nullopt,
      std::optional<YAML::Node> proxy_client_cfg = std::nullopt,
      std::optional<YAML::Node> schema_reg_cfg = std::nullopt,
      std::optional<YAML::Node> schema_reg_client_cfg = std::nullopt,
      std::optional<YAML::Node> audit_log_client_cfg = std::nullopt);
    void check_environment();
    void wire_up_and_start(
      ::stop_signal&, bool test_mode = false, test_cfg cfg = {});
    void post_start_tasks();

    void init_crashtracker(::stop_signal& app_signal);
    void schedule_crash_tracker_file_cleanup();

    explicit application(ss::sstring = "main");
    ~application();

    void shutdown();

    ss::future<> set_proxy_config(ss::sstring name, std::any val);
    ss::future<> set_proxy_client_config(ss::sstring name, std::any val);

    smp_groups smp_service_groups;
    ss::sharded<stress_fiber_manager> stress_fiber_manager;

    // Sorted list of services (public members)
    ss::sharded<cloud_io::cache> shadow_index_cache;
    ss::sharded<cloud_storage::partition_recovery_manager>
      partition_recovery_manager;
    ss::sharded<cloud_storage_clients::upstream_registry> upstreams;
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
    ss::sharded<kafka::group_initializer> group_initializer;
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
    ss::sharded<kafka::datalake_throttle_manager> datalake_throttle_manager;

    kafka::server_app _kafka_server;
    ss::sharded<rpc::connection_cache> _connection_cache;
    ss::sharded<kafka::group_manager> _group_manager;
    std::unique_ptr<cloud_topics::app> cloud_topics_app;

    ss::sharded<cluster_link::service> _cluster_link_service;

    const std::unique_ptr<pandaproxy::schema_registry::api>& schema_registry() {
        return _schema_registry;
    }

    ss::sharded<transform::rpc::client>& transforms_client() {
        return _transform_rpc_client;
    }

    ss::sharded<datalake::coordinator::frontend>&
    datalake_coordinator_frontend() {
        return _datalake_coordinator_fe;
    }

    // At a minimum, we need to construct the feature table and storage systems
    // in order to properly bootstrap the system. Public for test fixture
    // access.
    void wire_up_bootstrap_services();

    // We need the RPC server and bootstrap service (at a minimum) running
    // before cluster discovery can be performed.
    void wire_up_and_start_rpc_service();

    // Before we can continue in the bootstrap process, we need to establish
    // a consistent view of the cluster-wide state - namely, the cluster
    // configuration and feature table state. We do that by:
    // 1. Applying any local kvstore snapshots (which contain potentially
    //    stale config and feature table state, as well as persisted
    //    node/cluster UUID information).
    // 2. For a first-time joiner, registering with the cluster and applying the
    //    controller_join_snapshot from the join reply. This covers non-seed
    //    joiners and wiped seeds that are rejoining an existing cluster.
    //    Genuine founders (which need the RPC service for the founder
    //    handshake) and node-ID overrides are resolved later in
    //    resolve_node_identity().
    // 3. For a restarting node with a persisted member set, refreshing that
    //    view by fetching an authoritative controller_join_snapshot from the
    //    controller leader via the `fetch_controller_snapshot` RPC.
    // 4. Marking shard_local_cfg() as ready, after which downstream
    //    services may safely read cluster configuration.
    // The abort source (owned by the caller) bounds cluster discovery.
    // Public for test fixture access.
    void establish_cluster_view(ss::abort_source&);

    // Constructs and starts the services required to provide cryptographic
    // algorithm support to Redpanda. Public for test fixture access.
    void wire_up_and_start_crypto_services();

    // Public for test fixture access.
    void hydrate_cluster_config(const YAML::Node& config);

    // Performs recovery on the local kvstore, applies a local feature table
    // snapshot, and sets in-memory node/cluster UUIDs.
    // Public for test fixture access.
    void bootstrap_from_kvstore();

private:
    // Constructs storage services across shards required early on in the
    // bootstrap process.
    void wire_up_storage_services();

    // Starts storage services across shards required early on in the
    // bootstrap process.
    void start_storage_services(test_cfg cfg);

    // Constructs services across shards meant for Redpanda runtime.
    void wire_up_runtime_services(
      model::node_id node_id,
      ::stop_signal& app_signal,
      cloud_topics::test_fixture_cfg ct_test_cfg);
    void configure_admin_server(model::node_id);
    void wire_up_redpanda_services(
      model::node_id,
      ::stop_signal& app_signal,
      std::optional<cloud_storage_clients::bucket_name>& bucket_name,
      cloud_topics::test_fixture_cfg ct_test_cfg);

    // Marks the shard_local_cfg as ready (or not ready) per the provided flag.
    ss::future<> mark_config_ready(bool ready);

    // Applies the provided feature_table_snapshot directly to the in-memory
    // feature table state.
    ss::future<>
    apply_feature_table_snapshot(const features::feature_table_snapshot& snap);
    // Attempts to read a local feature table snapshot from the kvstore and
    // apply it.
    ss::future<> maybe_apply_local_feature_table_snapshot();
    // How this node obtains its node ID at startup.
    enum class node_id_source {
        // A node ID is already persisted (this node ran a controller before):
        // reuse it. A restarting node.
        established,
        // An operator override supplies the node ID, rewriting the persisted
        // configuration_invariants.
        overridden,
        // No usable node ID yet. The node must register with the cluster to be
        // assigned one. A first-time founder or joining node.
        unregistered,
    };
    // Returns true if this node is present in the local node config's list of
    // seed servers, false otherwise.
    bool is_seed_node() const;
    // Classifies how this node obtains its node ID from persisted invariants,
    // node config and node-ID overrides. Reads the kvstore; call once and cache
    // the result in _node_id_source.
    node_id_source classify_node_id_source();
    // Performs cluster discovery for first time cluster joiners, or resolves
    // node identity from persisted kvstore state. Also persists the node UUID.
    // No-op for the discovery/snapshot step if identity was already resolved
    // early via prime_node_identity().
    ss::future<> resolve_node_identity();
    // Applies an operator node-ID override: rewrites the persisted
    // configuration_invariants and returns the overridden node ID.
    ss::future<model::node_id> apply_node_id_override();
    // Registers with the cluster per the retry_policy and, if a
    // controller_join_snapshot is returned in the join reply, applies it and
    // returns the assigned node ID. When defer_needs_restart is set,
    // needs_restart configs are applied as pending rather than promoted live.
    ss::future<std::optional<model::node_id>> register_and_apply_join_snapshot(
      cluster::cluster_discovery::join_retry_policy policy,
      cluster::defer_needs_restart = cluster::defer_needs_restart::no);
    // Resolves node identity early by registering with the cluster and applying
    // the join snapshot. Used for a first-time joiner (a non-seed, or a seed
    // that finds an existing cluster).
    ss::future<> prime_node_identity();
    // Fetches and applies a view of the controller_stm from the current
    // controller leader.
    ss::future<> bootstrap_controller_view();
    // Refreshes the cluster config and feature table from the provided
    // snapshot. When defer_needs_restart is set, needs_restart config
    // properties are applied as pending.
    ss::future<> apply_controller_snapshot(
      const cluster::controller_join_snapshot&,
      cluster::defer_needs_restart = cluster::defer_needs_restart::no);

    void trigger_abort_source();

    // Starts the services meant for Redpanda runtime. Must be called after
    // having constructed the subsystems via the corresponding `wire_up` calls.
    void start_runtime_services(
      ::stop_signal&, cloud_topics::test_fixture_cfg ct_test_cfg);
    void start_kafka(const model::node_id&, ::stop_signal&);
    void add_runtime_rpc_services(rpc::rpc_server&, bool start_raft_rpc_early);

    // All methods are calleds from Seastar thread
    ss::app_template::seastar_options setup_app_config();
    void validate_arguments(const po::variables_map&);
    YAML::Node hydrate_node_config(const po::variables_map&);
    void log_cluster_config();

    bool requires_cloud_io();

    bool archival_storage_enabled();

    bool wasm_data_transforms_enabled();

    bool datalake_enabled();

    ss::shared_ptr<kafka::datalake_usage_api> make_datalake_usage_aggregator();

    void setup_metrics();
    void setup_public_metrics();
    void setup_internal_metrics();
    ss::future<> register_cluster_identity_metrics();
    std::unique_ptr<ss::app_template> _app;

    // Early in startup, we load config from disk or from the response to
    // a cluster join request: this is used to prime config_manager's state
    // so that the config doesn't walk through all intermediate states
    // in the log during startup.
    cluster::config_manager::preload_result _config_preload;

    std::unique_ptr<cluster::cluster_discovery> _cluster_discovery;
    // Set once node identity has been resolved, either early via
    // prime_node_identity() or later via resolve_node_identity().
    bool _node_identity_resolved{false};
    std::optional<node_id_source> _node_id_source;

    // When joining a cluster, we are tipped off as to the last applied
    // offset of the controller stm from another node.  We will wait for
    // this offset to be replicated to our controller log before listening
    // for Kafka requests.
    std::optional<model::offset> _await_controller_last_applied;

    std::optional<pandaproxy::rest::configuration> _proxy_config;
    std::optional<kafka::client::configuration> _proxy_client_config;
    std::optional<pandaproxy::schema_registry::configuration>
      _schema_reg_config;
    std::optional<kafka::client::configuration> _schema_reg_client_config;
    std::optional<kafka::client::configuration> _audit_log_client_config;
    ss::sharded<scheduling_groups_probe> _scheduling_groups_probe;

    std::optional<config::binding<bool>> _abort_on_oom;
    std::optional<config::binding<bool>> _code_hugepages_binding;

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
    // Cluster identity metrics register in a background fiber once the
    // cluster UUID becomes available (immediately on restart, after
    // bootstrap on a brand new cluster). The gate is closed via _deferred
    // at shutdown.
    ss::gate _cluster_identity_gate;
    metrics::all_metrics_groups _cluster_identity_metrics;
    std::unique_ptr<kafka::rm_group_proxy_impl> _rm_group_proxy;
    ss::sharded<cluster::data_migrations::group_proxy>
      _data_migrations_group_proxy;

    ss::sharded<resources::cpu_profiler> _cpu_profiler;
    ss::sharded<debug_bundle::service> _debug_bundle_service;

    std::unique_ptr<cluster::node_isolation_watcher> _node_isolation_watcher;

    // Small helpers to execute one-time upgrade actions
    std::vector<std::unique_ptr<features::feature_migrator>> _migrators;

    ss::sharded<datalake::credential_manager> _datalake_credential_mgr;
    ss::sharded<datalake::coordinator::coordinator_manager>
      _datalake_coordinator_mgr;
    ss::sharded<datalake::coordinator::frontend> _datalake_coordinator_fe;
    ss::sharded<datalake::datalake_manager> _datalake_manager;

    ss::sharded<kafka::data::rpc::local_service> _kafka_data_rpc_service;
    ss::sharded<kafka::data::rpc::client> _kafka_data_rpc_client;

    ss::sharded<aggregate_metrics_watcher> _aggregate_metrics_watcher;

    // instantiated only in recovery mode
    std::unique_ptr<cluster::tx_manager_migrator> _tx_manager_migrator;

    config::node_override_store _node_overrides{};

    std::unique_ptr<crash_tracker::service> _crash_tracker_service;

    std::unique_ptr<metrics::host_metrics_watcher> _host_metrics_watcher;

    std::unique_ptr<instance_info::instance_metrics> _instance_metrics;

    ss::sharded<admin::kafka_connections_service> _kafka_connections_service;

    ss::sharded<ss::abort_source> _as;
};

namespace debug {
extern application* app;
}
