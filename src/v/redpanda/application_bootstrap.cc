// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cluster/bootstrap_service.h"
#include "cluster/cluster_discovery.h"
#include "cluster/cluster_uuid.h"
#include "cluster/controller.h"
#include "cluster/controller_snapshot.h"
#include "cluster/feature_manager.h"
#include "cluster_link/service.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/tls_config.h"
#include "crypto/ossl_context_service.h"
#include "features/feature_table_snapshot.h"
#include "migrations/migrators.h"
#include "migrations/rbac_migrator.h"
#include "migrations/topic_id_migrator.h"
#include "net/dns.h"
#include "net/server.h"
#include "net/tls_certificate_probe.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/schema_registry/api.h"
#include "raft/group_manager.h"
#include "redpanda/admin/server.h"
#include "redpanda/application.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "rpc/rpc_utils.h"
#include "security/audit/audit_log_manager.h"
#include "ssx/thread_worker.h"
#include "storage/api.h"
#include "storage/chunk_cache.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "transform/api.h"
#include "transform/rpc/client.h"
#include "wasm/cache.h"
#include "wasm/engine.h"

#include <seastar/core/memory.hh>
#include <seastar/core/smp.hh>

void application::wire_up_and_start_crypto_services() {
    construct_single_service(thread_worker);
    thread_worker->start({.name = "worker"}).get();
    auto fips_mode_flag = config::node().fips_mode();
    // config file and module path are not necessary when not
    // running in FIPS mode
    construct_service(
      ossl_context_service,
      std::ref(*thread_worker),
      ss::sstring{config::node().openssl_config_file().value_or("")},
      ss::sstring{config::node().openssl_module_directory().value_or("")},
      config::fips_mode_enabled(fips_mode_flag) ? crypto::is_fips_mode::yes
                                                : crypto::is_fips_mode::no)
      .get();
    ossl_context_service.invoke_on_all(&crypto::ossl_context_service::start)
      .get();
    ossl_context_service.map([](auto& s) { return s.fips_mode(); })
      .then([fips_mode_flag](auto fips_mode_vals) {
          auto expected = config::fips_mode_enabled(fips_mode_flag)
                            ? crypto::is_fips_mode::yes
                            : crypto::is_fips_mode::no;
          for (auto fips_mode : fips_mode_vals) {
              vassert(
                fips_mode == expected,
                "Mismatch in FIPS mode: {} != {}",
                fips_mode,
                expected);
          }
      })
      .get();
}

// Forward declarations of helper functions defined in application_config.cc
std::optional<storage::file_sanitize_config> read_file_sanitizer_config();

storage::kvstore_config kvstore_config_from_global_config(
  std::optional<storage::file_sanitize_config> sanitizer_config);

storage::log_config manager_config_from_global_config(
  scheduling_groups& sgs,
  std::optional<storage::file_sanitize_config> sanitizer_config);

void application::wire_up_bootstrap_services() {
    // Wire up local storage.
    ss::smp::invoke_on_all([] {
        return storage::internal::chunks().start();
    }).get();
    _deferred.emplace_back([] {
        ss::smp::invoke_on_all([] {
            return storage::internal::chunks().stop();
        }).get();
    });
    construct_service(stress_fiber_manager).get();
    syschecks::systemd_message("Constructing storage services").get();
    construct_single_service_sharded(
      storage_node,
      config::node().data_directory().as_sstring(),
      config::node().cloud_storage_cache_path().string())
      .get();
    construct_single_service_sharded(
      local_monitor,
      config::shard_local_cfg().storage_space_alert_free_threshold_bytes.bind(),
      config::shard_local_cfg()
        .storage_space_alert_free_threshold_percent.bind(),
      std::ref(storage_node))
      .get();

    const auto sanitizer_config = read_file_sanitizer_config();

    construct_service(
      storage,
      [c = sanitizer_config]() mutable {
          return kvstore_config_from_global_config(std::move(c));
      },
      [c = sanitizer_config]() mutable {
          auto log_cfg = manager_config_from_global_config(
            scheduling_groups::instance(), std::move(c));
          log_cfg.reclaim_opts.background_reclaimer_sg
            = scheduling_groups::instance().cache_background_reclaim_sg();
          return log_cfg;
      },
      std::ref(feature_table))
      .get();

    // Hook up local_monitor to update storage_resources when disk state changes
    auto storage_disk_notification
      = storage_node.local().register_disk_notification(
        storage::node::disk_type::data,
        [this](storage::node::disk_space_info info) {
            return storage.invoke_on_all([info](storage::api& api) {
                api.handle_disk_notification(info.total, info.free, info.alert);
            });
        });
    _deferred.emplace_back([this, storage_disk_notification] {
        storage_node.local().unregister_disk_notification(
          storage::node::disk_type::data, storage_disk_notification);
    });

    // Start empty, populated from snapshot in start_bootstrap_services
    syschecks::systemd_message("Creating feature table").get();
    construct_service(feature_table).get();

    // Wire up the internal RPC server.
    ss::sharded<net::server_configuration> rpc_cfg;
    rpc_cfg.start(ss::sstring("internal_rpc")).get();
    auto stop_cfg = ss::defer([&rpc_cfg] { rpc_cfg.stop().get(); });
    rpc_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              auto rpc_server_addr
                = net::resolve_dns(config::node().rpc_server()).get();
              // Use port based load_balancing_algorithm to make connection
              // shard assignment deterministic.
              c.load_balancing_algo
                = ss::server_socket::load_balancing_algorithm::port;
              c.max_service_memory_per_core = int64_t(
                memory_groups().rpc_total_memory());
              c.disable_metrics = net::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
              c.disable_public_metrics = net::public_metrics_disabled(
                config::shard_local_cfg().disable_public_metrics());
              c.listen_backlog
                = config::shard_local_cfg().rpc_server_listen_backlog;
              c.tcp_recv_buf
                = config::shard_local_cfg().rpc_server_tcp_recv_buf;
              c.tcp_send_buf
                = config::shard_local_cfg().rpc_server_tcp_send_buf;
              config::tls_config tls_config{
                config::node().rpc_server_tls().is_enabled(),
                config::node().rpc_server_tls().get_key_cert_files(),
                config::node().rpc_server_tls().get_truststore_file(),
                config::node().rpc_server_tls().get_crl_file(),
                config::node().rpc_server_tls().get_require_client_auth(),
                config::node()
                  .rpc_server_tls()
                  .get_tls_v1_2_cipher_suites()
                  .value_or(ss::sstring{}),
                config::node()
                  .rpc_server_tls()
                  .get_tls_v1_3_cipher_suites()
                  .value_or(ss::sstring{net::tls_v1_3_cipher_suites_strict}),
                config::node().rpc_server_tls().get_min_tls_version().value_or(
                  config::tls_version::v1_3),
                config::node()
                  .rpc_server_tls()
                  .get_enable_renegotiation()
                  .value_or(false)};
              auto credentials
                = net::build_reloadable_server_credentials_with_probe(
                    tls_config,
                    "rpc",
                    "",
                    [this](
                      const std::unordered_set<ss::sstring>& updated,
                      const std::exception_ptr& eptr) {
                        rpc::log_certificate_reload_event(
                          _log, "Internal RPC TLS", updated, eptr);
                    })
                    .get();
              c.addrs.emplace_back(rpc_server_addr, credentials);
          });
      })
      .get();

    syschecks::systemd_message(
      "Constructing internal RPC services {}", rpc_cfg.local())
      .get();
    construct_service(_rpc, &rpc_cfg).get();
}

void application::start_bootstrap_services() {
    syschecks::systemd_message("Starting storage services").get();

    // single instance
    storage_node.invoke_on_all(&storage::node::start).get();
    local_monitor.invoke_on_all(&cluster::node::local_monitor::start).get();

    storage.invoke_on_all(&storage::api::start).get();

    // As soon as storage is up, load our feature_table snapshot, if any,
    // so that all other services may rely on having features activated as soon
    // as they start.
    load_feature_table_snapshot();

    // Before we start up our bootstrapping RPC service, load any relevant
    // on-disk state we may need: existing cluster UUID, node ID, etc.
    if (
      std::optional<iobuf> cluster_uuid_buf = storage.local().kvs().get(
        cluster::cluster_uuid_key_space,
        bytes::from_string(cluster::cluster_uuid_key));
      cluster_uuid_buf) {
        const auto cluster_uuid = model::cluster_uuid{
          serde::from_iobuf<uuid_t>(std::move(*cluster_uuid_buf))};
        storage
          .invoke_on_all([&cluster_uuid](storage::api& storage) {
              storage.set_cluster_uuid(cluster_uuid);
          })
          .get();
    }

    // If the feature table is blank, and we have not yet joined a cluster,
    // then assume we are about to join a cluster or form a new one, and
    // fast-forward the feature table before we do any network operations:
    // this way features like rpc_v2_by_default will be present before the
    // first network I/O we do.
    //
    // Absence of a cluster_uuid is not evidence of not having joined a cluster,
    // because we might have joined via an earlier version of redpanda, and
    // just upgraded to a version that stores cluster and node UUIDs.  We must
    // also check for an controller log state on disk.
    //
    // Ordering: bootstrap_backend writes a feature table snapshot _before_
    // persisting the cluster UUID to kvstore, so if restart in the middle,
    // we will hit this path again: this is important to avoid ever starting
    // network requests before we have reached a defined cluster version.

    auto controller_log_exists = storage.local()
                                   .kvs()
                                   .get(
                                     storage::kvstore::key_space::consensus,
                                     raft::details::serialize_group_key(
                                       raft::group_id{0},
                                       raft::metadata_key::config_map))
                                   .has_value();

    if (
      feature_table.local().get_active_version() == cluster::invalid_version
      && !storage.local().get_cluster_uuid().has_value()
      && !controller_log_exists) {
        feature_table
          .invoke_on_all([](features::feature_table& ft) {
              ft.bootstrap_active_version(
                features::feature_table::get_earliest_logical_version(),
                features::feature_table::version_durability::ephemeral);

              // We do _not_ write a snapshot here: the persistent record of
              // feature table state is only set for the first time in
              // bootstrap_backend (or feature_backend).  This is important,
              // so that someone who starts a too-new Redpanda that can't join
              // their cluster can easily stop it and run an older version,
              // before we've committed any version info to disk.
          })
          .get();
    }

    auto configured_node_id = config::node().node_id();
    if (
      auto invariants_buf = storage.local().kvs().get(
        storage::kvstore::key_space::controller,
        cluster::controller::invariants_key());
      invariants_buf) {
        auto invariants
          = reflection::from_iobuf<cluster::configuration_invariants>(
            std::move(*invariants_buf));
        const auto& stored_node_id = invariants.node_id;
        vlog(_log.info, "Loaded stored node ID for node: {}", stored_node_id);
        if (
          configured_node_id != std::nullopt
          && *configured_node_id != stored_node_id) {
            throw std::invalid_argument(
              ssx::sformat(
                "Configured node ID {} doesn't match stored node ID {}",
                *configured_node_id,
                stored_node_id));
        }
        ss::smp::invoke_on_all([stored_node_id] {
            config::node().node_id.set_value(
              std::make_optional(stored_node_id));
        }).get();
    }

    // Load the local node UUID, or create one if none exists.
    auto& kvs = storage.local().kvs();
    static const auto node_uuid_key = bytes::from_string("node_uuid");
    model::node_uuid node_uuid;
    auto node_uuid_buf = kvs.get(
      storage::kvstore::key_space::controller, node_uuid_key);
    if (node_uuid_buf) {
        node_uuid = serde::from_iobuf<model::node_uuid>(
          std::move(*node_uuid_buf));
        vlog(
          _log.info,
          "Loaded existing UUID for node: {}",
          model::node_uuid(node_uuid));
    } else {
        node_uuid = model::node_uuid(uuid_t::create());
        vlog(_log.info, "Generated new UUID for node: {}", node_uuid);
        kvs
          .put(
            storage::kvstore::key_space::controller,
            node_uuid_key,
            serde::to_iobuf(node_uuid))
          .get();
    }

    _node_overrides.maybe_set_overrides(
      node_uuid, config::node().node_id_overrides());

    // Apply UUID override to node config if present
    if (auto u = _node_overrides.node_uuid(); u.has_value()) {
        vlog(
          _log.warn,
          "Overriding UUID for node: {} -> {}",
          node_uuid,
          u.value());
        node_uuid = u.value();
        kvs
          .put(
            storage::kvstore::key_space::controller,
            node_uuid_key,
            serde::to_iobuf(node_uuid))
          .get();
    }
    storage
      .invoke_on_all([node_uuid](storage::api& storage) mutable {
          storage.set_node_uuid(node_uuid);
      })
      .get();

    syschecks::systemd_message("Starting internal RPC bootstrap service").get();
    _rpc
      .invoke_on_all([this](rpc::rpc_server& s) {
          std::vector<std::unique_ptr<rpc::service>> bootstrap_service;
          bootstrap_service.push_back(
            std::make_unique<cluster::bootstrap_service>(
              scheduling_groups::instance().cluster_sg(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(storage)));
          s.add_services(std::move(bootstrap_service));
      })
      .get();
    _rpc.invoke_on_all(&rpc::rpc_server::start).get();
    vlog(
      _log.info,
      "Started RPC server listening at {}",
      config::node().rpc_server());
}

void application::wire_up_and_start(
  ::stop_signal& app_signal,
  bool test_mode,
  cloud_topics::test_fixture_cfg ct_test_cfg) {
    // Setup the app level abort service
    construct_service(_as).get();

    // Bootstrap services.
    wire_up_and_start_crypto_services();
    wire_up_bootstrap_services();
    start_bootstrap_services();

    // Begin the cluster discovery manager so we can confirm our initial node
    // ID. A valid node ID is required before we can initialize the rest of our
    // subsystems.
    const auto& node_uuid = storage.local().node_uuid();
    cluster::cluster_discovery cd(
      node_uuid, storage.local(), app_signal.abort_source());

    auto invariants_buf = storage.local().kvs().get(
      storage::kvstore::key_space::controller,
      cluster::controller::invariants_key());

    bool ever_ran_controller = invariants_buf.has_value();

    bool has_id = config::node().node_id().has_value() && ever_ran_controller;

    bool force_override = _node_overrides.node_id().has_value()
                          && _node_overrides.ignore_existing_node_id();

    model::node_id node_id;
    if (has_id && !force_override) {
        vlog(
          _log.info,
          "Running with already-established node ID {}",
          config::node().node_id());
        node_id = config::node().node_id().value();
    } else if (auto id = _node_overrides.node_id(); id.has_value()) {
        vlog(
          _log.warn,
          "Overriding node ID: {} -> {} [ignore_existing_node_id? {}]",
          config::node().node_id(),
          id,
          has_id && force_override);
        node_id = id.value();
        // null out the config'ed ID indiscriminately; it will be set outside
        // the conditional
        ss::smp::invoke_on_all([] {
            config::node().node_id.set_value(std::nullopt);
        }).get();
        if (invariants_buf.has_value()) {
            auto invariants
              = reflection::from_iobuf<cluster::configuration_invariants>(
                std::move(invariants_buf.value()));
            invariants.node_id = node_id;
            storage.local()
              .kvs()
              .put(
                storage::kvstore::key_space::controller,
                cluster::controller::invariants_key(),
                reflection::to_iobuf(
                  cluster::configuration_invariants{invariants}))
              .get();
            vlog(_log.debug, "Force-updated local node_id to {}", node_id);
        }
    } else {
        auto registration_result = cd.register_with_cluster().get();
        node_id = registration_result.assigned_node_id;

        if (registration_result.newly_registered) {
            vlog(
              _log.info,
              "Registered with cluster as node ID {}",
              registration_result.assigned_node_id);
            if (registration_result.controller_snapshot.has_value()) {
                // Do something with the controller snapshot
                auto snap
                  = serde::from_iobuf<cluster::controller_join_snapshot>(
                    std::move(registration_result.controller_snapshot.value()));

                // The controller is not started yet, so write state directly
                // into the feature table and configuration object.  We do not
                // currently use the rest of the snapshot, but reserve the right
                // to do so in future (e.g. to prime all the controller stms
                // from the snapshot)
                auto ftsnap = std::move(snap.features.snap);
                ss::smp::invoke_on_all([ftsnap, &ft = feature_table] {
                    ftsnap.apply(ft.local());
                }).get();
                cluster::feature_backend::do_save_local_snapshot(
                  storage.local(), ftsnap)
                  .get();

                // The preload object is usually generated from loading a local
                // cache or from the bootstrap file.  The configuration received
                // from the cluster during join takes precedence over either of
                // these, and we replace it.
                _config_preload
                  = cluster::config_manager::preload_join(snap).get();
                cluster::config_manager::write_local_cache(
                  _config_preload.version, _config_preload.raw_values)
                  .get();

                // During controller::start, we wait to reach an applied offset.
                // By priming this from the join snapshot, we may ensure that
                // we wait until this node has replicated all the controller
                // metadata since it joined, before we proceed with e.g.
                // listening for Kafka API requests.
                _await_controller_last_applied = snap.last_applied;
            }
        }
    }

    if (config::node().node_id() == std::nullopt) {
        // If we previously didn't have a node ID, set it in the config. We
        // will persist it in the kvstore when the controller starts up.
        ss::smp::invoke_on_all([node_id] {
            config::node().node_id.set_value(
              std::make_optional<model::node_id>(node_id));
        }).get();
    }

    vlog(
      _log.info,
      "Starting Redpanda with node_id {}, cluster UUID {}",
      node_id,
      storage.local().get_cluster_uuid());

    wire_up_runtime_services(node_id, app_signal, ct_test_cfg);

    if (test_mode) {
        // When running inside a unit test fixture, we may fast-forward
        // some of initialization that would usually wait for the controller
        // to commit some state to its log.
        vlog(_log.warn, "Running in unit test mode");
        if (
          feature_table.local().get_active_version()
          == cluster::invalid_version) {
            vlog(_log.info, "Switching on all features");
            feature_table
              .invoke_on_all(
                [](features::feature_table& ft) { ft.testing_activate_all(); })
              .get();
        }
    } else {
        // Only populate migrators in non-unit-test mode
        _migrators.push_back(
          std::make_unique<features::migrators::cloud_storage_config>(
            *controller));
        _migrators.push_back(
          std::make_unique<features::migrators::rbac_migrator>(*controller));
        _migrators.push_back(
          std::make_unique<features::migrators::shard_placement_migrator>(
            *controller));
        _migrators.push_back(
          std::make_unique<features::migrators::topic_id_migrator>(
            *controller));
    }

    if (cd.is_cluster_founder().get()) {
        controller->set_ready().get();
    }

    start_runtime_services(cd, app_signal, ct_test_cfg);

    if (_proxy_config && !config::node().recovery_mode_enabled) {
        _proxy->start().get();
        vlog(
          _log.info,
          "Started Pandaproxy listening at {}",
          _proxy_config->pandaproxy_api());
    }

    if (_schema_reg_config && !config::node().recovery_mode_enabled) {
        _schema_registry->start().get();
        vlog(
          _log.info,
          "Started Schema Registry listening at {}",
          _schema_reg_config->schema_registry_api());
    }

    audit_mgr.invoke_on_all(&security::audit::audit_log_manager::start).get();

    if (!audit_mgr.local().report_redpanda_app_event(
          security::audit::is_started::yes)) {
        vlog(
          _log.error,
          "Failed to enqueue startup audit event!  Possible issue with audit "
          "system");
        throw std::runtime_error("Failed to enqueue startup audit event!");
    }

    start_kafka(node_id, app_signal);
    controller->set_ready().get();

    if (
      wasm_data_transforms_enabled() && !config::node().recovery_mode_enabled) {
        const auto& cluster = config::shard_local_cfg();
        wasm::runtime::config config = {
          .heap_memory = {
            .per_core_pool_size_bytes = cluster.data_transforms_per_core_memory_reservation.value(),
            .per_engine_memory_limit = cluster.data_transforms_per_function_memory_limit.value(),
          },
          .stack_memory = {
            .debug_host_stack_usage = false,
          },
          .cpu = {
            .per_invocation_timeout = cluster.data_transforms_runtime_limit_ms.value(),
          },
        };
        _wasm_runtime->start(config).get();
        _transform_rpc_client.invoke_on_all(&transform::rpc::client::start)
          .get();
        _transform_service.invoke_on_all(&transform::service::start).get();
    }

    _cluster_link_service.invoke_on_all(&cluster_link::service::start).get();

    construct_service(_aggregate_metrics_watcher).get();

    _admin.invoke_on_all([](admin_server& admin) { admin.set_ready(); }).get();
    _monitor_unsafe->start().get();

    vlog(_log.info, "Successfully started Redpanda!");
    syschecks::systemd_notify_ready().get();
}

/**
 * Feature table is generally updated via controller, but we need it to
 * be initialized very early in startup so that other subsystems (including
 * e.g. the controller raft group) may rely on up to date knowledge of which
 * feature bits are enabled.
 */
void application::load_feature_table_snapshot() {
    auto val_bytes_opt = storage.local().kvs().get(
      storage::kvstore::key_space::controller,
      features::feature_table_snapshot::kvstore_key());

    if (!val_bytes_opt) {
        // No snapshot?  Probably we are yet to join cluster.
        return;
    }

    features::feature_table_snapshot snap;
    try {
        snap = serde::from_iobuf<features::feature_table_snapshot>(
          std::move(*val_bytes_opt));
    } catch (...) {
        // Do not block redpanda from starting if there is something invalid
        // here: the feature table should get replayed eventually via
        // the controller.
        vlog(
          _log.error,
          "Exception decoding feature table snapshot: {}",
          std::current_exception());
#ifndef NDEBUG
        vunreachable("Snapshot decode failed");
#endif
        return;
    }

    auto my_version = features::feature_table::get_latest_logical_version();
    if (my_version < snap.version) {
        vlog(
          _log.error,
          "Incompatible downgrade detected!  My version {}, feature table {} "
          "indicates that all nodes in cluster were previously >= that version",
          my_version,
          snap.version);
        vassert(
          config::node().upgrade_override_checks || my_version >= snap.version,
          "Incompatible downgrade detected");
    } else {
        vlog(
          _log.debug,
          "Loaded feature table snapshot at cluster version {} (vs my binary "
          "{})",
          snap.version,
          my_version);
    }

    feature_table
      .invoke_on_all([snap](features::feature_table& ft) { snap.apply(ft); })
      .get();

    // Having loaded a snapshot, do our strict check for version compat.
    feature_table.local().assert_compatible_version(
      config::node().upgrade_override_checks);
}

/**
 * Contains tasks that should only run after all other services have been
 * initialized and started.
 */
void application::post_start_tasks() {
    // This warning is set after we start RP since we want to allow
    // services to make large allocations if need be during startup.
    auto warning_threshold
      = config::node().memory_allocation_warning_threshold();
    if (warning_threshold.has_value()) {
        ss::smp::invoke_on_all([threshold = warning_threshold.value()] {
            ss::memory::set_large_allocation_warning_threshold(threshold);
        }).get();
    }

    // We schedule the deletion _after_ the application fully
    // starts up. This ensures that any errors like
    // misconfigurations are also treated as unclean shutdowns
    // thus avoiding crashloops.
    schedule_crash_tracker_file_cleanup();
}
