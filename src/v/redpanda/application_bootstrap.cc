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
#include "cluster/members_manager.h"
#include "cluster/types.h"
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

#include <chrono>

namespace {

bytes node_uuid_key() {
    static const auto key = bytes::from_string("node_uuid");
    return key;
}

} // namespace

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

void application::wire_up_storage_services() {
    // Wire up local storage.
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
}

void application::bootstrap_from_kvstore() {
    // We need to recover the kvstore's state from snapshots & segments for
    // read-only purposes during bootstrapping.
    storage.invoke_on_all(&storage::api::recover_kvstore).get();

    // As soon as storage is up, load our local feature_table snapshot, if any,
    // so that all other services may rely on having features activated as soon
    // as they start.
    maybe_apply_local_feature_table_snapshot().get();

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
    model::node_uuid node_uuid;
    bool node_uuid_is_new = false;
    auto node_uuid_buf = kvs.get(
      storage::kvstore::key_space::controller, node_uuid_key());
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
        node_uuid_is_new = true;
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
        node_uuid_is_new = true;
    }
    // Read+bump+persist the node boot id, so peers can tell a post-restart
    // health report apart from a pre-restart one even when the in-memory
    // health version counter has reset to 1.
    static const auto node_boot_id_key = bytes::from_string("node_boot_id");
    auto kvs_boot_id = kvs.get(
      storage::kvstore::key_space::controller, node_boot_id_key);
    auto next_boot = kvs_boot_id ? serde::from_iobuf<model::node_boot_id>(
                                     std::move(*kvs_boot_id))
                                     + uint64_t{1}
                                 : model::node_boot_id{1};
    kvs
      .persist_pre_start(
        storage::kvstore::key_space::controller,
        node_boot_id_key,
        serde::to_iobuf(next_boot))
      .get();
    vlog(_log.info, "Node boot id: {}", next_boot);

    storage
      .invoke_on_all([node_uuid, next_boot](storage::api& storage) {
          storage.set_node_identity(node_uuid, next_boot);
      })
      .get();

    if (node_uuid_is_new) {
        kvs
          .persist_pre_start(
            storage::kvstore::key_space::controller,
            node_uuid_key(),
            serde::to_iobuf(node_uuid))
          .get();
    }
}

void application::start_storage_services(test_cfg cfg) {
    syschecks::systemd_message("Starting storage services").get();

    // single instance
    storage_node.invoke_on_all(&storage::node::start).get();
    local_monitor.invoke_on_all(&cluster::node::local_monitor::start).get();

    storage::internal::chunk_cache::prealloc prealloc{cfg.chunk_cache_prealloc};
    storage
      .invoke_on_all([prealloc](storage::api& a) { return a.start(prealloc); })
      .get();
}

ss::future<std::optional<model::node_id>>
application::register_and_apply_join_snapshot(
  cluster::cluster_discovery::join_retry_policy policy,
  cluster::defer_needs_restart defer_needs_restart) {
    auto registration_result
      = co_await _cluster_discovery->register_with_cluster(policy);

    if (!registration_result.has_value()) {
        vassert(
          policy
            == cluster::cluster_discovery::join_retry_policy::
              require_existing_cluster,
          "register_with_cluster returned no result under retry_until_joined");
        // No existing cluster was found (this node is likely a founder).
        // Registration is deferred until later in the bootstrapping process.
        co_return std::nullopt;
    }

    if (registration_result->newly_registered) {
        vlog(
          _log.info,
          "Registered with cluster as node ID {}",
          registration_result->assigned_node_id);
        if (registration_result->controller_snapshot.has_value()) {
            auto snap = serde::from_iobuf<cluster::controller_join_snapshot>(
              std::move(registration_result->controller_snapshot.value()));
            co_await apply_controller_snapshot(snap, defer_needs_restart);
        }
    }
    co_return registration_result->assigned_node_id;
}

application::node_id_source application::classify_node_id_source() {
    bool ever_ran_controller = storage.local()
                                 .kvs()
                                 .get(
                                   storage::kvstore::key_space::controller,
                                   cluster::controller::invariants_key())
                                 .has_value();
    bool has_id = config::node().node_id().has_value() && ever_ran_controller;
    bool force_override = _node_overrides.node_id().has_value()
                          && _node_overrides.ignore_existing_node_id();
    if (has_id && !force_override) {
        return node_id_source::established;
    }
    if (_node_overrides.node_id().has_value()) {
        return node_id_source::overridden;
    }
    return node_id_source::unregistered;
}

bool application::is_seed_node() const {
    const auto& self_addr = config::node().advertised_rpc_api();
    const auto& seeds = config::node().seed_servers();
    return std::ranges::any_of(
      seeds, [&](const auto& s) { return s.addr == self_addr; });
}

ss::future<> application::prime_node_identity() {
    // A non-seed joiner always has an existing cluster to join, so register,
    // retrying until it succeeds. A seed is either a genuine founder (no
    // cluster exists yet) or a wiped seed rejoining.
    // For the former, we fast-fail and defer to the authoritative founder
    // handshake later in the bootstrapping process. For the latter, we take the
    // fast joiner path.
    const auto policy
      = is_seed_node()
          ? cluster::cluster_discovery::join_retry_policy::
              require_existing_cluster
          : cluster::cluster_discovery::join_retry_policy::retry_until_joined;
    auto node_id = co_await register_and_apply_join_snapshot(policy);
    if (!node_id.has_value()) {
        co_return;
    }
    if (config::node().node_id() == std::nullopt) {
        // If we previously didn't have a node ID, set it in the config. We
        // will persist it in the kvstore when the controller starts up.
        co_await ss::smp::invoke_on_all([id = *node_id] {
            config::node().node_id.set_value(
              std::make_optional<model::node_id>(id));
        });
    }
    _node_identity_resolved = true;
    vlog(
      _log.info,
      "Resolved node identity early as node_id {}, cluster UUID {}",
      *node_id,
      storage.local().get_cluster_uuid());
}

ss::future<model::node_id> application::apply_node_id_override() {
    auto node_id = _node_overrides.node_id().value();
    vlog(
      _log.warn,
      "Overriding node ID: {} -> {} [ignore_existing_node_id? {}]",
      config::node().node_id(),
      node_id,
      _node_overrides.ignore_existing_node_id());
    // Null out the config'ed ID; the caller re-sets it after we return.
    co_await ss::smp::invoke_on_all(
      [] { config::node().node_id.set_value(std::nullopt); });
    if (
      auto invariants_buf = storage.local().kvs().get(
        storage::kvstore::key_space::controller,
        cluster::controller::invariants_key());
      invariants_buf.has_value()) {
        auto invariants
          = reflection::from_iobuf<cluster::configuration_invariants>(
            std::move(invariants_buf.value()));
        invariants.node_id = node_id;
        co_await storage.local().kvs().put(
          storage::kvstore::key_space::controller,
          cluster::controller::invariants_key(),
          reflection::to_iobuf(cluster::configuration_invariants{invariants}));
        vlog(_log.debug, "Force-updated local node_id to {}", node_id);
    }
    co_return node_id;
}

ss::future<> application::resolve_node_identity() {
    if (_node_identity_resolved) {
        co_return;
    }

    model::node_id node_id;
    switch (_node_id_source.value()) {
    case node_id_source::established:
        node_id = config::node().node_id().value();
        vlog(_log.info, "Running with already-established node ID {}", node_id);
        break;
    case node_id_source::overridden:
        node_id = co_await apply_node_id_override();
        break;
    case node_id_source::unregistered:
        // Reached only by an unregistered seed (a founding node, or a wiped
        // seed rejoining whose early registration found no cluster).
        // This runs after initialization has already occured, so apply the
        // join snapshot with needs_restart configs pending.
        node_id = (co_await register_and_apply_join_snapshot(
                     cluster::cluster_discovery::join_retry_policy::
                       retry_until_joined,
                     cluster::defer_needs_restart::yes))
                    .value();
        break;
    }

    if (config::node().node_id() == std::nullopt) {
        // If we previously didn't have a node ID, set it in the config. We
        // will persist it in the kvstore when the controller starts up.
        co_await ss::smp::invoke_on_all([node_id] {
            config::node().node_id.set_value(
              std::make_optional<model::node_id>(node_id));
        });
    }

    _node_identity_resolved = true;
    vlog(
      _log.info,
      "Starting Redpanda with node_id {}, cluster UUID {}",
      node_id,
      storage.local().get_cluster_uuid());
}

ss::future<> application::bootstrap_controller_view() {
    if (!feature_table.local().is_active(
          features::feature::fetch_controller_snapshot_rpc)) {
        vlog(
          _log.debug,
          "fetch_controller_snapshot_rpc feature not active locally; "
          "skipping bootstrap snapshot fetch (shard_local_cfg and feature "
          "table will reflect local cache only)");
        co_return;
    }
    // Use the cluster-members snapshot persisted by members_manager as
    // the candidate set of peers. A founder on first boot or a
    // non-founder joiner that has not yet completed
    // register_with_cluster will have no persisted members; in that
    // case, fall through to the local cache.
    auto persisted = cluster::members_manager::read_members_from_kvstore(
      storage.local().kvs());
    const auto self_id = config::node().node_id();
    if (self_id.has_value()) {
        std::erase_if(persisted.members, [self_id](const model::broker& b) {
            return b.id() == *self_id;
        });
    }
    if (persisted.members.empty()) {
        vlog(
          _log.debug,
          "No persisted cluster members; skipping controller snapshot "
          "fetch (shard_local_cfg and feature table will reflect local "
          "cache only)");
        co_return;
    }
    auto fetched = co_await cluster::cluster_discovery::
      fetch_controller_snapshot_from_leader(persisted.members);
    if (!fetched.has_value()) {
        vlog(
          _log.warn,
          "Failed to fetch controller snapshot from any persisted "
          "member; shard_local_cfg and feature table will reflect local "
          "cache only");
        co_return;
    }

    auto snap = serde::from_iobuf<cluster::controller_join_snapshot>(
      std::move(fetched.value()));
    co_await apply_controller_snapshot(snap);
}

ss::future<> application::apply_controller_snapshot(
  const cluster::controller_join_snapshot& snap,
  cluster::defer_needs_restart defer_needs_restart) {
    co_await apply_feature_table_snapshot(snap.features.snap);

    // Only apply the snapshot's cluster config state if its version is higher
    // than the existing preloaded state.
    if (snap.config.version > _config_preload.version) {
        _config_preload = co_await cluster::config_manager::preload_join(
          snap, defer_needs_restart);
        co_await cluster::config_manager::write_local_cache(
          _config_preload.version, _config_preload.raw_values);
    }

    _await_controller_last_applied = snap.last_applied;
}

void application::wire_up_and_start_rpc_service() {
    // Construct the rpc service.
    ss::sharded<net::server_configuration> rpc_cfg;
    rpc_cfg.start(ss::sstring("internal_rpc")).get();
    auto stop_cfg = ss::defer([&rpc_cfg] { rpc_cfg.stop().get(); });
    rpc_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              auto rpc_server_addr
                = net::resolve_dns(config::node().rpc_server()).get();
              auto& cfg = config::shard_local_cfg();
              auto& node_cfg = config::node();
              // Use port based load_balancing_algorithm to make connection
              // shard assignment deterministic.
              c.load_balancing_algo
                = ss::server_socket::load_balancing_algorithm::port;
              c.max_service_memory_per_core = int64_t(
                memory_groups().rpc_total_memory());
              c.disable_metrics = net::metrics_disabled(cfg.disable_metrics());
              c.disable_public_metrics = net::public_metrics_disabled(
                cfg.disable_public_metrics());
              c.listen_backlog = cfg.rpc_server_listen_backlog();
              c.tcp_recv_buf = cfg.rpc_server_tcp_recv_buf();
              c.tcp_send_buf = cfg.rpc_server_tcp_send_buf();
              config::tls_config tls_config{
                node_cfg.rpc_server_tls().is_enabled(),
                node_cfg.rpc_server_tls().get_key_cert_files(),
                node_cfg.rpc_server_tls().get_truststore_file(),
                node_cfg.rpc_server_tls().get_crl_file(),
                node_cfg.rpc_server_tls().get_require_client_auth(),
                node_cfg.rpc_server_tls().get_tls_v1_2_cipher_suites().value_or(
                  ss::sstring{}),
                node_cfg.rpc_server_tls().get_tls_v1_3_cipher_suites().value_or(
                  ss::sstring{net::tls_v1_3_cipher_suites_strict}),
                node_cfg.rpc_server_tls().get_min_tls_version().value_or(
                  config::tls_version::v1_3),
                node_cfg.rpc_server_tls().get_enable_renegotiation().value_or(
                  false)};
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
  ::stop_signal& app_signal, bool test_mode, test_cfg cfg) {
    // Setup the app level abort service
    construct_service(_as).get();

    auto& cfg_ref = config::shard_local_cfg();
    smp_groups::config smp_groups_cfg{
      .raft_group_max_non_local_requests
      = cfg_ref.raft_smp_max_non_local_requests().value_or(
        smp_groups::default_raft_non_local_requests(
          cfg_ref.topic_partitions_per_shard())),
      .proxy_group_max_non_local_requests
      = cfg_ref.pp_sr_smp_max_non_local_requests().value_or(
        smp_groups::default_max_nonlocal_requests)};
    smp_service_groups.create_groups(smp_groups_cfg).get();
    _deferred.emplace_back(
      [this] { smp_service_groups.destroy_groups().get(); });

    // Storage services.
    wire_up_storage_services();
    start_storage_services(cfg);

    wire_up_and_start_rpc_service();

    resolve_node_identity().get();

    vassert(
      config::node().node_id().has_value(),
      "config::node().node_id() should have an assigned value at this point in "
      "the start-up process.");
    auto node_id = config::node().node_id().value();
    wire_up_runtime_services(node_id, app_signal, cfg.ct_test_cfg);

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

    vassert(_cluster_discovery, "_cluster_discovery not constructed");
    if (_cluster_discovery->is_cluster_founder().get()) {
        controller->set_ready().get();
    }

    start_runtime_services(app_signal, cfg.ct_test_cfg);

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
ss::future<> application::apply_feature_table_snapshot(
  const features::feature_table_snapshot& snap) {
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

    co_await feature_table.invoke_on_all(
      [snap](features::feature_table& ft) { snap.apply(ft); });

    // Having loaded a snapshot, do our strict check for version compat.
    feature_table.local().assert_compatible_version(
      config::node().upgrade_override_checks);
}

ss::future<> application::maybe_apply_local_feature_table_snapshot() {
    auto val_bytes_opt = storage.local().kvs().get(
      storage::kvstore::key_space::controller,
      features::feature_table_snapshot::kvstore_key());

    if (!val_bytes_opt) {
        // No snapshot?  Probably we are yet to join cluster.
        co_return;
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
        co_return;
    }

    co_await apply_feature_table_snapshot(snap);
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
