// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"

#include "cloud_storage/configuration.h"
#include "cluster/cloud_metadata/cluster_recovery_backend.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller_forced_reconfiguration_manager.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/crash_reporter.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/data_migration_group_proxy.h"
#include "cluster/data_migration_table.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metrics_reporter.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_manager.h"
#include "cluster/scheduling/leader_balancer.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/shard_placement_table.h"
#include "cluster/topic_metrics_watcher.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential_store.h"
#include "security/oidc_service.h"
#include "security/role_store.h"
#include "storage/api.h"

#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

bytes controller::invariants_key() {
    return bytes::from_string("configuration_invariants");
}

controller::controller(
  config_manager::preload_result&& config_preload,
  ss::sharded<rpc::connection_cache>& ccache,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<storage::api>& storage,
  ss::sharded<node::local_monitor>& local_monitor,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<cloud_io::cache>& cloud_cache,
  ss::sharded<node_status_table>& node_status_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::scheduling_group scheduling_group)
  : _config_preload(std::move(config_preload))
  , _connections(ccache)
  , _partition_manager(pm)
  , _shard_table(st)
  , _storage(storage)
  , _local_monitor(local_monitor)
  , _tp_updates_dispatcher(
      _partition_allocator, _tp_state, _partition_balancer_state)
  , _security_manager(_credentials, _authorizer, _roles)
  , _raft_manager(raft_manager)
  , _feature_table(feature_table)
  , _cloud_storage_api(cloud_storage_api)
  , _cloud_cache(cloud_cache)
  , _node_status_table(node_status_table)
  , _metadata_cache(metadata_cache)
  , _probe(*this)
  , _cfr_m(std::make_unique<controller_forced_reconfiguration_manager>(this))
  , _scheduling_group(scheduling_group) {}

// Explicit destructor in the .cc file just to avoid bloating the header with
// includes for destructors of all its members (e.g. the metadata uploader).
controller::~controller() = default;

ss::future<model::offset> controller::get_last_applied_offset() {
    return _stm.invoke_on(controller_stm_shard, [](auto& stm) {
        return stm.get_last_applied_offset();
    });
}

ss::future<>
controller::wait_for_offset(model::offset target, ss::abort_source& shard0_as) {
    return _stm.invoke_on(
      controller_stm_shard, [target, &as = shard0_as](auto& stm) {
          return stm.wait(target, model::no_timeout, as);
      });
}

std::optional<std::reference_wrapper<cloud_metadata::uploader>>
controller::metadata_uploader() {
    if (_metadata_uploader) {
        return std::ref<cloud_metadata::uploader>(*_metadata_uploader);
    }
    return std::nullopt;
}

std::optional<cloud_storage_clients::bucket_name>
controller::get_configured_bucket() {
    auto& bucket_property = cloud_storage::configuration::get_bucket_config();
    if (
      !bucket_property.is_overriden() || !bucket_property().has_value()
      || !_cloud_storage_api.local_is_initialized()) {
        return std::nullopt;
    }
    return cloud_storage_clients::bucket_name(bucket_property().value());
}

ss::future<> controller::wire_up() {
    return _as.start()
      .then([this] { return _members_table.start(); })
      .then([this] {
          return _partition_allocator.start_single(
            std::ref(_members_table),
            std::ref(_feature_table),
            config::shard_local_cfg().topic_fds_per_partition.bind(),
            config::shard_local_cfg().topic_partitions_per_shard.bind(),
            config::shard_local_cfg().topic_partitions_reserve_shard0.bind(),
            config::shard_local_cfg().kafka_nodelete_topics.bind(),
            config::shard_local_cfg().enable_rack_awareness.bind());
      })
      .then([this] { return _credentials.start(); })
      .then([this] { return _ephemeral_credentials.start(); })
      .then([this] { return _roles.start(); })
      .then([this] { return _data_migrated_resources.start(); })
      .then([this] {
          return _data_migration_table.start_on(
            data_migrations::data_migrations_shard,
            std::ref(_data_migrated_resources),
            std::ref(_tp_state),
            config::shard_local_cfg().cloud_storage_enabled()
              && config::shard_local_cfg()
                   .cloud_storage_disable_archiver_manager());
      })
      .then([this] {
          return _authorizer.start(
            ss::sharded_parameter(
              []() { return config::shard_local_cfg().superusers.bind(); }),
            ss::sharded_parameter([this] { return &_roles.local(); }));
      })
      .then([this] {
          return _oidc_service.start(
            ss::sharded_parameter(
              [] { return config::shard_local_cfg().sasl_mechanisms.bind(); }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .sasl_mechanisms_overrides.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().http_authentication.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_discovery_url.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_token_audience.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .oidc_clock_skew_tolerance.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_principal_mapping.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg()
                  .oidc_keys_refresh_interval.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().oidc_group_claim_path.bind();
            }),
            ss::sharded_parameter([] {
                return config::shard_local_cfg().nested_group_behavior.bind();
            }));
      })
      .then([this] {
          return _tp_state.start(
            ss::sharded_parameter(
              [this] { return std::ref(_data_migrated_resources.local()); }),
            config::node().node_id().value());
      })
      .then([this] {
          return _partition_balancer_state.start_single(
            std::ref(_tp_state),
            std::ref(_members_table),
            std::ref(_partition_allocator),
            std::ref(_node_status_table));
      })
      .then([this] {
          return _shard_placement.start(
            ss::sharded_parameter([] { return ss::this_shard_id(); }),
            ss::sharded_parameter(
              [this] { return std::ref(_storage.local().kvs()); }));
      })
      .then([this] { _probe.start(); });
}

ss::future<> controller::set_ready() {
    if (_is_ready) {
        return ss::now();
    }
    _is_ready = true;
    return _raft_manager.invoke_on_all(&raft::group_manager::set_ready);
}

metrics_reporter::metrics_contributor_id
controller::register_metrics_contributor(
  metrics_reporter::metrics_contributor_fn fn) {
    vassert(
      ss::this_shard_id() == 0,
      "Only shard 0 can register metrics contributors");
    return _metrics_reporter.local().register_metrics_contributor(
      std::move(fn));
}

void controller::unregister_metrics_contributor(
  metrics_reporter::metrics_contributor_id id) {
    vassert(
      ss::this_shard_id() == 0,
      "Only shard 0 can unregister metrics contributors");
    _metrics_reporter.local().unregister_metrics_contributor(id);
}

/**
 * Helper for subsystems that create internal topics, to discover
 * how many replicas they should use.
 */
int16_t controller::internal_topic_replication() const {
    auto replication_factor
      = (int16_t)config::shard_local_cfg().internal_topic_replication_factor();
    if (replication_factor > (int16_t)_members_table.local().node_count()) {
        // Fall back to r=1 if we do not have sufficient nodes
        return 1;
    } else {
        // Respect `internal_topic_replication_factor` if enough
        // nodes were available.
        return replication_factor;
    }
}

ss::future<result<std::vector<partition_state>>>
controller::get_controller_partition_state() {
    std::vector<model::node_id> nodes_to_query
      = _members_table.local().node_ids();

    std::vector<ss::future<result<partition_state_reply>>> futures;
    futures.reserve(nodes_to_query.size());
    for (const auto& node : nodes_to_query) {
        futures.push_back(do_get_controller_partition_state(node));
    }

    auto finished_futures = co_await ss::when_all(
      futures.begin(), futures.end());
    std::vector<partition_state> results;
    results.reserve(finished_futures.size());
    for (auto& fut : finished_futures) {
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(
              clusterlog.info,
              "Failed to get controller partition state, failure: {}",
              ex);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              clusterlog.info,
              "Failed to get controller state, result: {}",
              result.error());
            continue;
        }
        auto res = std::move(result.value());
        if (res.error_code != errc::success || !res.state) {
            vlog(
              clusterlog.debug,
              "Error during controller partition state fetch, error: {}",
              res.error_code);
            continue;
        }
        results.push_back(std::move(*res.state));
    }
    co_return results;
}

ss::future<result<partition_state_reply>>
controller::do_get_controller_partition_state(model::node_id target_node) {
    if (target_node == _raft0->self().id()) {
        partition_state_reply reply{};

        return _partition_manager.invoke_on(
          controller_stm_shard,
          [reply = std::move(reply)](partition_manager& pm) mutable {
              auto partition = pm.get(model::controller_ntp);
              if (!partition) {
                  reply.error_code = errc::partition_not_exists;
                  return ss::make_ready_future<result<partition_state_reply>>(
                    reply);
              }
              reply.state = ::cluster::get_partition_state(partition);
              reply.error_code = errc::success;
              return ss::make_ready_future<result<partition_state_reply>>(
                reply);
          });
    }
    auto timeout = model::timeout_clock::now() + 5s;
    return _connections.local().with_node_client<controller_client_protocol>(
      _raft0->self().id(),
      ss::this_shard_id(),
      target_node,
      timeout,
      [timeout](controller_client_protocol client) mutable {
          return client
            .get_partition_state(
              partition_state_request{.ntp = model::controller_ntp},
              rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<partition_state_reply>);
      });
}

ss::future<cluster::error_info>
controller::initialize_controller_forced_reconfiguration(
  std::vector<model::node_id> dead_nodes, uint16_t surviving_node_count) {
    vassert(
      ss::this_shard_id() == controller_stm_shard,
      "programmer error, controller_force_recovery should only be called on "
      "shard {}",
      controller_stm_shard);

    co_return co_await _cfr_m->initialize_controller_forced_reconfiguration(
      std::move(dead_nodes), surviving_node_count);
}

} // namespace cluster
