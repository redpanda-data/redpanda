// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topics_frontend.h"

#include "cloud_storage/remote.h"
#include "cloud_storage_clients/configuration.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/remote_topic_configuration_source.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/shard_balancer.h"
#include "cluster/shard_table.h"
#include "cluster/topic_recovery_validator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "data_migration_types.h"
#include "features/feature_table.h"
#include "fwd.h"
#include "model/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
#include "topic_configuration.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <iterator>
#include <memory>
#include <regex>
#include <sstream>
#include <system_error>

namespace cluster {

topics_frontend::topics_frontend(
  model::node_id self,
  ss::sharded<controller_stm>& s,
  ss::sharded<rpc::connection_cache>& con,
  ss::sharded<partition_allocator>& pal,
  ss::sharded<partition_leaders_table>& l,
  ss::sharded<topic_table>& topics,
  ss::sharded<health_monitor_frontend>& hm_frontend,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<cloud_storage::remote>& cloud_storage_api,
  ss::sharded<features::feature_table>& features,
  ss::sharded<cluster::members_table>& members_table,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& shard_table,
  ss::sharded<shard_balancer>& sb,
  ss::sharded<storage::api>& storage,
  data_migrations::migrated_resources& migrated_resources,
  plugin_table& plugin_table,
  metadata_cache& metadata_cache,
  config::binding<unsigned> hard_max_disk_usage_ratio,
  config::binding<int16_t> minimum_topic_replication,
  config::binding<bool> partition_autobalancing_topic_aware)
  : _self(self)
  , _stm(s)
  , _allocator(pal)
  , _connections(con)
  , _leaders(l)
  , _topics(topics)
  , _hm_frontend(hm_frontend)
  , _as(as)
  , _cloud_storage_api(cloud_storage_api)
  , _features(features)
  , _shard_balancer(sb)
  , _storage(storage)
  , _plugin_table(plugin_table)
  , _metadata_cache(metadata_cache)
  , _members_table(members_table)
  , _pm(pm)
  , _shard_table(shard_table)
  , _migrated_resources(migrated_resources)
  , _hard_max_disk_usage_ratio(hard_max_disk_usage_ratio)
  , _minimum_topic_replication(minimum_topic_replication)
  , _partition_autobalancing_topic_aware(
      std::move(partition_autobalancing_topic_aware)) {
    if (ss::this_shard_id() == 0) {
        _minimum_topic_replication.watch(
          [this]() { print_rf_warning_message(); });
    }
}

static bool
needs_linearizable_barrier(const std::vector<topic_result>& results) {
    return std::any_of(
      results.cbegin(), results.cend(), [](const topic_result& r) {
          return r.ec == errc::success;
      });
}

ss::future<std::vector<topic_result>> topics_frontend::create_topics(
  custom_assignable_topic_configuration_vector topics,
  model::timeout_clock::time_point timeout) {
    for (auto& tp : topics) {
        /**
         * The shadow_indexing properties
         * ('redpanda.remote.(read|write|delete)') are special "sticky" topic
         * properties that are always set as a topic-level override.
         *
         * See: https://github.com/redpanda-data/redpanda/issues/7451
         *
         * Note that a manually created topic will have this assigned already by
         * kafka/server/handlers/topics/types.cc::to_cluster_type, dependent on
         * client-provided topic properties.
         *
         * tp.cfg.properties.remote_delete is stored as a bool (not
         * std::optional<bool>) defaulted to its default value
         * (ntp_config::default_remote_delete) on the construction of
         * topic_properties(), so there is no need to overwrite it here.
         */
        if (!tp.cfg.properties.shadow_indexing.has_value()) {
            tp.cfg.properties.shadow_indexing
              = _metadata_cache.get_default_shadow_indexing_mode();
        }
    }

    vlog(clusterlog.info, "Create topics {}", topics);
    // make sure that STM is up to date (i.e. we have the most recent state
    // available) before allocating topics
    return stm_linearizable_barrier(timeout)
      .then([this, topics = std::move(topics), timeout](
              result<model::offset> result) mutable {
          if (!result) {
              return ss::make_ready_future<std::vector<topic_result>>(
                make_error_topic_results(topics, errc::not_leader_controller));
          }
          std::vector<ss::future<topic_result>> futures;
          futures.reserve(topics.size());

          std::transform(
            std::begin(topics),
            std::end(topics),
            std::back_inserter(futures),
            [this, timeout](custom_assignable_topic_configuration& t_cfg) {
                return do_create_topic(std::move(t_cfg), timeout);
            });

          return ss::when_all_succeed(futures.begin(), futures.end());
      })
      .then([this, timeout](std::vector<topic_result> results) {
          if (needs_linearizable_barrier(results)) {
              return stm_linearizable_barrier(timeout).then(
                [results = std::move(results)](result<model::offset>) mutable {
                    return results;
                });
          }
          return ss::make_ready_future<std::vector<topic_result>>(
            std::move(results));
      });
}

cluster::errc map_errc(std::error_code ec) {
    if (ec == errc::success) {
        return errc::success;
    }
    // error comming from raft
    if (ec.category() == raft::error_category()) {
        switch (static_cast<raft::errc>(ec.value())) {
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        default:
            return errc::replication_error;
        }
    }

    // error comming from raft
    if (ec.category() == rpc::error_category()) {
        switch (static_cast<rpc::errc>(ec.value())) {
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        default:
            return errc::replication_error;
        }
    }
    // cluster errors, just forward
    if (ec.category() == cluster::error_category()) {
        return static_cast<errc>(ec.value());
    }

    return errc::replication_error;
}

ss::future<std::vector<topic_result>> topics_frontend::update_topic_properties(
  topic_properties_update_vector updates,
  model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!cluster_leader) {
        co_return make_error_topic_results(updates, errc::no_leader_controller);
    }

    if (!_features.local().is_active(features::feature::cloud_retention)) {
        // The ADL encoding for cluster::incremental_topic_updates has evolved
        // in v22.3. ADL is not forwards compatible, so we need to safe-guard
        // against sending a message from the future to older nodes.

        vlog(
          clusterlog.info,
          "Refusing to update topics as not all cluster nodes are running "
          "v22.3");
        co_return make_error_topic_results(updates, errc::feature_disabled);
    }

    // current node is a leader, just replicate
    if (cluster_leader == _self) {
        // replicate empty batch to make sure leader local state is up to date.
        auto result = co_await stm_linearizable_barrier(timeout);
        if (!result) {
            co_return make_error_topic_results(
              updates, map_errc(result.error()));
        }

        auto results = co_await ssx::parallel_transform(
          std::move(updates), [this, timeout](topic_properties_update update) {
              return do_update_topic_properties(std::move(update), timeout);
          });

        // we are not really interested in the result coming from the
        // linearizable barrier, results coming from the previous steps will be
        // propagated to clients, this is just an optimization, this doesn't
        // affect correctness of the protocol
        if (needs_linearizable_barrier(results)) {
            co_await stm_linearizable_barrier(timeout).discard_result();
        }

        co_return results;
    }

    auto updates2 = updates.copy();
    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *cluster_leader,
        timeout,
        [updates{std::move(updates)},
         timeout](controller_client_protocol client) mutable {
            return client
              .update_topic_properties(
                update_topic_properties_request{.updates = std::move(updates)},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<update_topic_properties_reply>);
        })
      .then([updates{std::move(updates2)}](
              result<update_topic_properties_reply> r) {
          if (r.has_error()) {
              return make_error_topic_results(updates, map_errc(r.error()));
          }
          return std::move(r.value().results);
      });
}

ss::future<std::error_code> topics_frontend::do_update_replication_factor(
  topic_properties_update& update, model::timeout_clock::time_point timeout) {
    switch (update.custom_properties.replication_factor.op) {
    case incremental_update_operation::set: {
        if (!_features.local().is_active(
              features::feature::replication_factor_change)) {
            co_return cluster::errc::feature_disabled;
        }

        if (_topics.local().is_fully_disabled(update.tp_ns)) {
            co_return errc::topic_disabled;
        }

        auto value = update.custom_properties.replication_factor.value;
        if (
          !value.has_value()
          || value.value() == cluster::replication_factor(0)) {
            co_return cluster::errc::topic_invalid_replication_factor;
        }

        co_return co_await change_replication_factor(
          update.tp_ns,
          update.custom_properties.replication_factor.value.value(),
          timeout);
    }
    case incremental_update_operation::none:
        co_return cluster::errc::success;
    default:
        co_return cluster::errc::invalid_configuration_update;
    }
}

ss::future<topic_result> topics_frontend::do_update_topic_properties(
  topic_properties_update update, model::timeout_clock::time_point timeout) {
    auto state = _migrated_resources.get_topic_state(update.tp_ns);
    if (state != data_migrations::migrated_resource_state::non_restricted) {
        vlog(
          clusterlog.warn,
          "cannot update topic {} properties as the topic is being migrated; "
          "restriction is {}",
          update.tp_ns,
          state);

        co_return topic_result{
          std::move(update.tp_ns), errc::resource_is_being_migrated};
    }

    update_topic_properties_cmd cmd(update.tp_ns, update.properties);
    try {
        auto update_rf_res = co_await do_update_replication_factor(
          update, timeout);
        if (update_rf_res != std::error_code(cluster::errc::success)) {
            co_return topic_result(
              update.tp_ns, cluster::errc(update_rf_res.value()));
        }

        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), timeout);
        co_return topic_result(std::move(update.tp_ns), map_errc(ec));
    } catch (...) {
        vlog(
          clusterlog.warn,
          "unable to update {} configuration properties - {}",
          update.tp_ns,
          std::current_exception());

        co_return topic_result(
          std::move(update.tp_ns), errc::replication_error);
    }
}

topic_result
make_error_result(const model::topic_namespace& tp_ns, std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return topic_result(tp_ns, cluster::errc(ec.value()));
    }

    return topic_result(tp_ns, errc::topic_operation_error);
}

static allocation_request make_allocation_request(
  const custom_assignable_topic_configuration& ca_cfg, bool topic_aware) {
    // no custom assignments, lets allocator decide based on partition count
    const auto& tp_ns = ca_cfg.cfg.tp_ns;
    allocation_request req(tp_ns);
    if (!ca_cfg.has_custom_assignment()) {
        req.partitions.reserve(ca_cfg.cfg.partition_count);
        for (auto p = 0; p < ca_cfg.cfg.partition_count; ++p) {
            req.partitions.emplace_back(
              model::partition_id(p), ca_cfg.cfg.replication_factor);
        }
    } else {
        req.partitions.reserve(ca_cfg.custom_assignments.size());
        for (auto& cas : ca_cfg.custom_assignments) {
            allocation_constraints constraints;
            constraints.add(on_nodes(cas.replicas));

            req.partitions.emplace_back(
              cas.id, cas.replicas.size(), std::move(constraints));
        }
    }
    if (topic_aware) {
        req.existing_replica_counts = node2count_t{};
    }
    return req;
}

errc topics_frontend::validate_topic_configuration(
  const custom_assignable_topic_configuration& assignable_config) {
    if (!validate_topic_name(assignable_config.cfg.tp_ns)) {
        return errc::invalid_topic_name;
    }

    if (assignable_config.cfg.partition_count < 1) {
        return errc::topic_invalid_partitions;
    }

    if (assignable_config.cfg.replication_factor < 1) {
        return errc::topic_invalid_replication_factor;
    }

    if (assignable_config.has_custom_assignment()) {
        for (auto& custom : assignable_config.custom_assignments) {
            if (
              static_cast<int16_t>(custom.replicas.size())
              != assignable_config.cfg.replication_factor) {
                return errc::topic_invalid_replication_factor;
            }
        }
    }
    if (
      (assignable_config.is_read_replica()
       || assignable_config.is_recovery_enabled())
      && !_cloud_storage_api.local_is_initialized()) {
        return errc::topic_invalid_config;
    }

    // the only way that cloud topics can be enabled on a topic is if the cloud
    // topics development feature is also enabled.
    if (!config::shard_local_cfg().development_enable_cloud_topics()) {
        if (assignable_config.cfg.properties.cloud_topic_enabled) {
            vlog(
              clusterlog.error,
              "Cloud topic flag on {} is set but development feature is "
              "disabled",
              assignable_config.cfg.tp_ns);
            return errc::topic_invalid_config;
        }
    }

    return errc::success;
}

ss::future<topic_result> topics_frontend::do_create_topic(
  custom_assignable_topic_configuration assignable_config,
  model::timeout_clock::time_point timeout) {
    auto& tp_ns = assignable_config.cfg.tp_ns;
    if (_topics.local().contains(tp_ns)) {
        vlog(
          clusterlog.trace,
          "unable to create topic {} as it already exists",
          tp_ns);
        co_return topic_result(tp_ns, errc::topic_already_exists);
    }

    bool blocked = assignable_config.cfg.is_migrated
                     ? _migrated_resources.get_topic_state(tp_ns)
                         > data_migrations::migrated_resource_state::create_only
                     : _migrated_resources.is_already_migrated(tp_ns);
    if (blocked) {
        vlog(
          clusterlog.warn,
          "unable to create topic {} as it is being migrated: "
          "cfg.is_migrated={}, migrated resource state is {}",
          assignable_config.cfg.tp_ns,
          assignable_config.cfg.is_migrated,
          _migrated_resources.get_topic_state(tp_ns));
        co_return topic_result(
          assignable_config.cfg.tp_ns, errc::resource_is_being_migrated);
    }

    auto validation_err = validate_topic_configuration(assignable_config);

    if (validation_err != errc::success) {
        co_return topic_result(assignable_config.cfg.tp_ns, validation_err);
    }

    if (assignable_config.is_read_replica()) {
        if (!assignable_config.cfg.properties.read_replica_bucket) {
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_invalid_config);
        }
        auto rr_manager = remote_topic_configuration_source(
          _cloud_storage_api.local());

        errc download_res = co_await rr_manager.set_remote_properties_in_config(
          assignable_config,
          cloud_storage_clients::bucket_name(
            assignable_config.cfg.properties.read_replica_bucket.value()),
          _as.local());

        if (download_res != errc::success) {
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_operation_error);
        }

        if (!assignable_config.cfg.properties.remote_topic_properties) {
            vassert(
              assignable_config.cfg.properties.remote_topic_properties,
              "remote_topic_properties not set after successful download of "
              "valid topic manifest");
        }
        assignable_config.cfg.partition_count
          = assignable_config.cfg.properties.remote_topic_properties
              ->remote_partition_count;
    }

    if (assignable_config.is_recovery_enabled()) {
        // Before running the recovery we need to download topic_manifest.

        const auto& bucket_config
          = cloud_storage::configuration::get_bucket_config();
        if (!bucket_config.value().has_value()) {
            vlog(
              clusterlog.error,
              "Can't run topic recovery for the topic {}, {} is not set",
              assignable_config.cfg.tp_ns,
              bucket_config.name());
            co_return make_error_result(
              assignable_config.cfg.tp_ns, errc::topic_operation_error);
        }

        auto bucket = cloud_storage_clients::bucket_name{
          bucket_config.value().value()};

        auto cfg_source = remote_topic_configuration_source(
          _cloud_storage_api.local());

        // If the caller is supplying the remote topic properties, presumably
        // the correct remote properties are already known (e.g. because this
        // is a part of a cluster recovery and the topic config is already
        // known).
        if (!assignable_config.cfg.properties.remote_topic_properties
               .has_value()) {
            errc download_res
              = co_await cfg_source.set_recovered_topic_properties(
                assignable_config, bucket, _as.local());

            if (download_res != errc::success) {
                vlog(
                  clusterlog.error,
                  "Can't run topic recovery for the topic {}",
                  assignable_config.cfg.tp_ns);
                co_return make_error_result(
                  assignable_config.cfg.tp_ns, errc::topic_invalid_config);
            }
            vassert(
              static_cast<bool>(
                assignable_config.cfg.properties.remote_topic_properties),
              "remote_topic_properties not set after successful download of "
              "valid "
              "topic manifest");
        }
        auto validation_map = co_await maybe_validate_recovery_topic(
          assignable_config, bucket, _cloud_storage_api.local(), _as.local());
        if (std::ranges::any_of(
              validation_map,
              [](const std::pair<model::partition_id, validation_result>& vp) {
                  using enum validation_result;
                  switch (vp.second) {
                  case passed:
                  case missing_manifest:
                      // passed or missing_manifest do not fail validation
                      return false;
                  case anomaly_detected:
                  case download_issue:
                      // failure needs to be handled by an operator,
                      // download_issue likely is a config issue
                      return true;
                  }
              })) {
            vlog(
              clusterlog.error,
              "Stopping recovery of {} due to validation error",
              assignable_config.cfg.tp_ns);
            co_return make_error_result(
              assignable_config.cfg.tp_ns,
              make_error_code(errc::validation_of_recovery_topic_failed));
        }

        vlog(
          clusterlog.info,
          "Configured topic recovery for {}, topic configuration: {}",
          assignable_config.cfg.tp_ns,
          assignable_config.cfg);
    }
    bool configured_label_from_manifest
      = assignable_config.is_read_replica()
        || assignable_config.is_recovery_enabled();
    // We set a remote label if:
    // - we haven't got a remote label from the cloud (i.e. this isn't a read
    //   replica or recovery topic),
    // - there is a cluster UUID (always expected),
    // - the remote labels feature is active,
    // - the config to disable remote labels is False
    if (
      !configured_label_from_manifest
      && !assignable_config.cfg.properties.remote_label.has_value()
      && _storage.local().get_cluster_uuid().has_value()
      && _features.local().is_active(features::feature::remote_labels)
      && !config::shard_local_cfg()
            .cloud_storage_disable_remote_labels_for_tests.value()) {
        auto remote_label = std::make_optional<cloud_storage::remote_label>(
          _storage.local().get_cluster_uuid().value());
        assignable_config.cfg.properties.remote_label = remote_label;
        vlog(
          clusterlog.debug,
          "Configuring topic {} with remote label {}",
          assignable_config.cfg.tp_ns,
          remote_label);
    }

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [assignable_config, topic_aware = _partition_autobalancing_topic_aware()](
        partition_allocator& al) {
          return al.allocate(
            make_allocation_request(assignable_config, topic_aware));
      });

    if (!units) {
        co_return make_error_result(assignable_config.cfg.tp_ns, units.error());
    }
    co_return co_await replicate_create_topic(
      std::move(assignable_config.cfg), std::move(units.value()), timeout);
}

ss::future<topic_result> topics_frontend::replicate_create_topic(
  topic_configuration cfg,
  allocation_units::pointer units,
  model::timeout_clock::time_point timeout) {
    auto tp_ns = cfg.tp_ns;
    create_topic_cmd cmd(
      tp_ns,
      topic_configuration_assignment(
        std::move(cfg), units->copy_assignments()));

    for (auto& p_as : cmd.value.assignments) {
        std::shuffle(
          p_as.replicas.begin(),
          p_as.replicas.end(),
          random_generators::internal::gen);
    }

    return replicate_and_wait(_stm, _as, std::move(cmd), timeout)
      .then_wrapped([tp_ns = std::move(tp_ns), units = std::move(units)](
                      ss::future<std::error_code> f) mutable {
          try {
              auto error_code = f.get();
              auto ret_f = ss::now();
              return ret_f.then(
                [tp_ns = std::move(tp_ns), error_code]() mutable {
                    return topic_result(std::move(tp_ns), map_errc(error_code));
                });

          } catch (...) {
              vlog(
                clusterlog.warn,
                "Unable to create topic - {}",
                std::current_exception());
              return ss::make_ready_future<topic_result>(
                topic_result(std::move(tp_ns), errc::replication_error));
          }
      });
}

ss::future<std::vector<topic_result>> topics_frontend::dispatch_delete_topics(
  std::vector<model::topic_namespace> topics,
  std::chrono::milliseconds timeout) {
    auto controller_leader = _leaders.local().get_leader(model::controller_ntp);
    if (controller_leader == _self) {
        co_return co_await delete_topics(
          std::move(topics), timeout + model::timeout_clock::now());
    }

    if (controller_leader) {
        vlog(
          clusterlog.debug,
          "dispatching delete topics request to {}",
          controller_leader);
        auto reply
          = co_await _connections.local()
              .with_node_client<cluster::controller_client_protocol>(
                _self,
                ss::this_shard_id(),
                *controller_leader,
                timeout,
                [topics, timeout](controller_client_protocol cp) mutable {
                    return cp.delete_topics(
                      delete_topics_request{
                        .topics_to_delete = std::move(topics),
                        .timeout = timeout},
                      rpc::client_opts(model::timeout_clock::now() + timeout));
                })
              .then(&rpc::get_ctx_data<delete_topics_reply>);

        if (reply.has_error()) {
            vlog(
              clusterlog.warn,
              "delete topics failed with an error - {}",
              reply.error().message());
            co_return make_error_topic_results(
              topics, errc::topic_operation_error);
        }
        co_return std::move(reply.value().results);
    }

    co_return make_error_topic_results(topics, errc::no_leader_controller);
}

ss::future<std::vector<topic_result>> topics_frontend::delete_topics(
  std::vector<model::topic_namespace> topics,
  model::timeout_clock::time_point timeout) {
    vlog(clusterlog.info, "Delete topics {}", topics);

    std::vector<ss::future<topic_result>> futures;
    futures.reserve(topics.size());

    std::transform(
      std::begin(topics),
      std::end(topics),
      std::back_inserter(futures),
      [this, timeout](model::topic_namespace& tp_ns) {
          return do_delete_topic(std::move(tp_ns), timeout, false);
      });

    return ss::when_all_succeed(futures.begin(), futures.end())
      .then([this, timeout](std::vector<topic_result> results) {
          if (needs_linearizable_barrier(results)) {
              return stm_linearizable_barrier(timeout).then(
                [results = std::move(results)](result<model::offset>) mutable {
                    return results;
                });
          }
          return ss::make_ready_future<std::vector<topic_result>>(
            std::move(results));
      });
}

ss::future<errc> topics_frontend::delete_topic_after_migration(
  model::topic_namespace nt, model::timeout_clock::time_point timeout) {
    auto result = co_await do_delete_topic(std::move(nt), timeout, true);
    if (result.ec == errc::success) {
        std::ignore = co_await stm_linearizable_barrier(timeout);
    }
    co_return result.ec;
}

ss::future<topic_result> topics_frontend::do_delete_topic(
  model::topic_namespace tp_ns,
  model::timeout_clock::time_point timeout,
  bool migrated_away) {
    // Look up config
    auto topic_meta_opt = _topics.local().get_topic_metadata_ref(tp_ns);
    if (!topic_meta_opt.has_value()) {
        topic_result result(std::move(tp_ns), errc::topic_not_exists);
        return ss::make_ready_future<topic_result>(result);
    }
    if (!migrated_away) {
        auto state = _migrated_resources.get_topic_state(tp_ns);
        if (state != data_migrations::migrated_resource_state::non_restricted) {
            vlog(
              clusterlog.warn,
              "can not delete topic as it is being {} by migration",
              state);
            topic_result result(
              std::move(tp_ns), errc::resource_is_being_migrated);
            return ss::make_ready_future<topic_result>(result);
        }
    }
    // Before deleting a topic we need to make sure there are no transforms
    // hooked up to it first.
    //
    // NOTE: This is best effort validation, it's possible for a plugin creation
    // racing in a suspension point and there being a dangling topic for a
    // plugin.
    auto source_transforms = _plugin_table.find_by_input_topic(tp_ns);
    auto sink_transforms = _plugin_table.find_by_output_topic(tp_ns);
    if (!source_transforms.empty() || !sink_transforms.empty()) {
        topic_result result(std::move(tp_ns), errc::source_topic_still_in_use);
        return ss::make_ready_future<topic_result>(result);
    }
    // Lifecycle marker driven deletion is added alongside the v2 manifest
    // format in Redpanda 23.2.  Before that, we write legacy one-shot
    // deletion records.
    if (
      !migrated_away
      && !_features.local().is_active(
        features::feature::cloud_storage_manifest_format_v2)) {
        // This is not unsafe, but emit a warning in case we have some bug that
        // causes a cluster to indefinitely use the legacy path, so that
        // someone has a chance to notice.
        vlog(
          clusterlog.warn,
          "Cluster upgrade in progress, using legacy deletion.",
          tp_ns);
        delete_topic_cmd cmd(tp_ns, tp_ns);

        return replicate_and_wait(_stm, _as, std::move(cmd), timeout)
          .then_wrapped(
            [tp_ns = std::move(tp_ns)](ss::future<std::error_code> f) mutable {
                try {
                    auto ec = f.get();
                    if (ec != errc::success) {
                        return topic_result(std::move(tp_ns), map_errc(ec));
                    } else {
                        vlog(clusterlog.info, "Deleting topic {}", tp_ns);
                    }
                    return topic_result(std::move(tp_ns), errc::success);
                } catch (...) {
                    vlog(
                      clusterlog.warn,
                      "Unable to delete topic - {}",
                      std::current_exception());
                    return topic_result(
                      std::move(tp_ns), errc::replication_error);
                }
            });
    }

    // Default to traditional deletion, without tombstones
    // Use tombstones for tiered storage topics that require remote erase
    auto& topic_meta = topic_meta_opt.value().get();
    topic_lifecycle_transition_mode mode;
    if (migrated_away) {
        mode = topic_lifecycle_transition_mode::delete_migrated;
        vlog(clusterlog.info, "Deleting migrated topic {}", tp_ns);
    } else if (topic_meta.get_configuration()
                 .properties.requires_remote_erase()) {
        mode = topic_lifecycle_transition_mode::pending_gc;
        vlog(clusterlog.info, "Created deletion marker for topic {}", tp_ns);
    } else {
        mode = topic_lifecycle_transition_mode::oneshot_delete;
        vlog(clusterlog.info, "Deleting topic {}", tp_ns);
    }

    auto remote_revision = topic_meta.get_remote_revision().value_or(
      model::initial_revision_id{topic_meta.get_revision()});

    topic_lifecycle_transition_cmd cmd(
      tp_ns,
      {.topic = {.nt = tp_ns, .initial_revision_id = remote_revision},
       .mode = mode});

    return replicate_and_wait(_stm, _as, std::move(cmd), timeout)
      .then_wrapped(
        [tp_ns = std::move(tp_ns)](ss::future<std::error_code> f) mutable {
            try {
                auto ec = f.get();
                if (ec != errc::success) {
                    return topic_result(std::move(tp_ns), map_errc(ec));
                }
                return topic_result(std::move(tp_ns), errc::success);
            } catch (...) {
                vlog(
                  clusterlog.warn,
                  "Unable to delete topic - {}",
                  std::current_exception());
                return topic_result(std::move(tp_ns), errc::replication_error);
            }
        });
}

ss::future<topic_result> topics_frontend::purged_topic(
  nt_revision topic, model::timeout_clock::duration timeout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<topic_result>(
          topic_result(topic.nt, errc::no_leader_controller));
    }
    // current node is a leader controller
    if (leader == _self) {
        return do_purged_topic(
          std::move(topic), model::timeout_clock::now() + timeout);
    } else {
        return dispatch_purged_topic_to_leader(
          leader.value(), std::move(topic), timeout);
    }
}

ss::future<topic_result> topics_frontend::do_purged_topic(
  nt_revision topic, model::timeout_clock::time_point deadline) {
    topic_lifecycle_transition_cmd cmd(
      topic.nt,
      topic_lifecycle_transition{
        .topic = topic, .mode = topic_lifecycle_transition_mode::drop});

    if (!_topics.local().get_lifecycle_markers().contains(topic)) {
        // Do not write to log if the marker is already gone
        vlog(
          clusterlog.info,
          "Dropping duplicate purge request for lifecycle marker {}",
          topic.nt);
        co_return topic_result(std::move(topic.nt), errc::success);
    }

    std::error_code repl_ec;
    try {
        repl_ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), deadline);
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Unable to mark topic {} purged - {}",
          topic.nt,
          std::current_exception());
        co_return topic_result(std::move(topic.nt), errc::replication_error);
    }

    if (repl_ec != errc::success) {
        co_return topic_result(std::move(topic.nt), map_errc(repl_ec));
    } else {
        vlog(clusterlog.info, "Finished deleting topic {}", topic);
        co_return topic_result(std::move(topic.nt), errc::success);
    }
}

ss::future<std::vector<topic_result>> topics_frontend::autocreate_topics(
  topic_configuration_vector topics, model::timeout_clock::duration timeout) {
    vlog(clusterlog.trace, "Auto create topics {}", topics);

    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::vector<topic_result>>(
          make_error_topic_results(topics, errc::no_leader_controller));
    }
    // current node is a leader controller
    if (leader == _self) {
        return create_topics(
          without_custom_assignments(std::move(topics)),
          model::timeout_clock::now() + timeout);
    }
    // dispatch to leader
    return dispatch_create_to_leader(
      leader.value(), std::move(topics), timeout);
}

ss::future<std::vector<topic_result>>
topics_frontend::dispatch_create_to_leader(
  model::node_id leader,
  topic_configuration_vector topics,
  model::timeout_clock::duration timeout) {
    vlog(clusterlog.trace, "Dispatching create topics to {}", leader);
    auto r = co_await _connections.local()
               .with_node_client<cluster::controller_client_protocol>(
                 _self,
                 ss::this_shard_id(),
                 leader,
                 timeout,
                 [topics{topics.copy()},
                  timeout](controller_client_protocol cp) mutable {
                     return cp.create_topics(
                       create_topics_request{
                         .topics = std::move(topics), .timeout = timeout},
                       rpc::client_opts(model::timeout_clock::now() + timeout));
                 })
               .then(&rpc::get_ctx_data<create_topics_reply>);
    if (r.has_error()) {
        co_return make_error_topic_results(topics, map_errc(r.error()));
    }
    co_return std::move(r.value().results);
}

ss::future<topic_result> topics_frontend::dispatch_purged_topic_to_leader(
  model::node_id leader,
  nt_revision topic,
  model::timeout_clock::duration timeout) {
    vlog(
      clusterlog.trace,
      "Dispatching purged topic ({}) to {}",
      topic.nt,
      leader);

    auto r = co_await _connections.local()
               .with_node_client<cluster::controller_client_protocol>(
                 _self,
                 ss::this_shard_id(),
                 leader,
                 timeout,
                 [topic, timeout](controller_client_protocol cp) mutable {
                     return cp.purged_topic(
                       purged_topic_request{
                         .topic = std::move(topic), .timeout = timeout},
                       rpc::client_opts(model::timeout_clock::now() + timeout));
                 })
               .then(&rpc::get_ctx_data<purged_topic_reply>);
    if (r.has_error()) {
        co_return topic_result(topic.nt, map_errc(r.error()));
    }
    co_return std::move(r.value().result);
}

bool topics_frontend::validate_topic_name(const model::topic_namespace& topic) {
    if (topic.ns == model::kafka_namespace) {
        const auto errc = model::validate_kafka_topic_name(topic.tp);
        if (static_cast<model::errc>(errc.value()) != model::errc::success) {
            vlog(clusterlog.info, "{} {}", errc.message(), topic.tp());
            return false;
        }
    }
    return true;
}

ss::future<std::error_code> topics_frontend::move_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  reconfiguration_policy policy,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(tout);
    if (!result) {
        co_return result.error();
    }

    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    if (_topics.local().is_update_in_progress(ntp)) {
        co_return errc::update_in_progress;
    }
    const auto fast_reconfiguration_active = _features.local().is_active(
      features::feature::fast_partition_reconfiguration);

    // fallback to old move command
    if (!fast_reconfiguration_active) {
        if (policy != reconfiguration_policy::full_local_retention) {
            vlog(
              clusterlog.warn,
              "Trying to move partition {} to {} with reconfiguration policy "
              "of {} but fast partition movement feature is not yet active",
              ntp,
              new_replica_set,
              policy);
        }
        move_partition_replicas_cmd cmd(
          std::move(ntp), std::move(new_replica_set));

        co_return co_await replicate_and_wait(
          _stm, _as, std::move(cmd), tout, term);
    }
    update_partition_replicas_cmd cmd(
      0, // unused
      update_partition_replicas_cmd_data{
        .ntp = std::move(ntp),
        .replicas = std::move(new_replica_set),
        .policy = policy});

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), tout, term);
}

ss::future<std::error_code> topics_frontend::force_update_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(tout);
    if (!result) {
        co_return result.error();
    }
    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    force_partition_reconfiguration_cmd cmd{
      std::move(ntp),
      force_partition_reconfiguration_cmd_data{std::move(new_replica_set)}};

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), tout, term);
}

ss::future<result<fragmented_vector<ntp_with_majority_loss>>>
topics_frontend::partitions_with_lost_majority(
  std::vector<model::node_id> dead_nodes) {
    try {
        fragmented_vector<ntp_with_majority_loss> result;
        const auto& topics = _topics.local();
        for (auto it = topics.topics_iterator_begin();
             it != topics.topics_iterator_end();
             ++it) {
            const auto& tn = it->first;
            const auto& assignments = (it->second).get_assignments();
            const auto topic_revision = it->second.get_revision();
            for (const auto& [_, assignment] : assignments) {
                const auto& current = assignment.replicas;
                auto remaining = subtract_replica_sets_by_node_id(
                  current, dead_nodes);
                auto lost_majority = remaining.size()
                                     < (current.size() / 2) + 1;
                if (!lost_majority) {
                    continue;
                }
                model::ntp ntp(tn.ns, tn.tp, assignment.id);
                if (topics.updates_in_progress().contains(ntp)) {
                    // force reconfiguration does not support in progress
                    // moves. this check can be relaxed once the limitation
                    // is fixed.
                    vlog(
                      clusterlog.debug,
                      "{} lost majority but skipping as an update is in "
                      "progress.",
                      ntp);
                    continue;
                }
                result.emplace_back(
                  std::move(ntp),
                  topic_revision,
                  assignment.replicas,
                  dead_nodes);
                co_await ss::coroutine::maybe_yield();
                it.check();
            }
        }
        auto validation_err
          = _topics.local().validate_force_reconfigurable_partitions(result);
        if (validation_err) {
            co_return errc::concurrent_modification_error;
        }
        co_return result;
    } catch (const concurrent_modification_error& e) {
        // state changed while generating the plan, force caller to retry;
        vlog(
          clusterlog.info,
          "Topic table state changed when generating force move plan: {}",
          e.what());
    }
    co_return errc::concurrent_modification_error;
}

ss::future<std::error_code>
topics_frontend::force_recover_partitions_from_nodes(
  std::vector<model::node_id> nodes,
  fragmented_vector<ntp_with_majority_loss>
    user_approved_force_recovery_partitions,
  model::timeout_clock::time_point timeout) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }
    // check if the state of partitions to recover tallies with their
    // current state.
    const auto& topics = _topics.local();
    auto reject = false;
    for (const auto& entry : user_approved_force_recovery_partitions) {
        // check if there is an in progress movemement, reject if so.
        // this is a conservative check and can be relaxed.
        auto in_progress_move = topics.is_update_in_progress(entry.ntp);
        auto current_assignment = topics.get_partition_assignment(entry.ntp);
        auto assignment_match = current_assignment
                                && are_replica_sets_equal(
                                  current_assignment->replicas,
                                  entry.assignment);
        if (in_progress_move || !assignment_match) {
            vlog(
              clusterlog.info,
              "rejecting force recovery of partitions from brokers {}, ntp: "
              "{}, move in progress: {}, expected replica set: {}, current "
              "assignment: {}, the state may have changed since the original "
              "request was made, try again.",
              nodes,
              entry.ntp,
              entry.assignment,
              current_assignment);
            reject = true;
        }
    }
    if (reject) {
        co_return errc::invalid_request;
    }
    auto validation_err
      = _topics.local().validate_force_reconfigurable_partitions(
        user_approved_force_recovery_partitions);
    if (validation_err) {
        co_return validation_err;
    }
    bulk_force_reconfiguration_cmd_data data;
    data.from_nodes = std::move(nodes);
    data.user_approved_force_recovery_partitions = std::move(
      user_approved_force_recovery_partitions);
    co_return co_await replicate_and_wait(
      _stm, _as, bulk_force_reconfiguration_cmd{0, std::move(data)}, timeout);
}

ss::future<std::error_code> topics_frontend::cancel_moving_partition_replicas(
  model::ntp ntp,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }
    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    if (!_topics.local().is_update_in_progress(ntp)) {
        co_return errc::no_update_in_progress;
    }

    cancel_moving_partition_replicas_cmd cmd(
      std::move(ntp),
      cancel_moving_partition_replicas_cmd_data(force_abort_update::no));

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), timeout, term);
}

ss::future<std::error_code> topics_frontend::abort_moving_partition_replicas(
  model::ntp ntp,
  model::timeout_clock::time_point timeout,
  std::optional<model::term_id> term) {
    auto result = co_await stm_linearizable_barrier(timeout);
    if (!result) {
        co_return result.error();
    }

    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }
    if (!_topics.local().is_update_in_progress(ntp)) {
        co_return errc::no_update_in_progress;
    }
    cancel_moving_partition_replicas_cmd cmd(
      std::move(ntp),
      cancel_moving_partition_replicas_cmd_data(force_abort_update::yes));

    co_return co_await replicate_and_wait(
      _stm, _as, std::move(cmd), timeout, term);
}

ss::future<std::error_code> topics_frontend::finish_moving_partition_replicas(
  model::ntp ntp,
  std::vector<model::broker_shard> new_replica_set,
  model::timeout_clock::time_point tout,
  dispatch_to_leader dispatch) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::error_code>(
          errc::no_leader_controller);
    }

    // current node is a leader, just replicate
    if (leader == _self) {
        // optimization: if update is not in progress return early
        if (!_topics.local().is_update_in_progress(ntp)) {
            return ss::make_ready_future<std::error_code>(
              errc::no_update_in_progress);
        }

        finish_moving_partition_replicas_cmd cmd(
          std::move(ntp), std::move(new_replica_set));

        return replicate_and_wait(_stm, _as, std::move(cmd), tout);
    }

    if (!dispatch) {
        return ss::make_ready_future<std::error_code>(
          errc::not_leader_controller);
    }

    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        tout,
        [ntp = std::move(ntp), replicas = std::move(new_replica_set), tout](
          controller_client_protocol client) mutable {
            return client
              .finish_partition_update(
                finish_partition_update_request{
                  .ntp = std::move(ntp),
                  .new_replica_set = std::move(replicas)},
                rpc::client_opts(tout))
              .then(&rpc::get_ctx_data<finish_partition_update_reply>);
        })
      .then([](result<finish_partition_update_reply> r) {
          return r.has_error() ? r.error() : r.value().result;
      });
}
ss::future<std::error_code> topics_frontend::revert_cancel_partition_move(
  model::ntp ntp, model::timeout_clock::time_point tout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<std::error_code>(
          errc::no_leader_controller);
    }
    // optimization: if update is not in progress return early
    if (!_topics.local().is_update_in_progress(ntp)) {
        return ss::make_ready_future<std::error_code>(
          errc::no_update_in_progress);
    }
    // current node is a leader, just replicate
    if (leader == _self) {
        revert_cancel_partition_move_cmd cmd(
          0, revert_cancel_partition_move_cmd_data{.ntp = std::move(ntp)});

        return replicate_and_wait(_stm, _as, std::move(cmd), tout);
    }

    return _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        tout,
        [ntp = std::move(ntp),
         tout](controller_client_protocol client) mutable {
            return client
              .revert_cancel_partition_move(
                revert_cancel_partition_move_request{.ntp = std::move(ntp)},
                rpc::client_opts(tout))
              .then(&rpc::get_ctx_data<revert_cancel_partition_move_reply>);
        })
      .then([](result<revert_cancel_partition_move_reply> r) {
          return r.has_error() ? r.error() : r.value().result;
      });
}

ss::future<result<model::offset>> topics_frontend::stm_linearizable_barrier(
  model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(controller_stm_shard, [timeout](controller_stm& stm) {
        return stm.insert_linearizable_barrier(timeout);
    });
}

ss::future<std::vector<topic_result>> topics_frontend::create_partitions(
  std::vector<create_partitions_configuration> partitions,
  model::timeout_clock::time_point timeout) {
    auto r = co_await stm_linearizable_barrier(timeout);
    if (!r) {
        std::vector<topic_result> results;
        results.reserve(partitions.size());
        std::transform(
          partitions.begin(),
          partitions.end(),
          std::back_inserter(results),
          [err = r.error()](const create_partitions_configuration& cfg) {
              return make_error_result(cfg.tp_ns, err);
          });
        co_return results;
    }

    auto result = co_await ssx::parallel_transform(
      partitions.begin(),
      partitions.end(),
      [this, timeout](create_partitions_configuration cfg) {
          return do_create_partition(std::move(cfg), timeout);
      });

    co_return result;
}

ss::future<std::error_code> topics_frontend::set_topic_partitions_disabled(
  model::topic_namespace_view ns_tp,
  std::optional<model::partition_id> p_id,
  bool disabled,
  model::timeout_clock::time_point timeout) {
    if (!_features.local().is_active(features::feature::disabling_partitions)) {
        co_return errc::feature_disabled;
    }

    if (!model::is_user_topic(ns_tp)) {
        co_return errc::invalid_partition_operation;
    }

    auto r = co_await stm_linearizable_barrier(timeout);
    if (!r) {
        co_return r.error();
    }

    // pre-replicate checks

    if (p_id) {
        if (!_topics.local().contains(ns_tp, *p_id)) {
            co_return errc::partition_not_exists;
        }
        if (_topics.local().is_disabled(ns_tp, *p_id) == disabled) {
            // no-op
            co_return errc::success;
        }
    } else {
        if (!_topics.local().contains(ns_tp)) {
            co_return errc::topic_not_exists;
        }
        if (disabled && _topics.local().is_fully_disabled(ns_tp)) {
            // no-op
            co_return errc::success;
        }
        if (!disabled && _topics.local().is_fully_enabled(ns_tp)) {
            // no-op
            co_return errc::success;
        }
    }

    // replicate the command

    set_topic_partitions_disabled_cmd cmd(
      0, // unused
      set_topic_partitions_disabled_cmd_data{
        .ns_tp = model::topic_namespace{ns_tp},
        .partition_id = p_id,
        .disabled = disabled,
      });

    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

ss::future<bool>
topics_frontend::validate_shard(model::node_id node, uint32_t shard) const {
    return _allocator.invoke_on(
      partition_allocator::shard, [node, shard](partition_allocator& al) {
          return al.state().validate_shard(node, shard);
      });
}

static allocation_request make_allocation_request(
  int16_t replication_factor,
  const int32_t current_partitions_count,
  std::optional<node2count_t> existing_replica_counts,
  const create_partitions_configuration& cfg) {
    const auto new_partitions_cnt = cfg.new_total_partition_count
                                    - current_partitions_count;
    allocation_request req(cfg.tp_ns);
    req.existing_replica_counts = std::move(existing_replica_counts);
    req.partitions.reserve(new_partitions_cnt);
    for (auto p = 0; p < new_partitions_cnt; ++p) {
        req.partitions.emplace_back(model::partition_id(p), replication_factor);
    }
    return req;
}

ss::future<topic_result> topics_frontend::do_create_partition(
  create_partitions_configuration p_cfg,
  model::timeout_clock::time_point timeout) {
    auto tp_cfg = _topics.local().get_topic_cfg(p_cfg.tp_ns);
    auto replication_factor = _topics.local().get_topic_replication_factor(
      p_cfg.tp_ns);
    if (!tp_cfg || !replication_factor) {
        co_return make_error_result(p_cfg.tp_ns, errc::topic_not_exists);
    }
    auto state = _migrated_resources.get_topic_state(p_cfg.tp_ns);
    if (state != data_migrations::migrated_resource_state::non_restricted) {
        vlog(
          clusterlog.warn,
          "can not create {} topic partitions as the topic is being migrated",
          p_cfg.tp_ns);

        co_return topic_result{
          std::move(p_cfg.tp_ns), errc::resource_is_being_migrated};
    }

    // we only support increasing number of partitions
    if (p_cfg.new_total_partition_count <= tp_cfg->partition_count) {
        co_return make_error_result(
          p_cfg.tp_ns, errc::topic_invalid_partitions_decreased);
    }
    if (_topics.local().is_fully_disabled(p_cfg.tp_ns)) {
        co_return make_error_result(p_cfg.tp_ns, errc::topic_disabled);
    }

    std::optional<node2count_t> existing_replica_counts;
    if (_partition_autobalancing_topic_aware()) {
        auto md_ref = _topics.local().get_topic_metadata_ref(p_cfg.tp_ns);
        if (!md_ref) {
            co_return make_error_result(p_cfg.tp_ns, errc::topic_not_exists);
        }

        node2count_t node2count;
        for (const auto& [_, p_as] : md_ref->get().get_assignments()) {
            for (const auto& r : p_as.replicas) {
                node2count[r.node_id] += 1;
            }
        }
        existing_replica_counts = std::move(node2count);
    }

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [p_cfg,
       current = tp_cfg->partition_count,
       existing_rc = std::move(existing_replica_counts),
       rf = replication_factor.value()](partition_allocator& al) mutable {
          return al.allocate(make_allocation_request(
            rf, current, std::move(existing_rc), p_cfg));
      });

    // no assignments, error
    if (!units) {
        co_return make_error_result(p_cfg.tp_ns, units.error());
    }

    auto tp_ns = p_cfg.tp_ns;
    create_partitions_configuration_assignment payload(
      std::move(p_cfg), units.value()->copy_assignments());
    create_partition_cmd cmd = create_partition_cmd(tp_ns, std::move(payload));

    try {
        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), timeout);
        co_return topic_result(tp_ns, map_errc(ec));
    } catch (...) {
        vlog(
          clusterlog.warn,
          "Unable to create topic {} partitions - {}",
          tp_ns,
          std::current_exception());
        co_return topic_result(std::move(tp_ns), errc::replication_error);
    }
}

ss::future<result<std::vector<move_cancellation_result>>>
topics_frontend::cancel_moving_partition_replicas_node(
  model::node_id node_id,
  partition_move_direction dir,
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<move_cancellation_result>>;
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        switch (dir) {
        case partition_move_direction::from_node:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().ntps_moving_from_node(node_id), timeout);
        case partition_move_direction::to_node:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().ntps_moving_to_node(node_id), timeout);
        case partition_move_direction::all:
            co_return co_await do_cancel_moving_partition_replicas(
              _topics.local().all_ntps_moving_per_node(node_id), timeout);
        }
        __builtin_unreachable();
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout, node_id, dir](controller_client_protocol client) mutable {
            return client
              .cancel_node_partition_movements(
                cancel_node_partition_movements_request{
                  .node_id = node_id, .direction = dir},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<cancel_partition_movements_reply>);
        })
      .then([](result<cancel_partition_movements_reply> r) {
          return r.has_error() ? ret_t(r.error())
                               : std::move(r.value().partition_results);
      });
}

ss::future<result<std::vector<move_cancellation_result>>>
topics_frontend::cancel_moving_all_partition_replicas(
  model::timeout_clock::time_point timeout) {
    using ret_t = result<std::vector<move_cancellation_result>>;
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        co_return errc::no_leader_controller;
    }
    if (leader == _self) {
        co_return co_await do_cancel_moving_partition_replicas(
          _topics.local().all_updates_in_progress(), timeout);
    }

    co_return co_await _connections.local()
      .with_node_client<controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout](controller_client_protocol client) mutable {
            return client
              .cancel_all_partition_movements(
                cancel_all_partition_movements_request{},
                rpc::client_opts(timeout))
              .then(&rpc::get_ctx_data<cancel_partition_movements_reply>);
        })
      .then([](result<cancel_partition_movements_reply> r) {
          return r.has_error() ? ret_t(r.error())
                               : std::move(r.value().partition_results);
      });
}

ss::future<std::vector<move_cancellation_result>>
topics_frontend::do_cancel_moving_partition_replicas(
  std::vector<model::ntp> ntps, model::timeout_clock::time_point timeout) {
    std::vector<move_cancellation_result> results;
    results.reserve(ntps.size());
    co_await ss::max_concurrent_for_each(
      ntps, 32, [this, &results, timeout](model::ntp& ntp) {
          auto f = cancel_moving_partition_replicas(ntp, timeout);
          return f.then(
            [ntp = std::move(ntp), &results](std::error_code ec) mutable {
                results.emplace_back(
                  std::move(ntp), map_update_interruption_error_code(ec));
            });
      });

    co_return results;
}

ss::future<std::error_code> topics_frontend::change_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    auto tp_metadata = _topics.local().get_topic_metadata_ref(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    auto current_replication_factor
      = tp_metadata.value().get().get_replication_factor();

    if (current_replication_factor == new_replication_factor) {
        co_return errc::success;
    }

    if (_topics.local().is_fully_disabled(topic)) {
        co_return errc::topic_disabled;
    }

    if (new_replication_factor < current_replication_factor) {
        co_return co_await decrease_replication_factor(
          topic, new_replication_factor, timeout);
    }
    co_return co_await increase_replication_factor(
      topic, new_replication_factor, timeout);
}

ss::future<topics_frontend::capacity_info> topics_frontend::get_health_info(
  model::topic_namespace topic, int32_t partition_count) const {
    capacity_info info;

    partitions_filter::partitions_set_t parititon_set;
    for (auto i = 0; i < partition_count; ++i) {
        parititon_set.emplace(i);
    }

    partitions_filter::topic_map_t topic_map;
    topic_map.emplace(topic.tp, std::move(parititon_set));

    partitions_filter partitions_for_report;
    partitions_for_report.namespaces.emplace(topic.ns, std::move(topic_map));

    node_report_filter filter;
    filter.ntp_filters = std::move(partitions_for_report);

    auto health_report = co_await _hm_frontend.local().get_cluster_health(
      cluster_report_filter{.node_report_filter = std::move(filter)},
      force_refresh::no,
      model::timeout_clock::now() + _get_health_report_timeout);

    if (!health_report) {
        vlog(
          clusterlog.info,
          "unable to get health report - {}",
          health_report.error().message());
        co_return info;
    }

    for (const auto& node_report : health_report.value().node_reports) {
        uint64_t total = 0;
        uint64_t free = 0;

        // This health report is just on the data disk.  If the cache has
        // a separate disk, it is not reflected in the node health.
        total += node_report->local_state.data_disk.total;
        free += node_report->local_state.data_disk.free;

        info.node_disk_reports.emplace(
          node_report->id,
          node_disk_space(node_report->id, total, total - free));
    }

    for (auto& node_report : health_report.value().node_reports) {
        co_await ss::max_concurrent_for_each(
          node_report->topics,
          32,
          [&info](const node_health_report::topics_t::value_type& status) {
              for (const auto& partition : status.second) {
                  info.ntp_sizes[partition.id] = partition.size_bytes;
              }
              return ss::now();
          });
    }

    co_return info;
}
namespace {

// Executed on the partition_allocator shard
ss::future<result<allocation_units::pointer>> do_increase_replication_factor(
  const model::topic_namespace& ns_tp,
  const assignments_set& assignments,
  partition_allocator& al,
  replication_factor new_rf,
  double max_disk_usage_ratio,
  const topics_frontend::capacity_info& capacity_info,
  std::optional<node2count_t> existing_replica_counts) {
    allocation_request req(ns_tp);
    req.partitions.reserve(assignments.size());
    co_await ssx::async_for_each(
      assignments.begin(),
      assignments.end(),
      [&](const assignments_set::value_type& assignment) {
          allocation_constraints allocation_constraints;

          auto partition_size_it = capacity_info.ntp_sizes.find(
            assignment.second.id);
          if (partition_size_it != capacity_info.ntp_sizes.end()) {
              // Add constraint on partition max_disk_usage_ratio overfill
              allocation_constraints.add(disk_not_overflowed_by_partition(
                max_disk_usage_ratio,
                partition_size_it->second,
                capacity_info.node_disk_reports));

              // Add constraint on least disk usage
              allocation_constraints.add(least_disk_filled(
                max_disk_usage_ratio,
                partition_size_it->second,
                capacity_info.node_disk_reports));
          }

          req.partitions.emplace_back(
            assignment.second, new_rf, std::move(allocation_constraints));
      });
    req.existing_replica_counts = existing_replica_counts;

    auto units = co_await al.allocate(std::move(req));
    if (units.has_error()) {
        vlog(
          clusterlog.warn,
          "attempt to replication factor for topic {} to {} failed, error: {}",
          ns_tp,
          new_rf,
          units.error().message());
    }
    co_return units;
}

} // namespace

ss::future<std::error_code> topics_frontend::increase_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    if (
      static_cast<size_t>(new_replication_factor)
      > _members_table.local().node_count()) {
        vlog(
          clusterlog.warn,
          "New replication factor({}) is greater than number of brokers({})",
          new_replication_factor,
          _members_table.local().node_count());
        co_return errc::topic_invalid_replication_factor;
    }

    auto tp_metadata = _topics.local().get_topic_metadata(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    if (_topics.local().is_fully_disabled(topic)) {
        co_return errc::topic_disabled;
    }

    auto partition_count = tp_metadata->get_configuration().partition_count;

    auto health_report = co_await get_health_info(topic, partition_count);

    auto hard_max_disk_usage_ratio = (100 - _hard_max_disk_usage_ratio())
                                     / 100.0;

    std::optional<node2count_t> existing_replica_counts;
    if (_partition_autobalancing_topic_aware()) {
        node2count_t node2count;
        for (const auto& [_, p_as] : tp_metadata->get_assignments()) {
            for (const auto& bs : p_as.replicas) {
                node2count[bs.node_id] += 1;
            }
        }
        existing_replica_counts = std::move(node2count);
    }

    // units shold exist during replicate_and_wait call
    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [&topic,
       &tp_metadata,
       new_replication_factor,
       hard_max_disk_usage_ratio,
       &health_report,
       existing_replica_counts = std::move(existing_replica_counts)](
        partition_allocator& al) mutable {
          return do_increase_replication_factor(
            topic,
            tp_metadata->get_assignments(),
            al,
            new_replication_factor,
            hard_max_disk_usage_ratio,
            health_report,
            std::move(existing_replica_counts));
      });
    if (units.has_error()) {
        co_return units.error();
    }

    std::vector<move_topic_replicas_data> new_assignments;
    new_assignments.reserve(units.value()->get_assignments().size());
    for (const auto& assignment : units.value()->get_assignments()) {
        new_assignments.emplace_back(assignment.id, assignment.replicas);
    }

    move_topic_replicas_cmd cmd(topic, std::move(new_assignments));
    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

ss::future<std::error_code> topics_frontend::decrease_replication_factor(
  model::topic_namespace topic,
  cluster::replication_factor new_replication_factor,
  model::timeout_clock::time_point timeout) {
    std::vector<move_topic_replicas_data> new_assignments;

    auto tp_metadata = _topics.local().get_topic_metadata(topic);
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    if (_topics.local().is_fully_disabled(topic)) {
        co_return errc::topic_disabled;
    }

    std::optional<std::error_code> error;

    co_await ss::max_concurrent_for_each(
      tp_metadata->get_assignments(),
      32,
      [&new_assignments, &error, topic, new_replication_factor](
        assignments_set::value_type& assignment) {
          if (error) {
              return ss::now();
          }
          if (assignment.second.replicas.size() < new_replication_factor()) {
              error = errc::topic_invalid_replication_factor;
              return ss::now();
          }

          new_assignments.emplace_back(move_topic_replicas_data());
          new_assignments.back().partition = assignment.second.id;
          new_assignments.back().replicas.resize(new_replication_factor);
          std::copy_n(
            assignment.second.replicas.begin(),
            new_replication_factor,
            new_assignments.back().replicas.begin());
          return ss::now();
      });

    if (error) {
        co_return error.value();
    }

    move_topic_replicas_cmd cmd(topic, std::move(new_assignments));

    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

allocation_request make_allocation_request(
  model::ntp ntp,
  replication_factor tp_replication_factor,
  const std::vector<model::node_id>& new_replicas) {
    auto nt = model::topic_namespace(ntp.ns, ntp.tp.topic);
    allocation_request req(nt);
    req.partitions.reserve(1);
    allocation_constraints constraints;
    constraints.add(on_nodes(new_replicas));
    req.partitions.emplace_back(
      ntp.tp.partition, tp_replication_factor, std::move(constraints));
    return req;
}

ss::future<result<ss::chunked_fifo<partition_assignment>>>
topics_frontend::generate_reassignments(
  model::ntp ntp, std::vector<model::node_id> new_replicas) {
    auto tp_metadata = _topics.local().get_topic_metadata_ref(
      model::topic_namespace{ntp.ns, ntp.tp.topic});
    if (!tp_metadata.has_value()) {
        co_return errc::topic_not_exists;
    }

    if (_topics.local().is_disabled(ntp)) {
        co_return errc::partition_disabled;
    }

    auto tp_replication_factor
      = tp_metadata.value().get().get_replication_factor();

    auto units = co_await _allocator.invoke_on(
      partition_allocator::shard,
      [ntp, tp_replication_factor, new_replicas{std::move(new_replicas)}](
        partition_allocator& al) {
          return al.allocate(
            make_allocation_request(ntp, tp_replication_factor, new_replicas));
      });

    if (!units) {
        co_return units.error();
    }

    auto assignments = units.value()->copy_assignments();
    if (assignments.empty()) {
        co_return errc::no_partition_assignments;
    }

    co_return assignments;
}

ss::future<std::error_code> topics_frontend::move_partition_replicas(
  model::ntp ntp,
  std::vector<model::node_id> new_replica_set,
  reconfiguration_policy policy,
  model::timeout_clock::time_point tout,
  std::optional<model::term_id> term) {
    auto assignments = co_await generate_reassignments(
      ntp, std::move(new_replica_set));

    // The return type from generate_reassignments is a
    // result<vector<partition_assignment>>. So we need to make sure
    // the result: 1) has a value and 2) the vector should only have one replica
    // set, so check that the front partition id matches the request.

    if (!assignments) {
        co_return assignments.error();
    }

    if (assignments.value().front().id != ntp.tp.partition) {
        co_return errc::allocation_error;
    }

    co_return co_await move_partition_replicas(
      ntp, std::move(assignments.value().front().replicas), policy, tout, term);
}

ss::future<result<partition_state_reply>>
topics_frontend::do_get_partition_state(model::node_id node, model::ntp ntp) {
    if (node == _self) {
        partition_state_reply reply{};
        auto shard = _shard_table.local().shard_for(ntp);
        if (!shard) {
            reply.error_code = errc::partition_not_exists;
            return ss::make_ready_future<result<partition_state_reply>>(reply);
        }
        return _pm.invoke_on(
          *shard,
          [ntp = std::move(ntp),
           reply = std::move(reply)](partition_manager& pm) mutable {
              auto partition = pm.get(ntp);
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
      _self,
      ss::this_shard_id(),
      node,
      timeout,
      [ntp = std::move(ntp),
       timeout](controller_client_protocol client) mutable {
          return client
            .get_partition_state(
              partition_state_request{.ntp = ntp}, rpc::client_opts(timeout))
            .then(&rpc::get_ctx_data<partition_state_reply>);
      });
}

ss::future<result<std::vector<partition_state>>>
topics_frontend::get_partition_state(model::ntp ntp) {
    const auto& topics = _topics.local();
    if (!topics.contains(model::topic_namespace_view(ntp))) {
        co_return errc::partition_not_exists;
    }
    std::set<model::node_id> nodes_to_query;
    const auto& current = topics.get_partition_assignment(ntp);
    if (current) {
        const auto& bss = current->replicas;
        std::for_each(
          bss.begin(),
          bss.end(),
          [&nodes_to_query](const model::broker_shard& bs) {
              nodes_to_query.insert(bs.node_id);
          });
    }
    const auto& prev = topics.get_previous_replica_set(ntp);
    if (prev) {
        std::for_each(
          prev.value().begin(),
          prev.value().end(),
          [&nodes_to_query](const model::broker_shard& bs) {
              nodes_to_query.insert(bs.node_id);
          });
    }

    if (nodes_to_query.empty()) {
        co_return errc::no_partition_assignments;
    }

    std::vector<ss::future<result<partition_state_reply>>> futures;
    futures.reserve(nodes_to_query.size());
    for (const auto& node : nodes_to_query) {
        futures.push_back(do_get_partition_state(node, ntp));
    }

    auto finished_futures = co_await ss::when_all(
      futures.begin(), futures.end());
    std::vector<partition_state> results;
    results.reserve(finished_futures.size());
    for (auto& fut : finished_futures) {
        if (fut.failed()) {
            auto ex = fut.get_exception();
            vlog(
              clusterlog.debug,
              "Failed to get partition state for ntp: {}, failure: {}",
              ntp,
              ex);
            continue;
        }
        auto result = fut.get();
        if (result.has_error()) {
            vlog(
              clusterlog.debug,
              "Failed to get partition state for ntp: {}, result: {}",
              ntp,
              result.error());
            continue;
        }
        auto res = std::move(result.value());
        if (res.error_code != errc::success || !res.state) {
            vlog(
              clusterlog.debug,
              "Error during partition state fetch for ntp: {}, error: "
              "{}",
              ntp,
              res.error_code);
            continue;
        }
        results.push_back(std::move(*res.state));
    }
    co_return results;
}

void topics_frontend::print_rf_warning_message() {
    const auto min_rf = _minimum_topic_replication();
    const auto& topics = _topics.local().topics_map();
    for (const auto& t : topics) {
        if (!model::is_user_topic(t.first)) {
            continue;
        }
        auto rf = t.second.get_replication_factor();
        if (rf() >= min_rf) {
            continue;
        }
        vlog(
          clusterlog.warn,
          "Topic {} has a replication factor less than specified "
          "minimum: {} < {}",
          t.first,
          rf,
          min_rf);
    }
}

bool topics_frontend::node_local_core_assignment_enabled() const {
    return _features.local().is_active(
      features::feature::node_local_core_assignment);
}

ss::future<std::error_code> topics_frontend::set_partition_replica_shard(
  model::ntp ntp,
  model::node_id replica,
  ss::shard_id shard,
  model::timeout_clock::time_point deadline) {
    if (!node_local_core_assignment_enabled()) {
        co_return errc::feature_disabled;
    }

    if (replica == _self) {
        co_return co_await set_local_partition_shard(ntp, shard);
    }

    auto replicas_view = _topics.local().get_replicas_view(ntp);
    if (!replicas_view) {
        co_return errc::partition_not_exists;
    }
    if (!log_revision_on_node(replicas_view.value(), replica)) {
        co_return errc::replica_does_not_exist;
    }

    auto reply = co_await _connections.local()
                   .with_node_client<cluster::controller_client_protocol>(
                     _self,
                     ss::this_shard_id(),
                     replica,
                     deadline,
                     [ntp, shard, deadline](
                       controller_client_protocol cp) mutable {
                         return cp
                           .set_partition_shard(
                             set_partition_shard_request{
                               .ntp = std::move(ntp), .shard = shard},
                             rpc::client_opts(deadline))
                           .then(&rpc::get_ctx_data<set_partition_shard_reply>);
                     });

    if (reply.has_error()) {
        co_return reply.error();
    }
    co_return reply.value().ec;
}

ss::future<errc>
topics_frontend::set_local_partition_shard(model::ntp ntp, ss::shard_id shard) {
    return _shard_balancer.invoke_on(
      shard_balancer::shard_id,
      [ntp = std::move(ntp), shard](shard_balancer& sb) {
          return sb.reassign_shard(ntp, shard);
      });
}

ss::future<errc> topics_frontend::trigger_local_partition_shard_rebalance() {
    return _shard_balancer.invoke_on(
      shard_balancer::shard_id,
      [](shard_balancer& sb) { return sb.trigger_rebalance(); });
}

} // namespace cluster
