// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/topics_frontend.h"

#include "base/type_traits.h"
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
#include "config/leaders_preference.h"
#include "data_migration_types.h"
#include "features/enterprise_feature_messages.h"
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
#include "scheduling/types.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "topic_configuration.h"
#include "topic_properties.h"
#include "topic_rules.h"

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

namespace {

std::vector<std::string_view>
get_enterprise_features(const cluster::topic_configuration& cfg) {
    std::vector<std::string_view> features;
    static const auto si_disabled = model::shadow_indexing_mode::disabled;
    // Only enforce tiered storage topic config sanctions when cloud storage is
    // enabled for the cluster
    if (config::shard_local_cfg().cloud_storage_enabled.is_restricted()) {
        if (
          (cfg.properties.shadow_indexing.value_or(si_disabled) != si_disabled)
          || (cfg.properties.storage_mode == model::redpanda_storage_mode::tiered)) {
            features.emplace_back("tiered storage");
        }
        if (cfg.is_recovery_enabled()) {
            features.emplace_back("topic recovery");
        }
        if (cfg.is_read_replica()) {
            features.emplace_back("remote read replicas");
        }
    }

    // Only enforce schema ID validation topic configs if Schema ID validation
    // is enabled for the cluster
    if (config::shard_local_cfg().enable_schema_id_validation.is_restricted()) {
        if (cfg.is_schema_id_validation_enabled()) {
            features.emplace_back("schema ID validation");
        }
    }

    // We are always enforcing leadership preference restrictions
    if (const auto& leaders_pref = cfg.properties.leaders_preference;
        leaders_pref.has_value()
        && config::shard_local_cfg()
             .default_leaders_preference.check_restricted(
               leaders_pref.value())) {
        features.emplace_back("leadership pinning");
    }

    if (config::shard_local_cfg().iceberg_enabled.is_restricted()) {
        if (cfg.properties.iceberg_mode != model::iceberg_mode::disabled) {
            features.emplace_back("iceberg");
        }
    }
    if (config::shard_local_cfg().cloud_topics_enabled.is_restricted()) {
        if (
          cfg.properties.storage_mode == model::redpanda_storage_mode::cloud) {
            features.emplace_back("cloud topics");
        }
    }
    return features;
}

std::vector<std::string_view> get_enterprise_features(
  const cluster::metadata_cache& metadata,
  const cluster::topic_properties_update& update) {
    auto tp_metadata = metadata.get_topic_metadata_ref(update.tp_ns);
    if (!tp_metadata.has_value()) {
        // Topic does not exist, nothing to validate
        return {};
    }

    const auto& properties = tp_metadata->get().get_configuration().properties;
    auto updated_properties = cluster::topic_table::update_topic_properties(
      properties, {update.tp_ns, update.properties});

    std::vector<std::string_view> features;
    static const auto si_disabled = model::shadow_indexing_mode::disabled;
    static const auto tiered = model::redpanda_storage_mode::tiered;
    // Only enforce tiered storage topic config sanctions when cloud storage is
    // enabled for the cluster
    if (config::shard_local_cfg().cloud_storage_enabled.is_restricted()) {
        // Check if tiered storage is being enabled (wasn't before, is now)
        auto old_si_mode = properties.shadow_indexing.value_or(si_disabled);
        auto new_si_mode = updated_properties.shadow_indexing.value_or(
          si_disabled);
        auto old_storage_mode = properties.storage_mode;
        auto new_storage_mode = updated_properties.storage_mode;
        if (
          old_si_mode < new_si_mode
          || (old_storage_mode != tiered && new_storage_mode == tiered)
          || (properties.remote_delete < updated_properties.remote_delete)) {
            features.emplace_back("tiered storage");
        }
    }

    static constexpr auto key_schema_id_validation_enabled =
      [](const cluster::topic_properties& pp) -> bool {
        return pp.record_key_schema_id_validation.value_or(false)
               || pp.record_key_schema_id_validation_compat.value_or(false);
    };

    static constexpr auto value_schema_id_validation_enabled =
      [](const cluster::topic_properties& pp) -> bool {
        return pp.record_value_schema_id_validation.value_or(false)
               || pp.record_value_schema_id_validation_compat.value_or(false);
    };

    static constexpr auto schema_id_validation_enabled =
      [](const cluster::topic_properties& pp) -> bool {
        return key_schema_id_validation_enabled(pp)
               || value_schema_id_validation_enabled(pp);
    };

    constexpr auto unset_or_unchanged =
      [](
        const reflection::is_std_optional auto& curr,
        const reflection::is_std_optional auto& nxt) -> bool {
        // allow anything -> null
        // allow non-null -> same non-null
        return !nxt.has_value() || curr == nxt;
    };

    auto sns_modified = [&unset_or_unchanged,
                         &pp = properties,
                         &up = updated_properties]() -> bool {
        return !(
          unset_or_unchanged(
            pp.record_key_subject_name_strategy,
            up.record_key_subject_name_strategy)
          && unset_or_unchanged(
            pp.record_key_subject_name_strategy_compat,
            up.record_key_subject_name_strategy_compat)
          && unset_or_unchanged(
            pp.record_value_subject_name_strategy,
            up.record_value_subject_name_strategy)
          && unset_or_unchanged(
            pp.record_value_subject_name_strategy_compat,
            up.record_value_subject_name_strategy_compat));
    };

    // Only enforce schema ID validation topic configs if Schema ID validation
    // is enabled for the cluster
    if (config::shard_local_cfg().enable_schema_id_validation.is_restricted()) {
        if (
          ((key_schema_id_validation_enabled(properties)
            < key_schema_id_validation_enabled(updated_properties))
           || (value_schema_id_validation_enabled(properties) < value_schema_id_validation_enabled(updated_properties)))
          || (schema_id_validation_enabled(updated_properties) && sns_modified())) {
            features.emplace_back("schema id validation");
        }
    }

    if (const auto& updated_pref = updated_properties.leaders_preference;
        updated_pref != properties.leaders_preference
        && updated_pref.has_value()
        && config::shard_local_cfg()
             .default_leaders_preference.check_restricted(
               updated_pref.value())) {
        features.emplace_back("leadership pinning");
    }
    if (config::shard_local_cfg().iceberg_enabled.is_restricted()) {
        if (properties.iceberg_mode != model::iceberg_mode::disabled) {
            features.emplace_back("iceberg");
        }
    }
    if (config::shard_local_cfg().cloud_topics_enabled.is_restricted()) {
        if (properties.storage_mode == model::redpanda_storage_mode::cloud) {
            features.emplace_back("cloud topics");
        }
    }
    return features;
}

cluster::simple_allocation_request make_simple_allocation_request(
  const cluster::custom_assignable_topic_configuration& ca_cfg,
  bool topic_aware) {
    vassert(
      !ca_cfg.has_custom_assignment(),
      "make_custom_allocation_request should have been called, instead");
    cluster::simple_allocation_request req{
      ca_cfg.cfg.tp_ns,
      ca_cfg.cfg.partition_count,
      ca_cfg.cfg.replication_factor};
    if (topic_aware) {
        req.existing_replica_counts = cluster::node2count_t{};
    }
    return req;
}

cluster::allocation_request make_custom_allocation_request(
  const cluster::custom_assignable_topic_configuration& ca_cfg,
  bool topic_aware) {
    vassert(
      ca_cfg.has_custom_assignment(),
      "make_simple_allocation_request should have been called, instead");
    // no custom assignments, lets allocator decide based on partition count
    cluster::allocation_request req(ca_cfg.cfg.tp_ns);
    req.partitions.reserve(ca_cfg.custom_assignments.size());
    for (auto& cas : ca_cfg.custom_assignments) {
        cluster::allocation_constraints constraints;
        constraints.add(cluster::on_nodes(cas.replicas));

        req.partitions.emplace_back(
          cas.id, cas.replicas.size(), std::move(constraints));
    }
    if (topic_aware) {
        req.existing_replica_counts = cluster::node2count_t{};
    }
    return req;
}

} // namespace

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
  config::binding<bool> partition_autobalancing_topic_aware,
  config::binding<std::optional<uint32_t>> max_user_topics)
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
      std::move(partition_autobalancing_topic_aware))
  , _max_user_topics(std::move(max_user_topics)) {
    if (ss::this_shard_id() == 0) {
        _minimum_topic_replication.watch(
          [this]() { print_rf_warning_message(); });
    }
}

namespace {

template<std::ranges::input_range R>
requires std::same_as<std::ranges::range_value_t<R>, topic_result>
bool needs_linearizable_barrier(const R& results) {
    return std::any_of(
      results.cbegin(), results.cend(), [](const topic_result& r) {
          return r.ec == errc::success;
      });
}

} // namespace

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

ss::future<chunked_vector<topic_result>>
topics_frontend::update_topic_properties(
  topic_properties_update_vector updates,
  model::timeout_clock::time_point timeout) {
    auto cluster_leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!cluster_leader) {
        co_return make_error_topic_results<chunked_vector>(
          updates, errc::no_leader_controller);
    }

    if (!_features.local().is_active(features::feature::cloud_retention)) {
        // The ADL encoding for cluster::incremental_topic_updates has evolved
        // in v22.3. ADL is not forwards compatible, so we need to safe-guard
        // against sending a message from the future to older nodes.

        vlog(
          clusterlog.info,
          "Refusing to update topics as not all cluster nodes are running "
          "v22.3");
        co_return make_error_topic_results<chunked_vector>(
          updates, errc::feature_disabled);
    }

    // current node is a leader, just replicate
    if (cluster_leader == _self) {
        // replicate empty batch to make sure leader local state is up to date.
        auto result = co_await stm_linearizable_barrier(timeout);
        if (!result) {
            co_return make_error_topic_results<chunked_vector>(
              updates, map_errc(result.error()));
        }

        auto results = co_await ssx::parallel_transform<chunked_vector>(
          std::move(updates), [this, timeout](topic_properties_update update) {
              if (
                _features.local().should_sanction()
                && is_user_topic(update.tp_ns)) {
                  if (auto f = get_enterprise_features(_metadata_cache, update);
                      !f.empty()) {
                      auto msg
                        = features::enterprise_error_message::topic_property(f);
                      vlog(clusterlog.warn, "{}", msg);
                      return ss::make_ready_future<topic_result>(topic_result(
                        update.tp_ns,
                        errc::topic_invalid_config,
                        std::move(msg)));
                  }
              }
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
              return make_error_topic_results<chunked_vector>(
                updates, map_errc(r.error()));
          }
          return std::move(r.value().results);
      });
}

ss::future<std::error_code> topics_frontend::do_update_replication_factor(
  topic_properties_update& update, model::timeout_clock::time_point timeout) {
    switch (update.custom_properties.replication_factor.op) {
    case incremental_update_operation::set: {
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

topic_result make_error_result(
  const model::topic_namespace& tp_ns,
  std::error_code ec,
  std::optional<ss::sstring> msg = std::nullopt) {
    errc error = ec.category() == cluster::error_category()
                   ? errc(ec.value())
                   : errc::topic_operation_error;
    if (msg.has_value()) {
        return {tp_ns, error, std::move(msg).value()};
    }
    return topic_result{tp_ns, error};
}

topic_result topics_frontend::validate_topic_configuration(
  const custom_assignable_topic_configuration& assignable_config) {
    const auto make_result = [&assignable_config](
                               errc err,
                               std::optional<ss::sstring> msg = std::nullopt) {
        return cluster::make_error_result(
          assignable_config.cfg.tp_ns, err, std::move(msg));
    };
    if (auto ec = validate_topic_name(assignable_config.cfg.tp_ns); ec) {
        return make_result(errc::invalid_topic_name, ec.message());
    }

    if (assignable_config.cfg.partition_count < 1) {
        return make_result(errc::topic_invalid_partitions);
    }

    if (assignable_config.cfg.replication_factor < 1) {
        return make_result(errc::topic_invalid_replication_factor);
    }

    if (assignable_config.has_custom_assignment()) {
        for (auto& custom : assignable_config.custom_assignments) {
            if (
              static_cast<int16_t>(custom.replicas.size())
              != assignable_config.cfg.replication_factor) {
                return make_result(errc::topic_invalid_replication_factor);
            }
        }
    }
    if (
      (assignable_config.is_read_replica()
       || assignable_config.is_recovery_enabled())
      && !_cloud_storage_api.local_is_initialized()) {
        return make_result(
          errc::topic_invalid_config, "Tiered storage is not enabled");
    }

    // the only way that cloud topics can be enabled on a topic is if the cloud
    // topics development feature is also enabled.
    if (!config::shard_local_cfg().cloud_topics_enabled()) {
        if (
          assignable_config.cfg.properties.storage_mode
          == model::redpanda_storage_mode::cloud) {
            auto msg = ssx::sformat(
              "Cloud storage mode on {} is set but development feature is "
              "disabled",
              assignable_config.cfg.tp_ns);
            vlog(clusterlog.error, "{}", msg);
            return make_result(errc::topic_invalid_config, std::move(msg));
        }
    }

    if (
      _features.local().should_sanction()
      && is_user_topic(assignable_config.cfg.tp_ns)) {
        if (auto f = get_enterprise_features(assignable_config.cfg);
            !f.empty()) {
            auto msg = features::enterprise_error_message::topic_property(f);
            vlog(clusterlog.warn, "{}", msg);
            return make_result(errc::topic_invalid_config, std::move(msg));
        }
    }

    return make_result(errc::success);
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

    // Enforce cluster-wide topic count limit only on user created topics.
    if (auto max_user_topics_opt = _max_user_topics()) {
        // This is intended to approximate the number of topics that are
        // user-defined and may return a count slightly smaller than the actual
        // amount.
        auto user_topic_count = std::max(
                                  _topics.local().all_topics_count(),
                                  model::non_user_topics.size())
                                - model::non_user_topics.size();
        if (
          user_topic_count >= *max_user_topics_opt
          && model::is_user_topic(tp_ns)) {
            vlog(
              clusterlog.warn,
              "unable to create topic {} as the number of user topics exceeds "
              "the cluster limit",
              tp_ns);
            co_return make_error_result(
              assignable_config.cfg.tp_ns,
              make_error_code(errc::topic_operation_error),
              "number of topics exceeds cluster limit");
        }
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

    if (!assignable_config.cfg.tp_id.has_value()) {
        assignable_config.cfg.tp_id = model::create_topic_id();
        vlog(
          clusterlog.debug,
          "Configuring topic {} with id {}",
          assignable_config.cfg.tp_ns,
          assignable_config.cfg.tp_id.value());
    }

    auto result = validate_topic_configuration(assignable_config);

    if (result.ec != errc::success) {
        co_return result;
    }

    auto is_cloud_topic = assignable_config.cfg.properties.storage_mode
                          == model::redpanda_storage_mode::cloud;
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

    // TODO: implement a recovery primitive for cloud topics.
    if (assignable_config.is_recovery_enabled() && !is_cloud_topic) {
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
              "valid topic manifest");
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
        auto ct_metastore_label
          = _topics.local()
              .get_topic_metadata_ref(model::l1_metastore_nt)
              .and_then([](const topic_metadata& m) {
                  return m.get_configuration().properties.remote_label;
              });
        auto remote_label = is_cloud_topic && ct_metastore_label
                              ? *ct_metastore_label
                              : cloud_storage::remote_label(
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
          if (assignable_config.has_custom_assignment()) {
              return al.allocate(
                make_custom_allocation_request(assignable_config, topic_aware));
          }
          return al.allocate(
            make_simple_allocation_request(assignable_config, topic_aware));
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
          random_generators::global().engine());
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
    } else {
        mode = topic_lifecycle_transition_mode::pending_gc;
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
  nt_revision topic,
  topic_purge_domain domain,
  model::timeout_clock::duration timeout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);

    // no leader available
    if (!leader) {
        return ss::make_ready_future<topic_result>(
          topic_result(topic.nt, errc::no_leader_controller));
    }
    // current node is a leader controller
    if (leader == _self) {
        return do_purged_topic(
          std::move(topic), domain, model::timeout_clock::now() + timeout);
    } else {
        return dispatch_purged_topic_to_leader(
          leader.value(), std::move(topic), domain, timeout);
    }
}

ss::future<topic_result> topics_frontend::do_purged_topic(
  nt_revision topic,
  topic_purge_domain domain,
  model::timeout_clock::time_point deadline) {
    topic_lifecycle_transition_cmd cmd(
      topic.nt,
      topic_lifecycle_transition{
        .topic = topic,
        .mode = topic_lifecycle_transition_mode::purged,
        .domain = domain});

    bool marker_exists = false;
    switch (domain) {
    case topic_purge_domain::cloud_storage:
        marker_exists = _topics.local().get_lifecycle_markers().contains(topic);
        break;
    case topic_purge_domain::iceberg:
        marker_exists = _topics.local().get_iceberg_tombstones().contains(
          topic.nt);
        break;
    case topic_purge_domain::cloud_topic:
        marker_exists = _topics.local().get_cloud_topic_tombstones().contains(
          topic);
        break;
    }

    if (!marker_exists) {
        // Do not write to log if the marker is already gone
        vlog(
          clusterlog.info,
          "Dropping duplicate purge request for lifecycle marker {} in domain "
          "{}",
          topic.nt,
          domain);
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
  topic_purge_domain domain,
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
                 [topic, timeout, domain](
                   controller_client_protocol cp) mutable {
                     return cp.purged_topic(
                       purged_topic_request{
                         .topic = std::move(topic),
                         .timeout = timeout,
                         .domain = domain},
                       rpc::client_opts(model::timeout_clock::now() + timeout));
                 })
               .then(&rpc::get_ctx_data<purged_topic_reply>);
    if (r.has_error()) {
        co_return topic_result(topic.nt, map_errc(r.error()));
    }
    co_return std::move(r.value().result);
}

std::error_code
topics_frontend::validate_topic_name(const model::topic_namespace& topic) {
    if (topic.ns == model::kafka_namespace) {
        const auto errc = model::validate_kafka_topic_name(topic.tp);
        if (static_cast<model::errc>(errc.value()) != model::errc::success) {
            vlog(clusterlog.info, "{} {}", errc.message(), topic.tp());
            return errc;
        }
    }
    return model::errc::success;
}

ss::future<result<model::offset>> topics_frontend::stm_linearizable_barrier(
  model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(controller_stm_shard, [timeout](controller_stm& stm) {
        return stm.insert_linearizable_barrier(timeout).then([](auto r) {
            if (r.has_error()) {
                return ss::make_ready_future<result<model::offset>>(r.error());
            }
            return ss::make_ready_future<result<model::offset>>(
              r.value().first);
        });
    });
}

} // namespace cluster
