/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/frontend.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "cluster_link/model/filter_utils.h"
#include "cluster_link/model/types.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "rpc/connection_cache.h"
#include "ssx/when_all.h"

namespace cluster::cluster_link {

namespace {
errc map_errc(std::error_code ec) {
    if (ec == errc::success) {
        return errc::success;
    }
    if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        default:
            return errc::replication_error;
        }
    }
    if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        default:
            return errc::rpc_error;
        }
    }
    if (ec.category() == cluster::error_category()) {
        switch (cluster::errc(ec.value())) {
        case cluster::errc::not_leader_controller:
            return errc::not_leader_controller;
        case cluster::errc::replication_error:
            return errc::replication_error;
        case cluster::errc::timeout:
            return errc::timeout;
        default:
            return errc::rpc_error;
        }
    }
    if (ec.category() == error_category()) {
        return errc(ec.value());
    }
    return errc::rpc_error;
}

} // namespace

errc frontend::validate_mutation(const cluster_link_cmd& cmd) const {
    // Initially for DR, we will only support a single cluster link at a time.
    static constexpr size_t max_links = 1;
    if (!cluster_linking_enabled()) {
        return errc::feature_disabled;
    }
    validator v{
      _table,
      max_links,
      absl::flat_hash_set<std::string_view>(
        ::cluster_link::model::disallowed_topic_properties.begin(),
        ::cluster_link::model::disallowed_topic_properties.end())};
    return v.validate_mutation(cmd);
}

bool frontend::is_sanctioned() { return _features->should_sanction(); }

frontend::validator::validator(
  table* table,
  size_t max_links,
  absl::flat_hash_set<std::string_view> excluded_topic_properties)
  : _table(table)
  , _max_links(max_links)
  , _excluded_topic_properties(std::move(excluded_topic_properties)) {}

errc frontend::validator::validate_mutation(const cluster_link_cmd& cmd) const {
    return ss::visit(
      cmd,
      [this](const cluster::cluster_link_upsert_cmd& cmd) {
          auto existing = _table->find_link_by_name(cmd.value.name);
          if (existing) {
              // upsert
              if (existing->uuid != cmd.value.uuid) {
                  // If the UUIDs do not match, it means we are trying to
                  // update an existing link with a different UUID.
                  vlog(
                    cluster::clusterlog.warn,
                    "Attempting to upsert a cluster link with name {} with a "
                    "different UUID ({}) than the existing one ({})",
                    cmd.value.name,
                    cmd.value.uuid,
                    existing->uuid);
                  return errc::uuid_conflict;
              }
              auto ec = validate_connection_config(cmd.value.connection);
              if (ec != errc::success) {
                  return ec;
              }

              return validate_metadata_mirroring_config(
                cmd.value.configuration.topic_metadata_mirroring_cfg);
          }
          // New item!
          if (cmd.value.name().empty()) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link without a name");
              return errc::link_name_invalid;
          }
          if (
            cmd.value.state.status
            != ::cluster_link::model::link_status::active) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link with invalid initial "
                "state: {}",
                cmd.value.state.status);
              return errc::invalid_create;
          }
          constexpr static size_t max_name_size = 128;
          if (cmd.value.name().size() > max_name_size) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link with too large of a "
                "name "
                "{} > {}",
                cmd.value.name().size(),
                max_name_size);
              return errc::link_name_invalid;
          }
          if (!std::ranges::all_of(cmd.value.name(), [](char c) {
                  return std::isalnum(c) || c == '.' || c == '-' || c == '_';
              })) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link with a name containing "
                "invalid characters");
              return errc::link_name_invalid;
          }
          if (_table->size() >= _max_links) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to create a cluster link when the maximum number "
                "of links ({}) is already reached",
                _max_links);
              return errc::limit_exceeded;
          }

          auto ec = validate_connection_config(cmd.value.connection);
          if (ec != errc::success) {
              return ec;
          }

          return validate_metadata_mirroring_config(
            cmd.value.configuration.topic_metadata_mirroring_cfg);
      },
      [this](const cluster::cluster_link_remove_cmd& cmd) {
          auto meta = _table->find_link_by_name(cmd.value.link_name);
          if (!meta) {
              return errc::does_not_exist;
          }

          const auto is_removable =
            [](const ::cluster_link::model::mirror_topic_status s) {
                switch (s) {
                case ::cluster_link::model::mirror_topic_status::active:
                case ::cluster_link::model::mirror_topic_status::paused:
                case ::cluster_link::model::mirror_topic_status::failing_over:
                case ::cluster_link::model::mirror_topic_status::promoting:
                    return false;
                case ::cluster_link::model::mirror_topic_status::promoted:
                case ::cluster_link::model::mirror_topic_status::failed_over:
                case ::cluster_link::model::mirror_topic_status::failed:
                    return true;
                }
            };
          const auto mirror_topic_states
            = meta->state.mirror_topics | std::views::values
              | std::views::transform(
                &::cluster_link::model::mirror_topic_metadata::status);
          if (
            cmd.value.force
            || std::ranges::all_of(mirror_topic_states, is_removable)) {
              return errc::success;
          }
          vlog(
            cluster::clusterlog.info,
            "Attempting to remove cluster link {} which still has active "
            "mirror topics",
            cmd.key);
          return errc::link_has_active_shadow_topics;
      },
      [this](const cluster::cluster_link_add_mirror_topic_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          if (
            cmd.value.metadata.status
            != ::cluster_link::model::mirror_topic_status::active) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to add mirror topic {} with invalid initial state "
                "{}",
                cmd.value.topic,
                cmd.value.metadata.status);
              return errc::invalid_create;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta) {
              return errc::does_not_exist;
          }
          const auto status = meta->state.status;
          if (status != ::cluster_link::model::link_status::active) {
              // fence any new topic additions if the link is not active
              vlog(
                cluster::clusterlog.warn,
                "Attempting to add mirror topic {} to link {} which is not in "
                "the active state (current state: {})",
                cmd.value.topic,
                meta->name,
                status);
              return errc::invalid_update;
          }

          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (id.has_value()) {
              if (id.value() != cmd.key) {
                  vlog(
                    cluster::clusterlog.warn,
                    "Attempting to add mirror topic '{}' to '{}', however it "
                    "is already mirrored by another link",
                    cmd.value.topic,
                    meta->name);
                  return errc::topic_being_mirrored_by_other_link;
              } else {
                  vlog(
                    cluster::clusterlog.warn,
                    "Topic '{}' is already mirrored by link '{}'",
                    cmd.value.topic,
                    meta->name);
                  return errc::topic_already_being_mirrored;
              }
          }
          if (cmd.value.metadata.partition_count < 1) {
              vlog(
                cluster::clusterlog.warn,
                "Invalid partition count for topic {} in link {}: {}",
                cmd.value.topic,
                meta->name,
                cmd.value.metadata.partition_count);
              return errc::invalid_update;
          }
          if (
            cmd.value.metadata.replication_factor.has_value()
            && cmd.value.metadata.replication_factor < 1) {
              vlog(
                cluster::clusterlog.warn,
                "Invalid replication factor: {}",
                cmd.value.metadata.replication_factor);
              return errc::invalid_update;
          }
          return errc::success;
      },
      [this](const cluster::cluster_link_delete_mirror_topic_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta) {
              return errc::does_not_exist;
          }
          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (!id.has_value()) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to delete mirror topic '{}' from link '{}', "
                "however topic is not being mirrored",
                cmd.value.topic,
                meta->name);
              return errc::topic_not_being_mirrored;
          }
          if (id.value() != cmd.key) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to delete mirror topic '{}' from link '{}', "
                "however topic "
                "is mirrored by another link",
                cmd.value.topic,
                meta->name);
              return errc::topic_being_mirrored_by_other_link;
          }
          return errc::success;
      },
      [this](const cluster::cluster_link_update_mirror_topic_status_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta) {
              return errc::does_not_exist;
          }
          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (!id.has_value()) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is not being mirrored",
                cmd.value.topic);
              return errc::topic_not_being_mirrored;
          } else if (id.value() != cmd.key) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is being mirrored by another link",
                cmd.value.topic);
              return errc::topic_being_mirrored_by_other_link;
          }
          auto status = _table->find_mirror_topic_status(cmd.value.topic);
          if (!status) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is not being mirrored",
                cmd.value.topic);
              return errc::topic_not_being_mirrored;
          }
          // If not a force update, ensure a valid status transition
          if (
            !cmd.value.force_update
            && !::cluster_link::model::is_valid_status_transition(
              *status, cmd.value.status)) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to change state of mirror topic {} from {} to "
                "invalid state {}",
                cmd.value.topic,
                *status,
                cmd.value.status);
              return errc::invalid_update;
          }
          return errc::success;
      },
      [this](
        const cluster::cluster_link_update_mirror_topic_properties_cmd& cmd) {
          auto ec = model::validate_kafka_topic_name(cmd.value.topic);
          if (ec) {
              vlog(cluster::clusterlog.warn, "Invalid topic name: {}", ec);
              return errc::mirror_topic_name_invalid;
          }
          auto meta = _table->find_link_by_id(cmd.key);
          if (!meta) {
              return errc::does_not_exist;
          }
          auto id = _table->find_id_by_topic(cmd.value.topic);
          if (!id.has_value()) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is not being mirrored",
                cmd.value.topic);
              return errc::topic_not_being_mirrored;
          }
          if (id.value() != cmd.key) {
              vlog(
                cluster::clusterlog.warn,
                "Topic '{}' is being mirrored by another link",
                cmd.value.topic);
              return errc::topic_being_mirrored_by_other_link;
          }
          const auto& mirror_state = meta->state;
          const auto it = mirror_state.mirror_topics.find(cmd.value.topic);

          vassert(
            it != mirror_state.mirror_topics.end(),
            "State inconsistency detected, should have been able to find {}",
            cmd.value.topic);

          if (
            mirror_state.status != ::cluster_link::model::link_status::active) {
              // fence any topic property updates if the link is not active
              vlog(
                cluster::clusterlog.warn,
                "Attempting to update mirror topic {} on link {} which is not "
                "in the active state (current state: {})",
                cmd.value.topic,
                meta->name,
                mirror_state.status);
              return errc::invalid_update;
          }

          if (cmd.value.partition_count < it->second.partition_count) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to update partition count of topic '{}' to {}, "
                "which is less than the current partition count {}",
                cmd.value.topic,
                cmd.value.partition_count,
                it->second.partition_count);
              return errc::invalid_update;
          }

          if (
            cmd.value.replication_factor.has_value()
            && cmd.value.replication_factor < 1) {
              vlog(
                cluster::clusterlog.warn,
                "Invalid replication factor: {}",
                cmd.value.replication_factor);
              return errc::invalid_update;
          }

          return errc::success;
      },
      [this](
        const cluster::cluster_link_update_cluster_link_configuration_cmd&
          cmd) {
          if (!_table->find_link_by_id(cmd.key)) {
              vlog(
                cluster::clusterlog.warn,
                "Attempting to update a non-existant link id {}",
                cmd.key);
              return errc::does_not_exist;
          }

          auto ec = validate_connection_config(cmd.value.connection);
          if (ec != errc::success) {
              return ec;
          }

          ec = validate_metadata_mirroring_config(
            cmd.value.link_config.topic_metadata_mirroring_cfg);
          if (ec != errc::success) {
              return ec;
          }

          return errc::success;
      });
}

errc frontend::validator::validate_connection_config(
  const ::cluster_link::model::connection_config& config) const {
    if (config.bootstrap_servers.empty()) {
        vlog(
          cluster::clusterlog.warn,
          "Attempting to create a cluster link without bootstrap servers");
        return errc::bootstrap_servers_empty;
    }

    if (config.cert.has_value() != config.key.has_value()) {
        vlog(
          cluster::clusterlog.warn,
          "If providing a certificate or key, both must be provided or "
          "neither");
        return errc::tls_configuration_invalid;
    }

    if (
      config.cert.has_value()
      && config.cert.value().index() != config.key.value().index()) {
        vlog(
          cluster::clusterlog.warn,
          "If providing a certificate or key, both must be file paths or "
          "both must be values");
        return errc::tls_configuration_invalid;
    }

    if (config.authn_config.has_value()) {
        auto ec = ss::visit(
          config.authn_config.value(),
          [](const ::cluster_link::model::scram_credentials& c) {
              if (c.username.empty()) {
                  vlog(
                    cluster::clusterlog.warn,
                    "Username for SCRAM credentials is empty");
                  return errc::scram_configuration_invalid;
              }

              if (c.password.empty()) {
                  vlog(
                    cluster::clusterlog.warn,
                    "Password for SCRAM credentials is empty");
                  return errc::scram_configuration_invalid;
              }

              if (
                c.mechanism != "SCRAM-SHA-256" && c.mechanism != "SCRAM-SHA-512"
                && c.mechanism != "PLAIN") {
                  vlog(
                    cluster::clusterlog.warn,
                    "Unsupported SCRAM mechanism: {}",
                    c.mechanism);
                  return errc::scram_configuration_invalid;
              }
              return errc::success;
          });

        if (ec != errc::success) {
            return ec;
        }
    }

    return errc::success;
}

errc frontend::validator::validate_metadata_mirroring_config(
  const ::cluster_link::model::topic_metadata_mirroring_config& config) const {
    // Validates that the pattern:
    // - is not empty
    // - does not contain the wildcard character '*' unless it is the only
    //   character in the pattern
    // - the characters are valid UTF-8
    // - wildcard only present in 'literal' patterns
    const auto check_filter_pattern =
      [](const ::cluster_link::model::resource_name_filter_pattern& p) {
          if (p.pattern.empty()) {
              vlog(cluster::clusterlog.info, "Filter pattern is empty");
              return true;
          }
          if (
            p.pattern.contains(
              ::cluster_link::model::resource_name_filter_pattern::wildcard)
            && p.pattern
                 != ::cluster_link::model::resource_name_filter_pattern::
                   wildcard) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern is invalid: Contains '*'");
              return true;
          }
          if (
            p.pattern
              == ::cluster_link::model::resource_name_filter_pattern::wildcard
            && p.pattern_type
                 != ::cluster_link::model::filter_pattern_type::literal) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern is invalid: Wildcard '*' can only be used in "
                "literal patterns");
              return true;
          }
          if (
            p.pattern
              != ::cluster_link::model::resource_name_filter_pattern::wildcard
            && !std::ranges::all_of(p.pattern, [](char c) {
                   return std::isalnum(c) || c == '.' || c == '-' || c == '_';
               })) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern contains invalid characters");
              return true;
          }
          // Do not permit specifying the consumer offsets or audit logging
          // topic
          if (
            p.pattern == ::model::kafka_consumer_offsets_topic()
            || p.pattern == ::model::kafka_audit_logging_topic()
            || p.pattern == ::model::schema_registry_internal_tp.topic()) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern filtering on invalid topic name: {}",
                p.pattern);
              return true;
          }
          // Do not permit specifying "_redpanda" or "__redpanda" as a topic
          // name prefix
          if (
            p.pattern_type == ::cluster_link::model::filter_pattern_type::prefix
            && (p.pattern == "_redpanda" || p.pattern == "__redpanda")) {
              vlog(
                cluster::clusterlog.info,
                "Filter pattern filtering on invalid topic name prefix: {}",
                p.pattern);
              return true;
          }
          return false;
      };
    if (std::ranges::any_of(config.topic_name_filters, check_filter_pattern)) {
        return errc::topic_filter_invalid;
    }
    for (const auto& prop : config.topic_properties_to_mirror) {
        if (_excluded_topic_properties.contains(prop)) {
            vlog(
              cluster::clusterlog.info,
              "Topic property '{}' is excluded from mirroring",
              prop);
            return errc::topic_property_excluded_from_mirroring;
        }
    }

    return errc::success;
}

ss::future<errc> frontend::failover_link_topics(
  ::cluster_link::model::id_t id, model::timeout_clock::duration timeout) {
    auto meta = _table->find_link_by_id(id);
    if (!meta) {
        co_return errc::does_not_exist;
    }
    if (meta->state.status != ::cluster_link::model::link_status::active) {
        vlog(
          cluster::clusterlog.warn,
          "Attempting to failover topics of link {} which is not in the active "
          "state (current state: {})",
          meta->name,
          meta->state.status);
        co_return errc::invalid_update;
    }

    const auto& topics = meta->state.mirror_topics;
    chunked_vector<model::topic> topics_to_failover;
    auto should_failover = [](::cluster_link::model::mirror_topic_status s) {
        switch (s) {
        case ::cluster_link::model::mirror_topic_status::active:
            return true;
        case ::cluster_link::model::mirror_topic_status::paused:
        case ::cluster_link::model::mirror_topic_status::failed:
        case ::cluster_link::model::mirror_topic_status::promoted:
        case ::cluster_link::model::mirror_topic_status::failed_over:
        case ::cluster_link::model::mirror_topic_status::failing_over:
        case ::cluster_link::model::mirror_topic_status::promoting:
            return false;
        }
    };
    for (const auto& [t, info] : topics) {
        if (should_failover(info.status)) {
            topics_to_failover.push_back(t);
        }
    }
    chunked_vector<errc> errors;
    errors.reserve(topics_to_failover.size());
    co_await ss::max_concurrent_for_each(
      topics_to_failover,
      32,
      [this, &errors, id, timeout](const model::topic& t) {
          return update_mirror_topic_status(
                   id,
                   {.topic = t,
                    .status
                    = ::cluster_link::model::mirror_topic_status::failing_over},
                   model::timeout_clock::now() + timeout)
            .then([&errors](errc err_code) {
                if (err_code != errc::success) {
                    errors.push_back(err_code);
                }
            });
      });

    if (!errors.empty()) {
        vlog(
          cluster::clusterlog.warn,
          "Encountered {} errors while failing over topics of link id {}",
          errors.size(),
          id);
        co_return map_errc(errors.front());
    }
    co_return errc::success;
}
} // namespace cluster::cluster_link
