// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"

#include "base/format_to.h"
#include "cluster/security_types.h"
#include "cluster/topic_properties.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "security/acl.h"
#include "utils/to_string.h"
#include "utils/tristate.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace cluster {
fmt::iterator format_to(recovery_stage e, fmt::iterator out) {
    switch (e) {
    case recovery_stage::initialized:
        return fmt::format_to(out, "recovery_stage::initialized");
    case recovery_stage::starting:
        return fmt::format_to(out, "recovery_stage::starting");
    case recovery_stage::recovered_license:
        return fmt::format_to(out, "recovery_stage::recovered_license");
    case recovery_stage::recovered_cluster_config:
        return fmt::format_to(out, "recovery_stage::recovered_cluster_config");
    case recovery_stage::recovered_users:
        return fmt::format_to(out, "recovery_stage::recovered_users");
    case recovery_stage::recovered_acls:
        return fmt::format_to(out, "recovery_stage::recovered_acls");
    case recovery_stage::recovered_remote_topic_data:
        return fmt::format_to(
          out, "recovery_stage::recovered_remote_topic_data");
    case recovery_stage::recovered_cloud_topics_metastore:
        return fmt::format_to(
          out, "recovery_stage::recovered_cloud_topics_metastore");
    case recovery_stage::recovered_cloud_topic_data:
        return fmt::format_to(
          out, "recovery_stage::recovered_cloud_topic_data");
    case recovery_stage::recovered_topic_data:
        return fmt::format_to(out, "recovery_stage::recovered_topic_data");
    case recovery_stage::recovered_controller_snapshot:
        return fmt::format_to(
          out, "recovery_stage::recovered_controller_snapshot");
    case recovery_stage::recovered_offsets_topic:
        return fmt::format_to(out, "recovery_stage::recovered_offsets_topic");
    case recovery_stage::recovered_tx_coordinator:
        return fmt::format_to(out, "recovery_stage::recovered_tx_coordinator");
    case recovery_stage::complete:
        return fmt::format_to(out, "recovery_stage::complete");
    case recovery_stage::failed:
        return fmt::format_to(out, "recovery_stage::failed");
    }
    return fmt::format_to(out, "recovery_stage::unknown");
}

kafka_stages::kafka_stages(
  ss::future<> enq, ss::future<result<kafka_result>> offset_future)
  : request_enqueued(std::move(enq))
  , replicate_finished(std::move(offset_future)) {}

kafka_stages::kafka_stages(raft::errc ec)
  : request_enqueued(ss::now())
  , replicate_finished(
      ss::make_ready_future<result<kafka_result>>(make_error_code(ec))) {};

ntp_reconciliation_state::ntp_reconciliation_state(
  model::ntp ntp,
  ss::chunked_fifo<backend_operation> ops,
  reconciliation_status status)
  : ntp_reconciliation_state(
      std::move(ntp), std::move(ops), status, errc::success) {}

ntp_reconciliation_state::ntp_reconciliation_state(
  model::ntp ntp,
  ss::chunked_fifo<backend_operation> ops,
  reconciliation_status status,
  errc ec)
  : _ntp(std::move(ntp))
  , _backend_operations(std::move(ops))
  , _status(status)
  , _error(ec) {}

ntp_reconciliation_state::ntp_reconciliation_state(
  model::ntp ntp, cluster::errc ec)
  : ntp_reconciliation_state(
      std::move(ntp), {}, reconciliation_status::error, ec) {}

create_partitions_configuration::create_partitions_configuration(
  model::topic_namespace tp_ns, int32_t cnt)
  : tp_ns(std::move(tp_ns))
  , new_total_partition_count(cnt) {}
fmt::iterator
update_topic_properties_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{updates: {}}}", updates);
}
fmt::iterator update_topic_properties_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{results: {}}}", results);
}
fmt::iterator topic_properties_update::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "tp_ns: {} properties: {} custom_properties: {}",
      tp_ns,
      properties,
      custom_properties);
}
fmt::iterator format_to(topic_purge_domain d, fmt::iterator out) {
    switch (d) {
    case topic_purge_domain::cloud_storage:
        return fmt::format_to(out, "cloud_storage");
    case topic_purge_domain::iceberg:
        return fmt::format_to(out, "iceberg");
    case topic_purge_domain::cloud_topic:
        return fmt::format_to(out, "cloud_topic");
    }
    return fmt::format_to(out, "unknown({})", static_cast<int>(d));
}
fmt::iterator topic_result::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "topic: {}, result: {}", tp_ns, ec);
}
fmt::iterator
finish_partition_update_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ntp: {}, new_replica_set: {}}}", ntp, new_replica_set);
}
fmt::iterator finish_partition_update_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{result: {}}}", result);
}
fmt::iterator configuration_invariants::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ version: {}, node_id: {}, core_count: {} }}",
      version,
      node_id,
      core_count);
}
fmt::iterator partition_bootstrap_params::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{start_offset: {}, next_offset: {}, initial_term: {}}}",
      start_offset,
      next_offset,
      initial_term);
}
fmt::iterator partition_assignment::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ id: {}, group_id: {}, replicas: {} }}", id, group, replicas);
}
fmt::iterator shard_placement_target::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{group: {}, log_revision: {}, shard: {}}}",
      group,
      log_revision,
      shard);
}
fmt::iterator format_to(partition_operation_type e, fmt::iterator out) {
    switch (e) {
    case partition_operation_type::add:
        return fmt::format_to(out, "addition");
    case partition_operation_type::remove:
        return fmt::format_to(out, "deletion");
    case partition_operation_type::reset:
        return fmt::format_to(out, "reset");
    case partition_operation_type::update:
        return fmt::format_to(out, "update");
    case partition_operation_type::force_update:
        return fmt::format_to(out, "force_update");
    case partition_operation_type::finish_update:
        return fmt::format_to(out, "update_finished");
    case partition_operation_type::update_properties:
        return fmt::format_to(out, "update_properties");
    case partition_operation_type::add_non_replicable:
        return fmt::format_to(out, "add_non_replicable_addition");
    case partition_operation_type::del_non_replicable:
        return fmt::format_to(out, "del_non_replicable_deletion");
    case partition_operation_type::cancel_update:
        return fmt::format_to(out, "cancel_update");
    case partition_operation_type::force_cancel_update:
        return fmt::format_to(out, "force_abort_update");
    }
    __builtin_unreachable();
}
fmt::iterator format_to(topic_table_topic_delta_type e, fmt::iterator out) {
    switch (e) {
    case topic_table_topic_delta_type::added:
        return fmt::format_to(out, "added");
    case topic_table_topic_delta_type::removed:
        return fmt::format_to(out, "removed");
    case topic_table_topic_delta_type::properties_updated:
        return fmt::format_to(out, "properties_updated");
    }
    __builtin_unreachable();
}
fmt::iterator topic_table_topic_delta::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{creation_revision:{}, ns_tp: {}, type: {}, revision: {}}}",
      creation_revision,
      ns_tp,
      type,
      revision);
}
fmt::iterator format_to(topic_table_ntp_delta_type e, fmt::iterator out) {
    switch (e) {
    case topic_table_ntp_delta_type::added:
        return fmt::format_to(out, "added");
    case topic_table_ntp_delta_type::removed:
        return fmt::format_to(out, "removed");
    case topic_table_ntp_delta_type::replicas_updated:
        return fmt::format_to(out, "replicas_updated");
    case topic_table_ntp_delta_type::properties_updated:
        return fmt::format_to(out, "properties_updated");
    case topic_table_ntp_delta_type::disabled_flag_updated:
        return fmt::format_to(out, "disabled_flag_updated");
    }
    __builtin_unreachable();
}
fmt::iterator topic_table_ntp_delta::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ntp: {}, type: {}, revision: {}}}", ntp, type, revision);
}
fmt::iterator backend_operation::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{partition_assignment: {}, shard: {},  type: {}, "
      "recovery_state: {}}}",
      p_as,
      source_shard,
      type,
      recovery_state);
}
fmt::iterator recovery_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{local_last_offset: {}, local_size: {}, replicas: {}}}",
      local_last_offset,
      local_size,
      replicas);
}
fmt::iterator replica_recovery_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{last_offset: {}, bytes_left: {}}}", last_offset, bytes_left);
}
fmt::iterator cluster_config_delta_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{cluster_config_delta_cmd_data: {} upserts, {} removes)}}",
      upsert.size(),
      remove.size());
}
fmt::iterator config_status::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{cluster_status: node {}, version: {}, restart: {} ({} invalid, {} "
      "unknown)}}",
      node,
      version,
      restart,
      invalid.size(),
      unknown.size());
}

bool config_status::operator==(const config_status& rhs) const {
    return std::tie(node, version, restart, unknown, invalid)
           == std::tie(
             rhs.node, rhs.version, rhs.restart, rhs.unknown, rhs.invalid);
}
fmt::iterator cluster_property_kv::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{cluster_property_kv: key {}, value: {})}}", key, value);
}
fmt::iterator config_update_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{config_update_request: upsert {}, remove: {})}}", upsert, remove);
}
fmt::iterator config_update_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{config_update_reply: error {}, latest_version: {})}}",
      error,
      latest_version);
}
fmt::iterator hello_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{hello_request: peer {}, start_time: {})}}", peer, start_time);
}
fmt::iterator hello_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{hello_reply: error {}}}", error);
}
fmt::iterator create_topics_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{create_topics_request: topics: {} timeout: {}}}", topics, timeout);
}
fmt::iterator create_topics_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{create_topics_reply: results: {} metadata: {} configs: {}}}",
      results,
      metadata,
      configs);
}
fmt::iterator incremental_topic_updates::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{incremental_topic_custom_updates: compression: {} "
      "cleanup_policy_bitflags: {} compaction_strategy: {} timestamp_type: {} "
      "segment_size: {} retention_bytes: {} retention_duration: {} "
      "shadow_indexing: {}, batch_max_bytes: {}, retention_local_target_bytes: "
      "{}, retention_local_target_ms: {}, remote_delete: {}, segment_ms: {}, "
      "record_key_schema_id_validation: {}"
      "record_key_schema_id_validation_compat: {}"
      "record_key_subject_name_strategy: {}"
      "record_key_subject_name_strategy_compat: {}"
      "record_value_schema_id_validation: {}"
      "record_value_schema_id_validation_compat: {}"
      "record_value_subject_name_strategy: {}"
      "record_value_subject_name_strategy_compat: {}, "
      "initial_retention_local_target_bytes: {}, "
      "initial_retention_local_target_ms: {}, write_caching: {}, flush_ms: {}, "
      "flush_bytes: {}, iceberg_enabled: {}, leaders_preference: {}, "
      "remote_read: {}, remote_write: {}, iceberg_delete: {}, "
      "iceberg_partition_spec: {}, "
      "iceberg_invalid_record_action: {}, "
      "iceberg_target_lag_ms: {}, "
      "remote_allow_gaps: {}, "
      "topic_id: {}, replicas_preference: {}}}",
      compression,
      cleanup_policy_bitflags,
      compaction_strategy,
      timestamp_type,
      segment_size,
      retention_bytes,
      retention_duration,
      get_shadow_indexing(),
      batch_max_bytes,
      retention_local_target_bytes,
      retention_local_target_ms,
      remote_delete,
      segment_ms,
      record_key_schema_id_validation,
      record_key_schema_id_validation_compat,
      record_key_subject_name_strategy,
      record_key_subject_name_strategy_compat,
      record_value_schema_id_validation,
      record_value_schema_id_validation_compat,
      record_value_subject_name_strategy,
      record_value_subject_name_strategy_compat,
      initial_retention_local_target_bytes,
      initial_retention_local_target_ms,
      write_caching,
      flush_ms,
      flush_bytes,
      iceberg_mode,
      leaders_preference,
      remote_read,
      remote_write,
      iceberg_delete,
      iceberg_partition_spec,
      iceberg_invalid_record_action,
      iceberg_target_lag_ms,
      remote_allow_gaps,
      topic_id,
      replicas_preference);
}

std::istream& operator>>(std::istream& i, replication_factor& cs) {
    ss::sstring s;
    i >> s;
    cs = replication_factor(boost::lexical_cast<replication_factor::type>(s));
    return i;
};

replication_factor parsing_replication_factor(const ss::sstring& value) {
    auto raw_value = boost::lexical_cast<int32_t>(value);
    if (
      raw_value
      > std::numeric_limits<cluster::replication_factor::type>::max()) {
        throw boost::bad_lexical_cast();
    }

    return cluster::replication_factor(raw_value);
}
fmt::iterator
incremental_topic_custom_updates::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{incremental_topic_custom_updates: data_policy: {}, "
      "replication_factor: {}}}",
      data_policy,
      replication_factor);
}

namespace {
cluster::assignments_set to_assignments_map(
  ss::chunked_fifo<cluster::partition_assignment> assignment_vector) {
    cluster::assignments_set ret;
    for (auto& p_as : assignment_vector) {
        const auto id = p_as.id;
        ret.emplace(id, std::move(p_as));
    }
    return ret;
}
} // namespace

topic_metadata_fields::topic_metadata_fields(
  topic_configuration cfg,
  std::optional<model::topic> st,
  model::revision_id rid,
  std::optional<model::initial_revision_id> remote_revision_id) noexcept
  : configuration(std::move(cfg))
  , source_topic(std::move(st))
  , revision(rid)
  , remote_revision(remote_revision_id) {}

topic_metadata::topic_metadata(
  topic_configuration_assignment c,
  model::revision_id rid,
  std::optional<model::initial_revision_id> remote_revision_id) noexcept
  : _fields(std::move(c.cfg), std::nullopt, rid, remote_revision_id)
  , _assignments(to_assignments_map(std::move(c.assignments))) {}

topic_metadata::topic_metadata(
  topic_configuration cfg,
  assignments_set assignments,
  model::revision_id rid,
  model::topic st,
  std::optional<model::initial_revision_id> remote_revision_id) noexcept
  : _fields(std::move(cfg), std::move(st), rid, remote_revision_id)
  , _assignments(std::move(assignments)) {}

topic_metadata::topic_metadata(
  topic_metadata_fields fields, assignments_set assignments) noexcept
  : _fields(std::move(fields))
  , _assignments(std::move(assignments)) {}

model::revision_id topic_metadata::get_revision() const {
    return _fields.revision;
}

std::optional<model::initial_revision_id>
topic_metadata::get_remote_revision() const {
    return _fields.remote_revision;
}

std::optional<ss::sstring> topic_metadata::get_remote_location_hint() const {
    const auto& remote_label = get_configuration().properties.remote_label;
    if (!remote_label) {
        return std::nullopt;
    }

    model::initial_revision_id remote_rev = get_remote_revision().value_or(
      model::initial_revision_id{get_revision()});
    return fmt::format("{}/{}", remote_label->cluster_uuid, remote_rev);
}

const topic_configuration& topic_metadata::get_configuration() const {
    return _fields.configuration;
}

const assignments_set& topic_metadata::get_assignments() const {
    return _assignments;
}

topic_configuration& topic_metadata::get_configuration() {
    return _fields.configuration;
}

assignments_set& topic_metadata::get_assignments() { return _assignments; }

replication_factor topic_metadata::get_replication_factor() const {
    // The main idea is do not use anymore replication_factor from topic_config.
    // replication factor is dynamic property. And it is size of assigments set.
    // So we will return rf for 0 partition, becasue it should exist for each
    // topic
    auto it = _assignments.find(model::partition_id(0));
    return replication_factor(
      static_cast<replication_factor::type>(it->second.replicas.size()));
}

topic_metadata topic_metadata::copy() const {
    return {_fields, _assignments.copy()};
}
fmt::iterator format_to(reconciliation_status e, fmt::iterator out) {
    switch (e) {
    case reconciliation_status::done:
        return fmt::format_to(out, "done");
    case reconciliation_status::error:
        return fmt::format_to(out, "error");
    case reconciliation_status::in_progress:
        return fmt::format_to(out, "in_progress");
    }
    __builtin_unreachable();
}
fmt::iterator ntp_reconciliation_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ntp: {}, backend_operations: [{}], error: {}, status: {}}}",
      _ntp,
      fmt::join(_backend_operations, ", "),
      _error,
      _status);
}
fmt::iterator
create_partitions_configuration::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topic: {}, new total partition count: {}, custom assignments: {}}}",
      tp_ns,
      new_total_partition_count,
      custom_assignments);
}
fmt::iterator
custom_assignable_topic_configuration::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{configuration: {}, custom_assignments: {}}}",
      cfg,
      custom_assignments);
}
fmt::iterator custom_partition_assignment::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{partition_id: {}, replicas: {}}}", id, replicas);
}
fmt::iterator leader_term::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{leader: {}, term: {}}}", leader, term);
}
fmt::iterator format_to(partition_move_direction e, fmt::iterator out) {
    switch (e) {
    case partition_move_direction::to_node:
        return fmt::format_to(out, "to_node");
    case partition_move_direction::from_node:
        return fmt::format_to(out, "from_node");
    case partition_move_direction::all:
        return fmt::format_to(out, "all");
    }
    __builtin_unreachable();
}
fmt::iterator move_cancellation_result::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ntp: {}, result: {}}}", ntp, result);
}
fmt::iterator
cancel_node_partition_movements_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{node_id: {}, direction: {}}}", node_id, direction);
}
fmt::iterator
cancel_partition_movements_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{general_error: {}, partition_results: {}}}",
      general_error,
      partition_results);
}
fmt::iterator feature_action_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{feature_update_request: {}}}", action);
}
fmt::iterator feature_action_response::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{error: {}}}", error);
}
fmt::iterator feature_barrier_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{tag: {} peer: {} entered: {}}}", tag, peer, entered);
}
fmt::iterator feature_barrier_response::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{entered: {} complete: {}}}", entered, complete);
}
fmt::iterator move_topic_replicas_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{partition: {}, replicas: {}}}", partition, replicas);
}
fmt::iterator
force_partition_reconfiguration_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{target replicas: {}}}", replicas);
}
fmt::iterator
set_topic_partitions_disabled_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topic: {}, partition_id: {}, disabled: {}}}",
      ns_tp,
      partition_id,
      disabled);
}
fmt::iterator
feature_update_license_update_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{redpanda_license {}}}", redpanda_license);
}
fmt::iterator config_status_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{status: {}}}", status);
}
fmt::iterator config_status_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{error: {}}}", error);
}
fmt::iterator configuration_update_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{broker: {} target_node: {}}}", node, target_node);
}
fmt::iterator configuration_update_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{success: {}}}", success);
}
fmt::iterator broker_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{membership_state: {}, maintenance_state: {}}}",
      _membership_state,
      _maintenance_state);
}
fmt::iterator node_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{broker: {}, state: {} }}", broker, state);
}
fmt::iterator format_to(node_update_type e, fmt::iterator out) {
    switch (e) {
    case node_update_type::added:
        return fmt::format_to(out, "added");
    case node_update_type::decommissioned:
        return fmt::format_to(out, "decommissioned");
    case node_update_type::recommissioned:
        return fmt::format_to(out, "recommissioned");
    case node_update_type::reallocation_finished:
        return fmt::format_to(out, "reallocation_finished");
    case node_update_type::removed:
        return fmt::format_to(out, "removed");
    case node_update_type::interrupted:
        return fmt::format_to(out, "interrupted");
    }
    return fmt::format_to(out, "unknown");
}
fmt::iterator format_to(reconfiguration_state e, fmt::iterator out) {
    switch (e) {
    case reconfiguration_state::in_progress:
        return fmt::format_to(out, "in_progress");
    case reconfiguration_state::force_update:
        return fmt::format_to(out, "force_update");
    case reconfiguration_state::cancelled:
        return fmt::format_to(out, "cancelled");
    case reconfiguration_state::force_cancelled:
        return fmt::format_to(out, "force_cancelled");
    }
    __builtin_unreachable();
}
fmt::iterator format_to(cloud_storage_mode e, fmt::iterator out) {
    switch (e) {
    case cloud_storage_mode::disabled:
        return fmt::format_to(out, "disabled");
    case cloud_storage_mode::write_only:
        return fmt::format_to(out, "write_only");
    case cloud_storage_mode::read_only:
        return fmt::format_to(out, "read_only");
    case cloud_storage_mode::full:
        return fmt::format_to(out, "full");
    case cloud_storage_mode::read_replica:
        return fmt::format_to(out, "read_replica");
    case cloud_storage_mode::cloud_topic:
        return fmt::format_to(out, "cloud_topic");
    case cloud_storage_mode::cloud_topic_read_replica:
        return fmt::format_to(out, "cloud_topic_read_replica");
    case cloud_storage_mode::tiered_cloud_topic:
        return fmt::format_to(out, "tiered_cloud_topic");
    }
    __builtin_unreachable();
}
fmt::iterator nt_revision::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ns: {}, topic: {}, revision: {}}}",
      nt.ns,
      nt.tp,
      initial_revision_id);
}
fmt::iterator format_to(reconfiguration_policy e, fmt::iterator out) {
    switch (e) {
    case reconfiguration_policy::full_local_retention:
        return fmt::format_to(out, "full_local_retention");
    case reconfiguration_policy::target_initial_retention:
        return fmt::format_to(out, "target_initial_retention");
    case reconfiguration_policy::min_local_retention:
        return fmt::format_to(out, "min_local_retention");
    }
    __builtin_unreachable();
}
fmt::iterator
update_partition_replicas_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ntp: {}, replicas: {} policy: {}}}", ntp, replicas, policy);
}
fmt::iterator topic_disabled_partitions_set::format_to(fmt::iterator it) const {
    if (partitions) {
        return fmt::format_to(
          it,
          "{{partitions: {}}}",
          std::vector(partitions->begin(), partitions->end()));
    } else {
        return fmt::format_to(it, "{{partitions: all}}");
    }
}

void topic_disabled_partitions_set::add(model::partition_id id) {
    if (partitions) {
        partitions->insert(id);
    } else {
        // do nothing, std::nullopt means all partitions are already
        // disabled.
    }
}

void topic_disabled_partitions_set::remove(
  model::partition_id id, const assignments_set& all_partitions) {
    if (!all_partitions.contains(id)) {
        return;
    }
    if (!partitions) {
        partitions = absl::node_hash_set<model::partition_id>{};
        partitions->reserve(all_partitions.size());
        for (const auto& [_, p] : all_partitions) {
            partitions->insert(p.id);
        }
    }
    partitions->erase(id);
}
fmt::iterator ntp_with_majority_loss::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ ntp: {}, topic_revision: {}, replicas: {}, dead nodes: {} }}",
      ntp,
      topic_revision,
      assignment,
      dead_nodes);
}

bulk_force_reconfiguration_cmd_data&
bulk_force_reconfiguration_cmd_data::operator=(
  const bulk_force_reconfiguration_cmd_data& other) {
    if (this != &other) {
        from_nodes = other.from_nodes;
        user_approved_force_recovery_partitions
          = other.user_approved_force_recovery_partitions.copy();
    }
    return *this;
}

bulk_force_reconfiguration_cmd_data::bulk_force_reconfiguration_cmd_data(
  const bulk_force_reconfiguration_cmd_data& other)
  : from_nodes(other.from_nodes) {
    user_approved_force_recovery_partitions
      = other.user_approved_force_recovery_partitions.copy();
}
fmt::iterator
cluster::feature_update_cmd_data::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{logical_version: {}, actions: {}}}", logical_version, actions);
}

fmt::iterator error_info::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{err: {}, message: {}}}", this->err, this->message);
}

} // namespace cluster

namespace reflection {

void adl<cluster::topic_result>::to(iobuf& out, cluster::topic_result&& t) {
    reflection::serialize(out, std::move(t.tp_ns), t.ec);
}

cluster::topic_result adl<cluster::topic_result>::from(iobuf_parser& in) {
    auto tp_ns = adl<model::topic_namespace>{}.from(in);
    auto ec = adl<cluster::errc>{}.from(in);
    return cluster::topic_result(std::move(tp_ns), ec);
}

void adl<cluster::configuration_invariants>::to(
  iobuf& out, cluster::configuration_invariants&& r) {
    reflection::serialize(out, r.version, r.node_id, r.core_count);
}

cluster::configuration_invariants
adl<cluster::configuration_invariants>::from(iobuf_parser& parser) {
    auto version = adl<uint8_t>{}.from(parser);
    vassert(
      version == cluster::configuration_invariants::current_version,
      "Currently only version 0 of configuration invariants is supported");

    auto node_id = adl<model::node_id>{}.from(parser);
    auto core_count = adl<uint16_t>{}.from(parser);

    cluster::configuration_invariants ret(node_id, core_count);

    return ret;
}

void adl<cluster::topic_properties_update>::to(
  iobuf& out, cluster::topic_properties_update&& r) {
    reflection::serialize(
      out, r.version, r.tp_ns, r.properties, r.custom_properties);
}

cluster::topic_properties_update
adl<cluster::topic_properties_update>::from(iobuf_parser& parser) {
    /**
     * We use the same versioning trick as for `cluster::topic_configuration`.
     *
     * NOTE: The first field of the topic_properties_update is a
     * model::topic_namespace. Serialized ss::string starts from either
     * int32_t for string length. We use negative version to encode new format
     * of incremental topic_properties_update
     */

    auto version = adl<int32_t>{}.from(parser.peek(4));
    if (version < 0) {
        // Consume version from stream
        parser.skip(4);
        vassert(
          version == cluster::topic_properties_update::version,
          "topic_properties_update version {} is not supported",
          version);
    } else {
        version = 0;
    }

    auto tp_ns = adl<model::topic_namespace>{}.from(parser);
    cluster::topic_properties_update ret(std::move(tp_ns));
    ret.properties = adl<cluster::incremental_topic_updates>{}.from(parser);
    if (version < 0) {
        ret.custom_properties
          = adl<cluster::incremental_topic_custom_updates>{}.from(parser);
    }

    return ret;
}

/*
 * Important information about ACL state serialization:
 *
 * The following serialization specializations are not part of a public
 * interface and are used to support the serialization of the public type
 * `cluster::create_acls_cmd_data` used by create acls api.
 *
 * They are private because they all depend on the embedded versioning of
 * `cluster::create_acls_cmd_data`, instead of their own independent versioning.
 * Because the same versioning applies to the entire AST rooted at this command
 * object type it should make transitions to the new serialization v2 much
 * simpler than having to deal with conversion of all of the constituent types.
 */
template<>
struct adl<security::acl_principal> {
    void to(iobuf& out, const security::acl_principal& p) {
        serialize(out, p.type(), p.name());
    }

    security::acl_principal from(iobuf_parser& in) {
        auto pt = adl<security::principal_type>{}.from(in);
        auto name = adl<ss::sstring>{}.from(in);
        return security::acl_principal(pt, std::move(name));
    }
};

template<>
struct adl<security::acl_host> {
    void to(iobuf& out, const security::acl_host& host) {
        bool ipv4 = false;
        std::optional<iobuf> data;
        if (host.address()) { // wildcard
            ipv4 = host.address()->is_ipv4();
            data = iobuf();
            data->append( // NOLINTNEXTLINE
              (const char*)host.address()->data(),
              host.address()->size());
        }
        serialize(out, ipv4, std::move(data));
    }

    security::acl_host from(iobuf_parser& in) {
        auto ipv4 = adl<bool>{}.from(in);
        auto opt_data = adl<std::optional<iobuf>>{}.from(in);

        if (opt_data) {
            auto data = iobuf_to_bytes(*opt_data);
            if (ipv4) {
                ::in_addr addr{};
                vassert(data.size() == sizeof(addr), "Unexpected ipv4 size");
                std::memcpy(&addr, data.data(), sizeof(addr));
                return security::acl_host(ss::net::inet_address(addr));
            } else {
                ::in6_addr addr{};
                vassert(data.size() == sizeof(addr), "Unexpected ipv6 size");
                std::memcpy(&addr, data.data(), sizeof(addr));
                return security::acl_host(ss::net::inet_address(addr));
            }
        }

        return security::acl_host::wildcard_host();
    }
};

template<>
struct adl<security::acl_entry> {
    void to(iobuf& out, const security::acl_entry& e) {
        serialize(out, e.principal(), e.host(), e.operation(), e.permission());
    }

    security::acl_entry from(iobuf_parser& in) {
        auto prin = adl<security::acl_principal>{}.from(in);
        auto host = adl<security::acl_host>{}.from(in);
        auto op = adl<security::acl_operation>{}.from(in);
        auto perm = adl<security::acl_permission>{}.from(in);
        return security::acl_entry(std::move(prin), host, op, perm);
    }
};

template<>
struct adl<security::resource_pattern> {
    void to(iobuf& out, const security::resource_pattern& b) {
        serialize(out, b.resource(), b.name(), b.pattern());
    }

    security::resource_pattern from(iobuf_parser& in) {
        auto r = adl<security::resource_type>{}.from(in);
        auto n = adl<ss::sstring>{}.from(in);
        auto p = adl<security::pattern_type>{}.from(in);
        return security::resource_pattern(r, std::move(n), p);
    }
};

template<>
struct adl<security::acl_binding> {
    void to(iobuf& out, const security::acl_binding& b) {
        serialize(out, b.pattern(), b.entry());
    }

    security::acl_binding from(iobuf_parser& in) {
        auto r = adl<security::resource_pattern>{}.from(in);
        auto e = adl<security::acl_entry>{}.from(in);
        return security::acl_binding(std::move(r), std::move(e));
    }
};

/*
 * A pattern_type_filter contains a normal pattern type in addition to a match
 * pattern type. However, match type isn't part of the enum for pattern type and
 * only makes sense in the context of the filter. To accomodate this we use a
 * variant type on the filter interface with tag dispatch, and as a result, need
 * special handling in serialization for this type.
 */
template<>
struct adl<security::resource_pattern_filter> {
    enum class pattern_type : int8_t {
        literal = 0,
        prefixed = 1,
        match = 2,
    };

    static pattern_type to_pattern(security::pattern_type from) {
        switch (from) {
        case security::pattern_type::literal:
            return pattern_type::literal;
        case security::pattern_type::prefixed:
            return pattern_type::prefixed;
        }
        __builtin_unreachable();
    }

    void to(iobuf& out, const security::resource_pattern_filter& b) {
        std::optional<pattern_type> pattern;
        if (b.pattern()) {
            if (
              std::holds_alternative<
                security::resource_pattern_filter::pattern_match>(
                *b.pattern())) {
                pattern = pattern_type::match;
            } else {
                auto source_pattern = std::get<security::pattern_type>(
                  *b.pattern());
                pattern = to_pattern(source_pattern);
            }
        }
        serialize(out, b.resource(), b.name(), pattern);
    }

    security::resource_pattern_filter from(iobuf_parser& in) {
        auto resource = adl<std::optional<security::resource_type>>{}.from(in);
        auto name = adl<std::optional<ss::sstring>>{}.from(in);
        auto pattern = adl<std::optional<pattern_type>>{}.from(in);

        if (!pattern) {
            return security::resource_pattern_filter(
              resource, std::move(name), std::nullopt);
        }

        switch (*pattern) {
        case pattern_type::literal:
            return security::resource_pattern_filter(
              resource, std::move(name), security::pattern_type::literal);

        case pattern_type::prefixed:
            return security::resource_pattern_filter(
              resource, std::move(name), security::pattern_type::prefixed);

        case pattern_type::match:
            return security::resource_pattern_filter(
              resource,
              std::move(name),
              security::resource_pattern_filter::pattern_match{});
        }
        __builtin_unreachable();
    }
};

template<>
struct adl<security::acl_entry_filter> {
    void to(iobuf& out, const security::acl_entry_filter& f) {
        serialize(out, f.principal(), f.host(), f.operation(), f.permission());
    }

    security::acl_entry_filter from(iobuf_parser& in) {
        auto prin = adl<std::optional<security::acl_principal>>{}.from(in);
        auto host = adl<std::optional<security::acl_host>>{}.from(in);
        auto op = adl<std::optional<security::acl_operation>>{}.from(in);
        auto perm = adl<std::optional<security::acl_permission>>{}.from(in);
        return security::acl_entry_filter(std::move(prin), host, op, perm);
    }
};

template<>
struct adl<security::acl_binding_filter> {
    void to(iobuf& out, const security::acl_binding_filter& b) {
        serialize(out, b.pattern(), b.entry());
    }

    security::acl_binding_filter from(iobuf_parser& in) {
        auto r = adl<security::resource_pattern_filter>{}.from(in);
        auto e = adl<security::acl_entry_filter>{}.from(in);
        return security::acl_binding_filter(std::move(r), std::move(e));
    }
};

void adl<cluster::create_acls_cmd_data>::to(
  iobuf& out, cluster::create_acls_cmd_data&& data) {
    adl<int8_t>{}.to(out, cluster::create_acls_cmd_data::current_version);
    serialize(out, std::move(data.bindings));
}

cluster::create_acls_cmd_data
adl<cluster::create_acls_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::create_acls_cmd_data::current_version,
      "Unexpected create acls cmd version {} (expected {})",
      version,
      cluster::create_acls_cmd_data::current_version);
    auto bindings = adl<std::vector<security::acl_binding>>().from(in);
    return cluster::create_acls_cmd_data{
      .bindings = std::move(bindings),
    };
}

void adl<cluster::delete_acls_cmd_data>::to(
  iobuf& out, cluster::delete_acls_cmd_data&& data) {
    adl<int8_t>{}.to(out, cluster::delete_acls_cmd_data::current_version);
    serialize(out, std::move(data.filters));
}

cluster::delete_acls_cmd_data
adl<cluster::delete_acls_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::delete_acls_cmd_data::current_version,
      "Unexpected delete acls cmd version {} (expected {})",
      version,
      cluster::delete_acls_cmd_data::current_version);
    auto filters = adl<std::vector<security::acl_binding_filter>>().from(in);
    return cluster::delete_acls_cmd_data{
      .filters = std::move(filters),
    };
}

void adl<cluster::create_partitions_configuration>::to(
  iobuf& out, cluster::create_partitions_configuration&& pc) {
    return serialize(
      out, pc.tp_ns, pc.new_total_partition_count, pc.custom_assignments);
}

cluster::create_partitions_configuration
adl<cluster::create_partitions_configuration>::from(iobuf_parser& in) {
    auto tp_ns = adl<model::topic_namespace>{}.from(in);
    auto partition_count = adl<int32_t>{}.from(in);
    auto custom_assignment = adl<std::vector<
      cluster::create_partitions_configuration::custom_assignment>>{}
                               .from(in);

    cluster::create_partitions_configuration ret(
      std::move(tp_ns), partition_count);
    ret.custom_assignments = std::move(custom_assignment);
    return ret;
}

void adl<cluster::incremental_topic_updates>::to(
  iobuf& out, cluster::incremental_topic_updates&& t) {
    // NOTE: no need to serialize new fields with ADL, as this format is no
    // longer used for new messages.
    reflection::serialize(
      out,
      cluster::incremental_topic_updates::version,
      t.compression,
      t.cleanup_policy_bitflags,
      t.compaction_strategy,
      t.timestamp_type,
      t.segment_size,
      t.retention_bytes,
      t.retention_duration,
      t.get_shadow_indexing(),
      t.batch_max_bytes,
      t.retention_local_target_bytes,
      t.retention_local_target_ms,
      t.remote_delete,
      t.segment_ms,
      t.record_key_schema_id_validation,
      t.record_key_schema_id_validation_compat,
      t.record_key_subject_name_strategy,
      t.record_key_subject_name_strategy_compat,
      t.record_value_schema_id_validation,
      t.record_value_schema_id_validation_compat,
      t.record_value_subject_name_strategy,
      t.record_value_subject_name_strategy_compat,
      t.initial_retention_local_target_bytes,
      t.initial_retention_local_target_ms,
      t.write_caching,
      t.flush_ms,
      t.flush_bytes);
}

cluster::incremental_topic_updates
adl<cluster::incremental_topic_updates>::from(iobuf_parser& in) {
    /**
     * We use the same versioning trick as for `cluster::topic_configuration`.
     *
     * NOTE: The first field of the incremental_topic_updates is a
     * property_value<std::optional<model::compression>>. Serialized
     * std::optional starts from either 0 or 1 (int8_t). We use negative version
     * to encode new format of incremental topic updates
     */

    auto version = adl<int8_t>{}.from(in.peek(1));
    if (version < 0) {
        // Consume version from stream
        in.skip(1);
        vassert(
          version >= cluster::incremental_topic_updates::version,
          "topic_configuration version {} is not supported",
          version);
    } else {
        version = 0;
    }

    cluster::incremental_topic_updates updates;
    updates.compression
      = adl<cluster::property_update<std::optional<model::compression>>>{}.from(
        in);
    updates.cleanup_policy_bitflags = adl<cluster::property_update<
      std::optional<model::cleanup_policy_bitflags>>>{}
                                        .from(in);
    updates.compaction_strategy
      = adl<
          cluster::property_update<std::optional<model::compaction_strategy>>>{}
          .from(in);
    updates.timestamp_type
      = adl<cluster::property_update<std::optional<model::timestamp_type>>>{}
          .from(in);
    updates.segment_size
      = adl<cluster::property_update<std::optional<size_t>>>{}.from(in);
    updates.retention_bytes
      = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
    updates.retention_duration
      = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
          .from(in);

    if (
      version == cluster::incremental_topic_updates::version_with_data_policy) {
        // data_policy property from update_topic_properties_cmd is never used.
        // data_policy_frontend replicates this property and store it to
        // create_data_policy_cmd_data, data_policy_manager handles it
        adl<cluster::property_update<std::optional<v8_engine::data_policy>>>{}
          .from(in);
    }
    if (
      version
      <= cluster::incremental_topic_updates::version_with_shadow_indexing) {
        updates.get_shadow_indexing() = adl<cluster::property_update<
          std::optional<model::shadow_indexing_mode>>>{}
                                          .from(in);
    }

    if (
      version <= cluster::incremental_topic_updates::
        version_with_batch_max_bytes_and_local_retention) {
        updates.batch_max_bytes
          = adl<cluster::property_update<std::optional<uint32_t>>>{}.from(in);
        updates.retention_local_target_bytes
          = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
        updates.retention_local_target_ms
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
              .from(in);
        updates.remote_delete = adl<cluster::property_update<bool>>{}.from(in);
    }

    if (
      version <= cluster::incremental_topic_updates::version_with_segment_ms) {
        updates.segment_ms
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
              .from(in);
    }

    if (
      version <= cluster::incremental_topic_updates::
        version_with_schema_id_validation) {
        updates.record_key_schema_id_validation
          = adl<cluster::property_update<std::optional<bool>>>{}.from(in);
        updates.record_key_schema_id_validation_compat
          = adl<cluster::property_update<std::optional<bool>>>{}.from(in);
        updates.record_key_subject_name_strategy = adl<cluster::property_update<
          std::optional<pandaproxy::schema_registry::subject_name_strategy>>>{}
                                                     .from(in);
        updates.record_key_subject_name_strategy_compat
          = adl<cluster::property_update<std::optional<
            pandaproxy::schema_registry::subject_name_strategy>>>{}
              .from(in);
        updates.record_value_schema_id_validation
          = adl<cluster::property_update<std::optional<bool>>>{}.from(in);
        updates.record_value_schema_id_validation_compat
          = adl<cluster::property_update<std::optional<bool>>>{}.from(in);
        updates
          .record_value_subject_name_strategy = adl<cluster::property_update<
          std::optional<pandaproxy::schema_registry::subject_name_strategy>>>{}
                                                  .from(in);
        updates.record_value_subject_name_strategy_compat
          = adl<cluster::property_update<std::optional<
            pandaproxy::schema_registry::subject_name_strategy>>>{}
              .from(in);
    }

    if (
      version
      <= cluster::incremental_topic_updates::version_with_initial_retention) {
        updates.initial_retention_local_target_bytes
          = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
        updates.initial_retention_local_target_ms
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
              .from(in);
    }

    if (
      version
      <= cluster::incremental_topic_updates::version_with_write_caching) {
        updates.write_caching = adl<cluster::property_update<
          std::optional<model::write_caching_mode>>>{}
                                  .from(in);
        updates.flush_ms = adl<cluster::property_update<
          std::optional<std::chrono::milliseconds>>>{}
                             .from(in);
        updates.flush_bytes
          = adl<cluster::property_update<std::optional<size_t>>>{}.from(in);
    }

    return updates;
}

void adl<cluster::config_status>::to(iobuf& out, cluster::config_status&& s) {
    return serialize(out, s.node, s.version, s.restart, s.unknown, s.invalid);
}

cluster::config_status adl<cluster::config_status>::from(iobuf_parser& in) {
    auto node_id = adl<model::node_id>().from(in);
    auto version = adl<cluster::config_version>().from(in);
    auto restart = adl<bool>().from(in);
    auto unknown = adl<std::vector<ss::sstring>>().from(in);
    auto invalid = adl<std::vector<ss::sstring>>().from(in);

    return cluster::config_status{
      .node = std::move(node_id),
      .version = std::move(version),
      .restart = std::move(restart),
      .unknown = std::move(unknown),
      .invalid = std::move(invalid),
    };
}

void adl<cluster::cluster_config_delta_cmd_data>::to(
  iobuf& out, cluster::cluster_config_delta_cmd_data&& cmd) {
    return serialize(
      out, cmd.current_version, std::move(cmd.upsert), std::move(cmd.remove));
}

cluster::cluster_config_delta_cmd_data
adl<cluster::cluster_config_delta_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::cluster_config_delta_cmd_data::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::cluster_config_delta_cmd_data::current_version);
    auto upsert = adl<std::vector<cluster::cluster_property_kv>>().from(in);
    auto remove = adl<std::vector<ss::sstring>>().from(in);

    return cluster::cluster_config_delta_cmd_data{
      .upsert = std::move(upsert),
      .remove = std::move(remove),
    };
}

void adl<cluster::cluster_config_status_cmd_data>::to(
  iobuf& out, cluster::cluster_config_status_cmd_data&& cmd) {
    return serialize(out, cmd.current_version, std::move(cmd.status));
}

cluster::cluster_config_status_cmd_data
adl<cluster::cluster_config_status_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::cluster_config_status_cmd_data::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::cluster_config_status_cmd_data::current_version);
    auto status = adl<cluster::config_status>().from(in);

    return cluster::cluster_config_status_cmd_data{
      .status = std::move(status),
    };
}

void adl<cluster::feature_update_action>::to(
  iobuf& out, cluster::feature_update_action&& cmd) {
    return serialize(
      out,
      cmd.current_version,
      std::move(cmd.feature_name),
      std::move(cmd.action));
}

cluster::feature_update_action
adl<cluster::feature_update_action>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == cluster::feature_update_action::current_version,
      "Unexpected version: {} (expected {})",
      version,
      cluster::feature_update_action::current_version);
    auto feature_name = adl<ss::sstring>().from(in);
    auto action = adl<cluster::feature_update_action::action_t>().from(in);

    return cluster::feature_update_action{
      .feature_name = std::move(feature_name),
      .action = std::move(action),
    };
}

void adl<cluster::incremental_topic_custom_updates>::to(
  iobuf& out, cluster::incremental_topic_custom_updates&& t) {
    reflection::serialize(out, t.data_policy);
}

cluster::incremental_topic_custom_updates
adl<cluster::incremental_topic_custom_updates>::from(iobuf_parser& in) {
    cluster::incremental_topic_custom_updates updates;
    updates.data_policy
      = adl<cluster::property_update<std::optional<v8_engine::data_policy>>>{}
          .from(in);
    return updates;
}

void adl<cluster::feature_update_cmd_data>::to(
  iobuf& out, cluster::feature_update_cmd_data&& data) {
    reflection::serialize(
      out, data.current_version, data.logical_version, data.actions);
}

cluster::feature_update_cmd_data
adl<cluster::feature_update_cmd_data>::from(iobuf_parser& in) {
    auto version = adl<int8_t>{}.from(in);
    std::ignore = version;

    auto logical_version = adl<cluster::cluster_version>{}.from(in);
    auto actions = adl<std::vector<cluster::feature_update_action>>{}.from(in);
    return {.logical_version = logical_version, .actions = std::move(actions)};
}

void adl<cluster::partition_assignment>::to(
  iobuf& out, cluster::partition_assignment&& p_as) {
    reflection::serialize(out, p_as.group, p_as.id, std::move(p_as.replicas));
}

cluster::partition_assignment
adl<cluster::partition_assignment>::from(iobuf_parser& parser) {
    auto group = reflection::adl<raft::group_id>{}.from(parser);
    auto id = reflection::adl<model::partition_id>{}.from(parser);
    auto replicas = reflection::adl<cluster::replicas_t>{}.from(parser);

    return {group, id, std::move(replicas)};
}

void adl<cluster::cluster_property_kv>::to(
  iobuf& out, cluster::cluster_property_kv&& kv) {
    reflection::serialize(out, std::move(kv.key), std::move(kv.value));
}

cluster::cluster_property_kv
adl<cluster::cluster_property_kv>::from(iobuf_parser& p) {
    cluster::cluster_property_kv kv;

    kv.key = adl<ss::sstring>{}.from(p);
    kv.value = adl<ss::sstring>{}.from(p);
    return kv;
}

} // namespace reflection
