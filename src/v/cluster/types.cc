// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"

#include "cluster/fwd.h"
#include "cluster/remote_topic_properties.h"
#include "cluster/topic_properties.h"
#include "config/configuration.h"
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

#include <fmt/ostream.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <type_traits>

namespace cluster {

std::ostream& operator<<(std::ostream& o, const recovery_stage& s) {
    switch (s) {
    case recovery_stage::initialized:
        o << "recovery_stage::initialized";
        break;
    case recovery_stage::starting:
        o << "recovery_stage::starting";
        break;
    case recovery_stage::recovered_license:
        o << "recovery_stage::recovered_license";
        break;
    case recovery_stage::recovered_cluster_config:
        o << "recovery_stage::recovered_cluster_config";
        break;
    case recovery_stage::recovered_users:
        o << "recovery_stage::recovered_users";
        break;
    case recovery_stage::recovered_acls:
        o << "recovery_stage::recovered_acls";
        break;
    case recovery_stage::recovered_remote_topic_data:
        o << "recovery_stage::recovered_remote_topic_data";
        break;
    case recovery_stage::recovered_topic_data:
        o << "recovery_stage::recovered_topic_data";
        break;
    case recovery_stage::recovered_controller_snapshot:
        o << "recovery_stage::recovered_controller_snapshot";
        break;
    case recovery_stage::recovered_offsets_topic:
        o << "recovery_stage::recovered_offsets_topic";
        break;
    case recovery_stage::recovered_tx_coordinator:
        o << "recovery_stage::recovered_tx_coordinator";
        break;
    case recovery_stage::complete:
        o << "recovery_stage::complete";
        break;
    case recovery_stage::failed:
        o << "recovery_stage::failed";
        break;
    }
    return o;
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

std::ostream&
operator<<(std::ostream& o, const update_topic_properties_request& r) {
    fmt::print(o, "{{updates: {}}}", r.updates);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const update_topic_properties_reply& r) {
    fmt::print(o, "{{results: {}}}", r.results);
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_properties_update& tpu) {
    fmt::print(
      o,
      "tp_ns: {} properties: {} custom_properties: {}",
      tpu.tp_ns,
      tpu.properties,
      tpu.custom_properties);
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_result& r) {
    fmt::print(o, "topic: {}, result: {}", r.tp_ns, r.ec);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const finish_partition_update_request& r) {
    fmt::print(o, "{{ntp: {}, new_replica_set: {}}}", r.ntp, r.new_replica_set);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const finish_partition_update_reply& r) {
    fmt::print(o, "{{result: {}}}", r.result);
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration_invariants& c) {
    fmt::print(
      o,
      "{{ version: {}, node_id: {}, core_count: {} }}",
      c.version,
      c.node_id,
      c.core_count);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_assignment& p_as) {
    fmt::print(
      o,
      "{{ id: {}, group_id: {}, replicas: {} }}",
      p_as.id,
      p_as.group,
      p_as.replicas);
    return o;
}

std::ostream& operator<<(std::ostream& o, const shard_placement_target& spt) {
    fmt::print(
      o,
      "{{group: {}, log_revision: {}, shard: {}}}",
      spt.group,
      spt.log_revision,
      spt.shard);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_operation_type& tp) {
    switch (tp) {
    case partition_operation_type::add:
        return o << "addition";
    case partition_operation_type::remove:
        return o << "deletion";
    case partition_operation_type::reset:
        return o << "reset";
    case partition_operation_type::update:
        return o << "update";
    case partition_operation_type::force_update:
        return o << "force_update";
    case partition_operation_type::finish_update:
        return o << "update_finished";
    case partition_operation_type::update_properties:
        return o << "update_properties";
    case partition_operation_type::add_non_replicable:
        return o << "add_non_replicable_addition";
    case partition_operation_type::del_non_replicable:
        return o << "del_non_replicable_deletion";
    case partition_operation_type::cancel_update:
        return o << "cancel_update";
    case partition_operation_type::force_cancel_update:
        return o << "force_abort_update";
    }
    __builtin_unreachable();
}

std::ostream&
operator<<(std::ostream& o, const topic_table_topic_delta_type& tp) {
    switch (tp) {
    case topic_table_topic_delta_type::added:
        return o << "added";
    case topic_table_topic_delta_type::removed:
        return o << "removed";
    case topic_table_topic_delta_type::properties_updated:
        return o << "properties_updated";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const topic_table_topic_delta& d) {
    fmt::print(
      o,
      "{{creation_revision:{}, ns_tp: {}, type: {}, revision: {}}}",
      d.creation_revision,
      d.ns_tp,
      d.type,
      d.revision);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const topic_table_ntp_delta_type& tp) {
    switch (tp) {
    case topic_table_ntp_delta_type::added:
        return o << "added";
    case topic_table_ntp_delta_type::removed:
        return o << "removed";
    case topic_table_ntp_delta_type::replicas_updated:
        return o << "replicas_updated";
    case topic_table_ntp_delta_type::properties_updated:
        return o << "properties_updated";
    case topic_table_ntp_delta_type::disabled_flag_updated:
        return o << "disabled_flag_updated";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const topic_table_ntp_delta& d) {
    fmt::print(
      o, "{{ntp: {}, type: {}, revision: {}}}", d.ntp, d.type, d.revision);
    return o;
}

std::ostream& operator<<(std::ostream& o, const backend_operation& op) {
    fmt::print(
      o,
      "{{partition_assignment: {}, shard: {},  type: {}}}",
      op.p_as,
      op.source_shard,
      op.type);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const cluster_config_delta_cmd_data& data) {
    fmt::print(
      o,
      "{{cluster_config_delta_cmd_data: {} upserts, {} removes)}}",
      data.upsert.size(),
      data.remove.size());
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_status& s) {
    fmt::print(
      o,
      "{{cluster_status: node {}, version: {}, restart: {} ({} invalid, {} "
      "unknown)}}",
      s.node,
      s.version,
      s.restart,
      s.invalid.size(),
      s.unknown.size());
    return o;
}

bool config_status::operator==(const config_status& rhs) const {
    return std::tie(node, version, restart, unknown, invalid)
           == std::tie(
             rhs.node, rhs.version, rhs.restart, rhs.unknown, rhs.invalid);
}

std::ostream& operator<<(std::ostream& o, const cluster_property_kv& kv) {
    fmt::print(
      o, "{{cluster_property_kv: key {}, value: {})}}", kv.key, kv.value);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_update_request& crq) {
    fmt::print(
      o,
      "{{config_update_request: upsert {}, remove: {})}}",
      crq.upsert,
      crq.remove);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_update_reply& crr) {
    fmt::print(
      o,
      "{{config_update_reply: error {}, latest_version: {})}}",
      crr.error,
      crr.latest_version);
    return o;
}

std::ostream& operator<<(std::ostream& o, const hello_request& h) {
    fmt::print(
      o, "{{hello_request: peer {}, start_time: {})}}", h.peer, h.start_time);
    return o;
}

std::ostream& operator<<(std::ostream& o, const hello_reply& h) {
    fmt::print(o, "{{hello_reply: error {}}}", h.error);
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_topics_request& r) {
    fmt::print(
      o,
      "{{create_topics_request: topics: {} timeout: {}}}",
      r.topics,
      r.timeout);
    return o;
}

std::ostream& operator<<(std::ostream& o, const create_topics_reply& r) {
    fmt::print(
      o,
      "{{create_topics_reply: results: {} metadata: {} configs: {}}}",
      r.results,
      r.metadata,
      r.configs);
    return o;
}

std::ostream& operator<<(std::ostream& o, const incremental_topic_updates& i) {
    fmt::print(
      o,
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
      "remote_read: {}, remote_write: {}",
      i.compression,
      i.cleanup_policy_bitflags,
      i.compaction_strategy,
      i.timestamp_type,
      i.segment_size,
      i.retention_bytes,
      i.retention_duration,
      i.get_shadow_indexing(),
      i.batch_max_bytes,
      i.retention_local_target_bytes,
      i.retention_local_target_ms,
      i.remote_delete,
      i.segment_ms,
      i.record_key_schema_id_validation,
      i.record_key_schema_id_validation_compat,
      i.record_key_subject_name_strategy,
      i.record_key_subject_name_strategy_compat,
      i.record_value_schema_id_validation,
      i.record_value_schema_id_validation_compat,
      i.record_value_subject_name_strategy,
      i.record_value_subject_name_strategy_compat,
      i.initial_retention_local_target_bytes,
      i.initial_retention_local_target_ms,
      i.write_caching,
      i.flush_ms,
      i.flush_bytes,
      i.iceberg_enabled,
      i.leaders_preference,
      i.remote_read,
      i.remote_write);
    return o;
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

std::ostream&
operator<<(std::ostream& o, const incremental_topic_custom_updates& i) {
    fmt::print(
      o,
      "{{incremental_topic_custom_updates: data_policy: {}, "
      "replication_factor: {}}}",
      i.data_policy,
      i.replication_factor);
    return o;
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

std::ostream& operator<<(std::ostream& o, const reconciliation_status& s) {
    switch (s) {
    case reconciliation_status::done:
        return o << "done";
    case reconciliation_status::error:
        return o << "error";
    case reconciliation_status::in_progress:
        return o << "in_progress";
    }
    __builtin_unreachable();
}

std::ostream&
operator<<(std::ostream& o, const ntp_reconciliation_state& state) {
    fmt::print(
      o,
      "{{ntp: {}, backend_operations: {}, error: {}, status: {}}}",
      state._ntp,
      state._backend_operations,
      state._error,
      state._status);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const create_partitions_configuration& cfg) {
    fmt::print(
      o,
      "{{topic: {}, new total partition count: {}, custom assignments: {}}}",
      cfg.tp_ns,
      cfg.new_total_partition_count,
      cfg.custom_assignments);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const custom_assignable_topic_configuration& catc) {
    fmt::print(
      o,
      "{{configuration: {}, custom_assignments: {}}}",
      catc.cfg,
      catc.custom_assignments);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const custom_partition_assignment& cas) {
    fmt::print(o, "{{partition_id: {}, replicas: {}}}", cas.id, cas.replicas);
    return o;
}

std::ostream& operator<<(std::ostream& o, const leader_term& lt) {
    fmt::print(o, "{{leader: {}, term: {}}}", lt.leader, lt.term);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_move_direction& s) {
    switch (s) {
    case partition_move_direction::to_node:
        return o << "to_node";
    case partition_move_direction::from_node:
        return o << "from_node";
    case partition_move_direction::all:
        return o << "all";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const move_cancellation_result& r) {
    fmt::print(o, "{{ntp: {}, result: {}}}", r.ntp, r.result);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const cancel_node_partition_movements_request& r) {
    fmt::print(o, "{{node_id: {}, direction: {}}}", r.node_id, r.direction);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const cancel_partition_movements_reply& r) {
    fmt::print(
      o,
      "{{general_error: {}, partition_results: {}}}",
      r.general_error,
      r.partition_results);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_action_request& far) {
    fmt::print(o, "{{feature_update_request: {}}}", far.action);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_action_response& far) {
    fmt::print(o, "{{error: {}}}", far.error);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_barrier_request& fbr) {
    fmt::print(
      o, "{{tag: {} peer: {} entered: {}}}", fbr.tag, fbr.peer, fbr.entered);
    return o;
}

std::ostream& operator<<(std::ostream& o, const feature_barrier_response& fbr) {
    fmt::print(o, "{{entered: {} complete: {}}}", fbr.entered, fbr.complete);
    return o;
}

std::ostream& operator<<(std::ostream& o, const move_topic_replicas_data& r) {
    fmt::print(o, "{{partition: {}, replicas: {}}}", r.partition, r.replicas);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const force_partition_reconfiguration_cmd_data& r) {
    fmt::print(o, "{{target replicas: {}}}", r.replicas);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const set_topic_partitions_disabled_cmd_data& r) {
    fmt::print(
      o,
      "{{topic: {}, partition_id: {}, disabled: {}}}",
      r.ns_tp,
      r.partition_id,
      r.disabled);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const feature_update_license_update_cmd_data& fulu) {
    fmt::print(o, "{{redpanda_license {}}}", fulu.redpanda_license);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_status_request& r) {
    fmt::print(o, "{{status: {}}}", r.status);
    return o;
}

std::ostream& operator<<(std::ostream& o, const config_status_reply& r) {
    fmt::print(o, "{{error: {}}}", r.error);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const configuration_update_request& cr) {
    fmt::print(o, "{{broker: {} target_node: {}}}", cr.node, cr.target_node);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const configuration_update_reply& cr) {
    fmt::print(o, "{{success: {}}}", cr.success);
    return o;
}

std::ostream& operator<<(std::ostream& o, const broker_state& state) {
    fmt::print(
      o,
      "{{membership_state: {}, maintenance_state: {}}}",
      state._membership_state,
      state._maintenance_state);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_metadata& nm) {
    fmt::print(o, "{{broker: {}, state: {} }}", nm.broker, nm.state);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_update_type& tp) {
    switch (tp) {
    case node_update_type::added:
        return o << "added";
    case node_update_type::decommissioned:
        return o << "decommissioned";
    case node_update_type::recommissioned:
        return o << "recommissioned";
    case node_update_type::reallocation_finished:
        return o << "reallocation_finished";
    case node_update_type::removed:
        return o << "removed";
    case node_update_type::interrupted:
        return o << "interrupted";
    }
    return o << "unknown";
}

std::ostream& operator<<(std::ostream& o, reconfiguration_state update) {
    switch (update) {
    case reconfiguration_state::in_progress:
        return o << "in_progress";
    case reconfiguration_state::force_update:
        return o << "force_update";
    case reconfiguration_state::cancelled:
        return o << "cancelled";
    case reconfiguration_state::force_cancelled:
        return o << "force_cancelled";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const cloud_storage_mode& mode) {
    switch (mode) {
    case cloud_storage_mode::disabled:
        return o << "disabled";
    case cloud_storage_mode::write_only:
        return o << "write_only";
    case cloud_storage_mode::read_only:
        return o << "read_only";
    case cloud_storage_mode::full:
        return o << "full";
    case cloud_storage_mode::read_replica:
        return o << "read_replica";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const nt_revision& ntr) {
    fmt::print(
      o,
      "{{ns: {}, topic: {}, revision: {}}}",
      ntr.nt.ns,
      ntr.nt.tp,
      ntr.initial_revision_id);
    return o;
}

std::ostream& operator<<(std::ostream& o, reconfiguration_policy policy) {
    switch (policy) {
    case reconfiguration_policy::full_local_retention:
        return o << "full_local_retention";
    case reconfiguration_policy::target_initial_retention:
        return o << "target_initial_retention";
    case reconfiguration_policy::min_local_retention:
        return o << "min_local_retention";
    }
    __builtin_unreachable();
}

std::ostream&
operator<<(std::ostream& o, const update_partition_replicas_cmd_data& data) {
    fmt::print(
      o,
      "{{ntp: {}, replicas: {} policy: {}}}",
      data.ntp,
      data.replicas,
      data.policy);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const topic_disabled_partitions_set& disabled) {
    if (disabled.partitions) {
        fmt::print(
          o,
          "{{partitions: {}}}",
          std::vector(
            disabled.partitions->begin(), disabled.partitions->end()));
    } else {
        fmt::print(o, "{{partitions: all}}");
    }
    return o;
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

std::ostream& operator<<(std::ostream& o, const ntp_with_majority_loss& entry) {
    fmt::print(
      o,
      "{{ ntp: {}, topic_revision: {}, replicas: {}, dead nodes: {} }}",
      entry.ntp,
      entry.topic_revision,
      entry.assignment,
      entry.dead_nodes);
    return o;
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
            if (std::holds_alternative<
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
        updates.record_value_subject_name_strategy
          = adl<cluster::property_update<std::optional<
            pandaproxy::schema_registry::subject_name_strategy>>>{}
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
