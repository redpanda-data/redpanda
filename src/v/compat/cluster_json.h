/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/health_monitor_types.h"
#include "cluster/types.h"
#include "compat/json.h"
#include "compat/model_json.h"
#include "compat/storage_json.h"

namespace json {

inline void read_value(const json::Value& rd, cluster::errc& e) {
    /// TODO: Make giant switch to confirm value is a proper cluster::errc
    auto err = rd.GetInt();
    e = static_cast<cluster::errc>(err);
}

template<typename T>
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::property_update<T>& pu) {
    w.StartObject();
    if constexpr (std::is_class<T>::value) {
        if constexpr (
          is_exceptional_enum<T> || is_exceptional_enum_wrapped_opt<T>) {
            write_exceptional_member_type(w, "value", pu.value);
        } else {
            write_member(w, "value", pu.value);
        }
    } else {
        write_member(w, "value", pu.value);
    }
    write_member(
      w,
      "op",
      static_cast<
        std::underlying_type_t<cluster::incremental_update_operation>>(pu.op));
    w.EndObject();
}

template<typename T>
void read_value(const json::Value& rd, cluster::property_update<T>& pu) {
    read_member(rd, "value", pu.value);
    auto op = read_enum_ut(rd, "op", pu.op);
    switch (op) {
    case 0:
        pu.op = cluster::incremental_update_operation::none;
        break;
    case 1:
        pu.op = cluster::incremental_update_operation::set;
        break;
    case 2:
        pu.op = cluster::incremental_update_operation::remove;
        break;
    default:
        vassert(
          false,
          "Unknown enum value for cluster::incremental_update_operation: {}",
          op);
    }
}

inline void
read_value(const json::Value& rd, cluster::cluster_property_kv& obj) {
    read_member(rd, "key", obj.key);
    read_member(rd, "value", obj.value);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::cluster_property_kv& kv) {
    w.StartObject();
    write_member(w, "key", kv.key);
    write_member(w, "value", kv.value);
    w.EndObject();
}

inline void
read_value(const json::Value& rd, cluster::remote_topic_properties& rtp) {
    read_member(rd, "remote_revision", rtp.remote_revision);
    read_member(rd, "remote_partition_count", rtp.remote_partition_count);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::remote_topic_properties& rtp) {
    w.StartObject();
    write_member(w, "remote_revision", rtp.remote_revision);
    write_member(w, "remote_partition_count", rtp.remote_partition_count);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::config_status& s) {
    w.StartObject();
    write_member(w, "node", s.node);
    write_member(w, "version", s.version);
    write_member(w, "restart", s.restart);
    write_member(w, "unknown", s.unknown);
    write_member(w, "invalid", s.invalid);
    w.EndObject();
}

inline void read_value(const json::Value& rd, cluster::config_status& s) {
    read_member(rd, "node", s.node);
    read_member(rd, "version", s.version);
    read_member(rd, "restart", s.restart);
    read_member(rd, "unknown", s.unknown);
    read_member(rd, "invalid", s.invalid);
}

inline void
read_value(const json::Value& rd, cluster::partition_move_direction& e) {
    auto direction = rd.GetInt();
    switch (direction) {
    case 0:
        e = cluster::partition_move_direction::to_node;
        break;
    case 1:
        e = cluster::partition_move_direction::from_node;
        break;
    case 2:
        e = cluster::partition_move_direction::all;
        break;
    default:
        vassert(
          false,
          "Unsupported enum for cluster::partition_move_direction, {}",
          direction);
    }
}

inline void
read_value(const json::Value& rd, cluster::feature_update_action::action_t& e) {
    auto action = rd.GetInt();
    switch (action) {
    case 1:
        e = cluster::feature_update_action::action_t::complete_preparing;
        break;
    case 2:
        e = cluster::feature_update_action::action_t::activate;
        break;
    case 3:
        e = cluster::feature_update_action::action_t::deactivate;
        break;
    default:
        vassert(
          false,
          "Unsupported enum value for "
          "cluster::feature_update_action::action_t, {}",
          action);
    }
}

inline void
read_value(const json::Value& rd, cluster::feature_update_action& obj) {
    read_member(rd, "feature_name", obj.feature_name);
    read_member(rd, "action", obj.action);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::feature_update_action& f) {
    w.StartObject();
    write_member(w, "feature_name", f.feature_name);
    write_member(w, "action", f.action);
    w.EndObject();
}

inline void read_value(const json::Value& v, cluster::tx::errc& obj) {
    obj = {v.GetInt()};
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::topic_result& t) {
    w.StartObject();
    w.Key("tp_ns");
    rjson_serialize(w, t.tp_ns);
    w.Key("ec");
    rjson_serialize(w, t.ec);
    w.EndObject();
}

inline void read_value(const json::Value& rd, cluster::topic_result& obj) {
    model::topic_namespace tp_ns;
    cluster::errc ec;
    read_member(rd, "tp_ns", tp_ns);
    read_member(rd, "ec", ec);
    obj = cluster::topic_result(std::move(tp_ns), ec);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::move_cancellation_result& r) {
    w.StartObject();
    w.Key("ntp");
    rjson_serialize(w, r.ntp);
    w.Key("result");
    rjson_serialize(w, r.result);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::partitions_filter& f) {
    w.StartObject();
    w.Key("namespaces");
    rjson_serialize(w, f.namespaces);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::node_report_filter& f) {
    w.StartObject();
    w.Key("include_partitions");
    rjson_serialize(w, f.include_partitions);
    w.Key("ntp_filters");
    rjson_serialize(w, f.ntp_filters);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::cluster_report_filter& f) {
    w.StartObject();
    w.Key("node_report_filter");
    rjson_serialize(w, f.node_report_filter);
    w.Key("nodes");
    rjson_serialize(w, f.nodes);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::drain_manager::drain_status& f) {
    w.StartObject();
    w.Key("finished");
    rjson_serialize(w, f.finished);
    w.Key("errors");
    rjson_serialize(w, f.errors);
    w.Key("partitions");
    rjson_serialize(w, f.partitions);
    w.Key("eligible");
    rjson_serialize(w, f.eligible);
    w.Key("transferring");
    rjson_serialize(w, f.transferring);
    w.Key("failed");
    rjson_serialize(w, f.failed);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::node::local_state& f) {
    w.StartObject();
    w.Key("redpanda_version");
    rjson_serialize(w, f.redpanda_version);
    w.Key("logical_version");
    rjson_serialize(w, f.logical_version);
    w.Key("uptime");
    rjson_serialize(w, f.uptime);
    w.Key("disks");
    rjson_serialize(w, f.disks());
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::partition_status& f) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, f.id);
    w.Key("term");
    rjson_serialize(w, f.term);
    w.Key("leader_id");
    rjson_serialize(w, f.leader_id);
    w.Key("revision_id");
    rjson_serialize(w, f.revision_id);
    w.Key("size_bytes");
    rjson_serialize(w, f.size_bytes);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::topic_status& f) {
    w.StartObject();
    w.Key("tp_ns");
    rjson_serialize(w, f.tp_ns);
    w.Key("partitions");
    rjson_serialize(w, f.partitions);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::node_health_report_serde& f) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, f.id);
    w.Key("local_state");
    rjson_serialize(w, f.local_state);
    w.Key("topics");
    rjson_serialize(w, f.topics);
    w.Key("drain_status");
    rjson_serialize(w, f.drain_status);
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::node_state& f) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, f.id());
    w.Key("membership_state");
    rjson_serialize(w, f.membership_state());
    w.Key("is_alive");
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
    rjson_serialize(w, f.is_alive());
#pragma clang diagnostic pop
    w.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::cluster_health_report& f) {
    w.StartObject();
    w.Key("raft0_leader");
    rjson_serialize(w, f.raft0_leader);
    w.Key("node_states");
    rjson_serialize(w, f.node_states);
    w.Key("node_reports");
    w.StartArray();
    for (auto& r : f.node_reports) {
        rjson_serialize(w, cluster::node_health_report_serde{*r});
    }
    w.EndArray();
    w.EndObject();
}

inline void read_value(const json::Value& rd, cluster::partitions_filter& obj) {
    cluster::partitions_filter::ns_map_t namespaces;
    read_member(rd, "namespaces", namespaces);
    obj = cluster::partitions_filter{{}, namespaces};
}

inline void
read_value(const json::Value& rd, cluster::node_report_filter& obj) {
    cluster::include_partitions_info include_partitions;
    cluster::partitions_filter ntp_filters;
    read_member(rd, "include_partitions", include_partitions);
    read_member(rd, "ntp_filters", ntp_filters);
    obj = cluster::node_report_filter{
      {}, include_partitions, std::move(ntp_filters)};
}

inline void
read_value(const json::Value& rd, cluster::cluster_report_filter& obj) {
    cluster::node_report_filter node_report_filter;
    std::vector<model::node_id> nodes;

    read_member(rd, "node_report_filter", node_report_filter);
    read_member(rd, "nodes", nodes);
    obj = cluster::cluster_report_filter{
      {}, node_report_filter, std::move(nodes)};
}

inline void
read_value(const json::Value& rd, cluster::drain_manager::drain_status& obj) {
    bool finished;
    bool errors;
    std::optional<size_t> partitions;
    std::optional<size_t> eligible;
    std::optional<size_t> transferring;
    std::optional<size_t> failed;

    read_member(rd, "finished", finished);
    read_member(rd, "errors", errors);
    read_member(rd, "partitions", partitions);
    read_member(rd, "eligible", eligible);
    read_member(rd, "transferring", transferring);
    read_member(rd, "failed", failed);
    obj = cluster::drain_manager::drain_status{
      {}, finished, errors, partitions, eligible, transferring, failed};
}

inline void read_value(const json::Value& rd, cluster::partition_status& obj) {
    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
    size_t size_bytes;

    read_member(rd, "id", id);
    read_member(rd, "term", term);
    read_member(rd, "leader_id", leader_id);
    read_member(rd, "revision_id", revision_id);
    read_member(rd, "size_bytes", size_bytes);
    obj = cluster::partition_status{
      {}, id, term, leader_id, revision_id, size_bytes};
}

inline void read_value(const json::Value& rd, cluster::topic_status& obj) {
    model::topic_namespace tp_ns;
    cluster::partition_statuses_t partitions;

    read_member(rd, "tp_ns", tp_ns);
    read_member(rd, "partitions", partitions);
    obj = cluster::topic_status(tp_ns, std::move(partitions));
}

inline void read_value(const json::Value& rd, cluster::node::local_state& obj) {
    cluster::node::application_version redpanda_version;
    cluster::cluster_version logical_version;
    std::chrono::milliseconds uptime;
    std::vector<storage::disk> disks;

    read_member(rd, "redpanda_version", redpanda_version);
    read_member(rd, "logical_version", logical_version);
    read_member(rd, "uptime", uptime);
    read_member(rd, "disks", disks);

    obj = cluster::node::local_state{
      {}, redpanda_version, logical_version, uptime};
    obj.set_disks(disks);
}

inline void
read_value(const json::Value& rd, cluster::node_health_report_serde& obj) {
    model::node_id id;
    cluster::node::local_state local_state;
    chunked_vector<cluster::topic_status> topics;
    std::optional<cluster::drain_manager::drain_status> drain_status;

    read_member(rd, "id", id);
    read_member(rd, "local_state", local_state);
    read_member(rd, "topics", topics);
    read_member(rd, "drain_status", drain_status);
    obj = cluster::node_health_report_serde(
      id, local_state, std::move(topics), drain_status);
}

inline void read_value(const json::Value& rd, cluster::node_state& obj) {
    model::node_id id;
    model::membership_state membership_state;
    cluster::alive is_alive;

    read_member(rd, "id", id);
    read_member(rd, "membership_state", membership_state);
    read_member(rd, "is_alive", is_alive);

    obj = cluster::node_state(id, membership_state, is_alive);
}

inline void
read_value(const json::Value& rd, cluster::cluster_health_report& obj) {
    std::optional<model::node_id> raft0_leader;
    std::vector<cluster::node_state> node_states;
    std::vector<cluster::node_health_report_ptr> node_reports;

    read_member(rd, "raft0_leader", raft0_leader);
    read_member(rd, "node_states", node_states);
    auto reports_v = rd.FindMember("node_reports");
    if (reports_v != rd.MemberEnd()) {
        for (const auto& e : reports_v->value.GetArray()) {
            cluster::node_health_report_serde report;
            read_value(e, report);
            node_reports.emplace_back(
              ss::make_lw_shared<cluster::node_health_report>(
                std::move(report).to_in_memory()));
        }
    }
    obj = cluster::cluster_health_report{
      {}, raft0_leader, node_states, std::move(node_reports)};
}

inline void
read_value(const json::Value& rd, cluster::move_cancellation_result& obj) {
    model::ntp ntp;
    cluster::errc result;
    read_member(rd, "ntp", ntp);
    read_member(rd, "result", result);
    obj = cluster::move_cancellation_result(std::move(ntp), result);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::partition_assignment& r) {
    w.StartObject();
    w.Key("group");
    rjson_serialize(w, r.group);
    w.Key("id");
    rjson_serialize(w, r.id);
    w.Key("replicas");
    rjson_serialize(w, r.replicas);
    w.EndObject();
}

inline void
read_value(const json::Value& rd, cluster::partition_assignment& obj) {
    json_read(group);
    json_read(id);
    json_read(replicas);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::backend_operation& r) {
    w.StartObject();
    w.Key("source_shard");
    rjson_serialize(w, r.source_shard);
    w.Key("partition_assignment");
    rjson_serialize(w, r.p_as);
    w.Key("op_type");
    rjson_serialize(w, r.type);
    w.EndObject();
}

inline void read_value(const json::Value& rd, cluster::backend_operation& obj) {
    json_read(source_shard);
    read_member(rd, "partition_assignment", obj.p_as);
    read_member(rd, "op_type", obj.type);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::ntp_reconciliation_state& r) {
    w.StartObject();
    w.Key("ntp");
    rjson_serialize(w, r.ntp());
    w.Key("operations");
    rjson_serialize(w, r.pending_operations());
    w.Key("status");
    rjson_serialize(w, r.status());
    w.Key("error");
    rjson_serialize(w, r.cluster_errc());
    w.EndObject();
}

inline void
read_value(const json::Value& rd, cluster::ntp_reconciliation_state& obj) {
    model::ntp ntp;
    ss::chunked_fifo<cluster::backend_operation> operations;
    cluster::reconciliation_status status;
    cluster::errc error;

    read_member(rd, "ntp", ntp);
    read_member(rd, "operations", operations);
    read_member(rd, "status", status);
    read_member(rd, "error", error);

    obj = cluster::ntp_reconciliation_state(
      std::move(ntp), std::move(operations), status, error);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::topic_properties& tps) {
    w.StartObject();
    write_exceptional_member_type(w, "compression", tps.compression);
    write_exceptional_member_type(
      w, "cleanup_policy_bitflags", tps.cleanup_policy_bitflags);
    write_member(w, "compaction_strategy", tps.compaction_strategy);
    write_exceptional_member_type(w, "timestamp_type", tps.timestamp_type);
    write_member(w, "segment_size", tps.segment_size);
    write_member(w, "retention_bytes", tps.retention_bytes);
    write_member(w, "retention_duration", tps.retention_duration);
    write_member(w, "recovery", tps.recovery);
    write_member(w, "shadow_indexing", tps.shadow_indexing);
    write_member(w, "read_replica", tps.read_replica);
    write_member(w, "read_replica_bucket", tps.read_replica_bucket);
    write_member(
      w,
      "remote_topic_namespace_override",
      tps.remote_topic_namespace_override);
    write_member(w, "remote_topic_properties", tps.remote_topic_properties);
    write_member(w, "batch_max_bytes", tps.batch_max_bytes);
    write_member(
      w, "retention_local_target_bytes", tps.retention_local_target_bytes);
    write_member(w, "retention_local_target_ms", tps.retention_local_target_ms);
    write_member(w, "remote_delete", tps.remote_delete);
    write_member(w, "segment_ms", tps.segment_ms);
    write_member(
      w,
      "record_key_schema_id_validation",
      tps.record_key_schema_id_validation);
    write_member(
      w,
      "record_key_schema_id_validation_compat",
      tps.record_key_schema_id_validation_compat);
    write_member(
      w,
      "record_key_subject_name_strategy",
      tps.record_key_subject_name_strategy);
    write_member(
      w,
      "record_key_subject_name_strategy_compat",
      tps.record_key_subject_name_strategy_compat);
    write_member(
      w,
      "record_value_schema_id_validation",
      tps.record_value_schema_id_validation);
    write_member(
      w,
      "record_value_schema_id_validation_compat",
      tps.record_value_schema_id_validation_compat);
    write_member(
      w,
      "record_value_subject_name_strategy",
      tps.record_value_subject_name_strategy);
    write_member(
      w,
      "record_value_subject_name_strategy_compat",
      tps.record_value_subject_name_strategy_compat);
    write_member(
      w,
      "initial_retention_local_target_bytes",
      tps.initial_retention_local_target_bytes);
    write_member(
      w,
      "initial_retention_local_target_ms",
      tps.initial_retention_local_target_ms);
    write_member(w, "mpx_virtual_cluster_id", tps.mpx_virtual_cluster_id);
    write_exceptional_member_type(w, "write_caching", tps.write_caching);
    write_member(w, "flush_bytes", tps.flush_bytes);
    write_member(w, "flush_ms", tps.flush_ms);
    write_member(w, "iceberg_enabled", tps.iceberg_enabled);
    w.EndObject();
}

inline void read_value(const json::Value& rd, cluster::topic_properties& obj) {
    read_member(rd, "compression", obj.compression);
    read_member(rd, "cleanup_policy_bitflags", obj.cleanup_policy_bitflags);
    read_member(rd, "compaction_strategy", obj.compaction_strategy);
    read_member(rd, "timestamp_type", obj.timestamp_type);
    read_member(rd, "segment_size", obj.segment_size);
    read_member(rd, "retention_bytes", obj.retention_bytes);
    read_member(rd, "retention_duration", obj.retention_duration);
    read_member(rd, "recovery", obj.recovery);
    read_member(rd, "shadow_indexing", obj.shadow_indexing);
    read_member(rd, "read_replica", obj.read_replica);
    read_member(rd, "read_replica_bucket", obj.read_replica_bucket);
    read_member(
      rd,
      "remote_topic_namespace_override",
      obj.remote_topic_namespace_override);
    read_member(rd, "remote_topic_properties", obj.remote_topic_properties);
    read_member(rd, "batch_max_bytes", obj.batch_max_bytes);
    read_member(
      rd, "retention_local_target_bytes", obj.retention_local_target_bytes);
    read_member(rd, "retention_local_target_ms", obj.retention_local_target_ms);
    read_member(rd, "remote_delete", obj.remote_delete);
    read_member(rd, "segment_ms", obj.segment_ms);
    read_member(
      rd,
      "record_key_schema_id_validation",
      obj.record_key_schema_id_validation);
    read_member(
      rd,
      "record_key_schema_id_validation_compat",
      obj.record_key_schema_id_validation_compat);
    read_member(
      rd,
      "record_key_subject_name_strategy",
      obj.record_key_subject_name_strategy);
    read_member(
      rd,
      "record_key_subject_name_strategy_compat",
      obj.record_key_subject_name_strategy_compat);
    read_member(
      rd,
      "record_value_schema_id_validation",
      obj.record_value_schema_id_validation);
    read_member(
      rd,
      "record_value_schema_id_validation_compat",
      obj.record_value_schema_id_validation_compat);
    read_member(
      rd,
      "record_value_subject_name_strategy",
      obj.record_value_subject_name_strategy);
    read_member(
      rd,
      "record_value_subject_name_strategy_compat",
      obj.record_value_subject_name_strategy_compat);
    read_member(
      rd,
      "initial_retention_local_target_bytes",
      obj.initial_retention_local_target_bytes);
    read_member(
      rd,
      "initial_retention_local_target_ms",
      obj.initial_retention_local_target_ms);
    read_member(rd, "mpx_virtual_cluster_id", obj.mpx_virtual_cluster_id);
    read_member(rd, "write_caching", obj.write_caching);
    read_member(rd, "flush_bytes", obj.flush_bytes);
    read_member(rd, "flush_ms", obj.flush_ms);
    read_member(rd, "iceberg_enabled", obj.iceberg_enabled);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::topic_configuration& cfg) {
    w.StartObject();
    write_member(w, "tp_ns", cfg.tp_ns);
    write_member(w, "partition_count", cfg.partition_count);
    write_member(w, "replication_factor", cfg.replication_factor);
    write_member(w, "is_migrated", cfg.is_migrated);
    write_member(w, "properties", cfg.properties);
    w.EndObject();
}

inline void
read_value(const json::Value& rd, cluster::topic_configuration& cfg) {
    read_member(rd, "tp_ns", cfg.tp_ns);
    read_member(rd, "partition_count", cfg.partition_count);
    read_member(rd, "replication_factor", cfg.replication_factor);
    read_member(rd, "is_migrated", cfg.is_migrated);
    read_member(rd, "properties", cfg.properties);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const v8_engine::data_policy& dp) {
    w.StartObject();
    write_member(w, "fn_name", dp.fn_name);
    write_member(w, "sct_name", dp.sct_name);
    w.EndObject();
}

inline void read_value(const json::Value& rd, v8_engine::data_policy& dp) {
    read_member(rd, "fn_name", dp.fn_name);
    read_member(rd, "sct_name", dp.sct_name);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::incremental_topic_custom_updates& itc) {
    w.StartObject();
    write_member(w, "data_policy", itc.data_policy);
    w.EndObject();
}

inline void read_value(
  const json::Value& rd, cluster::incremental_topic_custom_updates& itc) {
    read_member(rd, "data_policy", itc.data_policy);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::incremental_topic_updates& itu) {
    w.StartObject();
    write_member(w, "compression", itu.compression);
    write_member(w, "cleanup_policy_bitflags", itu.cleanup_policy_bitflags);
    write_member(w, "compaction_strategy", itu.compaction_strategy);
    write_member(w, "timestamp_type", itu.timestamp_type);
    write_member(w, "segment_size", itu.segment_size);
    write_member(w, "retention_bytes", itu.retention_bytes);
    write_member(w, "retention_duration", itu.retention_duration);
    write_member(w, "shadow_indexing", itu.get_shadow_indexing());
    write_member(w, "remote_delete", itu.remote_delete);
    write_member(w, "segment_ms", itu.segment_ms);
    w.EndObject();
}

inline void
read_value(const json::Value& rd, cluster::incremental_topic_updates& itu) {
    read_member(rd, "compression", itu.compression);
    read_member(rd, "cleanup_policy_bitflags", itu.cleanup_policy_bitflags);
    read_member(rd, "compaction_strategy", itu.compaction_strategy);
    read_member(rd, "timestamp_type", itu.timestamp_type);
    read_member(rd, "segment_size", itu.segment_size);
    read_member(rd, "retention_bytes", itu.retention_bytes);
    read_member(rd, "retention_duration", itu.retention_duration);
    read_member(rd, "shadow_indexing", itu.get_shadow_indexing());
    read_member(rd, "remote_delete", itu.remote_delete);
    read_member(rd, "segment_ms", itu.segment_ms);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const cluster::topic_properties_update& tpu) {
    w.StartObject();
    write_member(w, "tp_ns", tpu.tp_ns);
    write_member(w, "properties", tpu.properties);
    write_member(w, "custom_properties", tpu.custom_properties);
    w.EndObject();
}

inline void
read_value(const json::Value& rd, cluster::topic_properties_update& tpu) {
    read_member(rd, "tp_ns", tpu.tp_ns);
    read_member(rd, "properties", tpu.properties);
    read_member(rd, "custom_properties", tpu.custom_properties);
}

} // namespace json
