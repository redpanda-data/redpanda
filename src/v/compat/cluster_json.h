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

#include "cluster/types.h"
#include "compat/json.h"

namespace json {

inline void read_value(json::Value const& rd, cluster::errc& e) {
    /// TODO: Make giant switch to confirm value is a proper cluster::errc
    auto err = rd.GetInt();
    e = static_cast<cluster::errc>(err);
}

inline void
read_value(json::Value const& rd, cluster::cluster_property_kv& obj) {
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
read_value(json::Value const& rd, cluster::partition_move_direction& e) {
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
read_value(json::Value const& rd, cluster::feature_update_action::action_t& e) {
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
read_value(json::Value const& rd, cluster::feature_update_action& obj) {
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

inline void read_value(json::Value const& v, cluster::tx_errc& obj) {
    obj = {v.GetInt()};
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::non_replicable_topic& t) {
    w.StartObject();
    w.Key("source");
    rjson_serialize(w, t.source);
    w.Key("name");
    rjson_serialize(w, t.name);
    w.EndObject();
}

inline void
read_value(json::Value const& rd, cluster::non_replicable_topic& obj) {
    model::topic_namespace source;
    model::topic_namespace name;
    read_member(rd, "source", source);
    read_member(rd, "name", name);
    obj = {.source = std::move(source), .name = std::move(name)};
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

inline void read_value(json::Value const& rd, cluster::topic_result& obj) {
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

inline void
read_value(json::Value const& rd, cluster::move_cancellation_result& obj) {
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
read_value(json::Value const& rd, cluster::partition_assignment& obj) {
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

inline void read_value(json::Value const& rd, cluster::backend_operation& obj) {
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
read_value(json::Value const& rd, cluster::ntp_reconciliation_state& obj) {
    model::ntp ntp;
    std::vector<cluster::backend_operation> operations;
    cluster::reconciliation_status status;
    cluster::errc error;

    read_member(rd, "ntp", ntp);
    read_member(rd, "operations", operations);
    read_member(rd, "status", status);
    read_member(rd, "error", error);

    obj = cluster::ntp_reconciliation_state(
      std::move(ntp), std::move(operations), status, error);
}

} // namespace json
