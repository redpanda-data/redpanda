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

} // namespace json
