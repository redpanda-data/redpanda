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

} // namespace json
