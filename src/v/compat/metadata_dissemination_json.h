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

#include "cluster/metadata_dissemination_types.h"
#include "compat/json.h"
#include "json/json.h"

namespace json {

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::ntp_leader& v) {
    w.StartObject();
    w.Key("ntp");
    rjson_serialize(w, v.ntp);
    w.Key("term");
    rjson_serialize(w, v.term);
    w.Key("leader_id");
    rjson_serialize(w, v.leader_id);
    w.EndObject();
}

inline void read_value(json::Value const& rd, cluster::ntp_leader& obj) {
    read_member(rd, "ntp", obj.ntp);
    read_member(rd, "term", obj.term);
    read_member(rd, "leader_id", obj.leader_id);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const cluster::ntp_leader_revision& v) {
    w.StartObject();
    w.Key("ntp");
    rjson_serialize(w, v.ntp);
    w.Key("term");
    rjson_serialize(w, v.term);
    w.Key("leader_id");
    rjson_serialize(w, v.leader_id);
    w.Key("revision_id");
    rjson_serialize(w, v.revision);
    w.EndObject();
}

inline void
read_value(json::Value const& rd, cluster::ntp_leader_revision& obj) {
    read_member(rd, "ntp", obj.ntp);
    read_member(rd, "term", obj.term);
    read_member(rd, "leader_id", obj.leader_id);
    read_member(rd, "revision_id", obj.revision);
}

} // namespace json
