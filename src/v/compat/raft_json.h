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

#include "compat/json.h"
#include "raft/types.h"

namespace json {

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const raft::vnode& v) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, v.id());
    w.Key("revision");
    rjson_serialize(w, v.revision());
    w.EndObject();
}

inline void read_value(json::Value const& rd, raft::vnode& obj) {
    model::node_id node_id;
    model::revision_id revision;
    read_member(rd, "id", node_id);
    read_member(rd, "revision", revision);
    obj = raft::vnode(node_id, revision);
}

} // namespace json
