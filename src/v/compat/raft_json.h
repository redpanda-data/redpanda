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

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const raft::protocol_metadata& v) {
    w.StartObject();
    w.Key("group");
    rjson_serialize(w, v.group);
    w.Key("commit_index");
    rjson_serialize(w, v.commit_index);
    w.Key("term");
    rjson_serialize(w, v.term);
    w.Key("prev_log_index");
    rjson_serialize(w, v.prev_log_index);
    w.Key("prev_log_term");
    rjson_serialize(w, v.prev_log_term);
    w.Key("last_visible_index");
    rjson_serialize(w, v.last_visible_index);
    w.EndObject();
}

inline void read_value(json::Value const& rd, raft::protocol_metadata& obj) {
    raft::protocol_metadata tmp;
    read_member(rd, "group", tmp.group);
    read_member(rd, "commit_index", tmp.commit_index);
    read_member(rd, "term", tmp.term);
    read_member(rd, "prev_log_index", tmp.prev_log_index);
    read_member(rd, "prev_log_term", tmp.prev_log_term);
    read_member(rd, "last_visible_index", tmp.last_visible_index);
    obj = tmp;
}

inline void read_value(json::Value const& rd, raft::heartbeat_metadata& obj) {
    raft::heartbeat_metadata tmp;
    read_member(rd, "meta", tmp.meta);
    read_member(rd, "node_id", tmp.node_id);
    read_member(rd, "target_node_id", tmp.target_node_id);
    obj = tmp;
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const raft::heartbeat_metadata& v) {
    w.StartObject();
    w.Key("meta");
    rjson_serialize(w, v.meta);
    w.Key("node_id");
    rjson_serialize(w, v.node_id);
    w.Key("target_node_id");
    rjson_serialize(w, v.target_node_id);
    w.EndObject();
}

inline void read_value(json::Value const& rd, raft::append_entries_reply& out) {
    raft::append_entries_reply obj;
    json_read(target_node_id);
    json_read(node_id);
    json_read(group);
    json_read(term);
    json_read(last_flushed_log_index);
    json_read(last_dirty_log_index);
    json_read(last_term_base_offset);
    auto result = read_member_enum(rd, "result", obj.result);
    switch (result) {
    case 0:
        obj.result = raft::append_entries_reply::status::success;
        break;
    case 1:
        obj.result = raft::append_entries_reply::status::failure;
        break;
    case 2:
        obj.result = raft::append_entries_reply::status::group_unavailable;
        break;
    case 3:
        obj.result = raft::append_entries_reply::status::timeout;
        break;
    default:
        vassert(false, "invalid result {}", result);
    }
    out = obj;
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const raft::append_entries_reply& obj) {
    json_write(target_node_id);
    json_write(node_id);
    json_write(group);
    json_write(term);
    json_write(last_flushed_log_index);
    json_write(last_dirty_log_index);
    json_write(last_term_base_offset);
    json_write(result);
}

} // namespace json
