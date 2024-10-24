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

inline void read_value(const json::Value& rd, raft::vnode& obj) {
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
    w.Key("dirty_offset");
    rjson_serialize(w, v.dirty_offset);
    w.EndObject();
}

inline void read_value(const json::Value& rd, raft::protocol_metadata& obj) {
    raft::protocol_metadata tmp;
    read_member(rd, "group", tmp.group);
    read_member(rd, "commit_index", tmp.commit_index);
    read_member(rd, "term", tmp.term);
    read_member(rd, "prev_log_index", tmp.prev_log_index);
    read_member(rd, "prev_log_term", tmp.prev_log_term);
    read_member(rd, "last_visible_index", tmp.last_visible_index);
    read_member(rd, "dirty_offset", tmp.dirty_offset);
    obj = tmp;
}

inline void read_value(const json::Value& rd, raft::heartbeat_metadata& obj) {
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

inline void read_value(const json::Value& rd, raft::append_entries_reply& out) {
    raft::append_entries_reply obj;
    json_read(target_node_id);
    json_read(node_id);
    json_read(group);
    json_read(term);
    json_read(last_flushed_log_index);
    json_read(last_dirty_log_index);
    json_read(last_term_base_offset);
    auto result = read_enum_ut(rd, "result", obj.result);
    switch (result) {
    case 0:
        obj.result = raft::reply_result::success;
        break;
    case 1:
        obj.result = raft::reply_result::failure;
        break;
    case 2:
        obj.result = raft::reply_result::group_unavailable;
        break;
    case 3:
        obj.result = raft::reply_result::timeout;
        break;
    default:
        vassert(false, "invalid result {}", result);
    }
    json_read(may_recover);
    out = obj;
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::timestamp& v) {
    rjson_serialize(wr, v.value());
}

inline void read_value(const json::Value& rd, model::timestamp& out) {
    out = model::timestamp(rd.GetInt64());
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
    json_write(may_recover);
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr,
  const model::record_batch_attributes& b) {
    rjson_serialize(wr, b.value());
}

inline void
read_value(const json::Value& rd, model::record_batch_attributes& out) {
    out = model::record_batch_attributes(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_attributes& b) {
    rjson_serialize(wr, b.value());
}

inline void read_value(const json::Value& rd, model::record_attributes& out) {
    out = model::record_attributes(rd.GetInt());
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_batch_header& obj) {
    wr.StartObject();
    json_write(header_crc);
    json_write(size_bytes);
    json_write(base_offset);
    json_write(type);
    json_write(crc);
    json_write(attrs);
    json_write(last_offset_delta);
    json_write(first_timestamp);
    json_write(max_timestamp);
    json_write(producer_id);
    json_write(producer_epoch);
    json_write(base_sequence);
    json_write(record_count);
    wr.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_header& obj) {
    wr.StartObject();
    json::write_member(wr, "key_size", obj.key_size());
    json::write_member(wr, "key", obj.key());
    json::write_member(wr, "value_size", obj.value_size());
    json::write_member(wr, "value", obj.value());
    wr.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record& obj) {
    wr.StartObject();
    json::write_member(wr, "size_bytes", obj.size_bytes());
    json::write_member(wr, "attributes", obj.attributes());
    json::write_member(wr, "timestamp_delta", obj.timestamp_delta());
    json::write_member(wr, "offset_delta", obj.offset_delta());
    json::write_member(wr, "key_size", obj.key_size());
    json::write_member(wr, "key", obj.key());
    json::write_member(wr, "value_size", obj.value_size());
    json::write_member(wr, "value", obj.value());
    json::write_member(wr, "headers", obj.headers());
    wr.EndObject();
}

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& wr, const model::record_batch& b) {
    wr.StartObject();
    wr.Key("header");
    rjson_serialize(wr, b.header());
    wr.Key("records");
    rjson_serialize(wr, b.copy_records());
    wr.EndObject();
}

inline void read_value(const json::Value& rd, model::record_batch_header& out) {
    model::record_batch_header obj;
    json_read(header_crc);
    json_read(size_bytes);
    json_read(base_offset);
    json_read(type);
    json_read(crc);
    json_read(attrs);
    json_read(last_offset_delta);
    json_read(first_timestamp);
    json_read(max_timestamp);
    json_read(producer_id);
    json_read(producer_epoch);
    json_read(base_sequence);
    json_read(record_count);
    out = obj;
}

/*
 * specialized circular_buffer for record_batch type because record_batch
 * doesn't support default constructor and the generic form for circular buffer
 * of type T needs to build a structure to deserialize into.
 *
 * since the components of the record batch aren't default constructable we also
 * circumvent the normal api to which requires default ctor is available.
 */
inline void read_value(
  const json::Value& v, ss::circular_buffer<model::record_batch>& target) {
    for (const auto& e : v.GetArray()) {
        model::record_batch_header header;
        std::vector<model::record> records;

        /*
         * header
         */
        json::read_member(e, "header", header);

        /*
         * records
         */
        vassert(
          e.HasMember("records") && e["records"].IsArray(),
          "invalid records field");
        for (const auto& r : e["records"].GetArray()) {
            vassert(r.IsObject(), "record is not an object");

            int32_t size_bytes{};
            model::record_attributes attributes;
            int64_t timestamp_delta{};
            int32_t offset_delta{};
            int32_t key_size{};
            iobuf key;
            int32_t value_size{};
            iobuf value;
            std::vector<model::record_header> headers;

            json::read_member(r, "size_bytes", size_bytes);
            json::read_member(r, "attributes", attributes);
            json::read_member(r, "timestamp_delta", timestamp_delta);
            json::read_member(r, "offset_delta", offset_delta);
            json::read_member(r, "key_size", key_size);
            json::read_member(r, "key", key);
            json::read_member(r, "value_size", value_size);
            json::read_member(r, "value", value);

            /*
             * record headers
             */
            vassert(
              r.HasMember("headers") && r["headers"].IsArray(),
              "invalid headers field");
            for (const auto& h : r["headers"].GetArray()) {
                int32_t key_size{};
                iobuf key;
                int32_t value_size{};
                iobuf value;

                json::read_member(h, "key_size", key_size);
                json::read_member(h, "key", key);
                json::read_member(h, "value_size", value_size);
                json::read_member(h, "value", value);

                headers.emplace_back(
                  key_size, std::move(key), value_size, std::move(value));
            }

            records.emplace_back(
              size_bytes,
              attributes,
              timestamp_delta,
              offset_delta,
              key_size,
              std::move(key),
              value_size,
              std::move(value),
              std::move(headers));
        }

        target.emplace_back(header, std::move(records));
    }
}

} // namespace json
