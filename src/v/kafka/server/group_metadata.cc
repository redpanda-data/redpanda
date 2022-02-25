/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/group_metadata.h"

#include "bytes/bytes.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/types.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "utils/to_string.h"
#include "vassert.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

/**
 * /Kafka
 * Messages stored for the group topic has versions for both the key and value
 * fields. Key version is used to indicate the type of the message (also to
 * differentiate different types of messages from being compacted together if
 * they have the same field values); and value version is used to evolve the
 * messages within their data types:
 *
 * key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp,
 * expire_timestamp]
 *
 * key version 2:       group metadata
 *    -> value version 0:       [protocol_type, generation, protocol, leader,
 * members]
 */
group_metadata_type decode_metadata_type(request_reader& key_reader) {
    auto version = read_metadata_version(key_reader);

    if (
      version == group_metadata_version{0}
      || version == group_metadata_version{1}) {
        return group_metadata_type::offset_commit;
    }

    if (version == group_metadata_version{2}) {
        return group_metadata_type::group_metadata;
    }
    throw std::invalid_argument(fmt::format(
      "unexpected group metadata record with key versions {}", version));
}

void group_metadata_key::encode(
  response_writer& writer, const group_metadata_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
}

namespace {
void validate_version_range(
  group_metadata_version read_version,
  std::string_view type_name,
  group_metadata_version max) {
    vassert(
      read_version >= group_metadata_version{0} && read_version <= max,
      "Valid version range for {}: [0,{}]. Read version: {}",
      type_name,
      max,
      read_version);
}
} // namespace

group_metadata_key group_metadata_key::decode(request_reader& reader) {
    group_metadata_key ret;
    auto version = read_metadata_version(reader);
    vassert(
      version == group_metadata_key::version,
      "Only valid version fro group_metadata_key is 2. Read version: {}",
      version);
    ret.group_id = kafka::group_id(reader.read_string());
    return ret;
}

void member_state::encode(response_writer& writer, const member_state& v) {
    writer.write(v.version);
    writer.write(v.id);
    writer.write(v.instance_id);
    writer.write(v.client_id);
    writer.write(v.client_host);
    writer.write(static_cast<int32_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(v.rebalance_timeout)
        .count()));
    writer.write(static_cast<int32_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(v.session_timeout)
        .count()));
    writer.write(iobuf_to_bytes(v.subscription.copy()));
    writer.write(iobuf_to_bytes(v.assignment.copy()));
}

member_state member_state::decode(request_reader& reader) {
    member_state ret;
    auto version = read_metadata_version(reader);
    validate_version_range(version, "members_state", member_state::version);

    ret.id = member_id(reader.read_string());
    if (version >= group_metadata_version{3}) {
        auto v = reader.read_nullable_string();
        if (v) {
            ret.instance_id = group_instance_id(std::move(*v));
        }
    }
    ret.client_id = kafka::client_id(reader.read_string());
    ret.client_host = kafka::client_host(reader.read_string());
    if (version >= group_metadata_version{1}) {
        ret.rebalance_timeout = std::chrono::milliseconds(reader.read_int32());
    }
    ret.session_timeout = std::chrono::milliseconds(reader.read_int32());
    ret.subscription = bytes_to_iobuf(reader.read_bytes());
    ret.assignment = bytes_to_iobuf(reader.read_bytes());
    return ret;
}

void group_metadata_value::encode(
  response_writer& writer, const group_metadata_value& v) {
    writer.write(v.version);
    writer.write(v.protocol_type);
    writer.write(v.generation);
    writer.write(v.protocol);
    writer.write(v.leader);
    writer.write(v.state_timestamp);
    writer.write_array(
      v.members, [](const member_state& member, response_writer writer) {
          member_state::encode(writer, member);
      });
}

group_metadata_value group_metadata_value::decode(request_reader& reader) {
    group_metadata_value ret;
    auto version = read_metadata_version(reader);
    validate_version_range(
      version, "group_metadata_value", group_metadata_value::version);
    ret.protocol_type = kafka::protocol_type(reader.read_string());
    ret.generation = kafka::generation_id(reader.read_int32());
    auto protocol_opt = reader.read_nullable_string();
    if (protocol_opt) {
        ret.protocol = kafka::protocol_name(std::move(protocol_opt.value()));
    }
    auto leader_opt = reader.read_nullable_string();
    if (leader_opt) {
        ret.leader = kafka::member_id(std::move(leader_opt.value()));
    }

    if (version >= group_metadata_version{2}) {
        ret.state_timestamp = model::timestamp(reader.read_int64());
    }

    ret.members = reader.read_array(
      [](request_reader& reader) { return member_state::decode(reader); });

    return ret;
}

void offset_metadata_key::encode(
  response_writer& writer, const offset_metadata_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
    writer.write(v.topic);
    writer.write(v.partition);
}

offset_metadata_key offset_metadata_key::decode(request_reader& reader) {
    offset_metadata_key ret;
    auto version = read_metadata_version(reader);
    validate_version_range(
      version, "offset_metadata_key", offset_metadata_key::version);
    ret.group_id = kafka::group_id(reader.read_string());
    ret.topic = model::topic(reader.read_string());
    ret.partition = model::partition_id(reader.read_int32());
    return ret;
}

void offset_metadata_value::encode(
  response_writer& writer, const offset_metadata_value& v) {
    writer.write(v.version);
    writer.write(v.offset);
    writer.write(v.leader_epoch);
    writer.write(v.metadata);
    writer.write(v.commit_timestamp);
}

offset_metadata_value offset_metadata_value::decode(request_reader& reader) {
    offset_metadata_value ret;
    auto version = read_metadata_version(reader);
    validate_version_range(
      version, "offset_metadata_value", offset_metadata_value::version);

    ret.offset = model::offset(reader.read_int64());
    if (version >= group_metadata_version{3}) {
        ret.leader_epoch = kafka::leader_epoch(reader.read_int32());
    }
    ret.metadata = reader.read_string();
    ret.commit_timestamp = model::timestamp(reader.read_int64());
    // read and ignore expiry_timestamp only present in version 1
    if (version == group_metadata_version{1}) {
        reader.read_int64();
    }

    return ret;
}

std::ostream& operator<<(std::ostream& o, const member_state& v) {
    fmt::print(
      o,
      "{{id: {}, instance_id: {}, client_id: {}, client_host: {}, "
      "rebalance_timeout: {}, session_timeout: {}, subscription_size: {}, "
      "assignment_size: {}}}",
      v.id,
      v.instance_id,
      v.client_id,
      v.client_host,
      v.rebalance_timeout,
      v.session_timeout,
      v.subscription.size_bytes(),
      v.assignment.size_bytes());
    return o;
}
std::ostream& operator<<(std::ostream& o, const group_metadata_key& v) {
    fmt::print(o, "{{group_id: {}}}", v.group_id);
    return o;
}
std::ostream& operator<<(std::ostream& o, const group_metadata_value& v) {
    fmt::print(
      o,
      "{{protocol_type: {}, generation: {}, protocol: {}, leader: {}, "
      "state_timestamp: {}, members: {}}}",
      v.protocol_type,
      v.generation,
      v.protocol,
      v.leader,
      v.state_timestamp,
      v.members);
    return o;
}
std::ostream& operator<<(std::ostream& o, const offset_metadata_key& v) {
    fmt::print(
      o,
      "{{group_id: {}, topic: {}, partition: {}}}",
      v.group_id,
      v.topic,
      v.partition);
    return o;
}
std::ostream& operator<<(std::ostream& o, const offset_metadata_value& v) {
    fmt::print(
      o,
      "{{offset: {}, leader_epoch: {}, metadata: {}, commit_timestap: {}}}",
      v.offset,
      v.leader_epoch,
      v.metadata,
      v.commit_timestamp);
    return o;
}
} // namespace kafka
