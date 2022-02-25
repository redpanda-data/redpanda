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

group_metadata_key group_metadata_key::decode(request_reader& reader) {
    group_metadata_key ret;
    read_metadata_version(reader);
    ret.group_id = kafka::group_id(reader.read_string());
    return ret;
}

void member_state::encode(response_writer& writer, const member_state& v) {
    writer.write(v.version);
    writer.write(v.id);
    writer.write(v.instance_id);
    writer.write(v.client_id);
    writer.write(v.client_host);
    writer.write(static_cast<int32_t>(v.rebalance_timeout.count()));
    writer.write(static_cast<int32_t>(v.session_timeout.count()));
    writer.write(iobuf_to_bytes(v.subscription.copy()));
    writer.write(iobuf_to_bytes(v.assignment.copy()));
}

member_state member_state::decode(request_reader& reader) {
    member_state ret;
    read_metadata_version(reader);
    ret.id = member_id(reader.read_string());

    auto v = reader.read_nullable_string();
    if (v) {
        ret.instance_id = group_instance_id(std::move(*v));
    }
    ret.client_id = kafka::client_id(reader.read_string());
    ret.client_host = kafka::client_host(reader.read_string());
    ret.rebalance_timeout = std::chrono::milliseconds(reader.read_int32());
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
    read_metadata_version(reader);
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
    ret.state_timestamp = model::timestamp(reader.read_int64());
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
    read_metadata_version(reader);
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
    writer.write(v.expire_timestamp);
}

offset_metadata_value offset_metadata_value::decode(request_reader& reader) {
    offset_metadata_value ret;
    read_metadata_version(reader);
    ret.offset = model::offset(reader.read_int64());
    ret.leader_epoch = kafka::leader_epoch(reader.read_int32());
    ret.metadata = reader.read_string();
    ret.commit_timestamp = model::timestamp(reader.read_int64());
    ret.expire_timestamp = model::timestamp(reader.read_int64());
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
      "{{offset: {}, leader_epoch: {}, metadata: {}, commit_timestap: {}, "
      "expire_timestamp: {}}}",
      v.offset,
      v.leader_epoch,
      v.metadata,
      v.commit_timestamp,
      v.expire_timestamp);
    return o;
}
namespace old {
std::ostream& operator<<(std::ostream& os, const group_log_offset_key& key) {
    fmt::print(
      os,
      "group {} topic {} partition {}",
      key.group(),
      key.topic(),
      key.partition());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const group_log_offset_metadata& md) {
    fmt::print(os, "offset {}", md.offset());
    return os;
}
} // namespace old
} // namespace kafka
namespace reflection {

namespace old {
struct member_state_v0 {
    kafka::member_id id;
    std::chrono::milliseconds session_timeout;
    std::chrono::milliseconds rebalance_timeout;
    std::optional<kafka::group_instance_id> instance_id;
    kafka::protocol_type protocol_type;
    std::vector<kafka::member_protocol> protocols;
    iobuf assignment;
};

struct group_log_group_metadata_v0 {
    kafka::protocol_type protocol_type;
    kafka::generation_id generation;
    std::optional<kafka::protocol_name> protocol;
    std::optional<kafka::member_id> leader;
    int32_t state_timestamp;
    std::vector<member_state_v0> members;
};

struct client_host_id {
    kafka::client_id id;
    kafka::client_host host;
};
} // namespace old

void adl<kafka::old::group_log_group_metadata>::to(
  iobuf& out, kafka::old::group_log_group_metadata&& data) {
    // create instance of old version of members
    std::vector<old::member_state_v0> members_v0;
    members_v0.reserve(data.members.size());
    for (auto& member : data.members) {
        members_v0.push_back(old::member_state_v0{
          .id = std::move(member.id),
          .session_timeout = member.session_timeout,
          .rebalance_timeout = member.rebalance_timeout,
          .instance_id = std::move(member.instance_id),
          .protocol_type = std::move(member.protocol_type),
          .protocols = std::move(member.protocols),
          .assignment = std::move(member.assignment),
        });
    }

    // create instance of old version of group metadata
    old::group_log_group_metadata_v0 metadata_v0{
      .protocol_type = std::move(data.protocol_type),
      .generation = data.generation,
      .protocol = std::move(data.protocol),
      .leader = std::move(data.leader),
      .state_timestamp = data.state_timestamp,
      .members = std::move(members_v0),
    };

    // this puts a version on disk that older code can read
    serialize(out, std::move(metadata_v0));

    // now append the client host/id information for new code
    std::vector<old::client_host_id> client_info;
    client_info.reserve(data.members.size());
    for (auto& member : data.members) {
        client_info.push_back(old::client_host_id{
          .id = std::move(member.client_id),
          .host = std::move(member.client_host),
        });
    }
    serialize(out, std::move(client_info));
}

kafka::old::group_log_group_metadata
adl<kafka::old::group_log_group_metadata>::from(iobuf_parser& in) {
    auto metadata_v0 = adl<old::group_log_group_metadata_v0>{}.from(in);

    std::vector<old::client_host_id> client_info;
    if (in.bytes_left()) {
        client_info = adl<std::vector<old::client_host_id>>{}.from(in);
        vassert(
          client_info.size() == metadata_v0.members.size(),
          "Expected client info size {} got {}",
          metadata_v0.members.size(),
          client_info.size());
    }

    std::vector<kafka::old::member_state> members_out;
    members_out.reserve(metadata_v0.members.size());

    for (auto& member : metadata_v0.members) {
        members_out.push_back(kafka::old::member_state{
          .id = std::move(member.id),
          .session_timeout = member.session_timeout,
          .rebalance_timeout = member.rebalance_timeout,
          .instance_id = std::move(member.instance_id),
          .protocol_type = std::move(member.protocol_type),
          .protocols = std::move(member.protocols),
          .assignment = std::move(member.assignment),
        });
    }

    for (size_t i = 0; i < client_info.size(); i++) {
        members_out[i].client_id = std::move(client_info[i].id);
        members_out[i].client_host = std::move(client_info[i].host);
    }

    kafka::old::group_log_group_metadata metadata_out{
      .protocol_type = std::move(metadata_v0.protocol_type),
      .generation = metadata_v0.generation,
      .protocol = std::move(metadata_v0.protocol),
      .leader = std::move(metadata_v0.leader),
      .state_timestamp = metadata_v0.state_timestamp,
      .members = std::move(members_out),
    };

    return metadata_out;
}

} // namespace reflection
