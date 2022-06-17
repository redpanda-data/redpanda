/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/server/logger.h"
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

namespace {
template<typename T>
std::optional<T> read_optional_value(std::optional<request_reader>& reader) {
    if (!reader) {
        return std::nullopt;
    }
    return T::decode(*reader);
}

template<typename T>
iobuf metadata_to_iobuf(const T& t) {
    iobuf buffer;
    response_writer writer(buffer);
    T::encode(writer, t);
    return buffer;
}
iobuf maybe_unwrap_from_iobuf(iobuf buffer) {
    /*
     * Previously in redpanda we had an error leading to wrapping
     * tombstone record keys with an iobuf serialization envelope. In
     * order to provide compatibility and be able to read keys wrapped
     * with an additional iobuf we introduce a check leveraging
     * serialization format.
     *
     * # New serializer format - __consumer_offset topic
     *
     *
     * In Kafka the only allowed values for group metadata versions are
     * 0, 1, and 2. Group metadata use big endian encoding hence the
     * first two bytes of a serialized group metadata key contains
     * information about a version. Both offset_metadata_key and
     * group_metadata_key first serialized field is a `group_id` string
     * which is encoded as 2 bytes of size and then array of characters.
     * This way 4 first bytes of encoded key are:
     *
     * b0 - denotes LSB
     * bN - denotes N byte indexed from LSB to MSB (LSB has index 0)
     *
     * Key bytes (big endian):
     *
     *      |version_b1|version_b0|size_b1|size_b0|...
     *
     * When key struct is wrapped with an iobuf serialization it is
     * prefixed with iobuf size which is serialized as 4 bytes of a size
     * (in a little endian encoding) and then the iobuf content
     *
     * iobuf size bytes (little endian):
     *
     *      |size_b0|size_b1|size_b2|size_b3|...
     *
     * To check if metadata key was wrapped with an iobuf we peek first
     * 4 bytes to try to decode iobuf size store in little endian format
     * if returned size is equal to size of remaining key buffer size it
     * means that we need to unwrap key from an iobuf.
     *
     * # Old serializer format
     *
     * When serialized with previous `kafka_internal/group` serializer a key was
     * represented by
     *
     * struct group_log_record_key {
     *  enum class type : int8_t { group_metadata, offset_commit, noop };
     *    type record_type;
     *    iobuf key;
     * };
     *
     * When serialized with ADL its binary representation is as follows
     *
     *      |record_tp_b0|size_b0|size_b1|size_b2|...
     *
     * Those 4 first bytes will be decoded as an iobuf size.
     *
     * TODO: remove this code in future releases
     */

    iobuf_const_parser parser(buffer);
    // peek first 4-byte
    auto deserialized_size = reflection::from_iobuf<int32_t>(parser.peek(4));

    if (unlikely(
          buffer.size_bytes() == static_cast<size_t>(deserialized_size) + 4)) {
        vlog(kafka::klog.debug, "Unwrapping group metadata key from iobuf");
        // unwrap from iobuf
        return reflection::from_iobuf<iobuf>(std::move(buffer));
    }
    return buffer;
}
} // namespace

group_metadata_serializer make_backward_compatible_serializer() {
    struct impl : group_metadata_serializer::impl {
        group_metadata_type get_metadata_type(iobuf buffer) final {
            auto key = reflection::from_iobuf<old::group_log_record_key>(
              maybe_unwrap_from_iobuf(std::move(buffer)));
            switch (key.record_type) {
            case old::group_log_record_key::type::offset_commit:
                return group_metadata_type::offset_commit;
            case old::group_log_record_key::type::group_metadata:
                return group_metadata_type::group_metadata;
            case old::group_log_record_key::type::noop:
                return group_metadata_type::noop;
            }
        };

        group_metadata_serializer::key_value to_kv(group_metadata_kv md) final {
            group_metadata_serializer::key_value ret;
            ret.key = reflection::to_iobuf(old::group_log_record_key{
              .record_type = old::group_log_record_key::type::group_metadata,
              .key = reflection::to_iobuf(md.key.group_id),
            });

            if (md.value) {
                old::group_log_group_metadata old_metadata{
                  .protocol_type = std::move(md.value->protocol_type),
                  .generation = md.value->generation,
                  .protocol = std::move(md.value->protocol),
                  .leader = std::move(md.value->leader),
                  .state_timestamp = static_cast<int32_t>(
                    md.value->state_timestamp.value()),
                };

                old_metadata.members.reserve(md.value->members.size());
                std::transform(
                  md.value->members.begin(),
                  md.value->members.end(),
                  std::back_inserter(old_metadata.members),
                  [](member_state& ms) {
                      return old::member_state{
                        .id = std::move(ms.id),
                        .session_timeout = ms.session_timeout,
                        .rebalance_timeout = ms.rebalance_timeout,
                        .instance_id = ms.instance_id,
                        .assignment = std::move(ms.assignment),
                        .client_id = std::move(ms.client_id),
                        .client_host = std::move(ms.client_host),
                      };
                  });
                ret.value = reflection::to_iobuf(std::move(old_metadata));
            }

            return ret;
        }
        group_metadata_serializer::key_value
        to_kv(offset_metadata_kv md) final {
            group_metadata_serializer::key_value ret;

            ret.key = reflection::to_iobuf(old::group_log_record_key{
              .record_type = old::group_log_record_key::type::offset_commit,
              .key = reflection::to_iobuf(old::group_log_offset_key{
                std::move(md.key.group_id),
                std::move(md.key.topic),
                md.key.partition,
              }),
            });

            if (md.value) {
                ret.value = reflection::to_iobuf(old::group_log_offset_metadata{
                  md.value->offset,
                  md.value->leader_epoch,
                  md.value->metadata,
                });
            }
            return ret;
        }

        group_metadata_kv decode_group_metadata(model::record record) final {
            group_metadata_kv ret;
            auto record_key = reflection::from_iobuf<old::group_log_record_key>(
              maybe_unwrap_from_iobuf(record.release_key()));
            auto group_id = kafka::group_id(
              reflection::from_iobuf<kafka::group_id::type>(
                std::move(record_key.key)));

            ret.key = group_metadata_key{.group_id = group_id};

            if (record.has_value()) {
                auto md = reflection::from_iobuf<old::group_log_group_metadata>(
                  record.release_value());

                ret.value = group_metadata_value{
                  .protocol_type = std::move(md.protocol_type),
                  .generation = md.generation,
                  .protocol = std::move(md.protocol),
                  .leader = std::move(md.leader),
                  .state_timestamp = model::timestamp(md.state_timestamp),
                };
                ret.value->members.reserve(md.members.size());
                std::transform(
                  md.members.begin(),
                  md.members.end(),
                  std::back_inserter(ret.value->members),
                  [](old::member_state& member) {
                      return member_state{
                        .id = std::move(member.id),
                        .instance_id = std::move(member.instance_id),
                        .client_id = std::move(member.client_id),
                        .client_host = std::move(member.client_host),
                        .rebalance_timeout = member.rebalance_timeout,
                        .session_timeout = member.session_timeout,
                        .subscription = iobuf{},
                        .assignment = std::move(member.assignment),
                      };
                  });
            }

            return ret;
        }

        offset_metadata_kv decode_offset_metadata(model::record r) final {
            offset_metadata_kv ret;
            auto record_key = reflection::from_iobuf<old::group_log_record_key>(
              maybe_unwrap_from_iobuf(r.release_key()));
            auto key = reflection::from_iobuf<old::group_log_offset_key>(
              std::move(record_key.key));

            ret.key = offset_metadata_key{
              .group_id = std::move(key.group),
              .topic = std::move(key.topic),
              .partition = key.partition,
            };

            if (r.has_value()) {
                auto metadata
                  = reflection::from_iobuf<old::group_log_offset_metadata>(
                    r.release_value());

                ret.value = offset_metadata_value{
                  .offset = metadata.offset,
                  .leader_epoch = kafka::leader_epoch(metadata.leader_epoch),
                  .metadata = metadata.metadata.value_or(""),
                  .commit_timestamp = model::timestamp(-1),
                };
            }
            return ret;
        }
    };
    return group_metadata_serializer(std::make_unique<impl>());
}

group_metadata_serializer make_consumer_offsets_serializer() {
    struct impl final : group_metadata_serializer::impl {
        group_metadata_type get_metadata_type(iobuf buffer) final {
            auto reader = request_reader(
              maybe_unwrap_from_iobuf(std::move(buffer)));
            return decode_metadata_type(reader);
        };

        group_metadata_serializer::key_value to_kv(group_metadata_kv md) final {
            group_metadata_serializer::key_value ret;
            ret.key = metadata_to_iobuf(md.key);
            if (md.value) {
                ret.value = metadata_to_iobuf(*md.value);
            }

            return ret;
        }

        group_metadata_serializer::key_value
        to_kv(offset_metadata_kv md) final {
            group_metadata_serializer::key_value ret;
            ret.key = metadata_to_iobuf(md.key);
            if (md.value) {
                ret.value = metadata_to_iobuf(*md.value);
            }

            return ret;
        }

        group_metadata_kv decode_group_metadata(model::record record) final {
            group_metadata_kv ret;
            request_reader k_reader(
              maybe_unwrap_from_iobuf(record.release_key()));
            ret.key = group_metadata_key::decode(k_reader);
            if (record.has_value()) {
                request_reader v_reader(record.release_value());
                ret.value = group_metadata_value::decode(v_reader);
            }

            return ret;
        }

        offset_metadata_kv decode_offset_metadata(model::record record) final {
            offset_metadata_kv ret;
            request_reader k_reader(
              maybe_unwrap_from_iobuf(record.release_key()));
            ret.key = offset_metadata_key::decode(k_reader);
            if (record.has_value()) {
                request_reader v_reader(record.release_value());
                ret.value = offset_metadata_value::decode(v_reader);
            }

            return ret;
        }
    };
    return group_metadata_serializer(std::make_unique<impl>());
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
      "state_timestamp: {}, member count: {}}}",
      v.protocol_type,
      v.generation,
      v.protocol,
      v.leader,
      v.state_timestamp,
      v.members.size());
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
