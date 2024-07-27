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

#include "base/vassert.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/logger.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "utils/to_string.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

group_metadata_kv group_metadata_kv::copy() const {
    group_metadata_kv cp{.key = key};
    if (value) {
        cp.value = value->copy();
    }
    return cp;
}

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
group_metadata_type decode_metadata_type(protocol::decoder& key_reader) {
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
  protocol::encoder& writer, const group_metadata_key& v) {
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

group_metadata_key group_metadata_key::decode(protocol::decoder& reader) {
    group_metadata_key ret;
    auto version = read_metadata_version(reader);
    vassert(
      version == group_metadata_key::version,
      "Only valid version fro group_metadata_key is 2. Read version: {}",
      version);
    ret.group_id = kafka::group_id(reader.read_string());
    return ret;
}

void member_state::encode(protocol::encoder& writer, const member_state& v) {
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

member_state member_state::decode(protocol::decoder& reader) {
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
  protocol::encoder& writer, const group_metadata_value& v) {
    writer.write(v.version);
    writer.write(v.protocol_type);
    writer.write(v.generation);
    writer.write(v.protocol);
    writer.write(v.leader);
    writer.write(v.state_timestamp);
    writer.write_array(
      v.members, [](const member_state& member, protocol::encoder writer) {
          member_state::encode(writer, member);
      });
}

group_metadata_value group_metadata_value::decode(protocol::decoder& reader) {
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
      [](protocol::decoder& reader) { return member_state::decode(reader); });

    return ret;
}

void offset_metadata_key::encode(
  protocol::encoder& writer, const offset_metadata_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
    writer.write(v.topic);
    writer.write(v.partition);
}

offset_metadata_key offset_metadata_key::decode(protocol::decoder& reader) {
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
  protocol::encoder& writer, const offset_metadata_value& v) {
    const auto version = v.expiry_timestamp != model::timestamp(-1)
                           ? group_metadata_version{1}
                           : offset_metadata_value::latest_version;
    writer.write(version);
    writer.write(v.offset);
    if (version >= group_metadata_version{3}) {
        writer.write(v.leader_epoch);
    }
    writer.write(v.metadata);
    writer.write(v.commit_timestamp);
    if (version == group_metadata_version{1}) {
        writer.write(v.expiry_timestamp);
    }
}

offset_metadata_value offset_metadata_value::decode(protocol::decoder& reader) {
    offset_metadata_value ret;
    const auto version = read_metadata_version(reader);
    validate_version_range(
      version, "offset_metadata_value", offset_metadata_value::latest_version);

    ret.offset = model::offset(reader.read_int64());
    if (version >= group_metadata_version{3}) {
        ret.leader_epoch = kafka::leader_epoch(reader.read_int32());
    }
    ret.metadata = reader.read_string();
    ret.commit_timestamp = model::timestamp(reader.read_int64());
    // read expiry_timestamp only present in version 1
    if (version == group_metadata_version{1}) {
        ret.expiry_timestamp = model::timestamp(reader.read_int64());
    }

    return ret;
}

namespace {
template<typename T>
std::optional<T> read_optional_value(std::optional<protocol::decoder>& reader) {
    if (!reader) {
        return std::nullopt;
    }
    return T::decode(*reader);
}

template<typename T>
iobuf metadata_to_iobuf(const T& t) {
    iobuf buffer;
    protocol::encoder writer(buffer);
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

group_metadata_serializer make_consumer_offsets_serializer() {
    struct impl final : group_metadata_serializer::impl {
        group_metadata_type get_metadata_type(iobuf buffer) final {
            auto reader = protocol::decoder(
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
            protocol::decoder k_reader(
              maybe_unwrap_from_iobuf(record.release_key()));
            ret.key = group_metadata_key::decode(k_reader);
            if (record.has_value()) {
                protocol::decoder v_reader(record.release_value());
                ret.value = group_metadata_value::decode(v_reader);
            }

            return ret;
        }

        offset_metadata_kv decode_offset_metadata(model::record record) final {
            offset_metadata_kv ret;
            protocol::decoder k_reader(
              maybe_unwrap_from_iobuf(record.release_key()));
            ret.key = offset_metadata_key::decode(k_reader);
            if (record.has_value()) {
                protocol::decoder v_reader(record.release_value());
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
      "{{offset: {}, leader_epoch: {}, metadata: {}, commit_timestamp: {}, "
      "expiry_timestamp: {}}}",
      v.offset,
      v.leader_epoch,
      v.metadata,
      v.commit_timestamp,
      v.expiry_timestamp);
    return o;
}
namespace group_tx {
std::ostream& operator<<(std::ostream& o, const offsets_metadata& md) {
    fmt::print(
      o,
      "{{group_id: {}, pid: {}, tx_seq: {}, offsets: {}}}",
      md.group_id,
      md.pid,
      md.tx_seq,
      fmt::join(md.offsets, ", "));
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_offset& po) {
    fmt::print(
      o,
      "{{partition: {}, offset: {}, leader_epoch: {}, metadata: {}}}",
      po.tp,
      po.offset,
      po.leader_epoch,
      po.metadata);
    return o;
}
std::ostream& operator<<(std::ostream& o, const fence_metadata_v0& fence) {
    fmt::print(o, "{{group_id: {}}}", fence.group_id);
    return o;
}
std::ostream& operator<<(std::ostream& o, const fence_metadata_v1& fence) {
    fmt::print(
      o,
      "{{group_id: {}, tx_seq: {}, tx_timeout: {} ms}}",
      fence.group_id,
      fence.tx_seq,
      fence.transaction_timeout_ms.count());
    return o;
}

std::ostream& operator<<(std::ostream& o, const fence_metadata& fence) {
    fmt::print(
      o,
      "{{tm_partition: {},  group_id: {}, tx_seq: {}, tx_timeout: {} ms}}",
      fence.tm_partition,
      fence.group_id,
      fence.tx_seq,
      fence.transaction_timeout_ms);
    return o;
}
} // namespace group_tx
} // namespace kafka
