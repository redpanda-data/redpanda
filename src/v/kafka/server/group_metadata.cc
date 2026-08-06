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
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "serde/rw/rw.h"
#include "utils/to_string.h"

#include <fmt/core.h>

#include <chrono>
#include <optional>
#include <string_view>

namespace kafka {

group_metadata_kv group_metadata_kv::copy() const {
    group_metadata_kv cp{.key = key};
    if (value) {
        cp.value = value->copy();
    }
    return cp;
}

consumer_group_member_metadata_kv
consumer_group_member_metadata_kv::copy() const {
    consumer_group_member_metadata_kv cp{.key = key};
    if (value) {
        cp.value = value->copy();
    }
    return cp;
}

consumer_group_target_assignment_member_kv
consumer_group_target_assignment_member_kv::copy() const {
    consumer_group_target_assignment_member_kv cp{.key = key};
    if (value) {
        cp.value = value->copy();
    }
    return cp;
}

consumer_group_current_member_assignment_kv
consumer_group_current_member_assignment_kv::copy() const {
    consumer_group_current_member_assignment_kv cp{.key = key};
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
 *
 * KIP-848 adds the consumer-protocol records below. Their key version is
 * Kafka's record apiKey; their values are serde envelopes, which carry their
 * own version. Key version 4 (ConsumerGroupPartitionMetadata) is absent because
 * Kafka deprecated it in favour of ConsumerGroupMetadata's MetadataHash and
 * Redpanda never writes it.
 *
 * key version 3:       consumer group metadata
 * key version 5:       consumer group member metadata
 * key version 6:       consumer group target assignment metadata
 * key version 7:       consumer group target assignment member
 * key version 8:       consumer group current member assignment
 */
group_metadata_type decode_metadata_type(protocol::decoder& key_reader) {
    auto version = read_metadata_version(key_reader);

    // The pre-KIP-98 offset commit has no struct of its own.
    if (
      version == group_metadata_version{0}
      || version == offset_metadata_key::version) {
        return group_metadata_type::offset_commit;
    }
    if (version == group_metadata_key::version) {
        return group_metadata_type::group_metadata;
    }
    if (version == consumer_group_metadata_key::version) {
        return group_metadata_type::consumer_group_metadata;
    }
    if (version == consumer_group_member_metadata_key::version) {
        return group_metadata_type::consumer_group_member_metadata;
    }
    if (version == consumer_group_target_assignment_metadata_key::version) {
        return group_metadata_type::consumer_group_target_assignment_metadata;
    }
    if (version == consumer_group_target_assignment_member_key::version) {
        return group_metadata_type::consumer_group_target_assignment_member;
    }
    if (version == consumer_group_current_member_assignment_key::version) {
        return group_metadata_type::consumer_group_current_member_assignment;
    }
    throw std::invalid_argument(
      fmt::format(
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

/// A record key's version is its type discriminator, so only one value is ever
/// valid for a given key type.
void validate_key_version(
  group_metadata_version read_version,
  std::string_view type_name,
  group_metadata_version expected) {
    vassert(
      read_version == expected,
      "Only valid version for {} is {}. Read version: {}",
      type_name,
      expected,
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
    writer.write(
      static_cast<int32_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
          v.rebalance_timeout)
          .count()));
    writer.write(
      static_cast<int32_t>(
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

    ret.members = reader.read_array<chunked_vector>(
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

void consumer_group_metadata_key::encode(
  protocol::encoder& writer, const consumer_group_metadata_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
}

consumer_group_metadata_key
consumer_group_metadata_key::decode(protocol::decoder& reader) {
    consumer_group_metadata_key ret;
    validate_key_version(
      read_metadata_version(reader),
      "consumer_group_metadata_key",
      consumer_group_metadata_key::version);
    ret.group_id = kafka::group_id(reader.read_string());
    return ret;
}

classic_member_metadata classic_member_metadata::copy() const {
    return classic_member_metadata{
      .session_timeout = session_timeout,
      .supported_protocols = supported_protocols.copy(),
    };
}

void consumer_group_member_metadata_key::encode(
  protocol::encoder& writer, const consumer_group_member_metadata_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
    writer.write(v.member_id);
}

consumer_group_member_metadata_key
consumer_group_member_metadata_key::decode(protocol::decoder& reader) {
    consumer_group_member_metadata_key ret;
    validate_key_version(
      read_metadata_version(reader),
      "consumer_group_member_metadata_key",
      consumer_group_member_metadata_key::version);
    ret.group_id = kafka::group_id(reader.read_string());
    ret.member_id = kafka::member_id(reader.read_string());
    return ret;
}

consumer_group_member_metadata_value
consumer_group_member_metadata_value::copy() const {
    consumer_group_member_metadata_value ret{
      .instance_id = instance_id,
      .rack_id = rack_id,
      .client_id = client_id,
      .client_host = client_host,
      .subscribed_topic_names = subscribed_topic_names.copy(),
      .subscribed_topic_regex = subscribed_topic_regex,
      .rebalance_timeout = rebalance_timeout,
      .server_assignor = server_assignor,
    };
    if (classic_metadata) {
        ret.classic_metadata = classic_metadata->copy();
    }
    return ret;
}

void consumer_group_target_assignment_metadata_key::encode(
  protocol::encoder& writer,
  const consumer_group_target_assignment_metadata_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
}

consumer_group_target_assignment_metadata_key
consumer_group_target_assignment_metadata_key::decode(
  protocol::decoder& reader) {
    consumer_group_target_assignment_metadata_key ret;
    validate_key_version(
      read_metadata_version(reader),
      "consumer_group_target_assignment_metadata_key",
      consumer_group_target_assignment_metadata_key::version);
    ret.group_id = kafka::group_id(reader.read_string());
    return ret;
}

target_assignment_topic_partitions
target_assignment_topic_partitions::copy() const {
    return target_assignment_topic_partitions{
      .topic_id = topic_id,
      .partitions = partitions.copy(),
    };
}

void consumer_group_target_assignment_member_key::encode(
  protocol::encoder& writer,
  const consumer_group_target_assignment_member_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
    writer.write(v.member_id);
}

consumer_group_target_assignment_member_key
consumer_group_target_assignment_member_key::decode(protocol::decoder& reader) {
    consumer_group_target_assignment_member_key ret;
    validate_key_version(
      read_metadata_version(reader),
      "consumer_group_target_assignment_member_key",
      consumer_group_target_assignment_member_key::version);
    ret.group_id = kafka::group_id(reader.read_string());
    ret.member_id = kafka::member_id(reader.read_string());
    return ret;
}

consumer_group_target_assignment_member_value
consumer_group_target_assignment_member_value::copy() const {
    consumer_group_target_assignment_member_value ret;
    ret.topic_partitions.reserve(topic_partitions.size());
    for (const auto& tp : topic_partitions) {
        ret.topic_partitions.push_back(tp.copy());
    }
    return ret;
}

current_assignment_topic_partitions
current_assignment_topic_partitions::copy() const {
    return current_assignment_topic_partitions{
      .topic_id = topic_id,
      .partitions = partitions.copy(),
      .assignment_epochs = assignment_epochs.copy(),
    };
}

void consumer_group_current_member_assignment_key::encode(
  protocol::encoder& writer,
  const consumer_group_current_member_assignment_key& v) {
    writer.write(v.version);
    writer.write(v.group_id);
    writer.write(v.member_id);
}

consumer_group_current_member_assignment_key
consumer_group_current_member_assignment_key::decode(
  protocol::decoder& reader) {
    consumer_group_current_member_assignment_key ret;
    validate_key_version(
      read_metadata_version(reader),
      "consumer_group_current_member_assignment_key",
      consumer_group_current_member_assignment_key::version);
    ret.group_id = kafka::group_id(reader.read_string());
    ret.member_id = kafka::member_id(reader.read_string());
    return ret;
}

namespace {
chunked_vector<current_assignment_topic_partitions> copy_topic_partitions(
  const chunked_vector<current_assignment_topic_partitions>& partitions) {
    chunked_vector<current_assignment_topic_partitions> ret;
    ret.reserve(partitions.size());
    for (const auto& tp : partitions) {
        ret.push_back(tp.copy());
    }
    return ret;
}
} // namespace

consumer_group_current_member_assignment_value
consumer_group_current_member_assignment_value::copy() const {
    return consumer_group_current_member_assignment_value{
      .member_epoch = member_epoch,
      .previous_member_epoch = previous_member_epoch,
      .state = state,
      .assigned_partitions = copy_topic_partitions(assigned_partitions),
      .partitions_pending_revocation = copy_topic_partitions(
        partitions_pending_revocation),
    };
}

namespace {
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

    if (
      unlikely(
        buffer.size_bytes() == static_cast<size_t>(deserialized_size) + 4)) {
        vlog(kafka::klog.debug, "Unwrapping group metadata key from iobuf");
        // unwrap from iobuf
        return reflection::from_iobuf<iobuf>(std::move(buffer));
    }
    return buffer;
}

/// Keys are always Kafka wire, since their version is the record-type
/// discriminator. Values are Kafka wire for the classic records and serde for
/// the `ConsumerGroup*` ones.
template<typename KV>
group_metadata_serializer::key_value to_kv_impl(KV md) {
    group_metadata_serializer::key_value ret;
    ret.key = metadata_to_iobuf(md.key);
    if (md.value) {
        if constexpr (
          serde::is_envelope<typename decltype(md.value)::value_type>) {
            ret.value = serde::to_iobuf(std::move(*md.value));
        } else {
            ret.value = metadata_to_iobuf(*md.value);
        }
    }
    return ret;
}

template<typename KV>
KV decode_kv(model::record record) {
    KV ret;
    using key_type = decltype(ret.key);
    using value_type = typename decltype(ret.value)::value_type;
    protocol::decoder k_reader(maybe_unwrap_from_iobuf(record.release_key()));
    ret.key = key_type::decode(k_reader);
    if (record.has_value()) {
        if constexpr (serde::is_envelope<value_type>) {
            ret.value = serde::from_iobuf<value_type>(record.release_value());
        } else {
            protocol::decoder v_reader(record.release_value());
            ret.value = value_type::decode(v_reader);
        }
    }
    return ret;
}
} // namespace

fmt::iterator group_block_info::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{is_blocked: {}, revision_id: {}}}", is_blocked, revision_id);
}

fmt::iterator group_block::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{group_id: {}, info: {}}}", group_id, info);
}

group_block::group_block(kafka::group_id group_id, group_block_info info)
  : group_id(std::move(group_id))
  , info(std::move(info)) {}

group_block::group_block(model::record record)
  : group_id(protocol::decoder(record.release_key()).read_string())
  , info(
      record.is_tombstone() || record.value().empty()
        // pre-serde legacy record
        ? group_block_info{.is_blocked = record.is_tombstone(), .revision_id = model::revision_id{}}
        // serde-encoded value
        : serde::from_iobuf<group_block_info>(record.release_value())) {}

void group_block::add_to_batch_builder(storage::record_batch_builder& b) const {
    iobuf key;
    protocol::encoder ke{key};
    ke.write(group_id);

    b.add_raw_kv(std::move(key), serde::to_iobuf(info));
}

namespace group_metadata_serializer {
group_metadata_type get_metadata_type(iobuf buffer) {
    auto reader = protocol::decoder(maybe_unwrap_from_iobuf(std::move(buffer)));
    return decode_metadata_type(reader);
};

key_value to_kv(group_metadata_kv md) { return to_kv_impl(std::move(md)); }

key_value to_kv(offset_metadata_kv md) { return to_kv_impl(std::move(md)); }

group_metadata_kv decode_group_metadata(model::record record) {
    return decode_kv<group_metadata_kv>(std::move(record));
}

offset_metadata_kv decode_offset_metadata(model::record record) {
    return decode_kv<offset_metadata_kv>(std::move(record));
}

key_value to_kv(consumer_group_metadata_kv md) {
    return to_kv_impl(std::move(md));
}

key_value to_kv(consumer_group_member_metadata_kv md) {
    return to_kv_impl(std::move(md));
}

key_value to_kv(consumer_group_target_assignment_metadata_kv md) {
    return to_kv_impl(std::move(md));
}

key_value to_kv(consumer_group_target_assignment_member_kv md) {
    return to_kv_impl(std::move(md));
}

key_value to_kv(consumer_group_current_member_assignment_kv md) {
    return to_kv_impl(std::move(md));
}

consumer_group_metadata_kv decode_consumer_group_metadata(model::record r) {
    return decode_kv<consumer_group_metadata_kv>(std::move(r));
}

consumer_group_member_metadata_kv
decode_consumer_group_member_metadata(model::record r) {
    return decode_kv<consumer_group_member_metadata_kv>(std::move(r));
}

consumer_group_target_assignment_metadata_kv
decode_consumer_group_target_assignment_metadata(model::record r) {
    return decode_kv<consumer_group_target_assignment_metadata_kv>(
      std::move(r));
}

consumer_group_target_assignment_member_kv
decode_consumer_group_target_assignment_member(model::record r) {
    return decode_kv<consumer_group_target_assignment_member_kv>(std::move(r));
}

consumer_group_current_member_assignment_kv
decode_consumer_group_current_member_assignment(model::record r) {
    return decode_kv<consumer_group_current_member_assignment_kv>(std::move(r));
}
} // namespace group_metadata_serializer

fmt::iterator member_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, instance_id: {}, client_id: {}, client_host: {}, "
      "rebalance_timeout: {}, session_timeout: {}, subscription_size: {}, "
      "assignment_size: {}}}",
      id,
      instance_id,
      client_id,
      client_host,
      rebalance_timeout,
      session_timeout,
      subscription.size_bytes(),
      assignment.size_bytes());
}

fmt::iterator group_metadata_key::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{group_id: {}}}", group_id);
}

fmt::iterator group_metadata_value::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{protocol_type: {}, generation: {}, protocol: {}, leader: {}, "
      "state_timestamp: {}, member count: {}}}",
      protocol_type,
      generation,
      protocol,
      leader,
      state_timestamp,
      members.size());
}

fmt::iterator offset_metadata_key::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{group_id: {}, topic: {}, partition: {}}}",
      group_id,
      topic,
      partition);
}
fmt::iterator offset_metadata_value::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{offset: {}, leader_epoch: {}, metadata: {}, commit_timestamp: {}, "
      "expiry_timestamp: {}}}",
      offset,
      leader_epoch,
      metadata,
      commit_timestamp,
      expiry_timestamp);
}

fmt::iterator
format_to(const consumer_group_member_state& state, fmt::iterator it) {
    switch (state) {
    case consumer_group_member_state::stable:
        return fmt::format_to(it, "stable");
    case consumer_group_member_state::unrevoked_partitions:
        return fmt::format_to(it, "unrevoked_partitions");
    case consumer_group_member_state::unreleased_partitions:
        return fmt::format_to(it, "unreleased_partitions");
    case consumer_group_member_state::unknown:
        return fmt::format_to(it, "unknown");
    }
    return fmt::format_to(it, "unrecognized({})", static_cast<int>(state));
}

fmt::iterator consumer_group_metadata_key::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{group_id: {}}}", group_id);
}

fmt::iterator consumer_group_metadata_value::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{epoch: {}, metadata_hash: {}}}", epoch, metadata_hash);
}

fmt::iterator classic_protocol::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{name: {}, metadata_size: {}}}", name, metadata.size());
}

fmt::iterator classic_member_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{session_timeout: {}, supported_protocols: {}}}",
      session_timeout,
      fmt::join(supported_protocols, ", "));
}

fmt::iterator
consumer_group_member_metadata_key::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{group_id: {}, member_id: {}}}", group_id, member_id);
}

fmt::iterator
consumer_group_member_metadata_value::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{instance_id: {}, rack_id: {}, client_id: {}, client_host: {}, "
      "subscribed_topic_names: {}, subscribed_topic_regex: {}, "
      "rebalance_timeout: {}, server_assignor: {}, classic_metadata: {}}}",
      instance_id,
      rack_id,
      client_id,
      client_host,
      fmt::join(subscribed_topic_names, ", "),
      subscribed_topic_regex,
      rebalance_timeout,
      server_assignor,
      classic_metadata);
}

fmt::iterator consumer_group_target_assignment_metadata_key::format_to(
  fmt::iterator it) const {
    return fmt::format_to(it, "{{group_id: {}}}", group_id);
}

fmt::iterator consumer_group_target_assignment_metadata_value::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{assignment_epoch: {}, assignment_timestamp: {}}}",
      assignment_epoch,
      assignment_timestamp);
}

fmt::iterator
target_assignment_topic_partitions::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic_id: {}, partition_count: {}}}", topic_id, partitions.size());
}

fmt::iterator
consumer_group_target_assignment_member_key::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{group_id: {}, member_id: {}}}", group_id, member_id);
}

fmt::iterator consumer_group_target_assignment_member_value::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic_partitions: {}}}", fmt::join(topic_partitions, ", "));
}

fmt::iterator
current_assignment_topic_partitions::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{topic_id: {}, partition_count: {}, assignment_epoch_count: {}}}",
      topic_id,
      partitions.size(),
      assignment_epochs.size());
}

fmt::iterator consumer_group_current_member_assignment_key::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it, "{{group_id: {}, member_id: {}}}", group_id, member_id);
}

fmt::iterator consumer_group_current_member_assignment_value::format_to(
  fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{member_epoch: {}, previous_member_epoch: {}, state: {}, "
      "assigned_partitions: {}, partitions_pending_revocation: {}}}",
      member_epoch,
      previous_member_epoch,
      state,
      fmt::join(assigned_partitions, ", "),
      fmt::join(partitions_pending_revocation, ", "));
}

namespace group_tx {
fmt::iterator offsets_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{group_id: {}, pid: {}, tx_seq: {}, offsets: {}}}",
      group_id,
      pid,
      tx_seq,
      fmt::join(offsets, ", "));
}

fmt::iterator partition_offset::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{partition: {}, offset: {}, leader_epoch: {}, metadata: {}}}",
      tp,
      offset,
      leader_epoch,
      metadata);
}

fmt::iterator fence_metadata_v0::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{group_id: {}}}", group_id);
}

fmt::iterator fence_metadata_v1::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{group_id: {}, tx_seq: {}, tx_timeout: {} ms}}",
      group_id,
      tx_seq,
      transaction_timeout_ms.count());
}

fmt::iterator fence_metadata::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{tm_partition: {},  group_id: {}, tx_seq: {}, tx_timeout: {} ms}}",
      tm_partition,
      group_id,
      tx_seq,
      transaction_timeout_ms);
}
} // namespace group_tx
} // namespace kafka
