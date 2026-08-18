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
#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/types.h"
#include "kafka/protocol/wire.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "serde/rw/rw.h"
#include "utils/named_type.h"

#include <seastar/util/noncopyable_function.hh>

namespace kafka {

/// `consumer_group_*` are the KIP-848 protocol's records; the rest predate it.
enum group_metadata_type {
    offset_commit,
    group_metadata,
    noop,
    consumer_group_metadata,
    consumer_group_member_metadata,
    consumer_group_target_assignment_metadata,
    consumer_group_target_assignment_member,
    consumer_group_current_member_assignment,
};

group_metadata_type decode_metadata_type(protocol::decoder& key_reader);

using group_metadata_version = named_type<int16_t, struct md_versio_tag>;

struct member_state {
    static constexpr group_metadata_version version{3};
    kafka::member_id id;
    std::optional<group_instance_id> instance_id;
    kafka::client_id client_id;
    kafka::client_host client_host;
    std::chrono::milliseconds rebalance_timeout;
    std::chrono::milliseconds session_timeout;
    iobuf subscription;
    iobuf assignment;

    member_state copy() const {
        return member_state{
          .id = id,
          .instance_id = instance_id,
          .client_id = client_id,
          .client_host = client_host,
          .rebalance_timeout = rebalance_timeout,
          .session_timeout = session_timeout,
          .subscription = subscription.copy(),
          .assignment = assignment.copy(),
        };
    }
    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(const member_state&, const member_state&) = default;

    static member_state decode(protocol::decoder&);
    static void encode(protocol::encoder&, const member_state&);
};

/**
 * Group log metadata consistent with Kafka internal format
 */
struct group_metadata_key {
    static constexpr group_metadata_version version{2};
    kafka::group_id group_id;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool
    operator==(const group_metadata_key&, const group_metadata_key&) = default;
    static group_metadata_key decode(protocol::decoder&);
    static void encode(protocol::encoder&, const group_metadata_key&);
};

/**
 * Group log metadata consistent with Kafka internal format
 */
struct group_metadata_value {
    static constexpr group_metadata_version version{3};
    kafka::protocol_type protocol_type;
    kafka::generation_id generation;
    std::optional<kafka::protocol_name> protocol;
    std::optional<kafka::member_id> leader;
    model::timestamp state_timestamp{-1};
    chunked_vector<member_state> members;

    group_metadata_value copy() const {
        group_metadata_value ret{
          .protocol_type = protocol_type,
          .generation = generation,
          .protocol = protocol,
          .leader = leader,
          .state_timestamp = state_timestamp,
        };
        ret.members.reserve(members.size());
        std::transform(
          members.begin(),
          members.end(),
          std::back_inserter(ret.members),
          [](const member_state& ms) { return ms.copy(); });

        return ret;
    }

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const group_metadata_value&, const group_metadata_value&) = default;

    static group_metadata_value decode(protocol::decoder&);
    static void encode(protocol::encoder&, const group_metadata_value&);
};

struct offset_metadata_key {
    static constexpr group_metadata_version version{1};
    kafka::group_id group_id;
    model::topic topic;
    model::partition_id partition;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const offset_metadata_key&, const offset_metadata_key&) = default;
    static offset_metadata_key decode(protocol::decoder&);
    static void encode(protocol::encoder&, const offset_metadata_key&);
};

/**
 * The value type for offset commit records, consistent with Kafka format
 */
struct offset_metadata_value {
    static constexpr group_metadata_version latest_version{3};
    model::offset offset;
    // present only in version >= 3
    kafka::leader_epoch leader_epoch = invalid_leader_epoch;
    ss::sstring metadata;
    model::timestamp commit_timestamp;
    // present only in version 1
    model::timestamp expiry_timestamp{-1};

    /*
     * this field is not written, and is only meaningful when filled in by the
     * the consumer offset recovery process. see group::offset_metadata for more
     * information on how it is used.
     */
    bool non_reclaimable{true};

    /// A commit made with no retention deadline leaves the expiry missing on
    /// the wire, which is what an absent deadline decodes back to.
    std::optional<model::timestamp> expiry() const {
        if (expiry_timestamp.is_missing()) {
            return std::nullopt;
        }
        return expiry_timestamp;
    }

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const offset_metadata_value&, const offset_metadata_value&) = default;
    static offset_metadata_value decode(protocol::decoder&);
    static void encode(protocol::encoder&, const offset_metadata_value&);
};

struct offset_metadata_kv {
    offset_metadata_key key;
    std::optional<offset_metadata_value> value;
};

struct group_metadata_kv {
    group_metadata_key key;
    std::optional<group_metadata_value> value;

    group_metadata_kv copy() const;
};

inline group_metadata_version read_metadata_version(protocol::decoder& reader) {
    return group_metadata_version{reader.read_int16()};
}

/*
 * The ConsumerGroup* records, lifted from Kafka's coordinator record schemas.
 * They share __consumer_offsets with the classic records above and use the same
 * key-version discriminator, but are framed differently. A key is prefixed with
 * its record type and is not flexible. A value is prefixed with its own version
 * and is flexible, so it carries compact strings and arrays plus a trailing
 * tagged-fields section. Each value keeps `unknown_tags` so tags written by a
 * newer version survive a round-trip.
 */

/// A member's state in the reconciliation FSM, as persisted in
/// `consumer_group_current_member_assignment_value::state`. Byte values are
/// Kafka's (org.apache.kafka.coordinator.group.modern.MemberState). An
/// unrecognized byte decodes to itself rather than being rejected.
enum class consumer_group_member_state : int8_t {
    stable = 0,
    unrevoked_partitions = 1,
    unreleased_partitions = 2,
    unknown = 127,
};

fmt::iterator format_to(const consumer_group_member_state&, fmt::iterator);

struct consumer_group_metadata_key {
    static constexpr group_metadata_version version{3};
    kafka::group_id group_id;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_metadata_key&,
      const consumer_group_metadata_key&) = default;
    static consumer_group_metadata_key decode(protocol::decoder&);
    static void encode(protocol::encoder&, const consumer_group_metadata_key&);
};

/// The group epoch, and the hash that detects subscription-metadata changes.
struct consumer_group_metadata_value {
    static constexpr group_metadata_version version{0};
    int32_t epoch{0};
    /// Hash over the subscribed topics' metadata, which detects subscription
    /// changes. Tagged field 0; not written when 0.
    int64_t metadata_hash{0};
    tagged_fields unknown_tags;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_metadata_value&,
      const consumer_group_metadata_value&) = default;
    static consumer_group_metadata_value decode(protocol::decoder&);
    static void
    encode(protocol::encoder&, const consumer_group_metadata_value&);
};

/// One protocol a classic-protocol member supports. Not written until
/// classic-to-consumer migration exists.
struct classic_protocol {
    kafka::protocol_name name;
    bytes metadata;
    tagged_fields unknown_tags;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool
    operator==(const classic_protocol&, const classic_protocol&) = default;
    static classic_protocol decode(protocol::decoder&);
    static void encode(protocol::encoder&, const classic_protocol&);
};

/// A classic-protocol member's session timeout and supported protocols. Absent
/// for a consumer-protocol member.
struct classic_member_metadata {
    std::chrono::milliseconds session_timeout{};
    chunked_vector<classic_protocol> supported_protocols;
    tagged_fields unknown_tags;

    classic_member_metadata copy() const;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const classic_member_metadata&, const classic_member_metadata&) = default;
    static classic_member_metadata decode(protocol::decoder&);
    static void encode(protocol::encoder&, const classic_member_metadata&);
};

struct consumer_group_member_metadata_key {
    static constexpr group_metadata_version version{5};
    kafka::group_id group_id;
    kafka::member_id member_id;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_member_metadata_key&,
      const consumer_group_member_metadata_key&) = default;
    static consumer_group_member_metadata_key decode(protocol::decoder&);
    static void
    encode(protocol::encoder&, const consumer_group_member_metadata_key&);
};

/// A member's subscription, rack, and chosen assignor.
struct consumer_group_member_metadata_value {
    static constexpr group_metadata_version version{0};
    std::optional<kafka::group_instance_id> instance_id;
    std::optional<model::rack_id> rack_id;
    kafka::client_id client_id;
    kafka::client_host client_host;
    chunked_vector<model::topic> subscribed_topic_names;
    std::optional<ss::sstring> subscribed_topic_regex;
    std::chrono::milliseconds rebalance_timeout{-1};
    std::optional<ss::sstring> server_assignor;
    /// Tagged field 0. Null is written explicitly and the default is omitted,
    /// so the default here must be present for an omitted tag to round-trip.
    std::optional<classic_member_metadata> classic_metadata{std::in_place};
    tagged_fields unknown_tags;

    consumer_group_member_metadata_value copy() const;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_member_metadata_value&,
      const consumer_group_member_metadata_value&) = default;
    static consumer_group_member_metadata_value decode(protocol::decoder&);
    static void
    encode(protocol::encoder&, const consumer_group_member_metadata_value&);
};

struct consumer_group_target_assignment_metadata_key {
    static constexpr group_metadata_version version{6};
    kafka::group_id group_id;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_target_assignment_metadata_key&,
      const consumer_group_target_assignment_metadata_key&) = default;
    static consumer_group_target_assignment_metadata_key
    decode(protocol::decoder&);
    static void encode(
      protocol::encoder&, const consumer_group_target_assignment_metadata_key&);
};

/// The group epoch the current target assignment was computed at.
struct consumer_group_target_assignment_metadata_value {
    static constexpr group_metadata_version version{0};
    int32_t assignment_epoch{0};
    /// Tagged field 0; not written when 0.
    int64_t assignment_timestamp{0};
    tagged_fields unknown_tags;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_target_assignment_metadata_value&,
      const consumer_group_target_assignment_metadata_value&) = default;
    static consumer_group_target_assignment_metadata_value
    decode(protocol::decoder&);
    static void encode(
      protocol::encoder&,
      const consumer_group_target_assignment_metadata_value&);
};

/// One topic's share of a member's target assignment.
struct target_assignment_topic_partitions {
    model::topic_id topic_id;
    chunked_vector<model::partition_id> partitions;
    tagged_fields unknown_tags;

    target_assignment_topic_partitions copy() const;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const target_assignment_topic_partitions&,
      const target_assignment_topic_partitions&) = default;
    static target_assignment_topic_partitions decode(protocol::decoder&);
    static void
    encode(protocol::encoder&, const target_assignment_topic_partitions&);
};

struct consumer_group_target_assignment_member_key {
    static constexpr group_metadata_version version{7};
    kafka::group_id group_id;
    kafka::member_id member_id;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_target_assignment_member_key&,
      const consumer_group_target_assignment_member_key&) = default;
    static consumer_group_target_assignment_member_key
    decode(protocol::decoder&);
    static void encode(
      protocol::encoder&, const consumer_group_target_assignment_member_key&);
};

/// The partitions the assignor wants a member to own.
struct consumer_group_target_assignment_member_value {
    static constexpr group_metadata_version version{0};
    chunked_vector<target_assignment_topic_partitions> topic_partitions;
    tagged_fields unknown_tags;

    consumer_group_target_assignment_member_value copy() const;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_target_assignment_member_value&,
      const consumer_group_target_assignment_member_value&) = default;
    static consumer_group_target_assignment_member_value
    decode(protocol::decoder&);
    static void encode(
      protocol::encoder&, const consumer_group_target_assignment_member_value&);
};

/// One topic's share of a member's current assignment. `assignment_epochs` is
/// parallel to `partitions`, holding the epoch each was assigned at, which
/// fences a stale offset commit.
struct current_assignment_topic_partitions {
    model::topic_id topic_id;
    chunked_vector<model::partition_id> partitions;
    /// Tagged field 0. Null is written explicitly and empty is omitted, so the
    /// default here must be present for an omitted tag to round-trip.
    std::optional<chunked_vector<int32_t>> assignment_epochs{std::in_place};
    tagged_fields unknown_tags;

    current_assignment_topic_partitions copy() const;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const current_assignment_topic_partitions&,
      const current_assignment_topic_partitions&) = default;
    static current_assignment_topic_partitions decode(protocol::decoder&);
    static void
    encode(protocol::encoder&, const current_assignment_topic_partitions&);
};

struct consumer_group_current_member_assignment_key {
    static constexpr group_metadata_version version{8};
    kafka::group_id group_id;
    kafka::member_id member_id;

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_current_member_assignment_key&,
      const consumer_group_current_member_assignment_key&) = default;
    static consumer_group_current_member_assignment_key
    decode(protocol::decoder&);
    static void encode(
      protocol::encoder&, const consumer_group_current_member_assignment_key&);
};

/// How far a member has converged on its target, and what it still owes. Its
/// epoch reaches the assignment epoch only once it owes nothing.
struct consumer_group_current_member_assignment_value {
    static constexpr group_metadata_version version{0};
    int32_t member_epoch{0};
    /// The epoch to accept if the member never saw the last bump.
    int32_t previous_member_epoch{0};
    consumer_group_member_state state{consumer_group_member_state::unknown};
    chunked_vector<current_assignment_topic_partitions> assigned_partitions;
    chunked_vector<current_assignment_topic_partitions>
      partitions_pending_revocation;
    tagged_fields unknown_tags;

    consumer_group_current_member_assignment_value copy() const;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(
      const consumer_group_current_member_assignment_value&,
      const consumer_group_current_member_assignment_value&) = default;
    static consumer_group_current_member_assignment_value
    decode(protocol::decoder&);
    static void encode(
      protocol::encoder&,
      const consumer_group_current_member_assignment_value&);
};

/*
 * A record as it sits on the log, for each of the types above. No value means a
 * tombstone. `group_metadata_serializer` converts between these and
 * `model::record`.
 */
struct consumer_group_metadata_kv {
    consumer_group_metadata_key key;
    std::optional<consumer_group_metadata_value> value;
};

struct consumer_group_member_metadata_kv {
    consumer_group_member_metadata_key key;
    std::optional<consumer_group_member_metadata_value> value;

    consumer_group_member_metadata_kv copy() const;
};

struct consumer_group_target_assignment_metadata_kv {
    consumer_group_target_assignment_metadata_key key;
    std::optional<consumer_group_target_assignment_metadata_value> value;
};

struct consumer_group_target_assignment_member_kv {
    consumer_group_target_assignment_member_key key;
    std::optional<consumer_group_target_assignment_member_value> value;

    consumer_group_target_assignment_member_kv copy() const;
};

struct consumer_group_current_member_assignment_kv {
    consumer_group_current_member_assignment_key key;
    std::optional<consumer_group_current_member_assignment_value> value;

    consumer_group_current_member_assignment_kv copy() const;
};

struct group_block_info
  : serde::
      envelope<group_block_info, serde::version<0>, serde::compat_version<0>> {
    bool is_blocked;
    // model::revision_id{} indicates that the request is coming from an older
    // node and should be applied unconditionally
    model::revision_id revision_id;

    auto serde_fields() { return std::tie(is_blocked, revision_id); }
    friend bool
    operator==(const group_block_info&, const group_block_info&) = default;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct group_block {
    kafka::group_id group_id;
    group_block_info info;

    group_block(kafka::group_id group_id, group_block_info info);
    explicit group_block(model::record record);
    void add_to_batch_builder(storage::record_batch_builder&) const;

    fmt::iterator format_to(fmt::iterator it) const;
};
namespace group_metadata_serializer {
struct key_value {
    iobuf key;
    std::optional<iobuf> value;
};
group_metadata_type get_metadata_type(iobuf buf);
key_value to_kv(group_metadata_kv md);
key_value to_kv(offset_metadata_kv md);
group_metadata_kv decode_group_metadata(model::record record);
offset_metadata_kv decode_offset_metadata(model::record record);

key_value to_kv(consumer_group_metadata_kv md);
key_value to_kv(consumer_group_member_metadata_kv md);
key_value to_kv(consumer_group_target_assignment_metadata_kv md);
key_value to_kv(consumer_group_target_assignment_member_kv md);
key_value to_kv(consumer_group_current_member_assignment_kv md);

consumer_group_metadata_kv decode_consumer_group_metadata(model::record);
consumer_group_member_metadata_kv
  decode_consumer_group_member_metadata(model::record);
consumer_group_target_assignment_metadata_kv
  decode_consumer_group_target_assignment_metadata(model::record);
consumer_group_target_assignment_member_kv
  decode_consumer_group_target_assignment_member(model::record);
consumer_group_current_member_assignment_kv
  decode_consumer_group_current_member_assignment(model::record);
}; // namespace group_metadata_serializer

namespace group_tx {

struct fence_metadata_v0 {
    kafka::group_id group_id;
    fmt::iterator format_to(fmt::iterator it) const;
};

struct fence_metadata_v1 {
    kafka::group_id group_id;
    model::tx_seq tx_seq;
    model::timeout_clock::duration transaction_timeout_ms;
    fmt::iterator format_to(fmt::iterator it) const;
};
/**
 * Fence is set by the transaction manager when consumer adds an offset to
 * transaction.
 */
struct fence_metadata {
    kafka::group_id group_id;
    model::tx_seq tx_seq;
    model::timeout_clock::duration transaction_timeout_ms;
    model::partition_id tm_partition;
    fmt::iterator format_to(fmt::iterator it) const;
};
/**
 * Single partition committed offset
 */
struct partition_offset {
    model::topic_partition tp;
    model::offset offset;
    int32_t leader_epoch;
    std::optional<ss::sstring> metadata;
    fmt::iterator format_to(fmt::iterator it) const;
};
/**
 * Consumer offsets commited as a part of transaction
 */
struct offsets_metadata {
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    std::vector<partition_offset> offsets;
    fmt::iterator format_to(fmt::iterator it) const;
};

/**
 * Content of transaction commit batch
 */
struct commit_metadata {
    kafka::group_id group_id;
};
/**
 * Content of transaction abort batch
 */
struct abort_metadata {
    kafka::group_id group_id;
    model::tx_seq tx_seq;
};
} // namespace group_tx
} // namespace kafka

namespace std {
template<>
struct hash<kafka::offset_metadata_key> {
    size_t operator()(const kafka::offset_metadata_key& key) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(key.group_id));
        boost::hash_combine(h, hash<ss::sstring>()(key.topic));
        boost::hash_combine(h, hash<model::partition_id>()(key.partition));
        return h;
    }
};

} // namespace std
