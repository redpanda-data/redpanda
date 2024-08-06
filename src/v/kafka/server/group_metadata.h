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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "kafka/protocol/wire.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "serde/rw/rw.h"
#include "utils/named_type.h"

#include <seastar/util/noncopyable_function.hh>

namespace kafka {

enum group_metadata_type {
    offset_commit,
    group_metadata,
    noop,
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
    friend std::ostream& operator<<(std::ostream&, const member_state&);

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

    friend std::ostream& operator<<(std::ostream&, const group_metadata_key&);
    friend bool operator==(const group_metadata_key&, const group_metadata_key&)
      = default;
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
    std::vector<member_state> members;

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

    friend std::ostream& operator<<(std::ostream&, const group_metadata_value&);
    friend bool
    operator==(const group_metadata_value&, const group_metadata_value&)
      = default;

    static group_metadata_value decode(protocol::decoder&);
    static void encode(protocol::encoder&, const group_metadata_value&);
};

struct offset_metadata_key {
    static constexpr group_metadata_version version{1};
    kafka::group_id group_id;
    model::topic topic;
    model::partition_id partition;

    friend std::ostream& operator<<(std::ostream&, const offset_metadata_key&);
    friend bool
    operator==(const offset_metadata_key&, const offset_metadata_key&)
      = default;
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

    friend std::ostream&
    operator<<(std::ostream&, const offset_metadata_value&);
    friend bool
    operator==(const offset_metadata_value&, const offset_metadata_value&)
      = default;
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

class group_metadata_serializer {
public:
    struct key_value {
        iobuf key;
        std::optional<iobuf> value;
    };
    struct impl {
        virtual group_metadata_type get_metadata_type(iobuf) = 0;
        virtual key_value to_kv(group_metadata_kv) = 0;
        virtual key_value to_kv(offset_metadata_kv) = 0;

        virtual group_metadata_kv decode_group_metadata(model::record) = 0;
        virtual offset_metadata_kv decode_offset_metadata(model::record) = 0;
        virtual ~impl() = default;
    };

    explicit group_metadata_serializer(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    group_metadata_type get_metadata_type(iobuf buf) {
        return _impl->get_metadata_type(std::move(buf));
    };

    key_value to_kv(group_metadata_kv md) {
        return _impl->to_kv(std::move(md));
    }
    key_value to_kv(offset_metadata_kv md) {
        return _impl->to_kv(std::move(md));
    }

    group_metadata_kv decode_group_metadata(model::record record) {
        return _impl->decode_group_metadata(std::move(record));
    }

    offset_metadata_kv decode_offset_metadata(model::record record) {
        return _impl->decode_offset_metadata(std::move(record));
    }

private:
    std::unique_ptr<impl> _impl;
};

group_metadata_serializer make_consumer_offsets_serializer();

using group_metadata_serializer_factory
  = ss::noncopyable_function<group_metadata_serializer()>;
namespace group_tx {
struct fence_metadata_v0 {
    kafka::group_id group_id;
    friend std::ostream& operator<<(std::ostream&, const fence_metadata_v0&);
};

struct fence_metadata_v1 {
    kafka::group_id group_id;
    model::tx_seq tx_seq;
    model::timeout_clock::duration transaction_timeout_ms;
    friend std::ostream& operator<<(std::ostream&, const fence_metadata_v1&);
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
    friend std::ostream& operator<<(std::ostream&, const fence_metadata&);
};
/**
 * Single partition committed offset
 */
struct partition_offset {
    model::topic_partition tp;
    model::offset offset;
    int32_t leader_epoch;
    std::optional<ss::sstring> metadata;
    friend std::ostream& operator<<(std::ostream&, const partition_offset&);
};
/**
 * Consumer offsets commited as a part of transaction
 */
struct offsets_metadata {
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    std::vector<partition_offset> offsets;
    friend std::ostream& operator<<(std::ostream&, const offsets_metadata&);
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
