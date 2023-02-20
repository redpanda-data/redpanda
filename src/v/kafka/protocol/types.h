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
#include "absl/container/btree_map.h"
#include "bytes/bytes.h"
#include "utils/named_type.h"

#include <concepts>

namespace kafka {

/// Kafka API key.
using api_key = named_type<int16_t, struct kafka_api_key>;

/// Kafka API version.
using api_version = named_type<int16_t, struct kafka_api_version>;

/// Kafka group identifier.
using group_id = named_type<ss::sstring, struct kafka_group_id>;

/// Kafka group member identifier.
using member_id = named_type<ss::sstring, struct kafka_member_id>;

/// An unknown / missing member id (Kafka protocol specific)
inline const member_id unknown_member_id("");

/// Kafka group instance identifier.
using group_instance_id
  = named_type<ss::sstring, struct kafka_group_instance_id>;

using leader_epoch = named_type<int32_t, struct leader_epoch_tag>;
/**
 * Used to mark that leader epoch is not intended to be used (Kafka protocol
 * specific)
 */
inline constexpr leader_epoch invalid_leader_epoch(-1);

/// Kafka group generation identifier.
using generation_id = named_type<int32_t, struct kafka_generation_id>;

/// Kafka group protocol type.
using protocol_type = named_type<ss::sstring, struct kafka_protocol_type>;

/// Kafka group protocol name.
using protocol_name = named_type<ss::sstring, struct kafka_protocol>;

/// Kafka transactional id identifier.
using transactional_id = named_type<ss::sstring, struct kafka_transactional_id>;

/// Kafka producer id identifier.
using producer_id = named_type<int64_t, struct kafka_producer_id>;

enum class config_resource_type : int8_t {
    topic = 2,
    broker = 4,
    broker_logger = 8,
};

enum class coordinator_type : int8_t {
    group = 0,
    transaction = 1,
};

/*
 * From where config values are sourced. For instance, a value might exist
 * because it is a default or because it was an override at the broker level.
 *
 * As our configuration becomes more sophisticated these should be taken into
 * account. Right now we only report a couple basic topic configs.
 */
enum class describe_configs_source : int8_t {
    topic = 1,
    static_broker_config = 4,
    default_config = 5,
    // DYNAMIC_BROKER_CONFIG((byte) 2),
    // DYNAMIC_DEFAULT_BROKER_CONFIG((byte) 3),
    // DYNAMIC_BROKER_LOGGER_CONFIG((byte) 6);
};

/**
 * Immutable UUID, represents a 128 bit value of the variant 2 (Leach-Salz)
 * version 4 UUID type. Conversions to/from string will expect or perform a b64
 * encoding/decoding
 */
class uuid {
public:
    static constexpr auto length = 16;
    using underlying_t = std::array<uint8_t, length>;

    uuid() = default;

    static uuid from_string(std::string_view encoded);

    explicit uuid(const underlying_t& uuid)
      : _uuid(uuid) {}

    bytes_view view() const { return {_uuid.data(), _uuid.size()}; }

    ss::sstring to_string() const;

    friend bool operator==(const uuid&, const uuid&) = default;

    friend std::ostream& operator<<(std::ostream& os, const uuid& u);

private:
    underlying_t _uuid{};
};

/// Types for tags and tagged fields
/// These structures are used for encoding / decoding flexible requests
/// Additional metadata is allowed to be stored in this dynamic structure
using tag_id = named_type<uint32_t, struct tag_id_type>;

using tagged_fields
  = named_type<absl::btree_map<tag_id, bytes>, struct tagged_fields_type>;

/// Used to signify if a kafka request will never be interpreted as flexible.
/// Consumed by our generator and flexible method helpers.
///
/// The only request that is never flexible is sasl_handshake_request - 17.
/// Older versions of schemas may also contain values of 'none' that map to -1
static constexpr api_version never_flexible = api_version(-1);

template<typename T>
concept KafkaApi = requires(T request) {
    { T::name } -> std::convertible_to<const char*>;
    { T::key } -> std::convertible_to<const api_key&>;
    { T::min_flexible } -> std::convertible_to<const api_version&>;
};

/*
 * Data type of the configuration entry.
 */
enum class describe_configs_type : int8_t {
    unknown = 0,
    boolean = 1,
    string = 2,
    int_type = 3,
    short_type = 4,
    long_type = 5,
    double_type = 6,
    list = 7,
    class_type = 8,
    password = 9
};

std::ostream& operator<<(std::ostream&, describe_configs_type t);

inline const kafka::protocol_type consumer_group_protocol_type("consumer");

} // namespace kafka
