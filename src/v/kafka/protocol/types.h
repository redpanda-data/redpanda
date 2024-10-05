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

#include "bytes/bytes.h"
#include "model/fundamental.h"
#include "utils/named_type.h"

#include <absl/container/btree_map.h>
#include <boost/numeric/conversion/cast.hpp>

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
inline constexpr api_version never_flexible = api_version(-1);

template<typename T>
concept KafkaApi = requires(T request) {
    { T::name } -> std::convertible_to<const char*>;
    { T::key } -> std::convertible_to<const api_key&>;
    { T::min_flexible } -> std::convertible_to<const api_version&>;
};

// TODO: use std::float64_t from <stdfloat> when clang has it (not in 18)
/// float64 Kafka protocol primitive type
using float64_t = double;
static_assert(
  sizeof(float64_t) == 8, "Kafka float64 type should be 8 bytes long");

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

/*
 * Data type for the match type of describe client quotas requests.
 * DO NOT CHANGE the values of the enum variants, as they correspond to the set
 * of match type values defined in the describe_client_quotas_request.json
 * schemata.
 */
enum class describe_client_quotas_match_type : int8_t {
    /// Return only values matching the specified match field
    exact_name = 0,
    /// Return only the default value (ignoring the match field)
    default_name = 1,
    /// Return all specified values, that includes both the default value and
    /// non-default values for the given entity type (ignoring the match field)
    any_specified_name = 2,
};

std::ostream& operator<<(std::ostream&, describe_client_quotas_match_type t);

/*
 * The names of group states.
 */
inline constexpr std::string_view group_state_name_empty = "Empty";
inline constexpr std::string_view group_state_name_preparing_rebalance
  = "PreparingRebalance";
inline constexpr std::string_view group_state_name_completing_rebalance
  = "CompletingRebalance";
inline constexpr std::string_view group_state_name_stable = "Stable";
inline constexpr std::string_view group_state_name_dead = "Dead";

/// An unknown / missing generation id (Kafka protocol specific)
inline constexpr generation_id unknown_generation_id(-1);

std::ostream& operator<<(std::ostream& os, coordinator_type t);

std::ostream& operator<<(std::ostream& os, config_resource_type t);

std::ostream& operator<<(std::ostream& os, describe_configs_source s);

/*
 * TODO this can be moved out of the protocol library and into the server if the
 * batch encoding utility in protocol/wire.h can remove the dependency on this,
 * for example by having the caller in the server perform this conversion.
 */
inline kafka::leader_epoch leader_epoch_from_term(model::term_id term) {
    try {
        return kafka::leader_epoch(
          boost::numeric_cast<kafka::leader_epoch::type>(term()));
    } catch (const boost::bad_numeric_cast&) {
        return kafka::invalid_leader_epoch;
    }
}

/// Kafka API request correlation.
using correlation_id = named_type<int32_t, struct kafka_correlation_type>;

using client_id = named_type<ss::sstring, struct kafka_client_id_type>;
using client_host = named_type<ss::sstring, struct kafka_client_host_type>;

using fetch_session_id = named_type<int32_t, struct session_id_tag>;
using fetch_session_epoch = named_type<int32_t, struct session_epoch_tag>;

// Unknown/missing/not initialized session (Kafka protocol specific)
inline constexpr fetch_session_id invalid_fetch_session_id(0);

/**
 * Used by the client to start new fetch session. (Kafka protocol specific)
 */
inline constexpr fetch_session_epoch initial_fetch_session_epoch(0);

/**
 * Used by the client to close existing fetch session. (Kafka protocol specific)
 */
inline constexpr fetch_session_epoch final_fetch_session_epoch(-1);

enum class config_resource_operation : int8_t {
    set = 0,
    remove = 1,
    append = 2,
    subtract = 3,
};

std::ostream& operator<<(std::ostream& os, config_resource_operation);

} // namespace kafka
