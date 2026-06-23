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
#include "base/format_to.h"
#include "bytes/bytes.h"
#include "model/fundamental.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/numeric/conversion/cast.hpp>

#include <concepts>

namespace kafka {

/// Kafka API key.
using api_key = named_type<int16_t, struct kafka_api_key>;

/// Base of the reserved key range for Redpanda-specific Kafka APIs. Keys at or
/// above this value are not part of the standard Kafka protocol and are
/// dispatched/metered through the reserved region of api_key_indexed_array.
inline constexpr api_key redpanda_api_key_base{15000};

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

inline fmt::iterator format_to(describe_configs_type t, fmt::iterator out) {
    switch (t) {
    case describe_configs_type::unknown:
        return fmt::format_to(out, "{{unknown}}");
    case describe_configs_type::boolean:
        return fmt::format_to(out, "{{boolean}}");
    case describe_configs_type::string:
        return fmt::format_to(out, "{{string}}");
    case describe_configs_type::int_type:
        return fmt::format_to(out, "{{int}}");
    case describe_configs_type::short_type:
        return fmt::format_to(out, "{{short}}");
    case describe_configs_type::long_type:
        return fmt::format_to(out, "{{long}}");
    case describe_configs_type::double_type:
        return fmt::format_to(out, "{{double}}");
    case describe_configs_type::list:
        return fmt::format_to(out, "{{list}}");
    case describe_configs_type::class_type:
        return fmt::format_to(out, "{{class}}");
    case describe_configs_type::password:
        return fmt::format_to(out, "{{password}}");
    }
    return fmt::format_to(out, "{{unsupported type}}");
}

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

inline fmt::iterator
format_to(describe_client_quotas_match_type t, fmt::iterator out) {
    switch (t) {
    case describe_client_quotas_match_type::exact_name:
        return fmt::format_to(out, "{{exact_name}}");
    case describe_client_quotas_match_type::default_name:
        return fmt::format_to(out, "{{default_name}}");
    case describe_client_quotas_match_type::any_specified_name:
        return fmt::format_to(out, "{{any_specified_name}}");
    }
    return fmt::format_to(out, "{{unsupported type}}");
}

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

inline fmt::iterator format_to(coordinator_type t, fmt::iterator out) {
    switch (t) {
    case coordinator_type::group:
        return fmt::format_to(out, "{{group}}");
    case coordinator_type::transaction:
        return fmt::format_to(out, "{{transaction}}");
    }
    return fmt::format_to(out, "{{unknown type}}");
}

inline fmt::iterator format_to(config_resource_type t, fmt::iterator out) {
    switch (t) {
    case config_resource_type::topic:
        return fmt::format_to(out, "{{topic}}");
    case config_resource_type::broker:
        [[fallthrough]];
    case config_resource_type::broker_logger:
        break;
    }
    return fmt::format_to(out, "{{unknown type}}");
}

inline fmt::iterator format_to(describe_configs_source s, fmt::iterator out) {
    switch (s) {
    case describe_configs_source::topic:
        return fmt::format_to(out, "{{topic}}");
    case describe_configs_source::static_broker_config:
        return fmt::format_to(out, "{{static_broker_config}}");
    case describe_configs_source::default_config:
        return fmt::format_to(out, "{{default_config}}");
    }
    return fmt::format_to(out, "{{unknown type}}");
}

/*
 * TODO this can be moved out of the protocol library and into the server if the
 * batch encoding utility in protocol/wire.h can remove the dependency on this,
 * for example by having the caller in the server perform this conversion.
 */
inline kafka::leader_epoch
leader_epoch_from_term(std::optional<model::term_id> term) {
    return term
      .and_then([](auto&& term) {
          try {
              return std::make_optional<kafka::leader_epoch>(
                boost::numeric_cast<kafka::leader_epoch::type>(term()));
          } catch (const boost::bad_numeric_cast&) {
              return std::optional<kafka::leader_epoch>{};
          }
      })
      .value_or(kafka::invalid_leader_epoch);
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

inline fmt::iterator format_to(config_resource_operation t, fmt::iterator out) {
    switch (t) {
    case config_resource_operation::set:
        return fmt::format_to(out, "set");
    case config_resource_operation::append:
        return fmt::format_to(out, "append");
    case config_resource_operation::remove:
        return fmt::format_to(out, "remove");
    case config_resource_operation::subtract:
        return fmt::format_to(out, "subtract");
    }
    return fmt::format_to(out, "unknown type");
}

using scram_user_name = named_type<ss::sstring, struct scram_user_name_tag>;

enum class scram_mechanism : int8_t {
    unknown = 0,
    scram_sha_256 = 1,
    scram_sha_512 = 2,
};

inline fmt::iterator format_to(scram_mechanism m, fmt::iterator out) {
    switch (m) {
    case scram_mechanism::scram_sha_256:
        return fmt::format_to(out, "SCRAM-SHA-256");
    case scram_mechanism::scram_sha_512:
        return fmt::format_to(out, "SCRAM-SHA-512");
    case scram_mechanism::unknown:
        return fmt::format_to(out, "unknown");
    }
    return fmt::format_to(out, "unsupported type");
}

using topic_authorized_operations
  = named_type<int32_t, struct topic_authorized_operations_tag>;

inline constexpr topic_authorized_operations
  topic_authorized_operations_not_set(-2147483648);

using cluster_authorized_operations
  = named_type<int32_t, struct cluster_authorized_operations_tag>;

inline constexpr cluster_authorized_operations
  cluster_authorized_operations_not_set(-2147483648);

} // namespace kafka
