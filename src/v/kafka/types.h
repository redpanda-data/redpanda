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
#include "reflection/adl.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace kafka {

/// Kafka API request correlation.
using correlation_id = named_type<int32_t, struct kafka_correlation_type>;

/// Kafka API key.
using api_key = named_type<int16_t, struct kafka_requests_api_key>;

/// Kafka API version.
using api_version = named_type<int16_t, struct kafka_requests_api_version>;

/// Kafka group identifier.
using group_id = named_type<ss::sstring, struct kafka_group_id>;

/// Kafka transactional id identifier.
using transactional_id = named_type<ss::sstring, struct kafka_transactional_id>;

/// Kafka producer id identifier.
using producer_id = named_type<int64_t, struct kafka_producer_id>;

/// Kafka group member identifier.
using member_id = named_type<ss::sstring, struct kafka_member_id>;

/// Kafka group generation identifier.
using generation_id = named_type<int32_t, struct kafka_generation_id>;

/// Kafka group instance identifier.
using group_instance_id
  = named_type<ss::sstring, struct kafka_group_instance_id>;

/// Kafka group protocol type.
using protocol_type = named_type<ss::sstring, struct kafka_protocol_type>;

/// Kafka group protocol name.
using protocol_name = named_type<ss::sstring, struct kafka_protocol>;

using client_id = named_type<ss::sstring, struct kafka_client_id_type>;
using client_host = named_type<ss::sstring, struct kafka_client_host_type>;

/// An unknown / missing member id (Kafka protocol specific)
static inline const member_id unknown_member_id("");

/// An unknown / missing generation id (Kafka protocol specific)
static inline const generation_id unknown_generation_id(-1);

using fetch_session_id = named_type<int32_t, struct session_id_tag>;
using fetch_session_epoch = named_type<int32_t, struct session_epoch_tag>;

// Unknown/missing/not initialized session (Kafka protocol specific)
static constexpr fetch_session_id invalid_fetch_session_id(0);
/**
 * Used by the client to start new fetch session. (Kafka protocol specific)
 */
static constexpr fetch_session_epoch initial_fetch_session_epoch(0);
/**
 * Used by the client to close existing fetch session. (Kafka protocol specific)
 */
static constexpr fetch_session_epoch final_fetch_session_epoch(-1);

enum class coordinator_type : int8_t {
    group = 0,
    transaction = 1,
};

using leader_epoch = named_type<int32_t, struct leader_epoch_tag>;
/**
 * Used to mark that leader epoch is not intended to be used (Kafka protocol
 * specific)
 */
static constexpr leader_epoch invalid_leader_epoch(-1);

// TODO Ben: Why is this an undefined reference for pandaproxy when defined in
// kafka/requests.cc
inline std::ostream& operator<<(std::ostream& os, coordinator_type t) {
    switch (t) {
    case coordinator_type::group:
        return os << "{group}";
    case coordinator_type::transaction:
        return os << "{transaction}";
    };
    return os << "{unknown type}";
}

enum class config_resource_type : int8_t {
    topic = 2,
    broker = 4,
    broker_logger = 8,
};

std::ostream& operator<<(std::ostream& os, config_resource_type);

enum class config_resource_operation : int8_t {
    set = 0,
    remove = 1,
    append = 2,
    subtract = 3,
};

std::ostream& operator<<(std::ostream& os, config_resource_operation);

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

std::ostream& operator<<(std::ostream& os, describe_configs_source);

/// \brief A protocol configuration supported by a group member.
///
/// NOTE: for efficiency this structure is shared between kafka request
/// processing and the rest of group membership. if it changes, make sure that
/// request processing is still correct.
struct member_protocol {
    protocol_name name;
    bytes metadata;

    bool operator==(const member_protocol& o) const {
        return name == o.name && metadata == o.metadata;
    }

    friend std::ostream& operator<<(std::ostream&, const member_protocol&);
};

using assignments_type = std::unordered_map<member_id, bytes>;

inline kafka::leader_epoch leader_epoch_from_term(model::term_id term) {
    try {
        return kafka::leader_epoch(
          boost::numeric_cast<kafka::leader_epoch::type>(term()));
    } catch (boost::bad_numeric_cast&) {
        return kafka::invalid_leader_epoch;
    }
}
} // namespace kafka

/*
 * TODO: bytes is on its way out in favor of iobuf. however its still lingering
 * around in some types that we'd like to checkpoint to disk. therefore, this
 * temporary hack serializes bytes as an iobuf so that we can avoid dealing with
 * on-disk data compatibility when finally removing the last bit of bytes.
 */
namespace reflection {
template<>
struct adl<kafka::member_protocol> {
    void to(iobuf& out, kafka::member_protocol p) {
        iobuf md = bytes_to_iobuf(p.metadata);
        reflection::serialize(out, p.name, md);
    }

    kafka::member_protocol from(iobuf_parser& in) {
        return kafka::member_protocol{
          .name = adl<kafka::protocol_name>{}.from(in),
          .metadata = iobuf_to_bytes(adl<iobuf>{}.from(in)),
        };
    }
};
} // namespace reflection
