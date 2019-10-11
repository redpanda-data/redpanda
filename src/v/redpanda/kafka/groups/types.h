#pragma once
#include "bytes/bytes.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace kafka {

/// Kafka group identifier.
using group_id = named_type<sstring, struct kafka_group_id>;

/// Kafka group member identifier.
using member_id = named_type<sstring, struct kafka_member_id>;

/// Kafka group generation identifier.
using generation_id = named_type<int32_t, struct kafka_generation_id>;

/// Kafka group instance identifier.
using group_instance_id = named_type<sstring, struct kafka_group_instance_id>;

/// Kafka group protocol type.
using protocol_type = named_type<sstring, struct kafka_protocol_type>;

/// Kafka group protocol name.
using protocol_name = named_type<sstring, struct kafka_protocol>;

/// An unknown / missing member id (Kafka protocol specific)
static inline const member_id unknown_member_id("");

/// An unknown / missing generation id (Kafka protocol specific)
static inline const generation_id unknown_generation_id(-1);

/// Note that this structure is shared for convenience between the group manager
/// and Kafka wire protocol. If it changes, make sure it remains compatible.
struct protocol_config {
    protocol_name name;
    bytes metadata;
};

/// Note that this structure is shared for convenience between the group manager
/// and Kafka wire protocol. If it changes, make sure it remains compatible.
struct member_config {
    kafka::member_id member_id;
    std::optional<kafka::group_instance_id> group_instance_id; // >= v5
    bytes metadata;
};

} // namespace kafka
