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
};

std::ostream& operator<<(std::ostream&, const member_protocol&);

using assignments_type = std::unordered_map<member_id, bytes>;

} // namespace kafka
