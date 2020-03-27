#pragma once
#include "bytes/bytes.h"
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
