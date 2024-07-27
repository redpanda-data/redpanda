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
#include "bytes/bytes.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace kafka {

/// Kafka API request correlation.
using correlation_id = named_type<int32_t, struct kafka_correlation_type>;

using client_id = named_type<ss::sstring, struct kafka_client_id_type>;
using client_host = named_type<ss::sstring, struct kafka_client_host_type>;

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

enum class config_resource_operation : int8_t {
    set = 0,
    remove = 1,
    append = 2,
    subtract = 3,
};

std::ostream& operator<<(std::ostream& os, config_resource_operation);

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

    friend std::ostream&
    operator<<(std::ostream& os, const member_protocol& p) {
        return os << p.name << ":" << p.metadata.size();
    }
};

using assignments_type = std::unordered_map<member_id, bytes>;

/**
 * Describes single partition replica. Used by replica selector
 */
struct replica_info {
    model::node_id id;
    model::offset high_watermark;
    model::offset log_end_offset;
    bool is_alive;
};

struct partition_info {
    std::vector<replica_info> replicas;
    std::optional<model::node_id> leader;
};

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
