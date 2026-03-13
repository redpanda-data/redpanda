// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "kafka/protocol/errors.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>

namespace kafka::nextgen {

/// \brief Result returned from a heartbeat call.
struct heartbeat_result {
    error_code ec{error_code::none};
    ss::sstring member_id;
    int32_t member_epoch{0};
};

/// \brief In-memory KIP-848 consumer group coordinator.
///
/// Each Seastar shard holds one instance (via ss::sharded<coordinator>).
/// All member state is shard-local; no cross-shard coordination is performed
/// by this prototype implementation.
///
/// Member lifecycle:
///   - member_epoch == -1 → join sentinel; a new UUID member_id is assigned
///     and the member starts in RECONCILING with epoch 0.
///   - known member, matching epoch → epoch advances; state moves toward STABLE.
///   - known member, mismatched epoch → fenced_member_epoch error.
///   - unknown member_id → unknown_member_id error.
class coordinator {
public:
    ss::future<heartbeat_result> heartbeat(
      ss::sstring group_id, ss::sstring member_id, int32_t member_epoch);

    ss::future<> stop();

private:
    enum class member_state : uint8_t { reconciling, stable };

    struct member_info {
        int32_t epoch{0};
        member_state state{member_state::reconciling};
    };

    struct group_info {
        chunked_hash_map<ss::sstring, member_info> members;
    };

    chunked_hash_map<ss::sstring, group_info> _groups;
};

} // namespace kafka::nextgen
