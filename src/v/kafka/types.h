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
