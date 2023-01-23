/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/metadata_cache.h"
#include "features/feature_table.h"
#include "kafka/server/request_context.h"

#include <seastarx.h>

namespace kafka {

using is_node_isolated_or_decommissioned
  = ss::bool_class<struct is_node_isolated_or_decommissioned_tag>;

inline is_node_isolated_or_decommissioned
node_isolated_or_decommissioned(request_context& ctx) {
    auto isoalted_or_decommissioned = is_node_isolated_or_decommissioned::no;
    if (ctx.feature_table().local().is_active(
          features::feature::node_isolation)) {
        isoalted_or_decommissioned = ctx.metadata_cache().is_node_isolated()
                                       ? is_node_isolated_or_decommissioned::yes
                                       : is_node_isolated_or_decommissioned::no;
    }

    return isoalted_or_decommissioned;
}

} // namespace kafka