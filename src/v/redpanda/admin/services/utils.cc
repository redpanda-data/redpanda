/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/utils.h"

#include "cluster/metadata_cache.h"
#include "features/feature_table.h"
#include "serde/protobuf/rpc.h"

namespace admin::utils {

void check_license(const features::feature_table& ft) {
    if (ft.should_sanction()) {
        const auto& license = ft.get_license();
        auto status = [&license]() {
            return !license.has_value()           ? "not present"
                   : license.value().is_expired() ? "expired"
                                                  : "unknown error";
        };
        throw serde::pb::rpc::failed_precondition_exception(
          fmt::format("Invalid license: {}", status()));
    }
}

std::optional<model::node_id> redirect_to_leader(
  const cluster::metadata_cache& md_cache,
  const model::ntp& ntp,
  model::node_id self) {
    auto leader_node = md_cache.get_leader_id(ntp);
    if (!leader_node) {
        throw serde::pb::rpc::unavailable_exception{
          ssx::sformat("Partition {} does not have a leader", ntp)};
    }

    if (leader_node.value() == self) {
        return std::nullopt;
    }

    return leader_node.value();
}

} // namespace admin::utils
