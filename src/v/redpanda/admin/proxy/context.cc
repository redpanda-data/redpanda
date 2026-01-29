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

#include "redpanda/admin/proxy/context.h"

namespace admin::proxy {

bool is_proxied(const serde::pb::rpc::context& ctx) {
    auto* list = ctx.get_optional_value<std::vector<model::node_id>>();
    return list != nullptr && !list->empty();
}

bool has_proxied_node(
  const serde::pb::rpc::context& ctx, model::node_id node_id) {
    auto* list = ctx.get_optional_value<std::vector<model::node_id>>();
    return list != nullptr && std::ranges::contains(*list, node_id);
}

std::vector<model::node_id>
get_proxied_nodes(const serde::pb::rpc::context& ctx) {
    auto* list = ctx.get_optional_value<std::vector<model::node_id>>();
    return list != nullptr ? *list : std::vector<model::node_id>{};
}

void append_proxied_nodes(
  serde::pb::rpc::context& ctx, std::vector<model::node_id> node_ids) {
    auto* list = ctx.get_optional_value<std::vector<model::node_id>>();
    if (list == nullptr) {
        ctx.set_value(std::move(node_ids));
    } else {
        list->append_range(std::move(node_ids));
    }
}

} // namespace admin::proxy
