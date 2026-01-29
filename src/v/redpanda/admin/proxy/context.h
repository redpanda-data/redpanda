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

#pragma once

#include "model/fundamental.h"
#include "serde/protobuf/rpc.h"

namespace admin::proxy {

// Check if this request has been proxied by another broker.
bool is_proxied(const serde::pb::rpc::context& ctx);

// Check if this node_id has already proxied this request.
bool has_proxied_node(
  const serde::pb::rpc::context& ctx, model::node_id node_id);

// Get the list of proxied nodes from this context.
std::vector<model::node_id>
get_proxied_nodes(const serde::pb::rpc::context& ctx);

// Mutate the context and add `node_ids` to the list of nodes it contains.
void append_proxied_nodes(
  serde::pb::rpc::context& ctx, std::vector<model::node_id>);

} // namespace admin::proxy
