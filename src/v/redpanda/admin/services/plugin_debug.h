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

#include "proto/redpanda/core/admin/v2/plugin_debug.proto.h"
#include "redpanda/admin/proxy/client.h"
#include "transform/fwd.h"

#include <seastar/core/sharded.hh>

namespace admin {

class plugin_debug_service_impl : public proto::admin::plugin_debug_service {
public:
    plugin_debug_service_impl(
      admin::proxy::client proxy_client,
      ss::sharded<transform::service>* transform_service);

    ss::future<proto::admin::list_committed_offsets_response>
      list_committed_offsets(
        serde::pb::rpc::context,
        proto::admin::list_committed_offsets_request) override;

    ss::future<proto::admin::garbage_collect_offsets_response>
      garbage_collect_offsets(
        serde::pb::rpc::context,
        proto::admin::garbage_collect_offsets_request) override;

private:
    admin::proxy::client _proxy_client;
    ss::sharded<transform::service>* _transform_service;
};

} // namespace admin
