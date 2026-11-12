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

#include "datalake/coordinator/coordinator_manager.h"
#include "datalake/coordinator/frontend.h"
#include "proto/redpanda/core/admin/internal/datalake/v1/datalake.proto.h"
#include "redpanda/admin/proxy/client.h"

namespace admin {

class datalake_service_impl : public proto::admin::datalake_service {
public:
    datalake_service_impl(
      admin::proxy::client proxy_client,
      ss::sharded<datalake::coordinator::frontend>* coordinator_fe,
      ss::sharded<datalake::coordinator::coordinator_manager>* coordinator_mgr);

    ss::future<proto::admin::get_coordinator_state_response>
      get_coordinator_state(
        serde::pb::rpc::context,
        proto::admin::get_coordinator_state_request) override;

    ss::future<proto::admin::coordinator_reset_topic_state_response>
      coordinator_reset_topic_state(
        serde::pb::rpc::context,
        proto::admin::coordinator_reset_topic_state_request) override;

    ss::future<proto::admin::describe_catalog_response> describe_catalog(
      serde::pb::rpc::context, proto::admin::describe_catalog_request) override;

    ss::future<proto::admin::migrate_iceberg_schema_response>
      migrate_iceberg_schema(
        serde::pb::rpc::context,
        proto::admin::migrate_iceberg_schema_request) override;

private:
    admin::proxy::client _proxy_client;
    ss::sharded<datalake::coordinator::frontend>* _coordinator_fe;
    ss::sharded<datalake::coordinator::coordinator_manager>* _coordinator_mgr;
};

} // namespace admin
