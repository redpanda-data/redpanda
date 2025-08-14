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

#include "proto/redpanda/core/admin/admin.proto.h"

namespace admin {

// An admin service that provides some reflection capabilities
// for the admin API, such as the version of Redpanda as well as
// what requests are available.
class admin_service_impl : public proto::admin::admin_service {
public:
    explicit admin_service_impl(
      std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services);

    ss::future<proto::admin::build_info>
      get_build_info(proto::admin::get_build_info_request) override;

    ss::future<proto::admin::list_rpc_routes_response>
      list_rpc_routes(proto::admin::list_rpc_routes_request) override;

private:
    std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* _services;
};

} // namespace admin
