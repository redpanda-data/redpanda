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

#include "redpanda/admin/services/admin.h"

#include "version/version.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

admin_service_impl::admin_service_impl(
  std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services)
  : _services(services) {}

ss::future<proto::build_info>
admin::admin_service_impl::get_build_info(proto::get_build_info_request) {
    proto::build_info build_info;
    build_info.set_version(ss::sstring(redpanda_git_version()));
    build_info.set_build_sha(ss::sstring(redpanda_git_revision()));
    co_return build_info;
}

ss::future<proto::list_rpc_routes_response>
admin::admin_service_impl::list_rpc_routes(proto::list_rpc_routes_request) {
    proto::list_rpc_routes_response resp;
    for (auto& service : *_services) {
        for (auto& route : service->all_routes()) {
            proto::rpc_route r;
            r.set_name(std::move(route.name));
            r.set_http_route(fmt::format("/v2{}", route.path));
            resp.get_routes().push_back(std::move(r));
        }
    }
    co_return resp;
}

} // namespace admin
