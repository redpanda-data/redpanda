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

#include "redpanda/admin/services/broker.h"

#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"
#include "version/version.h"

#include <seastar/core/coroutine.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger brlog{"admin_api_server/broker_service"};

} // namespace

broker_service_impl::broker_service_impl(
  admin::proxy::client client,
  std::vector<std::unique_ptr<serde::pb::rpc::base_service>>* services)
  : _proxy_client(std::move(client))
  , _services(services) {}

ss::future<proto::admin::get_broker_response> broker_service_impl::get_broker(
  serde::pb::rpc::context ctx, proto::admin::get_broker_request req) {
    if (req.has_node_id()) {
        auto target = model::node_id(req.get_node_id());
        if (target != -1 && target != _proxy_client.self_node_id()) {
            co_return co_await _proxy_client
              .make_client_for_node<proto::admin::broker_service_client>(target)
              .get_broker(ctx, std::move(req));
        }
    }
    proto::admin::get_broker_response resp;
    resp.set_broker(self_broker());
    co_return resp;
}

ss::future<proto::admin::list_brokers_response>
broker_service_impl::list_brokers(
  serde::pb::rpc::context ctx, proto::admin::list_brokers_request) {
    proto::admin::list_brokers_response list_resp;
    list_resp.get_brokers().push_back(self_broker());
    auto clients
      = _proxy_client
          .make_clients_for_other_nodes<proto::admin::broker_service_client>();
    for (auto& [node_id, _] : clients) {
        auto& broker = list_resp.get_brokers().emplace_back();
        broker.set_node_id(node_id());

        // Populate build_info for all brokers with this broker's version for
        // backwards compatibility with rpk's cluster connections list version
        // compatibility check.
        broker.set_build_info(std::move(self_broker().get_build_info()));
    }
    co_return list_resp;
}

proto::admin::broker broker_service_impl::self_broker() const {
    proto::admin::broker b;
    b.set_node_id(_proxy_client.self_node_id());
    proto::admin::build_info build_info;
    build_info.set_version(ss::sstring(redpanda_git_version()));
    build_info.set_build_sha(ss::sstring(redpanda_git_revision()));
    b.set_build_info(std::move(build_info));
    proto::admin::admin_server admin_server_info;
    for (auto& service : *_services) {
        for (auto& route : service->all_routes()) {
            proto::rpc_route r;
            r.set_name(
              fmt::format("{}.{}", route.service_name, route.method_name));
            r.set_http_route(ss::sstring(route.path));
            admin_server_info.get_routes().push_back(std::move(r));
        }
    }
    b.set_admin_server(std::move(admin_server_info));
    return b;
}

} // namespace admin
