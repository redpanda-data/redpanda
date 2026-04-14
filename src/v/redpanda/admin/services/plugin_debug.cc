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

#include "redpanda/admin/services/plugin_debug.h"

#include "cluster/errc.h"
#include "proto/redpanda/core/admin/v2/plugin_debug.proto.h"
#include "serde/protobuf/rpc.h"
#include "transform/api.h"
#include "wasm/errc.h"

#include <seastar/core/coroutine.hh>

#include <fmt/core.h>

namespace admin {

namespace {

// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger debuglog{"admin_api_server/plugin_debug_service"};

void check_transforms_enabled(
  const ss::sharded<transform::service>* transform_service) {
    if (!transform_service->local_is_initialized()) {
        throw serde::pb::rpc::failed_precondition_exception(
          "data transforms disabled - use `rpk cluster config set "
          "data_transforms_enabled true` to enable");
    }
}

void throw_on_transform_error(
  std::string_view context, const std::error_code& ec) {
    if (!ec) {
        return;
    }
    if (ec.category() == cluster::error_category()) {
        if (
          static_cast<cluster::errc>(ec.value())
          == cluster::errc::feature_disabled) {
            throw serde::pb::rpc::failed_precondition_exception(
              fmt::format("{}: feature disabled", context));
        }
    } else if (ec.category() == wasm::error_category()) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("{}: {}", context, ec.message()));
    }
    throw serde::pb::rpc::internal_exception(
      fmt::format("{}: {}", context, ec.message()));
}

} // namespace

plugin_debug_service_impl::plugin_debug_service_impl(
  admin::proxy::client proxy_client,
  ss::sharded<transform::service>* transform_service)
  : _proxy_client(std::move(proxy_client))
  , _transform_service(transform_service) {}

ss::future<proto::admin::list_committed_offsets_response>
plugin_debug_service_impl::list_committed_offsets(
  serde::pb::rpc::context,
  proto::admin::list_committed_offsets_request req) {
    check_transforms_enabled(_transform_service);

    auto result = co_await _transform_service->local().list_committed_offsets(
      {.show_unknown = req.get_show_unknown()});
    if (result.has_error()) {
        throw_on_transform_error("list_committed_offsets", result.error());
    }

    proto::admin::list_committed_offsets_response resp;
    for (const auto& committed : result.value()) {
        proto::admin::committed_offset co;
        co.set_transform_name(ss::sstring(committed.name()));
        co.set_partition(committed.partition());
        co.set_offset(committed.offset());
        resp.get_offsets().push_back(std::move(co));
    }
    co_return resp;
}

ss::future<proto::admin::garbage_collect_offsets_response>
plugin_debug_service_impl::garbage_collect_offsets(
  serde::pb::rpc::context,
  proto::admin::garbage_collect_offsets_request) {
    check_transforms_enabled(_transform_service);

    vlog(debuglog.info, "garbage_collect_offsets");
    auto ec
      = co_await _transform_service->local()
          .garbage_collect_committed_offsets();
    throw_on_transform_error("garbage_collect_offsets", ec);
    co_return proto::admin::garbage_collect_offsets_response{};
}

} // namespace admin
