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

#include "redpanda/admin/proxy/client.h"

#include "base/vlog.h"
#include "redpanda/admin/proxy/context.h"
#include "redpanda/admin/proxy/proxy_service.h"
#include "redpanda/admin/proxy/types.h"
#include "rpc/connection_cache.h"
#include "serde/protobuf/rpc.h"

#include <exception>
#include <utility>

namespace admin::proxy {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,*-err58-cpp)
ss::logger log{"admin/proxy/client"};

template<typename T>
[[noreturn]] void throw_rpc_exception(proxy_response resp) {
    auto ei = resp.info.transform([](const proxy_response::error_info& ei) {
        serde::pb::rpc::error_info copy{
          .reason = ei.reason,
          .domain = ei.domain,
        };
        for (const auto& [k, v] : ei.metadata) {
            copy.metadata.emplace(k, v);
        }
        return copy;
    });
    if (resp.payload.empty()) {
        throw T(std::move(ei));
    }
    iobuf_parser parser(std::move(resp.payload));
    constexpr size_t max_error_message_size = 4_KiB;
    auto msg = parser.read_string_safe(
      std::min(parser.bytes_left(), max_error_message_size));
    throw T(std::move(msg), std::move(ei));
}

} // namespace

ss::future<iobuf> client::send(
  model::node_id target, serde::pb::rpc::context ctx, iobuf payload) noexcept {
    if (has_proxied_node(ctx, _self)) {
        vlog(
          log.warn,
          "proxy loop detected (self={}, via={})",
          target,
          get_proxied_nodes(ctx));
        throw serde::pb::rpc::unavailable_exception("proxy loop detected");
    }
    proxy_request req;
    req.service = ss::sstring(ctx.service_name);
    req.method = ss::sstring(ctx.method_name);
    req.payload = std::move(payload);
    req.via = get_proxied_nodes(ctx);
    req.via.push_back(_self);
    // TODO: Cluster config for this timeout
    constexpr static std::chrono::milliseconds rpc_timeout = 10s;
    proxy_response proxy_resp;
    try {
        auto result = co_await _conn_cache->local()
                        .with_node_client<proxy_client_protocol>(
                          _self,
                          ss::this_shard_id(),
                          target,
                          rpc_timeout,
                          [req = std::move(req)](
                            proxy_client_protocol client) mutable {
                              return client.proxy_rpc(
                                std::move(req), rpc::client_opts(rpc_timeout));
                          });
        if (!result) {
            vlog(
              log.warn,
              "failed to proxy admin request to {}: {}",
              target,
              result.error());
            proxy_resp.error_code = errc::unavailable;
            // Leave it as a generic error message
        } else {
            proxy_resp = std::move(result.value().data);
        }
    } catch (...) {
        vlog(
          log.warn,
          "exception while proxying admin request to {}: {}",
          target,
          std::current_exception());
        proxy_resp.error_code = errc::internal_error;
        // Leave it as a generic error message
    }
    switch (proxy_resp.error_code) {
    case errc::ok:
        co_return std::move(proxy_resp.payload);
    case errc::cancelled:
        throw_rpc_exception<serde::pb::rpc::cancelled_exception>(
          std::move(proxy_resp));
    case errc::unknown:
        throw_rpc_exception<serde::pb::rpc::unknown_exception>(
          std::move(proxy_resp));
    case errc::invalid_argument:
        throw_rpc_exception<serde::pb::rpc::invalid_argument_exception>(
          std::move(proxy_resp));
    case errc::deadline_exceeded:
        throw_rpc_exception<serde::pb::rpc::deadline_exceeded_exception>(
          std::move(proxy_resp));
    case errc::not_found:
        throw_rpc_exception<serde::pb::rpc::not_found_exception>(
          std::move(proxy_resp));
    case errc::already_exists:
        throw_rpc_exception<serde::pb::rpc::already_exists_exception>(
          std::move(proxy_resp));
    case errc::permission_denied:
        throw_rpc_exception<serde::pb::rpc::permission_denied_exception>(
          std::move(proxy_resp));
    case errc::resource_exhausted:
        throw_rpc_exception<serde::pb::rpc::resource_exhausted_exception>(
          std::move(proxy_resp));
    case errc::failed_precondition:
        throw_rpc_exception<serde::pb::rpc::failed_precondition_exception>(
          std::move(proxy_resp));
    case errc::aborted:
        throw_rpc_exception<serde::pb::rpc::aborted_exception>(
          std::move(proxy_resp));
    case errc::out_of_range:
        throw_rpc_exception<serde::pb::rpc::out_of_range_exception>(
          std::move(proxy_resp));
    case errc::unimplemented:
        throw_rpc_exception<serde::pb::rpc::unimplemented_exception>(
          std::move(proxy_resp));
    case errc::internal_error:
        throw_rpc_exception<serde::pb::rpc::internal_exception>(
          std::move(proxy_resp));
    case errc::unavailable:
        throw_rpc_exception<serde::pb::rpc::unavailable_exception>(
          std::move(proxy_resp));
    case errc::data_loss:
        throw_rpc_exception<serde::pb::rpc::data_loss_exception>(
          std::move(proxy_resp));
    case errc::unauthenticated:
        throw_rpc_exception<serde::pb::rpc::unauthenticated_exception>(
          std::move(proxy_resp));
    }
    std::unreachable();
}

} // namespace admin::proxy
