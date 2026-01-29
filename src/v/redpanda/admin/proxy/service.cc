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

#include "redpanda/admin/proxy/service.h"

#include "base/vlog.h"
#include "redpanda/admin/proxy/context.h"
#include "serde/protobuf/rpc.h"

#include <exception>

namespace admin::proxy {

namespace {
// NOLINTNEXTLINE(*-non-const-global-variables,*-err58-cpp)
ss::logger log{"admin/proxy/service"};

void handle_exception(
  errc ec, const serde::pb::rpc::base_exception& ex, proxy_response* resp) {
    resp->error_code = ec;
    resp->payload = iobuf::from(ex.message());
    if (const auto& ei = ex.info()) {
        proxy_response::error_info copy{
          .reason = ei->reason,
          .domain = ei->domain,
        };
        for (const auto& [k, v] : ei->metadata) {
            copy.metadata.emplace(k, v);
        }
        resp->info = std::move(copy);
    }
}

} // namespace

ss::future<proxy_response>
service_impl::proxy_rpc(proxy_request req, rpc::streaming_context&) {
    serde::pb::rpc::context ctx{
      .service_name = req.service,
      .method_name = req.method,
      .content_type = serde::pb::rpc::content_type::proto,
    };
    append_proxied_nodes(ctx, req.via);
    proxy_response response;
    try {
        auto payload = co_await _handler(
          std::move(ctx), std::move(req.payload));
        response.error_code = errc::ok;
        response.payload = std::move(payload);
        co_return response;
    } catch (const serde::pb::rpc::cancelled_exception& e) {
        handle_exception(errc::cancelled, e, &response);
    } catch (const serde::pb::rpc::unknown_exception& e) {
        handle_exception(errc::unknown, e, &response);
    } catch (const serde::pb::rpc::invalid_argument_exception& e) {
        handle_exception(errc::invalid_argument, e, &response);
    } catch (const serde::pb::rpc::deadline_exceeded_exception& e) {
        handle_exception(errc::deadline_exceeded, e, &response);
    } catch (const serde::pb::rpc::not_found_exception& e) {
        handle_exception(errc::not_found, e, &response);
    } catch (const serde::pb::rpc::already_exists_exception& e) {
        handle_exception(errc::already_exists, e, &response);
    } catch (const serde::pb::rpc::permission_denied_exception& e) {
        handle_exception(errc::permission_denied, e, &response);
    } catch (const serde::pb::rpc::resource_exhausted_exception& e) {
        handle_exception(errc::resource_exhausted, e, &response);
    } catch (const serde::pb::rpc::failed_precondition_exception& e) {
        handle_exception(errc::failed_precondition, e, &response);
    } catch (const serde::pb::rpc::aborted_exception& e) {
        handle_exception(errc::aborted, e, &response);
    } catch (const serde::pb::rpc::out_of_range_exception& e) {
        handle_exception(errc::out_of_range, e, &response);
    } catch (const serde::pb::rpc::unimplemented_exception& e) {
        handle_exception(errc::unimplemented, e, &response);
    } catch (const serde::pb::rpc::internal_exception& e) {
        handle_exception(errc::internal_error, e, &response);
    } catch (const serde::pb::rpc::unavailable_exception& e) {
        handle_exception(errc::unavailable, e, &response);
    } catch (const serde::pb::rpc::data_loss_exception& e) {
        handle_exception(errc::data_loss, e, &response);
    } catch (const serde::pb::rpc::unauthenticated_exception& e) {
        handle_exception(errc::unauthenticated, e, &response);
    } catch (...) {
        vlog(
          log.warn,
          "unexpected error handling RPC {}.{}: {}",
          req.service,
          req.method,
          std::current_exception());
        response.error_code = errc::internal_error;
    }
    co_return response;
}

} // namespace admin::proxy
