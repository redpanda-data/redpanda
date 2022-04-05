/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <system_error>

namespace rpc {

enum class errc {
    success = 0, // must be 0
    disconnected_endpoint,
    exponential_backoff,
    missing_node_rpc_client,
    client_request_timeout,
    service_error,
    method_not_found
};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "rpc::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "rpc::errc::success";
        case errc::disconnected_endpoint:
            return "rpc::errc::disconnected_endpoint(node down)";
        case errc::exponential_backoff:
            return "rpc::errc::exponential_backoff";
        case errc::missing_node_rpc_client:
            return "rpc::errc::missing_node_rpc_client";
        case errc::client_request_timeout:
            return "rpc::errc::client_request_timeout";
        default:
            return "rpc::errc::unknown";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace rpc
namespace std {
template<>
struct is_error_code_enum<rpc::errc> : true_type {};
} // namespace std
