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
    method_not_found,
    version_not_supported,
    connection_timeout,
    shutting_down,
    service_unavailable,

    // Used when receiving an undefined errc (e.g. from a newer version of
    // Redpanda).
    unknown = std::numeric_limits<uint8_t>::max(),
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
        case errc::service_error:
            return "rpc::errc::service_error";
        case errc::method_not_found:
            return "rpc::errc::method_not_found";
        case errc::version_not_supported:
            return "rpc::errc::version_not_supported";
        case errc::connection_timeout:
            return "rpc::errc::connection_timeout";
        case errc::shutting_down:
            return "rpc::errc::shutting_down";
        case errc::service_unavailable:
            return "rpc::errc::service_unavailable";
        case errc::unknown:
            break;
        }
        return "rpc::errc::unknown(" + std::to_string(c) + ")";
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
