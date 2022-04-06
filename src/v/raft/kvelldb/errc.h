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

namespace raft::kvelldb {

enum class errc {
    success = 0, // must be 0
    not_found,
    conflict,
    unknown_command,
    raft_error,
    timeout
};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "raft::kvelldb::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "raft::kvelldb::errc::success";
        case errc::not_found:
            return "raft::kvelldb::errc::not_fount";
        case errc::conflict:
            return "raft::kvelldb::errc::conflict";
        case errc::raft_error:
            return "raft::kvelldb::errc::raft_error";
        case errc::timeout:
            return "raft::kvelldb::errc::timeout";
        default:
            return "raft::kvelldb::errc::unknown";
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
} // namespace raft::kvelldb
namespace std {
template<>
struct is_error_code_enum<raft::kvelldb::errc> : true_type {};
} // namespace std
