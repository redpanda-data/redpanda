

/**
 * Copyright 2024 Redpanda Data, Inc.
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

namespace cluster::tx {
enum class errc {
    none = 0,
    leader_not_found,
    shard_not_found,
    partition_not_found,
    stm_not_found,
    partition_not_exists,
    pid_not_found,
    // when a request times out a client should not do any assumptions about its
    // effect. the request may time out before reaching the server, the request
    // may be successfully processed or may fail and the reply times out
    timeout,
    conflict,
    fenced,
    stale,
    not_coordinator,
    coordinator_not_available,
    preparing_rebalance,
    rebalance_in_progress,
    coordinator_load_in_progress,
    // an unspecified error happened, the effect of the request is unknown
    // the error code is very similar to timeout
    unknown_server_error,
    // an unspecified error happened, a client may assume it had zero effect on
    // the target node
    request_rejected,
    invalid_producer_id_mapping,
    invalid_txn_state,
    invalid_producer_epoch,
    tx_not_found,
    tx_id_not_found,
    partition_disabled,
    concurrent_transactions,
    // invalid timeout requested by the client.
    invalid_timeout,
};

std::ostream& operator<<(std::ostream& o, errc err);

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::tx::errc"; }

    std::string message(int ec) const final;
};

inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return {static_cast<int>(e), error_category()};
}
} // namespace cluster::tx

namespace std {
template<>
struct is_error_code_enum<cluster::tx::errc> : true_type {};
} // namespace std
