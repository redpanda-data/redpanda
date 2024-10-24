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

#include "cluster/tx_errc.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <iostream>

namespace cluster::tx {

std::ostream& operator<<(std::ostream& o, errc err) {
    switch (err) {
    case errc::none:
        return o << "tx::errc::none";
    case errc::leader_not_found:
        return o << "tx::errc::leader_not_found";
    case errc::shard_not_found:
        return o << "tx::errc::shard_not_found";
    case errc::partition_not_found:
        return o << "tx::errc::partition_not_found";
    case errc::stm_not_found:
        return o << "tx::errc::stm_not_found";
    case errc::partition_not_exists:
        return o << "tx::errc::partition_not_exists";
    case errc::pid_not_found:
        return o << "tx::errc::pid_not_found";
    case errc::timeout:
        return o << "tx::errc::timeout";
    case errc::conflict:
        return o << "tx::errc::conflict";
    case errc::fenced:
        return o << "tx::errc::fenced";
    case errc::stale:
        return o << "tx::errc::stale";
    case errc::not_coordinator:
        return o << "tx::errc::not_coordinator";
    case errc::coordinator_not_available:
        return o << "tx::errc::coordinator_not_available";
    case errc::preparing_rebalance:
        return o << "tx::errc::preparing_rebalance";
    case errc::rebalance_in_progress:
        return o << "tx::errc::rebalance_in_progress";
    case errc::coordinator_load_in_progress:
        return o << "tx::errc::coordinator_load_in_progress";
    case errc::unknown_server_error:
        return o << "tx::errc::unknown_server_error";
    case errc::request_rejected:
        return o << "tx::errc::request_rejected";
    case errc::invalid_producer_epoch:
        return o << "tx::errc::invalid_producer_epoch";
    case errc::invalid_txn_state:
        return o << "tx::errc::invalid_txn_state";
    case errc::invalid_producer_id_mapping:
        return o << "tx::errc::invalid_producer_id_mapping";
    case errc::tx_not_found:
        return o << "tx::errc::tx_not_found";
    case errc::tx_id_not_found:
        return o << "tx::errc::tx_id_not_found";
    case errc::partition_disabled:
        return o << "tx::errc::partition_disabled";
    case errc::concurrent_transactions:
        return o << "tx::errc::concurrent_transactions";
    case errc::invalid_timeout:
        return o << "tx::errc::invalid_timeout";
    }
    return o;
}

std::string errc_category::message(int ec) const {
    return fmt::to_string(errc(ec));
}

} // namespace cluster::tx
