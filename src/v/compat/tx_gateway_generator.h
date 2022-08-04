/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/types.h"
#include "compat/generator.h"
#include "random/generators.h"

#include <vector>

namespace compat {

template<>
struct instance_generator<cluster::tx_errc> {
    static cluster::tx_errc random() {
        return random_generators::random_choice({
          cluster::tx_errc::none,
          cluster::tx_errc::leader_not_found,
          cluster::tx_errc::shard_not_found,
          cluster::tx_errc::partition_not_found,
          cluster::tx_errc::stm_not_found,
          cluster::tx_errc::partition_not_exists,
          cluster::tx_errc::pid_not_found,
          cluster::tx_errc::timeout,
          cluster::tx_errc::conflict,
          cluster::tx_errc::fenced,
          cluster::tx_errc::stale,
          cluster::tx_errc::not_coordinator,
          cluster::tx_errc::coordinator_not_available,
          cluster::tx_errc::preparing_rebalance,
          cluster::tx_errc::rebalance_in_progress,
          cluster::tx_errc::coordinator_load_in_progress,
          cluster::tx_errc::unknown_server_error,
          cluster::tx_errc::request_rejected,
          cluster::tx_errc::invalid_producer_id_mapping,
          cluster::tx_errc::invalid_txn_state,
        });
    }

    static std::vector<raft::errc> limits() { return {}; }
};

} // namespace compat
