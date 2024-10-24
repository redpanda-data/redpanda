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
struct instance_generator<cluster::tx::errc> {
    static cluster::tx::errc random() {
        return random_generators::random_choice({
          cluster::tx::errc::none,
          cluster::tx::errc::leader_not_found,
          cluster::tx::errc::shard_not_found,
          cluster::tx::errc::partition_not_found,
          cluster::tx::errc::stm_not_found,
          cluster::tx::errc::partition_not_exists,
          cluster::tx::errc::pid_not_found,
          cluster::tx::errc::timeout,
          cluster::tx::errc::conflict,
          cluster::tx::errc::fenced,
          cluster::tx::errc::stale,
          cluster::tx::errc::not_coordinator,
          cluster::tx::errc::coordinator_not_available,
          cluster::tx::errc::preparing_rebalance,
          cluster::tx::errc::rebalance_in_progress,
          cluster::tx::errc::coordinator_load_in_progress,
          cluster::tx::errc::unknown_server_error,
          cluster::tx::errc::request_rejected,
          cluster::tx::errc::invalid_producer_id_mapping,
          cluster::tx::errc::invalid_txn_state,
        });
    }

    static std::vector<raft::errc> limits() { return {}; }
};

} // namespace compat
