/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/tx_protocol_types.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace kafka {

/// \brief What a consumer group asks the transaction coordinator.
///
/// A group only asks about transactions it has open and cannot finish on its
/// own: the ones whose deadline has passed.
class tx_coordinator_client {
public:
    tx_coordinator_client() = default;
    tx_coordinator_client(const tx_coordinator_client&) = delete;
    tx_coordinator_client& operator=(const tx_coordinator_client&) = delete;
    tx_coordinator_client(tx_coordinator_client&&) = delete;
    tx_coordinator_client& operator=(tx_coordinator_client&&) = delete;
    virtual ~tx_coordinator_client() = default;

    /// \brief Ask the coordinator to abort a producer's transaction.
    ///
    /// The reply says what the coordinator already knows: the transaction was
    /// committed, or aborted, or neither is settled yet.
    virtual ss::future<cluster::try_abort_reply> try_abort(
      model::partition_id coordinator_partition,
      model::producer_identity pid,
      model::tx_seq tx_seq) = 0;
};

/// The request as it is routed to the coordinator that owns the transaction.
class routed_tx_coordinator_client final : public tx_coordinator_client {
public:
    explicit routed_tx_coordinator_client(
      ss::sharded<cluster::tx_gateway_frontend>& tx_frontend);

    ss::future<cluster::try_abort_reply> try_abort(
      model::partition_id coordinator_partition,
      model::producer_identity pid,
      model::tx_seq tx_seq) final;

private:
    ss::sharded<cluster::tx_gateway_frontend>& _tx_frontend;
};

} // namespace kafka
