/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster/metadata_dissemination_rpc_service.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

/// Handler for metadata service. The service implements two methods
///
/// 1. update_leadership - send by newly elected leader to all nodes
///                        that does not contain the instance of raft group
///                        that the new leader belongs to
///
/// 2. get_leadership - send to any node that already belong to cluster
///                     after controller recovery to get the up to date
///                     leadership metadata

class metadata_dissemination_handler
  : public metadata_dissemination_rpc_service {
public:
    metadata_dissemination_handler(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<partition_leaders_table>&);

    ss::future<update_leadership_reply> update_leadership(
      update_leadership_request&&, rpc::streaming_context&) final;

    ss::future<get_leadership_reply>
    get_leadership(get_leadership_request&&, rpc::streaming_context&) final;

private:
    ss::future<update_leadership_reply>
    do_update_leadership(update_leadership_request&&);

    ss::sharded<partition_leaders_table>& _leaders;
}; // namespace cluster

} // namespace cluster
