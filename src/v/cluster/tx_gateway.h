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
#include "cluster/fwd.h"
#include "cluster/rm_group_proxy.h"
#include "cluster/tx_gateway_service.h"

#include <seastar/core/sharded.hh>

namespace cluster {

// transaction manager (tx_gateway_frontend) uses tx_gateway to interact with
// remote resource managers (partitions and consumer groups), it checks which
// node is a resource's leader and sends a requests there. the receiver's side
// (handlers invoked by tx_gateway) either processes the requests or fails them
// in case it lost leadership but it doesn't redirect requests to new leader
// to prevent infinite cross node message bouncing caused by stale metadata.
// this is the reason why tx_gateway uses methods like begin_group_tx_locally
// instead of begin_group_tx

class tx_gateway final : public tx_gateway_service {
public:
    tx_gateway(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<cluster::tx_gateway_frontend>&,
      rm_group_proxy*,
      ss::sharded<cluster::rm_partition_frontend>&);

    ss::future<init_tm_tx_reply>
    init_tm_tx(init_tm_tx_request&&, rpc::streaming_context&) override;

    ss::future<fetch_tx_reply>
    fetch_tx(fetch_tx_request&&, rpc::streaming_context&) override;

    ss::future<try_abort_reply>
    try_abort(try_abort_request&&, rpc::streaming_context&) override;

    ss::future<begin_tx_reply>
    begin_tx(begin_tx_request&&, rpc::streaming_context&) override;

    ss::future<prepare_tx_reply>
    prepare_tx(prepare_tx_request&&, rpc::streaming_context&) override;

    ss::future<commit_tx_reply>
    commit_tx(commit_tx_request&&, rpc::streaming_context&) override;

    ss::future<abort_tx_reply>
    abort_tx(abort_tx_request&&, rpc::streaming_context&) override;

    ss::future<begin_group_tx_reply>
    begin_group_tx(begin_group_tx_request&&, rpc::streaming_context&) override;

    ss::future<prepare_group_tx_reply> prepare_group_tx(
      prepare_group_tx_request&&, rpc::streaming_context&) override;

    ss::future<commit_group_tx_reply> commit_group_tx(
      commit_group_tx_request&&, rpc::streaming_context&) override;

    ss::future<abort_group_tx_reply>
    abort_group_tx(abort_group_tx_request&&, rpc::streaming_context&) override;

private:
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    rm_group_proxy* _rm_group_proxy;
    ss::sharded<cluster::rm_partition_frontend>& _rm_partition_frontend;
};
} // namespace cluster
