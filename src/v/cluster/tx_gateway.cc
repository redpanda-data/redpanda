// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway.h"

#include "cluster/logger.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

tx_gateway::tx_gateway(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  rm_group_proxy* group_proxy,
  ss::sharded<cluster::rm_partition_frontend>& rm_partition_frontend)
  : tx_gateway_service(sg, ssg)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _rm_group_proxy(group_proxy)
  , _rm_partition_frontend(rm_partition_frontend) {}

ss::future<init_tm_tx_reply>
tx_gateway::init_tm_tx(init_tm_tx_request&&, rpc::streaming_context&) {
    return ss::make_ready_future<init_tm_tx_reply>(init_tm_tx_reply());
}

ss::future<begin_tx_reply>
tx_gateway::begin_tx(begin_tx_request&&, rpc::streaming_context&) {
    return ss::make_ready_future<begin_tx_reply>(begin_tx_reply());
}

ss::future<prepare_tx_reply>
tx_gateway::prepare_tx(prepare_tx_request&&, rpc::streaming_context&) {
    return ss::make_ready_future<prepare_tx_reply>(prepare_tx_reply());
}

ss::future<commit_tx_reply>
tx_gateway::commit_tx(commit_tx_request&&, rpc::streaming_context&) {
    return ss::make_ready_future<commit_tx_reply>(commit_tx_reply());
}

ss::future<abort_tx_reply>
tx_gateway::abort_tx(abort_tx_request&&, rpc::streaming_context&) {
    return ss::make_ready_future<abort_tx_reply>(abort_tx_reply());
}

ss::future<begin_group_tx_reply>
tx_gateway::begin_group_tx(begin_group_tx_request&&, rpc::streaming_context&) {
    co_return begin_group_tx_reply();
};

ss::future<prepare_group_tx_reply> tx_gateway::prepare_group_tx(
  prepare_group_tx_request&&, rpc::streaming_context&) {
    co_return prepare_group_tx_reply();
};

ss::future<commit_group_tx_reply> tx_gateway::commit_group_tx(
  commit_group_tx_request&&, rpc::streaming_context&) {
    co_return commit_group_tx_reply();
};

ss::future<abort_group_tx_reply>
tx_gateway::abort_group_tx(abort_group_tx_request&&, rpc::streaming_context&) {
    co_return abort_group_tx_reply();
}

} // namespace cluster
