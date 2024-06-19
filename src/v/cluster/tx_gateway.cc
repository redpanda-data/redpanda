// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway.h"

#include "cluster/logger.h"
#include "cluster/rm_group_proxy.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "tx_protocol_types.h"

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

ss::future<fetch_tx_reply>
tx_gateway::fetch_tx(fetch_tx_request, rpc::streaming_context&) {
    co_return fetch_tx_reply{};
}

ss::future<try_abort_reply>
tx_gateway::try_abort(try_abort_request request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().route_locally(std::move(request));
}

ss::future<init_tm_tx_reply>
tx_gateway::init_tm_tx(init_tm_tx_request request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().init_tm_tx(
      request.tx_id,
      request.transaction_timeout_ms,
      request.timeout,
      std::nullopt);
}

ss::future<begin_tx_reply>
tx_gateway::begin_tx(begin_tx_request request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().begin_tx_locally(
      request.ntp,
      request.pid,
      request.tx_seq,
      request.transaction_timeout_ms,
      request.tm_partition);
}

ss::future<prepare_tx_reply>
tx_gateway::prepare_tx(prepare_tx_request, rpc::streaming_context&) {
    // prepare_tx is unsupported starting 23.3.x original transactions
    // design used a preparing/prepare phase as an intent to commit
    // transaction and it has been redesigned since then starting 22.3.0
    // this RPC is no longer used.
    return ss::make_exception_future<prepare_tx_reply>(std::runtime_error{
      "prepare_tx is no longer supported, this is only possible if the cluster "
      "is running mixed versions of Redpanda and the initiator of this RPC is "
      "pre-22.3.x. Check your installation."});
}

ss::future<commit_tx_reply>
tx_gateway::commit_tx(commit_tx_request request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().commit_tx_locally(
      request.ntp, request.pid, request.tx_seq, request.timeout);
}

ss::future<abort_tx_reply>
tx_gateway::abort_tx(abort_tx_request request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().abort_tx_locally(
      request.ntp, request.pid, request.tx_seq, request.timeout);
}

ss::future<begin_group_tx_reply> tx_gateway::begin_group_tx(
  begin_group_tx_request request, rpc::streaming_context&) {
    return _rm_group_proxy->begin_group_tx_locally(std::move(request));
};

ss::future<prepare_group_tx_reply> tx_gateway::prepare_group_tx(
  prepare_group_tx_request, rpc::streaming_context&) {
    // group_prepare is no longer part of the transaction lifecycle after
    // 22.3.0 redesign. There are no callers for this.
    return ss::make_exception_future<cluster::prepare_group_tx_reply>(
      std::runtime_error{
        "prepare_group_tx is no longer supported, this is only possible if the "
        "cluster is running mixed versions of Redpanda and the initiator of "
        "this RPC is pre-22.3.x. Check your installation."});
};

ss::future<commit_group_tx_reply> tx_gateway::commit_group_tx(
  commit_group_tx_request request, rpc::streaming_context&) {
    return _rm_group_proxy->commit_group_tx_locally(std::move(request));
};

ss::future<abort_group_tx_reply> tx_gateway::abort_group_tx(
  abort_group_tx_request request, rpc::streaming_context&) {
    return _rm_group_proxy->abort_group_tx_locally(std::move(request));
}

ss::future<find_coordinator_reply> tx_gateway::find_coordinator(
  find_coordinator_request r, rpc::streaming_context&) {
    co_return co_await _tx_gateway_frontend.local().find_coordinator(r.tid);
}

} // namespace cluster
