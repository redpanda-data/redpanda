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
#include "cluster/rm_partition_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/record.h"
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

ss::future<fetch_tx_reply>
tx_gateway::fetch_tx(fetch_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().fetch_tx_locally(
      request.tx_id, request.term, request.tm);
}

ss::future<try_abort_reply>
tx_gateway::try_abort(try_abort_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().try_abort_locally(
      request.tm, request.pid, request.tx_seq, request.timeout);
}

ss::future<init_tm_tx_reply>
tx_gateway::init_tm_tx(init_tm_tx_request&& request, rpc::streaming_context&) {
    return _tx_gateway_frontend.local().init_tm_tx(
      request.tx_id,
      request.transaction_timeout_ms,
      request.timeout,
      model::unknown_pid);
}

ss::future<begin_tx_reply>
tx_gateway::begin_tx(begin_tx_request&& request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().begin_tx_locally(
      request.ntp,
      request.pid,
      request.tx_seq,
      request.transaction_timeout_ms,
      request.tm_partition);
}

ss::future<prepare_tx_reply>
tx_gateway::prepare_tx(prepare_tx_request&& request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().prepare_tx_locally(
      request.ntp,
      request.etag,
      request.tm,
      request.pid,
      request.tx_seq,
      request.timeout);
}

ss::future<commit_tx_reply>
tx_gateway::commit_tx(commit_tx_request&& request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().commit_tx_locally(
      request.ntp, request.pid, request.tx_seq, request.timeout);
}

ss::future<abort_tx_reply>
tx_gateway::abort_tx(abort_tx_request&& request, rpc::streaming_context&) {
    return _rm_partition_frontend.local().abort_tx_locally(
      request.ntp, request.pid, request.tx_seq, request.timeout);
}

ss::future<begin_group_tx_reply> tx_gateway::begin_group_tx(
  begin_group_tx_request&& request, rpc::streaming_context&) {
    return _rm_group_proxy->begin_group_tx_locally(std::move(request));
};

ss::future<prepare_group_tx_reply> tx_gateway::prepare_group_tx(
  prepare_group_tx_request&& request, rpc::streaming_context&) {
    return _rm_group_proxy->prepare_group_tx_locally(std::move(request));
};

ss::future<commit_group_tx_reply> tx_gateway::commit_group_tx(
  commit_group_tx_request&& request, rpc::streaming_context&) {
    return _rm_group_proxy->commit_group_tx_locally(std::move(request));
};

ss::future<abort_group_tx_reply> tx_gateway::abort_group_tx(
  abort_group_tx_request&& request, rpc::streaming_context&) {
    return _rm_group_proxy->abort_group_tx_locally(std::move(request));
}

} // namespace cluster
