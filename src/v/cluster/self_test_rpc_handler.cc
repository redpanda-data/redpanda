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

#include "cluster/self_test_rpc_handler.h"

namespace cluster {

self_test_rpc_handler::self_test_rpc_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<self_test_backend>& backend)
  : self_test_rpc_service(sg, ssg)
  , _self_test_backend(backend) {}

ss::future<get_status_response> self_test_rpc_handler::start_test(
  start_test_request&& r, rpc::streaming_context&) {
    return _self_test_backend.invoke_on(
      self_test_backend::shard,
      [r](auto& service) { return service.start_test(r); });
}

ss::future<get_status_response>
self_test_rpc_handler::stop_test(empty_request&&, rpc::streaming_context&) {
    return _self_test_backend.invoke_on(
      self_test_backend::shard,
      [](auto& service) { return service.stop_test(); });
}

ss::future<get_status_response>
self_test_rpc_handler::get_status(empty_request&&, rpc::streaming_context&) {
    return _self_test_backend.invoke_on(
      self_test_backend::shard,
      [](auto& service) { return service.get_status(); });
}

ss::future<netcheck_response>
self_test_rpc_handler::netcheck(netcheck_request&& r, rpc::streaming_context&) {
    return _self_test_backend.invoke_on(
      self_test_backend::shard, [r = std::move(r)](auto& service) mutable {
          return service.netcheck(r.source, std::move(r.buf));
      });
}

} // namespace cluster
