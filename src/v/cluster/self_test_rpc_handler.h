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

#include "cluster/fwd.h"
#include "cluster/self_test_rpc_service.h"

namespace cluster {

class self_test_rpc_handler final : public self_test_rpc_service {
public:
    self_test_rpc_handler(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<self_test_backend>&);

    ss::future<get_status_response>
    start_test(start_test_request, rpc::streaming_context&) final;

    ss::future<get_status_response>
    stop_test(empty_request, rpc::streaming_context&) final;

    ss::future<get_status_response>
    get_status(empty_request, rpc::streaming_context&) final;

    ss::future<netcheck_response>
    netcheck(netcheck_request, rpc::streaming_context&) final;

private:
    ss::sharded<self_test_backend>& _self_test_backend;
};

} // namespace cluster
