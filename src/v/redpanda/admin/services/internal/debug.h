/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "proto/redpanda/core/admin/internal/v1/debug.proto.h"
#include "redpanda/admin/proxy/client.h"

#include <seastar/core/distributed.hh>

class stress_fiber_manager;

namespace admin {

// An admin service that provides some reflection capabilities
// for the admin API, such as the version of Redpanda as well as
// what requests are available.
class debug_service_impl : public proto::admin::debug_service {
public:
    explicit debug_service_impl(
      admin::proxy::client client,
      ss::sharded<stress_fiber_manager>& stress_fiber_manager)
      : _client(std::move(client))
      , _stress_fiber_manager(stress_fiber_manager) {}

    seastar::future<proto::admin::throw_structured_exception_response>
      throw_structured_exception(
        serde::pb::rpc::context,
        proto::admin::throw_structured_exception_request) override;

    seastar::future<proto::admin::start_stress_fiber_response>
      start_stress_fiber(
        serde::pb::rpc::context,
        proto::admin::start_stress_fiber_request) override;

    seastar::future<proto::admin::stop_stress_fiber_response> stop_stress_fiber(
      serde::pb::rpc::context,
      proto::admin::stop_stress_fiber_request) override;

    seastar::future<proto::admin::log_message_response> log_message(
      serde::pb::rpc::context, proto::admin::log_message_request) override;

private:
    admin::proxy::client _client;
    ss::sharded<stress_fiber_manager>& _stress_fiber_manager;
};

} // namespace admin
