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

#include "proto/redpanda/core/admin/internal/debug.proto.h"

#include <seastar/core/distributed.hh>

class stress_fiber_manager;

namespace admin {

// An admin service that provides some reflection capabilities
// for the admin API, such as the version of Redpanda as well as
// what requests are available.
class debug_service_impl : public proto::admin::debug_service {
public:
    explicit debug_service_impl(
      ss::sharded<stress_fiber_manager>& stress_fiber_manager)
      : _stress_fiber_manager(stress_fiber_manager) {}

    seastar::future<proto::admin::start_stress_fiber_response>
      start_stress_fiber(proto::admin::start_stress_fiber_request) override;

    seastar::future<proto::admin::stop_stress_fiber_response>
      stop_stress_fiber(proto::admin::stop_stress_fiber_request) override;

private:
    ss::sharded<stress_fiber_manager>& _stress_fiber_manager;
};

} // namespace admin
