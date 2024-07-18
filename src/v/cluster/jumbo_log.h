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
#include "cluster/jumbo_log_service.h"
#include "jumbo_log/rpc.h"
#include "rpc/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace cluster {

class jumbo_log_service final : public jumbo_log::rpc::jumbo_log_service {
public:
    jumbo_log_service(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<cluster::jumbo_log_frontend>&);

    ss::future<jumbo_log::rpc::create_write_intent_reply> create_write_intent(
      jumbo_log::rpc::create_write_intent_request,
      rpc::streaming_context&) final;

    ss::future<jumbo_log::rpc::get_write_intents_reply> get_write_intents(
      jumbo_log::rpc::get_write_intents_request, rpc::streaming_context&) final;

private:
    ss::sharded<cluster::jumbo_log_frontend>& _jumbo_log_frontend;
};
} // namespace cluster
