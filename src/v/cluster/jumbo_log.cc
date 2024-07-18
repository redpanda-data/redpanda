// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/jumbo_log.h"

#include "cluster/jumbo_log_frontend.h"
#include "cluster/jumbo_log_service.h"
#include "jumbo_log/rpc.h"
#include "model/namespace.h"
#include "rpc/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace cluster {

jumbo_log_service::jumbo_log_service(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<cluster::jumbo_log_frontend>& jumbo_log_frontend)
  : jumbo_log::rpc::jumbo_log_service(sg, ssg)
  , _jumbo_log_frontend(jumbo_log_frontend) {}

ss::future<jumbo_log::rpc::create_write_intent_reply>
jumbo_log_service::create_write_intent(
  jumbo_log::rpc::create_write_intent_request req, rpc::streaming_context&) {
    auto timeout = req.timeout;
    return _jumbo_log_frontend.local()
      .get_create_write_intent_router()
      .find_shard_and_process(req, model::jumbo_log_ntp, timeout);
}

ss::future<jumbo_log::rpc::get_write_intents_reply>
jumbo_log_service::get_write_intents(
  jumbo_log::rpc::get_write_intents_request req, rpc::streaming_context&) {
    auto timeout = req.timeout;
    return _jumbo_log_frontend.local()
      .get_get_write_intents_router()
      .find_shard_and_process(req, model::jumbo_log_ntp, timeout);
}

} // namespace cluster
