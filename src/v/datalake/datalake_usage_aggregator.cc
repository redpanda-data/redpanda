/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/datalake_usage_aggregator.h"

#include "cluster/controller.h"
#include "datalake/logger.h"

namespace datalake {

disabled_datalake_usage_api_impl::disabled_datalake_usage_api_impl(
  cluster::controller* controller)
  : _controller(controller) {
    vassert(
      _controller,
      "Controller must not be null for disabled datalake usage API");
}

ss::future<kafka::datalake_usage_api::usage_stats>
disabled_datalake_usage_api_impl::compute_usage(ss::abort_source&) {
    usage_stats stats;
    if (!_controller->is_raft0_leader()) {
        stats.missing_reason = kafka::datalake_usage_api::stats_missing_reason::
          not_controller_leader;
    } else {
        stats.missing_reason
          = kafka::datalake_usage_api::stats_missing_reason::feature_disabled;
        vlog(
          datalake_log.debug,
          "Datalake usage API is disabled, returning empty stats");
    }
    return ss::make_ready_future<usage_stats>(std::move(stats));
}

} // namespace datalake
