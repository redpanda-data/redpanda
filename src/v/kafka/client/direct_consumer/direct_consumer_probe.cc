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

#include "kafka/client/direct_consumer/direct_consumer_probe.h"

#include "config/configuration.h"

#include <seastar/core/metrics.hh>

namespace kafka {
namespace client {

direct_consumer_probe::direct_consumer_probe(configuration config)
  : n_fetch_errors{}
  , _public_metrics{} {
    setup_metrics(std::move(config));
}

void direct_consumer_probe::setup_metrics(configuration config) {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    // client metrics
    auto client_metrics = std::to_array({
      sm::make_counter(
        "client_errors",
        [this] { return n_fetch_errors; },
        sm::description("Number of errors seen by the client"),
        config.labels),
    });

    std::vector<sm::metric_definition> defs;
    defs.reserve(client_metrics.size());
    std::ranges::copy(
      client_metrics | std::views::as_rvalue, std::back_inserter(defs));

    _public_metrics.add_group(config.group_name, defs);
}

} // namespace client
} // namespace kafka
