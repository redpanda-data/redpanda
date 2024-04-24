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

#include "metrics/metrics.h"

#include <seastar/core/metrics_registration.hh>

namespace net {

/*
 * Metrics probe that is common to all net::base_transport instances.
 */
class client_probe {
public:
    client_probe() = default;
    client_probe(const client_probe&) = delete;
    client_probe& operator=(const client_probe&) = delete;
    client_probe(client_probe&&) = delete;
    client_probe& operator=(client_probe&&) = delete;
    ~client_probe() = default;

    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_error() { ++_connection_errors; }

    void connection_closed() { --_connections; }

    /*
     * setup metrics
     *
     * name: name of the metrics group
     * defs: additional metrics to register
     */
    void setup_metrics(
      std::string_view name,
      const std::vector<ss::metrics::label_instance>& labels,
      const std::vector<ss::metrics::label>& aggregate_labels,
      std::vector<ss::metrics::metric_definition> defs);

protected:
    uint64_t _connects = 0;
    uint32_t _connections = 0;
    uint32_t _connection_errors = 0;
    metrics::internal_metric_groups _metrics;
};

}; // namespace net
