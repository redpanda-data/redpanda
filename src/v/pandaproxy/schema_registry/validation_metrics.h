/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/configuration.h"
#include "metrics/metrics.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

namespace pandaproxy::schema_registry {

class schema_id_validation_probe {
public:
    void setup_metrics() {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        _metrics.add_group(
          "kafka_schema_id_cache",
          {
            sm::make_counter(
              "hits",
              [this]() { return _hits; },
              sm::description("Total number of hits for the server-side schema "
                              "ID validation cache (see cluster config: "
                              "kafka_schema_id_validation_cache_capacity)"),
              {}),
            sm::make_counter(
              "misses",
              [this]() { return _misses; },
              sm::description("Total number of misses for the server-side "
                              "schema ID validation cache (see cluster config: "
                              "kafka_schema_id_validation_cache_capacity)"),
              {}),
            sm::make_counter(
              "batches_decompressed",
              [this]() { return _batches_decompressed; },
              sm::description("Total number of batches decompressed for "
                              "server-side schema ID validation"),
              {}),
          },
          {},
          {sm::shard_label});
    }

    void hit() { ++_hits; }
    void miss() { ++_misses; }
    void decompressed() { ++_batches_decompressed; }

private:
    metrics::internal_metric_groups _metrics;
    int64_t _hits;
    int64_t _misses;
    int64_t _batches_decompressed;
};

} // namespace pandaproxy::schema_registry
