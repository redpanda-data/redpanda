// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "metrics/metrics.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_set.h>

namespace cluster {

class topic_table_probe {
public:
    explicit topic_table_probe(const topic_table&);

    topic_table_probe(const topic_table_probe&) = delete;
    topic_table_probe(topic_table_probe&&) = delete;
    topic_table_probe& operator=(const topic_table_probe&) = delete;
    topic_table_probe& operator=(topic_table_probe&&) = delete;

    void handle_topic_creation(create_topic_cmd::key_t);
    void handle_topic_deletion(const delete_topic_cmd::key_t&);

    void handle_update(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);
    void handle_update_finish(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);
    void handle_update_cancel(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);
    void handle_update_cancel_finish(
      const std::vector<model::broker_shard>& previous_replicas,
      const std::vector<model::broker_shard>& result_replicas);

private:
    void setup_metrics();
    void setup_public_metrics();
    void setup_internal_metrics();

    const topic_table& _topic_table;
    model::node_id _node_id;
    absl::btree_map<model::topic_namespace, ss::metrics::metric_groups>
      _topics_metrics;
    metrics::internal_metric_groups _internal_metrics;
    metrics::public_metric_groups _public_metrics;
    int32_t _moving_to_partitions = 0;
    int32_t _moving_from_partitions = 0;
    int32_t _cancelling_movements = 0;
};

} // namespace cluster
