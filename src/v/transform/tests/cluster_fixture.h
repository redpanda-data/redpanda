/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/tests/cluster_test_fixture.h"
#include "model/transform.h"

/**
 Full machinery of rp test fixture with transforms enabled.
 Use create_node_application(node_id) to add nodes to the cluster.
 */
class WasmClusterFixture : public cluster_test_fixture {
    fixture_ptr make_redpanda_fixture(
      model::node_id node_id,
      int16_t kafka_port,
      int16_t rpc_port,
      int16_t proxy_port,
      int16_t schema_reg_port,
      std::vector<config::seed_server> seeds,
      configure_node_id use_node_id,
      empty_seed_starts_cluster empty_seed_starts_cluster_val) override {
        return std::make_unique<redpanda_thread_fixture>(
          node_id,
          kafka_port,
          rpc_port,
          proxy_port,
          schema_reg_port,
          seeds,
          ssx::sformat("{}.{}", _base_dir, node_id()),
          _sgroups,
          false,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          use_node_id,
          empty_seed_starts_cluster_val,
          std::nullopt,
          /* enable transforms */ true);
    }

public:
    ss::sharded<transform::rpc::client>& transforms_client(model::node_id id) {
        return get_node_application(id)->transforms_client();
    }

    void create_transform_offsets_topic(int num_partitions = 3) {
        create_topic(
          {model::kafka_internal_namespace, model::transform_offsets_topic},
          num_partitions);
    }
};
