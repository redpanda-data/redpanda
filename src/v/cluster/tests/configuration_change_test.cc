// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>

FIXTURE_TEST(test_updating_node_rpc_ip_address, cluster_test_fixture) {
    // add three nodes
    model::node_id node_0(0);
    model::node_id node_1(1);
    model::node_id node_2(2);
    auto node_app_0 = create_node_application(node_0);
    wait_for_controller_leadership(node_0).get();
    auto node_app_1 = create_node_application(node_1);
    auto node_app_2 = create_node_application(node_2);

    // wait for cluster to be stable
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [this, node_0, node_1, node_2] {
          return get_local_cache(node_0).all_brokers().size() == 3
                 && get_local_cache(node_1).all_brokers().size() == 3
                 && get_local_cache(node_2).all_brokers().size() == 3;
      })
      .get0();

    remove_node_application(node_2);
    // Change RPC port from 11000 to 13000
    info("Restarting node {} with changed configuration", node_2);
    node_app_2 = create_node_application(node_2, 9092, 13000);

    tests::cooperative_spin_wait_with_timeout(
      5s,
      [this, node_0, node_1, node_2] {
          auto updated_broker = get_local_cache(node_0).get_broker(node_2);
          if (!updated_broker) {
              return false;
          }
          auto broker = get_local_cache(node_1).get_broker(node_2);
          if (!broker || *updated_broker.value() != *broker.value()) {
              return false;
          }

          broker = get_local_cache(node_1).get_broker(node_2);
          if (!broker || *updated_broker.value() != *broker.value()) {
              return false;
          }

          return updated_broker.value()->rpc_address()
                 == unresolved_address("127.0.0.1", 13002);
      })
      .get0();
}

FIXTURE_TEST(test_single_node_update, cluster_test_fixture) {
    // single node
    model::node_id node_id(0);

    auto node = create_node_application(node_id);
    wait_for_controller_leadership(node_id).get();

    remove_node_application(node_id);
    // Change kafka port from 9092 to 15000
    node = create_node_application(node_id, 15000, 13000);

    tests::cooperative_spin_wait_with_timeout(5s, [this, node_id] {
        auto updated_broker = get_local_cache(node_id).get_broker(node_id);
        info("updated broker {}", updated_broker);
        if (!updated_broker) {
            return false;
        }

        return updated_broker.value()->kafka_advertised_listeners()[0].address
               == unresolved_address("127.0.0.1", 15000);
    }).get0();
}
