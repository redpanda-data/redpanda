// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "config/tls_config.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "rpc/logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

#include <chrono>

SEASTAR_THREAD_TEST_CASE(connection_allocation_strategy_add_test) {
    constexpr int number_of_nodes = 10;
    constexpr int max_connections_per_node = 8;
    constexpr int shards_per_node = 80;
    static_assert(
      shards_per_node % max_connections_per_node == 0,
      "Max connections per node needs to be a multiple of shards_per_node");
    static_assert(
      (number_of_nodes * max_connections_per_node) % shards_per_node == 0,
      "Total connections need to be multiple of shards");

    rpc::connection_allocation_strategy alloc_strat(
      max_connections_per_node, shards_per_node);

    absl::flat_hash_map<ss::shard_id, size_t> shards_with_con;

    for (int i = 0; i < number_of_nodes; i++) {
        auto change = alloc_strat.create_connection_assignments_for(
          model::node_id(i));

        BOOST_REQUIRE_EQUAL(
          change.add_connections.size(), max_connections_per_node);
        BOOST_REQUIRE_EQUAL(change.add_mapping.size(), shards_per_node);

        absl::flat_hash_map<ss::shard_id, size_t> shards_depending_on_con;

        for (auto con : change.add_connections) {
            shards_depending_on_con[con.shard] = 0;
            shards_with_con[con.shard] += 1;
        }

        for (auto dep : change.add_mapping) {
            shards_depending_on_con[dep.conn_shard]++;
        }

        for (auto& [shard, count] : shards_depending_on_con) {
            BOOST_REQUIRE_EQUAL(
              count, shards_per_node / max_connections_per_node);
        }
    }

    for (auto& [shard, count] : shards_with_con) {
        BOOST_REQUIRE_EQUAL(
          count,
          (number_of_nodes * max_connections_per_node) / shards_per_node);
    }
}

SEASTAR_THREAD_TEST_CASE(connection_allocation_strategy_remove_test) {
    constexpr int max_connections_per_node = 5;
    constexpr int shards_per_node = 17;

    rpc::connection_allocation_strategy alloc_strat(
      max_connections_per_node, shards_per_node);
    auto change_add = alloc_strat.create_connection_assignments_for(
      model::node_id(0));
    auto change_rm = alloc_strat.remove_connection_assignments_for(
      model::node_id(0));

    BOOST_REQUIRE_EQUAL(
      change_rm.remove_connections.size(), max_connections_per_node);
    BOOST_REQUIRE_EQUAL(change_rm.remove_mapping.size(), shards_per_node);

    BOOST_REQUIRE_EQUAL(
      change_add.add_connections.size(), change_rm.remove_connections.size());
    for (auto con_add : change_add.add_connections) {
        auto con_rm = std::find_if(
          change_rm.remove_connections.begin(),
          change_rm.remove_connections.end(),
          [con_add](auto con_rm) {
              return con_rm.node == con_add.node
                     && con_rm.shard == con_add.shard;
          });

        BOOST_REQUIRE(con_rm != change_rm.remove_connections.end());
    }

    BOOST_REQUIRE_EQUAL(
      change_add.add_mapping.size(), change_rm.remove_mapping.size());
    for (auto map_add : change_add.add_mapping) {
        auto map_rm = std::find_if(
          change_rm.remove_mapping.begin(),
          change_rm.remove_mapping.end(),
          [map_add](auto map_rm) {
              return map_add.node == map_rm.node
                     && map_add.conn_shard == map_rm.shard;
          });

        BOOST_REQUIRE(map_rm != change_rm.remove_mapping.end());
    }
}
