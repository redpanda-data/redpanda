// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/archival/archiver_manager.h"
#include "cluster/archival/tests/archival_service_fixture.h"
#include "cluster/archival/types.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "http/tests/http_imposter.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>

#include <type_traits>

namespace {
static ss::logger arch_test_log("archival_service_test");
} // namespace

// Check that the archiver_service receives notifications and manages the
// partitions correctly. When the topic is created the archiver_service should
// receive notification about all new partitions on the same shard and start
// managing them. When partition becomes a leader it should create and start
// the archiver for it. When the partition is deleted it should stop managing
// it.
FIXTURE_TEST(
  test_archiver_service_manage_notifications, archiver_cluster_fixture) {
    auto node_id0 = add_node();
    auto node_id1 = add_node();
    auto node_id2 = add_node();
    auto node_id3 = add_node();

    std::vector<model::node_id> replica_set1 = {node_id0, node_id1, node_id2};

    std::vector<model::node_id> replica_set2 = {node_id0, node_id1, node_id3};

    wait_for_controller_leadership(node_id0).get();

    wait_for_all_members(3s).get();

    // create topic
    auto panda_topic1 = create_topic(
      model::topic("panda-topic1"), 5, replica_set1);

    wait_all_partitions_managed(panda_topic1);
    wait_all_partition_leaders(panda_topic1);

    auto s1 = set_of_managed_partitions();
    std::set<model::ntp> pt1;
    pt1.insert(panda_topic1.begin(), panda_topic1.end());
    BOOST_REQUIRE(pt1 == s1);

    auto panda_topic2 = create_topic(
      model::topic("panda-topic2"), 5, replica_set2);

    decltype(panda_topic2) all_panda_topics;
    std::copy(
      panda_topic1.begin(),
      panda_topic1.end(),
      std::back_inserter(all_panda_topics));
    std::copy(
      panda_topic2.begin(),
      panda_topic2.end(),
      std::back_inserter(all_panda_topics));

    // all panda topics are sorted
    wait_all_partitions_managed(all_panda_topics);
    wait_all_partition_leaders(all_panda_topics);

    delete_topic(model::topic("panda-topic1"));
    wait_all_partitions_managed(panda_topic2);
    wait_all_partition_leaders(panda_topic2);

    delete_topic(model::topic("panda-topic2"));
    wait_all_partitions_managed({});

    vlog(arch_test_log.info, "Test run completed");
}

// Check that archiver_service manages partitions correctly
// when the partitions are moved both between the nodes and
// between the shards.
FIXTURE_TEST(
  test_archiver_service_partition_movement_notifications,
  archiver_cluster_fixture) {
    auto node_id0 = add_node();
    wait_for_controller_leadership(node_id0).get();
    auto node_id1 = add_node();
    auto node_id2 = add_node();
    auto node_id3 = add_node();
    std::vector<model::node_id> replica_set1 = {node_id0, node_id1, node_id2};
    std::vector<model::node_id> replica_set2 = {node_id3, node_id1, node_id2};

    wait_for_all_members(3s).get();

    // create topic with one partition and one replica
    auto panda_topic = create_topic(
      model::topic("panda-topic"), 1, replica_set1);
    for (auto id : replica_set1) {
        wait_all_partitions_managed(panda_topic, id);
    }
    wait_all_partition_leaders(panda_topic);

    vlog(
      arch_test_log.info,
      "Moving partition {} from node 0 to 3",
      panda_topic.back());

    // move one partition
    move_partition(panda_topic.back(), replica_set2);
    wait_partition_movement_complete(panda_topic.back());

    for (auto id : replica_set2) {
        wait_all_partitions_managed(panda_topic, id);
    }
    wait_all_partition_leaders(panda_topic);

    for (int i = 0; i < 10; i++) {
        // force x-shard movement on all replicas including
        // the leader
        auto replica_set3 = replica_locations(panda_topic.back());
        for (auto& n_s : replica_set3) {
            n_s.shard = n_s.shard == 0 ? 1 : 0;
        }

        vlog(
          arch_test_log.info,
          "Moving partition {} x-shard",
          panda_topic.back());

        move_partition(panda_topic.back(), replica_set3);
        wait_partition_movement_complete(panda_topic.back(), replica_set3);

        for (auto [id, shard] : replica_set3) {
            wait_all_partitions_managed(panda_topic, id);
        }
        wait_all_partition_leaders(panda_topic);
    }

    vlog(arch_test_log.info, "Test run completed");
}

// Check that archiver_service follows leadership transfers
// in presence of partition movements correctly. Test generates
// transfers/movements in a very unreasonable manner in a loop
// up to 100 times.
FIXTURE_TEST(test_archiver_service_torture_test, archiver_cluster_fixture) {
    auto node_id0 = add_node();
    auto node_id1 = add_node();
    auto node_id2 = add_node();
    auto node_id3 = add_node();
    std::vector<model::node_id> all_nodes = {
      node_id0, node_id1, node_id2, node_id3};
    std::vector<model::node_id> replica_set = {node_id0, node_id1, node_id2};

    // The test is using only shard 0 and all 'nodes' are sharing the same
    // config values stored in the TSL.
    scoped_config cfg;
    cfg.get("cloud_storage_disable_upload_loop_for_tests").set_value(true);

    wait_for_controller_leadership(node_id0).get();
    wait_for_all_members(3s).get();

    // create topic with one partition and one replica
    auto panda_topic = create_topic(
      model::topic("panda-topic"), 1, replica_set);
    std::vector<model::ntp> all_ntp(panda_topic.begin(), panda_topic.end());
    for (auto id : replica_set) {
        wait_all_partitions_managed(panda_topic, id);
    }
    wait_all_partition_leaders(panda_topic);

#ifdef NDEBUG
    const int num_iterations = 100;
#else
    const int num_iterations = 20;
#endif
    const std::chrono::seconds max_time = 300s;

    auto test_start = ss::lowres_clock::now();
    for (int i = 0; i < num_iterations; i++) {
        if (ss::lowres_clock::now() - test_start > max_time) {
            vlog(arch_test_log.info, "Time limit reached");
            break;
        }
        for (auto ntp : panda_topic) {
            // On every iteration we either shuffling the leadership or moving
            // one of the replicas between the nodes or performing the
            // cross-shard partition movement
            constexpr int max_r_val = 9;
            constexpr int leadership_transfer_threshold = 5;
            constexpr int x_shard_move_threshold = 8;
            int r_val = random_generators::get_int(0, max_r_val);
            if (r_val < leadership_transfer_threshold) {
                vlog(arch_test_log.info, "Moving leadership for {}", ntp);
                shuffle_leadership_smp(ntp);
            } else if (r_val < x_shard_move_threshold) {
                // Move partition to another shard on every replica
                auto ntp = random_generators::random_choice(all_ntp);
                vlog(arch_test_log.info, "Moving partition {} x-shard", ntp);

                // Flip all shards
                auto loc_list = replica_locations(ntp);
                for (auto& n_s : loc_list) {
                    n_s.shard = n_s.shard == 0 ? 1 : 0;
                }
                move_partition(panda_topic.back(), loc_list);
                wait_partition_movement_complete(
                  panda_topic.back(), loc_list, 60s);
            } else {
                // Move partition to another node
                auto ntp = random_generators::random_choice(all_ntp);
                auto loc_list = replica_locations(ntp);
                auto old_node = random_generators::random_choice(loc_list);
                auto it = std::find_if_not(
                  all_nodes.begin(),
                  all_nodes.end(),
                  [&loc_list](model::node_id n) {
                      for (auto l : loc_list) {
                          if (l.node_id == n) {
                              return true;
                          }
                      }
                      return false;
                  });

                BOOST_REQUIRE(it != all_nodes.end());
                auto new_node = *it;
                vlog(
                  arch_test_log.info,
                  "Moving partition {} from node {} to node {}",
                  ntp,
                  old_node,
                  new_node);

                // Perform the actual move
                std::vector<model::node_id> replica_set;
                for (auto [node, shard] : loc_list) {
                    if (node == old_node.node_id) {
                        replica_set.push_back(new_node);
                    } else {
                        replica_set.push_back(node);
                    }
                }
                move_partition(panda_topic.back(), replica_set);
                wait_partition_movement_complete(panda_topic.back(), 60s);
            }
        }
        wait_all_partition_leaders(panda_topic);
    }

    vlog(arch_test_log.info, "Test run completed");
}
