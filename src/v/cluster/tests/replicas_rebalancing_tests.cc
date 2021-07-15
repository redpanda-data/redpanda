
#include "cluster/metadata_cache.h"
#include "cluster/tests/rebalancing_tests_fixture.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "test_utils/async.h"

#include <absl/container/node_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

#include <algorithm>

absl::node_hash_map<model::node_id, size_t>
calculate_replicas_per_node(const cluster::metadata_cache& cache) {
    absl::node_hash_map<model::node_id, size_t> ret;

    for (auto& tp_md : cache.all_topics_metadata()) {
        if (tp_md.tp_ns.ns == model::redpanda_ns) {
            continue;
        }
        for (auto& p_md : tp_md.partitions) {
            for (auto replica : p_md.replicas) {
                ret[replica.node_id]++;
            }
        }
    }
    return ret;
}

void wait_for_even_replicas_distribution(
  int min_expected,
  int max_expected,
  model::node_id added_node,
  const cluster::metadata_cache& cache) {
    tests::cooperative_spin_wait_with_timeout(
      30s,
      [&cache, added_node, min_expected, max_expected] {
          auto per_node = calculate_replicas_per_node(cache);
          for (auto& [id, count] : per_node) {
              fmt::print("node/replicas {}:{}\n", id, count);
          };
          if (!per_node.contains(added_node)) {
              return false;
          }

          return std::all_of(
            per_node.begin(),
            per_node.end(),
            [min_expected, max_expected](const auto& p) {
                return p.second < max_expected && p.second > min_expected;
            });
      })
      .get();
}

FIXTURE_TEST(test_adding_single_node_to_cluster, rebalancing_tests_fixture) {
    start_cluster(3);
    // in total we have 24 partition replicas
    create_topic(create_topic_cfg("test-1", 3, 1));
    create_topic(create_topic_cfg("test-2", 4, 3));
    create_topic(create_topic_cfg("test-3", 1, 3));
    create_topic(create_topic_cfg("test-4", 2, 3));
    populate_all_topics_with_data();

    // add node
    add_node(10);

    // wait until all the nodes will host between 4 and 8 replicas
    wait_for_even_replicas_distribution(
      3, 9, model::node_id(10), apps.begin()->second->metadata_cache.local());
}

FIXTURE_TEST(test_adding_multiple_nodes, rebalancing_tests_fixture) {
    start_cluster(1);
    // start with 36 partition replicas on single node
    create_topic(create_topic_cfg("test-1", 12, 1));
    create_topic(create_topic_cfg("test-2", 24, 1));

    populate_all_topics_with_data();
    // add node
    add_node(2);

    // wait until all nodes will host between 16 and 20 partitons
    wait_for_even_replicas_distribution(
      14, 22, model::node_id(2), apps.begin()->second->metadata_cache.local());

    // add second node
    add_node(3);
    // wait until all nodes will host between 10 and 14 partitons
    wait_for_even_replicas_distribution(
      8, 16, model::node_id(3), apps.begin()->second->metadata_cache.local());
}
