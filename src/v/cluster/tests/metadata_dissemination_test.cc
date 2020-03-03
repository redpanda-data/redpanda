#include "cluster/controller.h"
#include "cluster/metadata_cache.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"

#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

#include <vector>
using namespace std::chrono_literals; // NOLINT

std::vector<model::node_id>
wait_for_leaders_updates(int id, cluster::metadata_cache& cache) {
    std::vector<model::node_id> leaders;
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache, &leaders, id] {
          leaders.clear();
          auto tp_md = cache.get_topic_metadata(model::topic("test_1"));
          if (!tp_md) {
              return false;
          }
          if (tp_md->partitions.size() != 3) {
              return false;
          }
          for (auto& p_md : tp_md->partitions) {
              if (!p_md.leader_node) {
                  return false;
              }
              leaders.push_back(*p_md.leader_node);
          }
          return true;
      })
      .get0();
    return leaders;
}

FIXTURE_TEST(
  test_metadata_dissemination_from_single_partition, cluster_test_fixture) {
    auto& cntrl_0 = create_controller(model::node_id{0});
    auto& cntrl_1 = create_controller(model::node_id{1});
    auto& cntrl_2 = create_controller(model::node_id{2});

    cntrl_0.start().get();
    cntrl_0.wait_for_leadership().get0();

    cntrl_1.start().get0();
    cntrl_2.start().get0();

    auto& cache_0 = get_local_cache(0);
    auto& cache_1 = get_local_cache(1);
    auto& cache_2 = get_local_cache(2);

    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache_1, &cache_2] {
          return cache_1.all_brokers().size() == 3
                 && cache_2.all_brokers().size() == 3;
      })
      .get0();

    // Make sure we have 3 working nodes
    BOOST_REQUIRE_EQUAL(cache_0.all_brokers().size(), 3);
    BOOST_REQUIRE_EQUAL(cache_1.all_brokers().size(), 3);
    BOOST_REQUIRE_EQUAL(cache_2.all_brokers().size(), 3);

    // Create topic with replication factor 1
    std::vector<cluster::topic_configuration> topics;
    topics.emplace_back(model::ns("default"), model::topic("test_1"), 3, 1);
    cntrl_0.create_topics(std::move(topics), model::no_timeout).get0();

    auto leaders_0 = wait_for_leaders_updates(0, cache_0);
    auto leaders_1 = wait_for_leaders_updates(1, cache_1);
    auto leaders_2 = wait_for_leaders_updates(2, cache_2);

    BOOST_REQUIRE_EQUAL(leaders_0, leaders_1);
    BOOST_REQUIRE_EQUAL(leaders_0, leaders_2);
}

FIXTURE_TEST(test_metadata_dissemination_joining_node, cluster_test_fixture) {
    auto& cntrl_0 = create_controller(model::node_id{0});
    auto& cntrl_1 = create_controller(model::node_id{1});

    cntrl_0.start().get();
    cntrl_0.wait_for_leadership().get0();

    cntrl_1.start().get0();

    auto& cache_0 = get_local_cache(0);
    auto& cache_1 = get_local_cache(1);

    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache_1] { return cache_1.all_brokers().size() == 2; })
      .get0();
    // Make sure we have 2 working nodes
    BOOST_REQUIRE_EQUAL(cache_0.all_brokers().size(), 2);
    BOOST_REQUIRE_EQUAL(cache_1.all_brokers().size(), 2);

    // Create topic with replication factor 1
    std::vector<cluster::topic_configuration> topics;
    topics.emplace_back(model::ns("default"), model::topic("test_1"), 3, 1);
    cntrl_0.create_topics(std::move(topics), model::no_timeout).get0();

    // Add new now to the cluster
    auto& cntrl_2 = create_controller(model::node_id{2});
    cntrl_2.start().get0();
    auto& cache_2 = get_local_cache(2);
    // Wait for node to join the cluster
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache_1, &cache_2] {
          return cache_1.all_brokers().size() == 3
                 && cache_2.all_brokers().size() == 3;
      })
      .get0();

    auto leaders_0 = wait_for_leaders_updates(0, cache_0);
    auto leaders_1 = wait_for_leaders_updates(1, cache_1);
    auto leaders_2 = wait_for_leaders_updates(2, cache_2);
}