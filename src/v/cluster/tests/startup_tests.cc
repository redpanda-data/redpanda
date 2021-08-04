
#include "cluster/tests/rebalancing_tests_fixture.h"
#include "model/namespace.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

#include <exception>

FIXTURE_TEST(test_three_node_startup, rebalancing_tests_fixture) {
    constexpr model::node_id node_0(0);
    constexpr model::node_id node_1(1);
    constexpr model::node_id node_2(2);
    constexpr model::node_id victim(2);
    const auto any_under_replicated = [this, victim]() -> ss::future<bool> {
        for (auto [id, _] : apps) {
            if (id != victim) {
                if (co_await is_node_under_replicated(id, victim)) {
                    co_return true;
                }
            }
        }
        co_return false;
    };
    const auto none_under_replicated = [this, victim]() -> ss::future<bool> {
        for (auto [id, _] : apps) {
            if (id != victim) {
                if (co_await is_node_under_replicated(id, victim)) {
                    co_return false;
                }
            }
        }
        co_return true;
    };

    test_logger.info("starting cluster of 3");
    start_cluster(3);

    test_logger.info("creating 3 topics");
    create_topic(create_topic_cfg("test-1", 3, 3));
    create_topic(create_topic_cfg("test-2", 3, 3));
    create_topic(create_topic_cfg("test-3", 3, 3));

    test_logger.info("populating all topics");
    populate_all_topics_with_data();

    test_logger.info("abdicating node: {}", victim);
    node_application(2)
      ->controller->get_members_frontend()
      .local()
      .abdicate_node(node_2)
      .get();

    test_logger.info("removing node: {}", victim);
    remove_node(victim);

    test_logger.info(
      "waiting for node 2 to not be a leader of any partition: {}", victim);
    tests::cooperative_spin_wait_with_timeout(60s, [this, node_0, victim] {
        auto md = get_local_cache(node_0).all_topics_metadata();
        return std::all_of(md.begin(), md.end(), [victim](const auto& tm) {
            return std::all_of(
              tm.partitions.begin(),
              tm.partitions.end(),
              [victim](const auto& p) { return p.leader_node != victim; });
        });
    }).get0();

    test_logger.info("populating all topics");
    populate_all_topics_with_data();

    test_logger.info("waiting for node {} to be under replicated", victim);
    tests::cooperative_spin_wait_with_timeout(60s, any_under_replicated).get();

    test_logger.info("Restarting node {}", victim);
    add_node(2);

    test_logger.info(
      "Expecting node: {} to self-report under_replicated", victim);
    BOOST_REQUIRE(is_node_under_replicated(victim, victim).get());

    test_logger.info("waiting for node {} to not be under replicated", victim);
    tests::cooperative_spin_wait_with_timeout(60s, none_under_replicated).get();

    test_logger.info("Expecting node: {} not under_replicated", 2);
    BOOST_REQUIRE(!is_node_under_replicated(victim, victim).get());
}
