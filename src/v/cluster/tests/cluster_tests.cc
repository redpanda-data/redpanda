#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"
using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(test_join_single_node, cluster_test_fixture) {
    add_controller(
      model::node_id{1},
      ss::smp::count,
      9092,
      11000,
      {{.id = model::node_id{1},
        .addr = unresolved_address("127.0.0.1", 11000)}});
    auto& cntrl = get_controller(0);
    cntrl.start().get0();
    wait_for_leadership(cntrl);

    auto brokers = get_local_cache(0).all_brokers();

    // single broker
    BOOST_REQUIRE_EQUAL(cntrl.is_leader(), true);
    BOOST_REQUIRE_EQUAL(brokers.size(), 1);
    BOOST_REQUIRE_EQUAL(brokers[0]->id(), model::node_id(1));
}

// Enable when Raft implementation will be working
#if 0
FIXTURE_TEST(test_join_on_behalf_of_other_controller, cluster_test_fixture) {
    add_controller(
      model::node_id{1},
      smp::count,
      9092,
      11000,
      {{.id = model::node_id{1},
        .addr = unresolved_address("127.0.0.1", 11000)}});
    // this
    add_controller(
      model::node_id{2},
      smp::count,
      9093,
      11001,
      {{.id = model::node_id{1},
        .addr = unresolved_address("127.0.0.1", 11000)}});

    auto& cntrl_0 = get_controller(0);
    cntrl_0.start().get0();
    wait_for_leadership(cntrl_0);
    auto& cntrl_1 = get_controller(1);
    cntrl_1.start().get0();

    // Check if we have one leader
    BOOST_REQUIRE_EQUAL(cntrl_0.is_leader(), true);
    BOOST_REQUIRE_EQUAL(cntrl_1.is_leader(), false);
    // Check if all brokers were registered
    wait_for(
      2s, [this] { return get_local_cache(0).all_brokers().size() == 2; });
    auto brokers = get_local_cache(0).all_brokers();
    BOOST_REQUIRE_EQUAL(brokers.size(), 2);
    // FIXME: Add checks in second metadata cache
}

FIXTURE_TEST(test_dispatching_to_leader, cluster_test_fixture) {
    add_controller(
      model::node_id{1},
      smp::count,
      9092,
      11000,
      {{.id = model::node_id{1},
        .addr = unresolved_address("127.0.0.1", 11000)}});
    add_controller(
      model::node_id{2},
      smp::count,
      9093,
      11001,
      {{.id = model::node_id{1},
        // Connects directly to leader
        .addr = unresolved_address("127.0.0.1", 11000)}});
    add_controller(
      model::node_id{3},
      smp::count,
      9094,
      11002,
      // Connects to non leader controller
      {{.id = model::node_id{2},
        .addr = unresolved_address("127.0.0.1", 11001)}});

    auto& cntrl_0 = get_controller(0);
    cntrl_0.start().get0();
    wait_for_leadership(cntrl_0);
    auto& cntrl_1 = get_controller(1);
    cntrl_1.start().get0();
    auto& cntrl_2 = get_controller(2);
    cntrl_2.start().get0();

    auto brokers = get_local_cache(0).all_brokers();
    // Check if we have one leader
    BOOST_REQUIRE_EQUAL(cntrl_0.is_leader(), true);
    BOOST_REQUIRE_EQUAL(cntrl_1.is_leader(), false);
    BOOST_REQUIRE_EQUAL(cntrl_2.is_leader(), false);
    // Check if all brokers were registered
    BOOST_REQUIRE_EQUAL(brokers.size(), 3);
    // FIXME: Add checks in others metadata cache when entries replication,
    //        will be working
}
#endif
