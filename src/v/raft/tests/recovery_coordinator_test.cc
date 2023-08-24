

#include "config/mock_property.h"
#include "raft/recovery_coordinator.h"
#include "test_utils/fixture.h"
#include "vlog.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/testing/thread_test_case.hh>

using recovery_coordinator_manual
  = raft::recovery_coordinator_ticker<ss::manual_clock>;

using namespace std::chrono_literals;

static ss::logger logger("test");

struct coordinator_fixture {
    config::mock_property<std::chrono::milliseconds> period{100ms};
    config::mock_property<std::chrono::milliseconds> grace_period{1000ms};
    config::mock_property<size_t> concurrency{1};

    coordinator_fixture()
      : coordinator(concurrency.bind(), period.bind(), grace_period.bind()) {
        coordinator.start().get();
    }

    ~coordinator_fixture() { coordinator.stop().get(); }

    void advance_time(std::chrono::milliseconds d) {
        ss::manual_clock::advance(d);

        // Hack: prompt seastar runtime to actually schedule and run the
        // tasks that were made ready by advancing the manual clock.
        ss::sleep(10ms).get();
    }

    recovery_coordinator_manual coordinator;
};

FIXTURE_TEST(test_recovery_basic, coordinator_fixture) {
    raft::recovery_coordinator_base* r = &coordinator;

    // Set one recovery at a time, check that the limit is respected
    // and the partitions go through in the expected order
    concurrency.update(1);

    // Exercise ordering rules:
    // - internal topics come first
    // - user topics are executed in order of how many offsets remain, smallest
    //   first
    auto p_internal = std::make_unique<raft::follower_recovery_state>(
      r, model::legacy_tm_ntp, model::offset{0}, model::offset{10}, false);
    auto p_user_small = std::make_unique<raft::follower_recovery_state>(
      r,
      model::ntp("kafka", "smalltopic", 0),
      model::offset{0},
      model::offset{10},
      false);
    auto p_user_big = std::make_unique<raft::follower_recovery_state>(
      r,
      model::ntp("kafka", "bigtopic", 0),
      model::offset{0},
      model::offset{100},
      false);

    // The internal partition should have been fast-started
    BOOST_REQUIRE(p_internal->recovering == true);
    BOOST_REQUIRE(coordinator.get_status().partitions_active == 1);

    // At the next tick, only the internal partition should
    // still be in progress, status should have updated
    advance_time(grace_period() + 1ms);
    BOOST_REQUIRE(p_internal->recovering == true);
    BOOST_REQUIRE(p_user_small->recovering == false);
    BOOST_REQUIRE(p_user_big->recovering == false);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_pending, 2);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_active, 1);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().offsets_pending, 120);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().offsets_hwm, 120);

    p_internal->update(model::offset{5});

    // Status doesn't update until next tick
    advance_time(period() / 2);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_pending, 2);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_active, 1);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().offsets_pending, 120);

    advance_time(period() / 2 + 1ms);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_active, 1);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().offsets_pending, 115);

    // Internal topic's recovery finishes
    p_internal.reset();

    advance_time(period() + 1ms);
    // p_user_small recovery starts
    BOOST_REQUIRE_EQUAL(p_user_small->recovering, true);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_active, 1);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_pending, 1);

    // p_user_small recovery completes
    p_user_small.reset();

    advance_time(period() + 1ms);
    // p_user_big recovery starts
    BOOST_REQUIRE_EQUAL(p_user_big->recovering, true);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_active, 1);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_pending, 0);

    // p_user_big recovery completes
    p_user_big.reset();

    // All recoveries done
    advance_time(period() + 1ms);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_active, 0);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().partitions_pending, 0);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().offsets_pending, 0);
    BOOST_REQUIRE_EQUAL(coordinator.get_status().offsets_hwm, 120);
}

FIXTURE_TEST(test_recovery_grace, coordinator_fixture) {
    raft::recovery_coordinator_base* r = &coordinator;

    // Set one recovery at a time, check that the limit is respected
    // and the partitions go through in the expected order
    concurrency.update(1);

    auto p_user = std::make_unique<raft::follower_recovery_state>(
      r,
      model::ntp("kafka", "topic", 0),
      model::offset{10},
      model::offset{20},
      false);

    BOOST_REQUIRE_EQUAL(p_user->recovering, false);

    // Advancing the usual period should not result in a tick, we
    // are still in grace.
    advance_time(period() + 1ms);
    BOOST_REQUIRE_EQUAL(p_user->recovering, false);

    // Advancing past the grace period should result in a tick
    advance_time(grace_period() - period() + 1ms);
    BOOST_REQUIRE_EQUAL(p_user->recovering, true);
}

// TODO: test fast dispatch of internal topics when units are available
// TODO: test handling of already_recovering=true follower states
