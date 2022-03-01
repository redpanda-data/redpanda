
#include "config/mock_property.h"
#include "net/conn_quota.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/preempt.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>

static ss::logger logger("test");

using namespace net;
using namespace tests;
using namespace std::chrono_literals;

conn_quota::config_fn get_config_fn(
  std::optional<uint32_t> max_con, std::optional<uint32_t> max_con_per_ip) {
    return [max_con, max_con_per_ip]() mutable {
        return conn_quota_config{
          .max_connections = config::mock_binding<std::optional<uint32_t>>(
            std::move(max_con)),
          .max_connections_per_ip
          = config::mock_binding<std::optional<uint32_t>>(
            std::move(max_con_per_ip))};
    };
}

// Some made up addresses to make tests terser
static ss::net::inet_address addr1("10.0.0.1");
static ss::net::inet_address addr2("10.0.0.2");
static ss::net::inet_address addr3("10.0.0.3");

struct conn_quota_fixture {
    conn_quota_fixture() {}

    ~conn_quota_fixture() {
        drop_shard_units();
        scq.stop().get();
        max_con_per_ip.stop().get();
        max_con.stop().get();
    }

    void start(
      std::optional<uint32_t> max_con_,
      std::optional<uint32_t> max_con_per_ip_) {
        max_con.start(max_con_).get();
        max_con_per_ip.start(max_con_per_ip_).get();
        scq
          .start([this]() {
              return conn_quota_config{
                .max_connections = max_con.local().bind(),
                .max_connections_per_ip = max_con_per_ip.local().bind()};
          })
          .get();
    }

    std::vector<conn_quota::units>
    take_units(ss::net::inet_address addr, size_t n) {
        std::vector<conn_quota::units> units;
        for (size_t i = 0; i < n; ++i) {
            auto u = scq.local().get(addr).get();
            BOOST_TEST_REQUIRE(u.live());
            units.push_back(std::move(u));
        }
        return units;
    }

    ss::future<> expect_no_units(ss::net::inet_address addr) {
        auto dead_unit = co_await scq.local().get(addr);
        BOOST_TEST_REQUIRE(!dead_unit.live());
    }

    void drop_shard_units() {
        for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
            scq
              .invoke_on(i, [i, this](conn_quota& cq) { shard_units.erase(i); })
              .get();
        }
    }

    void set_limit(
      ss::sharded<config::mock_property<std::optional<uint32_t>>>& limit,
      std::optional<uint32_t> new_value) {
        limit
          .invoke_on_all(
            [new_value](config::mock_property<std::optional<uint32_t>>& p) {
                p.update(std::optional<uint32_t>(new_value));
            })
          .get();
    }

    void take_on_shard(
      ss::shard_id shard, ss::net::inet_address addr, uint32_t take_units) {
        scq
          .invoke_on(
            shard,
            [shard, this, take_units, addr](conn_quota& cq) -> ss::future<> {
                for (unsigned int j = 0; j < take_units; ++j) {
                    auto u = co_await cq.get(addr);
                    BOOST_TEST_REQUIRE(u.live());
                    shard_units[shard].push_back(std::move(u));
                }
            })
          .get();
    }

    void drop_on_shard(
      ss::shard_id shard, ss::net::inet_address addr, uint32_t take_units) {
        assert(shard_units[shard].size() >= take_units);

        scq
          .invoke_on(
            shard,
            [shard, this, take_units, addr](conn_quota& cq) {
                for (size_t i = 0; i < take_units; ++i) {
                    shard_units[shard].pop_back();
                }
            })
          .get();
    }

    /**
     * Helper for acquiring units on all the shards at once.
     */
    void take_on_all(uint32_t take_units, ss::net::inet_address addr = addr1) {
        for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
            take_on_shard(i, addr, take_units);
        }
    }

    void test_borrows(
      unsigned int,
      unsigned int,
      std::optional<uint32_t>,
      std::optional<uint32_t>);

    ss::sharded<conn_quota> scq;
    ss::sharded<config::mock_property<std::optional<uint32_t>>> max_con;
    ss::sharded<config::mock_property<std::optional<uint32_t>>> max_con_per_ip;

    // Stash your foreign shard units here, so that the fixture can
    // clean them up for you on destruction (e.g. when handling exception)
    std::map<ss::shard_id, std::vector<conn_quota::units>> shard_units;
};

FIXTURE_TEST(test_total_limit, conn_quota_fixture) {
    start(10, std::nullopt);

    std::vector<conn_quota::units> units;
    for (size_t i = 0; i < 10; ++i) {
        auto u = scq.local().get(addr1).get();
        BOOST_TEST_REQUIRE(u.live());
        units.push_back(std::move(u));
    }

    auto dead_unit = scq.local().get(addr1).get();
    BOOST_TEST_REQUIRE(!dead_unit.live());

    // Other addresses should also be limited by hitting the total limit
    dead_unit = scq.local().get(addr2).get();
    BOOST_TEST_REQUIRE(!dead_unit.live());
    dead_unit = scq.local().get(addr3).get();
    BOOST_TEST_REQUIRE(!dead_unit.live());

    // Releasing one unit should enable another address to get a unit
    units.pop_back();

    auto live_unit = scq.local().get(addr3).get();
    BOOST_TEST_REQUIRE(live_unit.live());
}

FIXTURE_TEST(test_addr_limits, conn_quota_fixture) {
    start(4, 2);

    vlog(logger.debug, "Taking 2 from addr1");
    auto addr1_units = take_units(addr1, 2);
    vlog(logger.debug, "Checking exhausted on addr1");
    expect_no_units(addr1).get();
    vlog(logger.debug, "Taking 2 from addr2");
    auto addr2_units = take_units(addr2, 2);
    vlog(logger.debug, "Checking exhausted on addr2");
    expect_no_units(addr2).get();

    // We have hit total limit.  Even though addr3 still has allowance, it
    // is refused.
    vlog(logger.debug, "Checking exhausted on addr3");
    expect_no_units(addr3).get();

    addr1_units.clear();
    auto addr3_units = take_units(addr3, 2);
}

/**
 * For testing token borrowing across shards.  May either exercise
 * per-IP or total limit, depending on args.
 *
 * @param core_count pretend there are this many shards, i.e. only take
 *                   units on these shards.
 * @param take_each  how many units to take on each shard
 * @param max_con    as for start()
 * @param max_con_per_ip  as for start()
 */
void conn_quota_fixture::test_borrows(
  unsigned int core_count,
  unsigned int take_each,
  std::optional<uint32_t> max_con,
  std::optional<uint32_t> max_con_per_ip) {
    start(max_con, max_con_per_ip);

    // Take one unit on each shard
    vlog(logger.debug, "Take units");
    for (ss::shard_id i = 0; i < core_count; ++i) {
        scq
          .invoke_on(
            i,
            [i, take_each, this](conn_quota& cq) -> ss::future<> {
                for (unsigned int j = 0; j < take_each; ++j) {
                    auto u = co_await cq.get(addr1);
                    BOOST_TEST_REQUIRE(u.live());
                    shard_units[i].push_back(std::move(u));
                }
            })
          .get();
    }

    // All shards should now see no units available
    vlog(logger.debug, "Check allowances used up");
    for (ss::shard_id i = 0; i < core_count; ++i) {
        scq
          .invoke_on(
            i, [this](conn_quota& cq) { return expect_no_units(addr1); })
          .get();
    }

    // Release a unit, then try taking it on a different shard.  This triggers
    // a reclaim.
    vlog(logger.debug, "Trigger a reclaim");
    scq.invoke_on(1, [this](conn_quota& cq) { shard_units.erase(1); }).get();
    scq
      .invoke_on(
        2,
        [this, i = 2](conn_quota& cq) -> ss::future<> {
            auto u = co_await cq.get(addr1);
            BOOST_TEST_REQUIRE(u.live());
            shard_units[i].push_back(std::move(u));
        })
      .get();

    // Clean up units on their respective cores
    vlog(logger.debug, "Dropping all units");
    drop_shard_units();

    // Now that all units are released, we should find that reclaim
    // flag is switched off everywhere.
    vlog(logger.debug, "Checking reclaim status");
    ss::thread::yield(); // give the backgrounded part of reclaim_to a chance
    cooperative_spin_wait_with_timeout(5s, [core_count, this]() {
        bool any_in_reclaim = false;

        for (ss::shard_id i = 0; i < core_count; ++i) {
            scq
              .invoke_on(
                i,
                [&any_in_reclaim](conn_quota& cq) {
                    any_in_reclaim |= cq.test_only_is_in_reclaim({});
                    any_in_reclaim = cq.test_only_is_in_reclaim(addr1);
                })
              .get();
        }

        return !any_in_reclaim;
    }).get();
}

FIXTURE_TEST(test_total_borrows, conn_quota_fixture) {
    auto core_count = ss::smp::count;

    // This test needs at least a few cores.  If you run it with -c 1 it
    // won't work.  We're testing sharded logic so we really do need multiple
    // shards for it to be a valid test.
    BOOST_REQUIRE(core_count >= 4);

    test_borrows(core_count, 2, core_count * 2, std::nullopt);
}

/**
 * Variant of test_total_borrows that stresses the per-IP limit instead
 */
FIXTURE_TEST(test_per_ip_borrows, conn_quota_fixture) {
    auto core_count = ss::smp::count;
    BOOST_REQUIRE(core_count >= 4);
    test_borrows(core_count, 2, std::nullopt, core_count * 2);
}

FIXTURE_TEST(test_change_limits, conn_quota_fixture) {
    auto core_count = ss::smp::count;
    uint32_t initial_limit = core_count * 3;
    start(initial_limit, std::nullopt);

    // Take units on each shard
    for (ss::shard_id i = 0; i < core_count; ++i) {
        scq
          .invoke_on(
            i,
            [i, this](conn_quota& cq) -> ss::future<> {
                for (unsigned int j = 0; j < 3; ++j) {
                    auto u = co_await cq.get(addr1);
                    BOOST_TEST_REQUIRE(u.live());
                    shard_units[i].push_back(std::move(u));
                }
            })
          .get();
    }

    // We took all the units, should not be able to get any.
    expect_no_units(addr1).get();

    // Non-null value
    vlog(logger.debug, "Updating total non-null");
    set_limit(max_con, initial_limit + 1);

    // We increased the limit by 1, we should be able to get one more token
    // and no more.
    {
        auto u = take_units(addr1, 1);
        expect_no_units(addr1).get();
    }

    // Null value
    vlog(logger.debug, "Updating total null");
    max_con
      .invoke_on_all([](config::mock_property<std::optional<uint32_t>>& p) {
          p.update(std::nullopt);
      })
      .get();

    // We cleared the limit, should be able to get as many tokens as we like
    { auto u = take_units(addr1, initial_limit * 2); }

    // Releasing a bunch of units after disabling the limit should
    // work (they should be dropped)
    drop_shard_units();
}

FIXTURE_TEST(test_decrease_limit, conn_quota_fixture) {
    auto core_count = ss::smp::count;
    uint32_t initial_limit = core_count * 3;
    start(initial_limit, std::nullopt);

    // Take units on each shard
    vlog(logger.debug, "Taking units");
    take_on_all(3);

    // We took all the units, should not be able to get any.
    expect_no_units(addr1).get();

    // Try decreasing the limit (we still hold initial_limit tokens)
    vlog(logger.debug, "Decreasing limit");
    set_limit(max_con, 1);
    // We should still be in an exhausted state
    expect_no_units(addr1).get();

    // If we drop our existing units, it should be possible to take a unit
    // from our new limit
    vlog(logger.debug, "Dropping units");
    drop_shard_units();

    // Drain futures for background cross-core releases
    for (uint32_t i = 0; i < initial_limit; ++i) {
        ss::thread::yield();
    }

    vlog(logger.debug, "Taking 1st unit");
    auto u = take_units(addr1, 1);
    // We set the limit to 1 so it should not be possible to take >1
    vlog(logger.debug, "Taking 2nd unit");
    expect_no_units(addr1).get();
}

FIXTURE_TEST(test_change_limits_per_ip, conn_quota_fixture) {
    auto core_count = ss::smp::count;
    uint32_t initial_limit = core_count * 3;
    start(std::nullopt, initial_limit);

    // Populate some state
    take_on_all(3);

    // Non-null value per IP
    uint32_t new_limit = initial_limit + 1;
    vlog(logger.debug, "Updating per-IP non-null");
    max_con_per_ip
      .invoke_on_all(
        [new_limit](config::mock_property<std::optional<uint32_t>>& p) {
            p.update(new_limit);
        })
      .get();

    // Having added one to the limit, we should be able to take exactly one
    // more connection
    auto took_1 = take_units(addr1, 1);
    expect_no_units(addr1).get();

    // Null value per IP
    vlog(logger.debug, "Updating per-IP null");
    max_con_per_ip
      .invoke_on_all([](config::mock_property<std::optional<uint32_t>>& p) {
          p.update(std::nullopt);
      })
      .get();

    // Having cleared the limit, we should be able to take as many
    // as we want.
    auto took_2 = take_units(addr1, initial_limit * 10);
}

FIXTURE_TEST(test_overlaps, conn_quota_fixture) {
    start(10, 10);

    // First take all the tokens on shard 1 and drop them
    // again, this puts all the borrowed tokens here
    take_on_shard(1, addr1, 10);
    drop_on_shard(1, addr1, 10);

    // Now take tokens from each shard
    for (int i = 0; i < 5; ++i) {
        take_on_shard(1, addr1, 1);
        take_on_shard(2, addr1, 1);
    }

    expect_no_units(addr1).get();
}
