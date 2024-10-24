/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/scrubber_scheduler.h"
#include "config/configuration.h"

#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

using namespace std::chrono_literals;
using cloud_storage::scrub_status;

BOOST_AUTO_TEST_CASE(test_scrubber_scheduling) {
    auto& partial_interval
      = config::shard_local_cfg().cloud_storage_partial_scrub_interval_ms;
    auto& full_interval
      = config::shard_local_cfg().cloud_storage_full_scrub_interval_ms;
    auto& jitter
      = config::shard_local_cfg().cloud_storage_scrubbing_interval_jitter_ms;

    partial_interval.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1h));
    full_interval.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(2h));
    jitter.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(10min));

    auto reset_configs = ss::defer(
      [&partial_interval, &full_interval, &jitter] {
          partial_interval.reset();
          full_interval.reset();
          jitter.reset();
      });

    model::timestamp last_scrub_time;
    scrub_status last_status{scrub_status::full};
    archival::scrubber_scheduler<ss::manual_clock> sched{
      [&last_scrub_time, &last_status] {
          return std::make_tuple(last_scrub_time, last_status);
      },
      partial_interval,
      full_interval,
      jitter};

    // Test that the first scrub happens after the jitter
    {
        sched.pick_next_scrub_time();
        const auto until_next = sched.until_next_scrub();
        BOOST_REQUIRE(until_next.has_value());

        BOOST_REQUIRE_LE(until_next, jitter());
        if (until_next > 0ms) {
            BOOST_REQUIRE(!sched.should_scrub());
        }

        ss::manual_clock::advance(*until_next);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::partial;
    }

    // Test that the second scrub happens after partial interval + jitter
    {
        sched.pick_next_scrub_time();
        const auto until_next = sched.until_next_scrub();
        BOOST_REQUIRE(until_next.has_value());

        BOOST_REQUIRE_GE(until_next, partial_interval());
        BOOST_REQUIRE_LE(until_next, partial_interval() + jitter());
        BOOST_REQUIRE(!sched.should_scrub());

        ss::manual_clock::advance(*until_next);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::full;
    }

    // Test that the second scrub happens after full interval + jitter.
    // We expect full interval, since the previous simulated scrub resulted
    // in scrub_status::full.
    {
        sched.pick_next_scrub_time();
        const auto until_next = sched.until_next_scrub();
        BOOST_REQUIRE(until_next.has_value());

        BOOST_REQUIRE_GE(until_next, full_interval());
        BOOST_REQUIRE_LE(until_next, full_interval() + jitter());
        BOOST_REQUIRE(!sched.should_scrub());

        ss::manual_clock::advance(*until_next);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::partial;
    }
}

BOOST_AUTO_TEST_CASE(test_no_scrub_scheduled) {
    auto partial_interval = config::shard_local_cfg()
                              .cloud_storage_partial_scrub_interval_ms.bind();
    auto full_interval
      = config::shard_local_cfg().cloud_storage_full_scrub_interval_ms.bind();
    auto jitter = config::shard_local_cfg()
                    .cloud_storage_scrubbing_interval_jitter_ms.bind();

    model::timestamp last_scrub_time;
    scrub_status last_status{scrub_status::full};
    archival::scrubber_scheduler<ss::manual_clock> sched{
      [&last_scrub_time, &last_status] {
          return std::make_tuple(last_scrub_time, last_status);
      },
      partial_interval,
      full_interval,
      jitter};

    BOOST_REQUIRE(!sched.should_scrub());
    BOOST_REQUIRE(!sched.until_next_scrub().has_value());
}

BOOST_AUTO_TEST_CASE(test_update_jitter) {
    auto& partial_interval
      = config::shard_local_cfg().cloud_storage_partial_scrub_interval_ms;
    auto& full_interval
      = config::shard_local_cfg().cloud_storage_full_scrub_interval_ms;
    auto& jitter
      = config::shard_local_cfg().cloud_storage_scrubbing_interval_jitter_ms;

    partial_interval.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(30min));
    full_interval.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1h));
    jitter.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1min));

    auto reset_configs = ss::defer(
      [&partial_interval, &full_interval, &jitter] {
          partial_interval.reset();
          full_interval.reset();
          jitter.reset();
      });

    model::timestamp last_scrub_time;
    scrub_status last_status;
    archival::scrubber_scheduler<ss::manual_clock> sched{
      [&last_scrub_time, &last_status] {
          return std::make_tuple(last_scrub_time, last_status);
      },
      partial_interval.bind(),
      full_interval.bind(),
      jitter.bind()};

    const auto updated_jitter = 10s;

    // Test that updating the jitter reschedules the scrub correctly
    {
        sched.pick_next_scrub_time();
        const auto until_next = sched.until_next_scrub();
        BOOST_REQUIRE(until_next.has_value());

        jitter.set_value(std::chrono::duration_cast<std::chrono::milliseconds>(
          updated_jitter));
        const auto until_next_after_jitter_change = sched.until_next_scrub();
        BOOST_REQUIRE(until_next_after_jitter_change.has_value());

        BOOST_REQUIRE_LE(until_next_after_jitter_change, updated_jitter);

        ss::manual_clock::advance(*until_next_after_jitter_change);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::partial;
    }

    // Test that the next schedulling call uses the new jitter value and old
    // partial duration
    {
        sched.pick_next_scrub_time();
        const auto until_next = sched.until_next_scrub();
        BOOST_REQUIRE(until_next.has_value());

        BOOST_REQUIRE_GE(until_next, partial_interval());
        BOOST_REQUIRE_LE(until_next, partial_interval() + updated_jitter);
        BOOST_REQUIRE(!sched.should_scrub());

        ss::manual_clock::advance(*until_next);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::full;
    }

    // Test that the next schedulling call uses the new jitter value and old
    // full duration
    {
        sched.pick_next_scrub_time();
        const auto until_next = sched.until_next_scrub();
        BOOST_REQUIRE(until_next.has_value());

        BOOST_REQUIRE_GE(until_next, full_interval());
        BOOST_REQUIRE_LE(until_next, full_interval() + updated_jitter);
        BOOST_REQUIRE(!sched.should_scrub());

        ss::manual_clock::advance(*until_next);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::full;
    }
}

BOOST_AUTO_TEST_CASE(test_update_interval) {
    auto& partial_interval
      = config::shard_local_cfg().cloud_storage_partial_scrub_interval_ms;
    auto& full_interval
      = config::shard_local_cfg().cloud_storage_full_scrub_interval_ms;
    auto& jitter
      = config::shard_local_cfg().cloud_storage_scrubbing_interval_jitter_ms;

    partial_interval.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(30min));
    full_interval.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1h));
    jitter.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1min));

    auto reset_configs = ss::defer(
      [&partial_interval, &full_interval, &jitter] {
          partial_interval.reset();
          full_interval.reset();
          jitter.reset();
      });

    model::timestamp last_scrub_time;
    scrub_status last_status{scrub_status::full};
    archival::scrubber_scheduler<ss::manual_clock> sched{
      [&last_scrub_time, &last_status] {
          return std::make_tuple(last_scrub_time, last_status);
      },
      partial_interval.bind(),
      full_interval.bind(),
      jitter.bind()};

    // Test that updating the interval before the first scrub reschedules.
    // We should reschedule before the jitter since this is the first scrub
    // still.
    {
        sched.pick_next_scrub_time();

        const auto updated_interval = 10min;
        partial_interval.set_value(
          std::chrono::duration_cast<std::chrono::milliseconds>(
            updated_interval));

        const auto until_next_after_after_interval_change
          = sched.until_next_scrub();
        BOOST_REQUIRE(until_next_after_after_interval_change.has_value());

        BOOST_REQUIRE_LE(until_next_after_after_interval_change, jitter());

        ss::manual_clock::advance(*until_next_after_after_interval_change);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
        last_status = scrub_status::partial;
    }

    // Test that updating the interval after the first job instance triggers
    // a rescheduling.
    {
        sched.pick_next_scrub_time();

        const auto updated_interval = 5min;
        partial_interval.set_value(
          std::chrono::duration_cast<std::chrono::milliseconds>(
            updated_interval));

        const auto until_next_after_after_interval_change
          = sched.until_next_scrub();
        BOOST_REQUIRE(until_next_after_after_interval_change.has_value());

        BOOST_REQUIRE_GE(
          until_next_after_after_interval_change, updated_interval);
        BOOST_REQUIRE_LE(
          until_next_after_after_interval_change, updated_interval + jitter());

        ss::manual_clock::advance(*until_next_after_after_interval_change);
        BOOST_REQUIRE(sched.should_scrub());

        last_scrub_time = model::to_timestamp(ss::manual_clock::now());
    }
}
