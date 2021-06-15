/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_slim_fixture.h"
#include "coproc/types.h"
#include "rpc/errc.h"
#include "test_utils/fixture.h"

#include <seastar/core/coroutine.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <algorithm>

static const auto id_copro
  = coproc::registry::type_identifier::identity_coprocessor;

FIXTURE_TEST(test_register, coproc_slim_fixture) {
    model::topic foo("foo");
    model::topic bar("bar");
    setup({{foo, 3}, {bar, 2}}).get();
    enable_coprocessors(
      {{.id = 8753, .data{.tid = id_copro, .topics = {{foo, tp_stored}}}},
       {.id = 32, .data{.tid = id_copro, .topics = {{bar, tp_stored}}}},
       {.id = 623,
        .data{
          .tid = id_copro, .topics = {{foo, tp_stored}, {bar, tp_earliest}}}}})
      .get();
    auto foos = shards_for_topic(foo).get();
    auto bars = shards_for_topic(bar).get();
    BOOST_CHECK_EQUAL(scripts_across_shards(8753).get(), foos.size());
    BOOST_CHECK_EQUAL(scripts_across_shards(32).get(), bars.size());
    /// A script that subscribes to multiple topics which may share
    /// shards cannot just assume the solution is the sum of
    /// shards_for_topic over each topic, since topics may reside on
    /// the same shard (common case). The solution is the union of
    /// unique shards
    std::set<ss::shard_id> unique;
    std::set_union(
      foos.begin(),
      foos.end(),
      bars.begin(),
      bars.end(),
      std::inserter(unique, unique.begin()));
    BOOST_CHECK_EQUAL(
      scripts_across_shards(coproc::script_id(623)).get(), unique.size());
}

FIXTURE_TEST(test_deregister, coproc_slim_fixture) {
    model::topic foo("foo");
    model::topic bar("bar");
    model::topic baz("baz");
    model::topic boom("boom");
    setup({{foo, 10}, {bar, 4}, {baz, 5}, {boom, 10}}).get();
    coproc::script_id a(1);
    coproc::script_id b(2);
    coproc::script_id c(3);
    coproc::script_id d(4);
    enable_coprocessors(
      {{.id = 1,
        .data{
          .tid = id_copro, .topics = {{foo, tp_stored}, {bar, tp_earliest}}}},
       {.id = 2, .data{.tid = id_copro, .topics = {{bar, tp_stored}}}},
       {.id = 3,
        .data{
          .tid = id_copro,
          .topics = {{foo, tp_stored}, {bar, tp_stored}, {baz, tp_stored}}}},
       {.id = 4,
        .data{
          .tid = id_copro, .topics = {{foo, tp_stored}, {boom, tp_stored}}}}})
      .get();
    disable_coprocessors({2, 4}).get();
    BOOST_CHECK_EQUAL(scripts_across_shards(2).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(4).get(), 0);
    get_script_dispatcher()->disable_all_coprocessors().get();
    BOOST_CHECK_EQUAL(scripts_across_shards(1).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(2).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(3).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(4).get(), 0);
}

/// A sanity check to ensure the unit test frameworks delayed
/// heartbeat mechanism is working as expected
FIXTURE_TEST(test_heartbeat, coproc_slim_fixture) {
    setup({}).get();
    auto& dispatcher = get_script_dispatcher();
    BOOST_CHECK(dispatcher->heartbeat().get());
    set_delay_heartbeat(true).get();
    auto result = dispatcher->heartbeat().get();
    BOOST_CHECK(!result.has_error());
    BOOST_CHECK_EQUAL(result.value().data(), -1);
}
