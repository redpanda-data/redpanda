/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/coproc_slim_fixture.h"
#include "coproc/tests/utils/supervisor_test_fixture.h"
#include "coproc/types.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>

class script_dispatcher_fixture
  : public coproc_slim_fixture
  , public supervisor_test_fixture {
public:
    ss::future<> enable_scripts(
      std::vector<std::tuple<uint64_t, std::vector<model::topic>>> data) {
        std::vector<coproc::enable_copros_request::data> d;
        std::transform(
          data.begin(), data.end(), std::back_inserter(d), [](const auto& dp) {
              /// Generate a random coprocessor payload
              coproc::wasm::cpp_enable_payload payload{
                .tid = coproc::registry::type_identifier::identity_coprocessor,
                .topics = std::get<1>(dp)};
              return coproc::enable_copros_request::data{
                .id = coproc::script_id(std::get<0>(dp)),
                .source_code = reflection::to_iobuf(std::move(payload))};
          });
        return get_script_dispatcher()->enable_coprocessors(
          coproc::enable_copros_request{.inputs = std::move(d)});
    }

    ss::future<> disable_scripts(std::vector<coproc::script_id> ids) {
        return get_script_dispatcher()->disable_coprocessors(
          coproc::disable_copros_request{.ids = std::move(ids)});
    }

    ss::future<size_t> scripts_across_shards(coproc::script_id id) {
        return get_pacemaker().map_reduce0(
          [id](coproc::pacemaker& p) {
              return p.local_script_id_exists(id) ? 1 : 0;
          },
          size_t(0),
          std::plus<>());
    }
};

FIXTURE_TEST(test_register, script_dispatcher_fixture) {
    model::topic foo("foo");
    model::topic bar("bar");
    setup({{foo, 3}, {bar, 2}}).get();
    coproc::script_id foo_subber(8753);
    coproc::script_id bar_subber(32);
    coproc::script_id all_subbed(623);
    enable_scripts(
      {{foo_subber, {foo}}, {bar_subber, {bar}}, {all_subbed, {foo, bar}}})
      .get();
    auto foos = shards_for_topic(foo).get();
    auto bars = shards_for_topic(bar).get();
    BOOST_CHECK_EQUAL(scripts_across_shards(foo_subber).get(), foos.size());
    BOOST_CHECK_EQUAL(scripts_across_shards(bar_subber).get(), bars.size());
    /// A script that subscribes to multiple topics which may share shards
    /// cannot just assume the solution is the sum of shards_for_topic over each
    /// topic, since topics may reside on the same shard (common case). The
    /// solution is the union of unique shards
    std::set<ss::shard_id> unique;
    std::set_union(
      foos.begin(),
      foos.end(),
      bars.begin(),
      bars.end(),
      std::inserter(unique, unique.begin()));
    BOOST_CHECK_EQUAL(scripts_across_shards(all_subbed).get(), unique.size());
}

FIXTURE_TEST(test_deregister, script_dispatcher_fixture) {
    model::topic foo("foo");
    model::topic bar("bar");
    model::topic baz("baz");
    model::topic boom("boom");
    setup({{foo, 10}, {bar, 4}, {baz, 5}, {boom, 10}}).get();
    coproc::script_id a(1);
    coproc::script_id b(2);
    coproc::script_id c(3);
    coproc::script_id d(4);
    enable_scripts(
      {{a, {foo, bar}}, {b, {bar}}, {c, {foo, bar, baz}}, {d, {foo, boom}}})
      .get();
    disable_scripts({b, d}).get();
    BOOST_CHECK_EQUAL(scripts_across_shards(b).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(d).get(), 0);
    get_script_dispatcher()->disable_all_coprocessors().get();
    BOOST_CHECK_EQUAL(scripts_across_shards(a).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(b).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(c).get(), 0);
    BOOST_CHECK_EQUAL(scripts_across_shards(d).get(), 0);
}

/// A sanity check to ensure the unit test frameworks delayed heartbeat
/// mechanism is working as expected
FIXTURE_TEST(test_heartbeat, script_dispatcher_fixture) {
    setup({}).get();
    auto& dispatcher = get_script_dispatcher();
    BOOST_CHECK(dispatcher->heartbeat().get());
    set_delay_heartbeat(true).get();
    BOOST_CHECK(!dispatcher->heartbeat().get());
}
