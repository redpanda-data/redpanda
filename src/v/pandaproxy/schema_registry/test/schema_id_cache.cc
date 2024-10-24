// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/schema_id_cache.h"

#include "config/config_store.h"

#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pps = pandaproxy::schema_registry;

constexpr auto key = pps::schema_id_cache::field::key;
constexpr auto val = pps::schema_id_cache::field::val;

constexpr auto topic_name = pps::subject_name_strategy::topic_name;
constexpr auto record_name = pps::subject_name_strategy::record_name;
constexpr auto topic_record_name
  = pps::subject_name_strategy::topic_record_name;

const model::topic_view tp1{"tp1"};
const model::topic_view tp2{"tp2"};
const model::topic_view tp3{"tp3"};

const pps::schema_id s_id1{1};
const pps::schema_id s_id2{2};
const pps::schema_id s_id3{3};

BOOST_AUTO_TEST_CASE(test_schema_id_cache_basic_retrieval) {
    pps::schema_id_cache c{config::mock_binding(size_t(2))};

    c.put(tp1, key, topic_name, s_id1, std::nullopt);
    c.put(tp2, key, topic_name, s_id2, {{3, 4, 5}});

    // Match
    BOOST_REQUIRE(c.has(tp1, key, topic_name, s_id1, std::nullopt));
    BOOST_REQUIRE(c.has(tp2, key, topic_name, s_id2, {{3, 4, 5}}));

    // Missing offsets
    BOOST_REQUIRE(!c.has(tp2, key, topic_name, s_id2, std::nullopt));
    // Empty offsets
    BOOST_REQUIRE(!c.has(tp2, key, topic_name, s_id2, {{}}));
    // Incorrect offsets
    BOOST_REQUIRE(!c.has(tp2, key, topic_name, s_id2, {{3, 4}}));

    // Wrong field
    BOOST_REQUIRE(!c.has(tp1, val, topic_name, s_id1, std::nullopt));

    // Wrong sns
    BOOST_REQUIRE(!c.has(tp1, key, record_name, s_id1, std::nullopt));

    // Wrong sns
    BOOST_REQUIRE(!c.has(tp1, key, topic_record_name, s_id1, std::nullopt));
}

BOOST_AUTO_TEST_CASE(test_schema_id_cache_capacity) {
    config::config_store store;

    config::property<size_t> p{
      store, "capacity", "", {.needs_restart = config::needs_restart::no}};
    auto reset_p = ss::defer([&p]() { p.reset(); });
    p.set_value(size_t{2});
    pps::schema_id_cache c{p.bind()};

    c.put(tp1, key, topic_name, s_id1, {});
    c.put(tp2, key, topic_name, s_id2, {});

    // Should evict tp1
    c.put(tp3, key, topic_name, s_id3, {});

    BOOST_REQUIRE(!c.has(tp1, key, topic_name, s_id1, {}));
    BOOST_REQUIRE(c.has(tp2, key, topic_name, s_id2, {}));
    BOOST_REQUIRE(c.has(tp3, key, topic_name, s_id3, {}));

    // should evict tp2
    p.set_value(size_t{1});

    BOOST_REQUIRE(!c.has(tp1, key, topic_name, s_id1, {}));
    BOOST_REQUIRE(!c.has(tp2, key, topic_name, s_id2, {}));
    BOOST_REQUIRE(c.has(tp3, key, topic_name, s_id3, {}));
}

BOOST_AUTO_TEST_CASE(test_schema_id_cache_invalidate) {
    pps::schema_id_cache c{config::mock_binding(size_t(16))};

    c.put(tp1, key, topic_name, s_id1, {});
    c.put(tp1, key, record_name, s_id2, {});
    c.put(tp1, key, topic_record_name, s_id3, {});
    c.put(tp2, key, topic_name, s_id1, {});
    c.put(tp2, key, record_name, s_id2, {});
    c.put(tp2, key, topic_record_name, s_id3, {});
    c.put(tp3, key, topic_name, s_id1, {});
    c.put(tp3, key, record_name, s_id2, {});
    c.put(tp3, key, topic_record_name, s_id3, {});

    BOOST_REQUIRE_EQUAL(c.invalidate(tp2), 3);

    BOOST_REQUIRE(c.has(tp1, key, topic_name, s_id1, {}));
    BOOST_REQUIRE(c.has(tp1, key, record_name, s_id2, {}));
    BOOST_REQUIRE(c.has(tp1, key, topic_record_name, s_id3, {}));
    BOOST_REQUIRE(!c.has(tp2, key, topic_name, s_id1, {}));
    BOOST_REQUIRE(!c.has(tp2, key, record_name, s_id2, {}));
    BOOST_REQUIRE(!c.has(tp2, key, topic_record_name, s_id3, {}));
    BOOST_REQUIRE(c.has(tp3, key, topic_name, s_id1, {}));
    BOOST_REQUIRE(c.has(tp3, key, record_name, s_id2, {}));
    BOOST_REQUIRE(c.has(tp3, key, topic_record_name, s_id3, {}));
}
