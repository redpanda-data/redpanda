// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_avro.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_avro_basic_backwards_store_compat) {
    // Backward compatibility: A new schema is backward compatible if it can be
    // used to read the data written in the previous schema.

    pps::sharded_store s;
    s.start(ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;

    s.set_compatibility(pps::compatibility_level::backward).get();
    auto sub = pps::subject{"sub"};
    s.upsert(
       dummy_marker,
       {sub, pps::canonical_schema_definition{schema1}},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();
    // add a defaulted field
    BOOST_REQUIRE(s.is_compatible(
                     pps::schema_version{1},
                     {sub, pps::canonical_schema_definition{schema2}})
                    .get());
    s.upsert(
       dummy_marker,
       {sub, pps::canonical_schema_definition{schema2}},
       pps::schema_id{2},
       pps::schema_version{2},
       pps::is_deleted::no)
      .get();

    // Test non-defaulted field
    BOOST_REQUIRE(!s.is_compatible(
                      pps::schema_version{1},
                      {sub, pps::canonical_schema_definition{schema3}})
                     .get());

    // Insert schema with non-defaulted field
    s.upsert(
       dummy_marker,
       {sub, pps::canonical_schema_definition{schema2}},
       pps::schema_id{2},
       pps::schema_version{2},
       pps::is_deleted::no)
      .get();

    // Test Remove defaulted field to previous
    BOOST_REQUIRE(s.is_compatible(
                     pps::schema_version{2},
                     {sub, pps::canonical_schema_definition{schema3}})
                    .get());

    // Test Remove defaulted field to first - should fail
    BOOST_REQUIRE(!s.is_compatible(
                      pps::schema_version{1},
                      {sub, pps::canonical_schema_definition{schema3}})
                     .get());

    s.set_compatibility(pps::compatibility_level::backward_transitive).get();

    // Test transitive defaulted field to previous - should fail
    BOOST_REQUIRE(!s.is_compatible(
                      pps::schema_version{2},
                      {sub, pps::canonical_schema_definition{schema3}})
                     .get());
}
