// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/test/compatibility_avro.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

BOOST_AUTO_TEST_CASE(test_avro_basic_backwards_store_compat) {
    // Backward compatibility: A new schema is backward compatible if it can be
    // used to read the data written in the previous schema.

    pps::store s;
    s.set_compatibility(pps::compatibility_level::backward).value();
    auto sub = pps::subject{"sub"};
    auto avro = pps::schema_type::avro;
    auto res = s.insert(sub, pps::schema_definition{schema1}, avro);
    // add a defaulted field
    BOOST_REQUIRE(
      s.is_compatible(
         sub, pps::schema_version{1}, pps::schema_definition{schema2}, avro)
        .value());
    res = s.insert(sub, pps::schema_definition{schema2}, avro);

    // Test non-defaulted field
    BOOST_REQUIRE(
      !s.is_compatible(
          sub, pps::schema_version{1}, pps::schema_definition{schema3}, avro)
         .value());

    // Insert schema with non-defaulted field
    res = s.insert(sub, pps::schema_definition{schema2}, avro);

    // Test Remove defaulted field to previous
    BOOST_REQUIRE(
      s.is_compatible(
         sub, pps::schema_version{2}, pps::schema_definition{schema3}, avro)
        .value());

    // Test Remove defaulted field to first - should fail
    BOOST_REQUIRE(
      !s.is_compatible(
          sub, pps::schema_version{1}, pps::schema_definition{schema3}, avro)
         .value());

    s.set_compatibility(pps::compatibility_level::backward_transitive).value();

    // Test transitive defaulted field to previous - should fail
    BOOST_REQUIRE(
      !s.is_compatible(
          sub, pps::schema_version{2}, pps::schema_definition{schema3}, avro)
         .value());
}
