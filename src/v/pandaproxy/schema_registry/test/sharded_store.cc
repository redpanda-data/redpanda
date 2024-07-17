// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/sharded_store.h"

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_sharded_store_referenced_by) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    const pps::schema_version ver1{1};

    // Insert simple
    auto referenced_schema = pps::canonical_schema{
      pps::subject{"simple.proto"}, simple.share()};
    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        referenced_schema.share(),
        pps::schema_id{1},
        ver1,
        pps::is_deleted::no)
      .get();

    // Insert referenced
    auto importing_schema = pps::canonical_schema{
      pps::subject{"imported.proto"}, imported.share()};

    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        importing_schema.share(),
        pps::schema_id{2},
        ver1,
        pps::is_deleted::no)
      .get();

    auto referenced_by
      = store.referenced_by(referenced_schema.sub(), ver1).get();

    BOOST_REQUIRE_EQUAL(referenced_by.size(), 1);
    BOOST_REQUIRE_EQUAL(referenced_by[0], pps::schema_id{2});

    BOOST_REQUIRE(
      store.is_referenced(pps::subject{"simple.proto"}, pps::schema_version{1})
        .get());

    auto importing = store.get_schema_definition(pps::schema_id{2}).get();
    BOOST_REQUIRE_EQUAL(importing.refs().size(), 1);
    BOOST_REQUIRE_EQUAL(importing.refs()[0].sub, imported.refs()[0].sub);
    BOOST_REQUIRE_EQUAL(
      importing.refs()[0].version, imported.refs()[0].version);
    BOOST_REQUIRE_EQUAL(importing.refs()[0].name, imported.refs()[0].name);

    // soft delete subject
    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        importing_schema.share(),
        pps::schema_id{2},
        ver1,
        pps::is_deleted::yes)
      .get();

    // Soft-deleted should not partake in reference calculations
    BOOST_REQUIRE(
      store.referenced_by(pps::subject{"simple.proto"}, pps::schema_version{1})
        .get()
        .empty());
    BOOST_REQUIRE(
      !store.is_referenced(pps::subject{"simple.proto"}, pps::schema_version{1})
         .get());
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_find_unordered) {
    pps::sharded_store store;
    store.start(pps::is_mutable::no, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    pps::unparsed_schema array_unsanitized{
      pps::subject{"array"},
      pps::unparsed_schema_definition{
        R"({"type": "array", "default": [], "items" : "string"})",
        pps::schema_type::avro}};

    pps::canonical_schema array_sanitized{
      pps::subject{"array"},
      pps::canonical_schema_definition{
        R"({"type":"array","items":"string","default":[]})",
        pps::schema_type::avro}};

    const pps::schema_version ver1{1};

    // Insert an unsorted schema "onto the topic".
    auto referenced_schema = pps::canonical_schema{
      pps::subject{"simple.proto"}, simple.share()};
    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        array_unsanitized.share(),
        pps::schema_id{1},
        ver1,
        pps::is_deleted::no)
      .get();

    auto res = store.has_schema(array_sanitized.share()).get();
    BOOST_REQUIRE_EQUAL(res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(res.version, ver1);
}
