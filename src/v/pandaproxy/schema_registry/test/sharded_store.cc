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
#include "pandaproxy/schema_registry/util.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_compat) {
    // Setting and retrieving global compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);

    // duplicate should return false
    BOOST_REQUIRE(store.clear_compatibility(pps::default_context).get() == false);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);

    expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected).get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_referenced_by) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    const pps::schema_version ver1{1};

    // Insert simple
    auto referenced_schema = pps::subject_schema{
      pps::context_subject::unqualified("simple.proto"), simple.share()};
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
    auto importing_schema = pps::subject_schema{
      pps::context_subject::unqualified("imported.proto"), imported.share()};

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
    BOOST_REQUIRE_EQUAL(referenced_by[0].id, pps::schema_id{2});

    BOOST_REQUIRE(store
                    .is_referenced(
                      pps::context_subject::unqualified("simple.proto"),
                      pps::schema_version{1})
                    .get());

    auto importing
      = store.get_schema_definition({pps::default_context, pps::schema_id{2}})
          .get();
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
    BOOST_REQUIRE(store
                    .referenced_by(
                      pps::context_subject::unqualified("simple.proto"),
                      pps::schema_version{1})
                    .get()
                    .empty());
    BOOST_REQUIRE(!store
                     .is_referenced(
                       pps::context_subject::unqualified("simple.proto"),
                       pps::schema_version{1})
                     .get());
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_find_unordered) {
    pps::sharded_store store;
    store.start(pps::is_mutable::no, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    pps::subject_schema array_unsanitized{
      pps::context_subject::unqualified("array"),
      pps::schema_definition{
        R"({"type": "array", "default": [], "items" : "string"})",
        pps::schema_type::avro}};

    pps::subject_schema array_sanitized{
      pps::context_subject::unqualified("array"),
      pps::schema_definition{
        R"({"type":"array","items":"string","default":[]})",
        pps::schema_type::avro}};

    const pps::schema_version ver1{1};

    // Insert an unsorted schema "onto the topic".
    auto referenced_schema = pps::subject_schema{
      pps::context_subject::unqualified("simple.proto"), simple.share()};
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
