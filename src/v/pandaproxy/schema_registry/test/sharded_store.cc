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
    BOOST_REQUIRE(
      store.clear_compatibility(pps::default_context).get() == false);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);

    expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected)
        .get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);
}

constexpr std::string_view sv_string_def0{R"({"type":"string"})"};
const pps::schema_definition string_def0{
  pps::make_schema_definition<json::UTF8<>>(sv_string_def0).value(),
  pps::schema_type::avro};
const auto subject0 = pps::context_subject::unqualified("subject0");

SEASTAR_THREAD_TEST_CASE(test_sharded_store_subject_compat) {
    // Setting and retrieving a subject compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;
    const pps::schema_version ver1{1};

    pps::compatibility_level global_expected{
      pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == global_expected);

    store
      .upsert(
        pps::seq_marker{
          .seq = std::nullopt,
          .node = std::nullopt,
          .version = ver1,
          .key_type = pps::seq_marker_key_type::schema},
        pps::subject_schema{subject0, string_def0.share()},
        pps::schema_id{1},
        ver1,
        pps::is_deleted::no)
      .get();

    auto sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, sub_expected).get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == sub_expected);

    // duplicate should return false
    sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, sub_expected).get()
      == false);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == sub_expected);

    sub_expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, sub_expected).get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == sub_expected);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == global_expected);

    // Clearing compatibility should fallback to global
    BOOST_REQUIRE(
      store.clear_compatibility(dummy_marker, subject0).get() == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == global_expected);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_subject_compat_fallback) {
    // A Subject should fallback to the current global setting
    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;
    const pps::schema_version ver1{1};

    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    store
      .upsert(
        pps::seq_marker{
          .seq = std::nullopt,
          .node = std::nullopt,
          .version = ver1,
          .key_type = pps::seq_marker_key_type::schema},
        pps::subject_schema{subject0, string_def0.share()},
        pps::schema_id{1},
        ver1,
        pps::is_deleted::no)
      .get();

    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == expected);

    expected = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected)
        .get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == expected);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_invalid_subject_compat) {
    // Setting and getting a compatibility for a non-existent subject should
    // fail
    auto fallback = pps::default_to_global::yes;

    pps::seq_marker dummy_marker;
    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(subject0, fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });

    expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, expected).get());
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_context_config) {
    // Test setting and getting compatibility (config) at the context level
    auto test_ctx = pps::context{".test"};
    pps::seq_marker dummy_marker;
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });
    auto fallback = pps::default_to_global::yes;

    // Default config is backward compatibility
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::compatibility_level::backward);
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::backward);

    // Set config on default context
    BOOST_REQUIRE(
      store
        .set_compatibility(
          dummy_marker, pps::default_context, pps::compatibility_level::full)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::compatibility_level::full);
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::backward);

    // Set different config on test context
    BOOST_REQUIRE(store
                    .set_compatibility(
                      dummy_marker, test_ctx, pps::compatibility_level::none)
                    .get());
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::compatibility_level::full);
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::none);

    // Clear config returns to default
    BOOST_REQUIRE(store.clear_compatibility(test_ctx).get());
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::backward);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_context_config_written_at) {
    // Test that config (compatibility) write markers are tracked correctly
    auto test_ctx = pps::context{".test"};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    // Initially no write markers
    auto markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE(markers.empty());

    // Create distinct markers
    auto marker1 = pps::seq_marker{
      .seq = model::offset{10},
      .node = model::node_id{1},
      .version = pps::schema_version{0},
      .key_type = pps::seq_marker_key_type::config};
    auto marker2 = pps::seq_marker{
      .seq = model::offset{20},
      .node = model::node_id{1},
      .version = pps::schema_version{0},
      .key_type = pps::seq_marker_key_type::config};

    // Set compatibility on test context, verify marker is tracked
    BOOST_REQUIRE(
      store.set_compatibility(marker1, test_ctx, pps::compatibility_level::full)
        .get());
    markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE_EQUAL(markers.size(), 1);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);

    // Set compatibility again, second marker is added
    BOOST_REQUIRE(
      store.set_compatibility(marker2, test_ctx, pps::compatibility_level::none)
        .get());
    markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE_EQUAL(markers.size(), 2);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);
    BOOST_REQUIRE_EQUAL(markers[1], marker2);

    // Default context should still have no markers
    markers = store.get_context_config_written_at(pps::default_context).get();
    BOOST_REQUIRE(markers.empty());

    // Clear compatibility clears all markers
    BOOST_REQUIRE(store.clear_compatibility(test_ctx).get());
    markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE(markers.empty());
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
