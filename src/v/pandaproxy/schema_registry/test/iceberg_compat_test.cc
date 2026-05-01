// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/iceberg_compat.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace {

pps::schema_definition make_schema_a_int() {
    return pps::sanitize_avro_schema_definition(
             {R"({"type":"record","name":"test","fields":[{"name":"a","type":"int"}]})",
              pps::schema_type::avro})
      .value();
}

pps::schema_definition make_schema_ab() {
    return pps::sanitize_avro_schema_definition(
             {R"({"type":"record","name":"test","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null}]})",
              pps::schema_type::avro})
      .value();
}

pps::schema_definition make_schema_a_string() {
    return pps::sanitize_avro_schema_definition(
             {R"({"type":"record","name":"test","fields":[{"name":"a","type":"string"}]})",
              pps::schema_type::avro})
      .value();
}

pps::schema_definition make_schema_ab_int() {
    return pps::sanitize_avro_schema_definition(
             {R"({"type":"record","name":"test","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","int"],"default":null}]})",
              pps::schema_type::avro})
      .value();
}

pps::schema_definition with_iceberg_flag(pps::schema_definition def) {
    pps::schema_metadata meta{
      .properties = absl::btree_map<ss::sstring, ss::sstring>{
        {"redpanda.iceberg.compatible", "true"}}};
    auto [raw, type, refs, _meta] = std::move(def).destructure();
    return {std::move(raw), type, std::move(refs), std::move(meta)};
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_flag_not_set) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    auto sub = pps::context_subject::unqualified("sub");

    // No metadata flag set on the candidate.
    auto candidate = make_schema_a_int();
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    // Should be nullopt (check skipped).
    BOOST_REQUIRE(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_single_version_with_flag) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    auto sub = pps::context_subject::unqualified("sub");

    // First version with iceberg flag: nothing to evolve from.
    auto candidate = with_iceberg_flag(make_schema_a_int());
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    BOOST_REQUIRE(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_compatible_addition) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;
    auto sub = pps::context_subject::unqualified("sub");

    // Register v1: {a: int}
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_a_int()},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();

    // Candidate v2: {a: int, b: string} with iceberg flag.
    auto candidate = with_iceberg_flag(make_schema_ab());
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    BOOST_REQUIRE(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_incompatible_type_change) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;
    auto sub = pps::context_subject::unqualified("sub");

    // Register v1: {a: string}
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_a_string()},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();

    // Candidate v2: {a: int} with iceberg flag -> type mismatch.
    auto candidate = with_iceberg_flag(make_schema_a_int());
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    BOOST_REQUIRE(result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_drop_and_reintroduce_same_type) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;
    auto sub = pps::context_subject::unqualified("sub");

    // v1: {a: int, b: string}
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_ab()},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();

    // v2: {a: int} (drop b)
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_a_int()},
       pps::schema_id{2},
       pps::schema_version{2},
       pps::is_deleted::no)
      .get();

    // Candidate v3: {a: int, b: string} (reintroduce b, same type).
    auto candidate = with_iceberg_flag(make_schema_ab());
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    BOOST_REQUIRE(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_drop_and_reintroduce_different_type) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;
    auto sub = pps::context_subject::unqualified("sub");

    // v1: {a: int, b: string}
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_ab()},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();

    // v2: {a: int} (drop b)
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_a_int()},
       pps::schema_id{2},
       pps::schema_version{2},
       pps::is_deleted::no)
      .get();

    // Candidate v3: {a: int, b: int} (reintroduce b with different type).
    auto candidate = with_iceberg_flag(make_schema_ab_int());
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    BOOST_REQUIRE(result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_error_includes_step_info) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;
    auto sub = pps::context_subject::unqualified("sub");

    // v1: {a: string}
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_a_string()},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();

    // Candidate v2: {a: int} with iceberg flag -> fails at step 1.
    auto candidate = with_iceberg_flag(make_schema_a_int());
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    BOOST_REQUIRE(result.has_value());
    // The error string should reference the version where evolution failed.
    BOOST_REQUIRE(result->find("version") != ss::sstring::npos);
}

SEASTAR_THREAD_TEST_CASE(test_metadata_inheritance_not_tested_here) {
    // Metadata inheritance is handled by the handler layer
    // (make_canonical_schema_with_metadata), not by
    // check_iceberg_compatibility. The function receives already-resolved
    // metadata. This test simply verifies that a candidate without the flag
    // but with existing versions still results in a skip.
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    pps::seq_marker dummy_marker;
    auto sub = pps::context_subject::unqualified("sub");

    // Register v1 with no flag.
    s.upsert(
       dummy_marker,
       pps::subject_schema{sub, make_schema_a_int()},
       pps::schema_id{1},
       pps::schema_version{1},
       pps::is_deleted::no)
      .get();

    // Candidate v2 also without flag.
    auto candidate = make_schema_ab();
    auto result = pps::check_iceberg_compatibility(s, sub, candidate).get();

    // Skipped because flag is not set.
    BOOST_REQUIRE(!result.has_value());
}
