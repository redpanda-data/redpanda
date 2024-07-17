// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/store.h"

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/test/compatibility_avro.h"
#include "pandaproxy/schema_registry/util.h"

#include <absl/algorithm/container.h>
#include <boost/test/unit_test.hpp>

namespace pps = pandaproxy::schema_registry;

constexpr std::string_view sv_string_def0{R"({"type":"string"})"};
constexpr std::string_view sv_string_def1{R"({"type": "string"})"};
constexpr std::string_view sv_int_def0{R"({"type": "int"})"};
const pps::canonical_schema_definition string_def0{
  pps::make_schema_definition<json::UTF8<>>(sv_string_def0).value(),
  pps::schema_type::avro};
const pps::canonical_schema_definition string_def1{
  pps::make_schema_definition<json::UTF8<>>(sv_string_def1).value(),
  pps::schema_type::avro};
const pps::canonical_schema_definition int_def0{
  pps::make_schema_definition<json::UTF8<>>(sv_int_def0).value(),
  pps::schema_type::avro};
const pps::subject subject0{"subject0"};
const pps::subject subject1{"subject1"};
const pps::subject subject2{"subject2"};

BOOST_AUTO_TEST_CASE(test_store_insert) {
    pps::store s;

    // First insert, expect id{1}, version{1}
    auto ins_res = s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert duplicate, expect id{1}, versions{1}
    ins_res = s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(!ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert duplicate, with spaces, expect id{1}, versions{1}
    ins_res = s.insert({subject0, string_def1.share()});
    BOOST_REQUIRE(!ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert on different subject, expect id{1}, version{1}
    ins_res = s.insert({subject1, string_def0.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert different schema, expect id{2}, version{2}
    ins_res = s.insert({subject0, int_def0.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{2});
}

/// Emulate how `sharded_store` does upserts on `store`
bool upsert(
  pps::store& store,
  pps::subject sub,
  pps::canonical_schema_definition def,
  pps::schema_type type,
  pps::schema_id id,
  pps::schema_version version,
  pps::is_deleted deleted) {
    store.upsert_schema(id, std::move(def));
    return store.upsert_subject(
      pps::seq_marker{}, std::move(sub), version, id, deleted);
}

BOOST_AUTO_TEST_CASE(test_store_upsert_in_order) {
    const auto expected = std::vector<pps::schema_version>(
      {pps::schema_version{0}, pps::schema_version{1}});

    pps::store s;
    BOOST_REQUIRE(upsert(
      s,
      subject0,
      string_def0.share(),
      pps::schema_type::avro,
      pps::schema_id{0},
      pps::schema_version{0},
      pps::is_deleted::no));
    BOOST_REQUIRE(upsert(
      s,
      subject0,
      string_def0.share(),
      pps::schema_type::avro,
      pps::schema_id{1},
      pps::schema_version{1},
      pps::is_deleted::no));

    auto res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      res.value().cbegin(),
      res.value().cend(),
      expected.cbegin(),
      expected.cend());
}

BOOST_AUTO_TEST_CASE(test_store_upsert_reverse_order) {
    const auto expected = std::vector<pps::schema_version>(
      {pps::schema_version{0}, pps::schema_version{1}});

    pps::store s;
    BOOST_REQUIRE(upsert(
      s,
      subject0,
      string_def0.share(),
      pps::schema_type::avro,
      pps::schema_id{1},
      pps::schema_version{1},
      pps::is_deleted::no));
    BOOST_REQUIRE(upsert(
      s,
      subject0,
      string_def0.share(),
      pps::schema_type::avro,
      pps::schema_id{0},
      pps::schema_version{0},
      pps::is_deleted::no));

    auto res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      res.value().cbegin(),
      res.value().cend(),
      expected.cbegin(),
      expected.cend());
}

BOOST_AUTO_TEST_CASE(test_store_upsert_override) {
    const auto expected = std::vector<pps::schema_version>(
      {pps::schema_version{0}});

    pps::store s;
    BOOST_REQUIRE(upsert(
      s,
      subject0,
      string_def0.share(),
      pps::schema_type::avro,
      pps::schema_id{0},
      pps::schema_version{0},
      pps::is_deleted::no));
    // override schema and version (should return no insertion)
    BOOST_REQUIRE(!upsert(
      s,
      subject0,
      int_def0.share(),
      pps::schema_type::avro,
      pps::schema_id{0},
      pps::schema_version{0},
      pps::is_deleted::no));

    auto v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(v_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      v_res.value().cbegin(),
      v_res.value().cend(),
      expected.cbegin(),
      expected.cend());
    auto s_res = s.get_subject_schema(
      subject0, pps::schema_version{0}, pps::include_deleted::no);
    BOOST_REQUIRE(s_res.has_value());
    BOOST_REQUIRE(s_res.value().schema.def() == int_def0.share());
}

BOOST_AUTO_TEST_CASE(test_store_get_schema) {
    pps::store s;

    auto res = s.get_schema_definition(pps::schema_id{1});
    BOOST_REQUIRE(res.has_error());
    auto err = std::move(res).assume_error();
    BOOST_REQUIRE(err.code() == pps::error_code::schema_id_not_found);

    // First insert, expect id{1}
    auto ins_res = s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    res = s.get_schema_definition(ins_res.id);
    BOOST_REQUIRE(res.has_value());

    auto def = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(def, string_def0.share());
}

BOOST_AUTO_TEST_CASE(test_store_get_schema_subject_versions) {
    pps::store s;

    pps::seq_marker dummy_marker;

    // First insert, expect id{1}
    auto ins_res = s.insert(
      {subject0, pps::canonical_schema_definition(schema1.share())});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    auto versions = s.get_schema_subject_versions(pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(versions.size(), 1);
    BOOST_REQUIRE_EQUAL(versions[0].sub, subject0);
    BOOST_REQUIRE_EQUAL(versions[0].version, pps::schema_version{1});

    versions = s.get_schema_subject_versions(pps::schema_id{2});
    BOOST_REQUIRE(versions.empty());

    // Second insert, expect id{2}
    ins_res = s.insert(
      {subject0, pps::canonical_schema_definition(schema2.share())});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{2});

    // expect [{schema 2, version 2}]
    versions = s.get_schema_subject_versions(pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(versions.size(), 1);
    BOOST_REQUIRE_EQUAL(versions[0].sub, subject0);
    BOOST_REQUIRE_EQUAL(versions[0].version, pps::schema_version{2});

    // delete version 1
    s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{1},
      pps::schema_id{1},
      pps::is_deleted::yes);

    // expect [{{schema 2, version 2}]
    versions = s.get_schema_subject_versions(pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(versions.size(), 1);
    BOOST_REQUIRE_EQUAL(versions[0].sub, subject0);
    BOOST_REQUIRE_EQUAL(versions[0].version, pps::schema_version{2});
}

BOOST_AUTO_TEST_CASE(test_store_get_schema_subjects) {
    auto is_equal = [](auto lhs) {
        return [lhs](auto rhs) { return lhs == rhs; };
    };

    pps::store s;

    pps::seq_marker dummy_marker;

    // First insert, expect id{1}
    auto ins_res = s.insert(
      {subject0, pps::canonical_schema_definition(schema1.share())});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    auto subjects = s.get_schema_subjects(
      pps::schema_id{1}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);

    // Second insert, same schema, expect id{1}
    ins_res = s.insert(
      {subject1, pps::canonical_schema_definition(schema1.share())});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert yet another schema associated with a different subject
    ins_res = s.insert(
      {subject2, pps::canonical_schema_definition(schema2.share())});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    subjects = s.get_schema_subjects(
      pps::schema_id{1}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject1)), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject2)), 0);

    subjects = s.get_schema_subjects(
      pps::schema_id{2}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 0);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject1)), 0);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject2)), 1);

    // Test deletion

    s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{1},
      pps::schema_id{1},
      pps::is_deleted::yes);

    subjects = s.get_schema_subjects(
      pps::schema_id{1}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject1)), 1);

    subjects = s.get_schema_subjects(
      pps::schema_id{1}, pps::include_deleted::yes);
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject1)), 1);
}

BOOST_AUTO_TEST_CASE(test_store_get_subject_schema) {
    pps::store s;

    auto res = s.get_subject_schema(
      subject0, pps::schema_version{1}, pps::include_deleted::no);
    BOOST_REQUIRE(res.has_error());
    auto err = std::move(res).assume_error();
    BOOST_REQUIRE(err.code() == pps::error_code::subject_not_found);

    // First insert, expect id{1}, version{1}
    auto ins_res = s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Request good version
    res = s.get_subject_schema(
      subject0, pps::schema_version{1}, pps::include_deleted::no);
    BOOST_REQUIRE(res.has_value());
    auto val = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(val.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(val.version, pps::schema_version{1});
    BOOST_REQUIRE_EQUAL(val.deleted, pps::is_deleted::no);
    BOOST_REQUIRE_EQUAL(val.schema.def(), string_def0.share());

    // Second insert, expect id{1}, version{1}
    ins_res = s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(!ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});

    // Request good version
    res = s.get_subject_schema(
      subject0, pps::schema_version{1}, pps::include_deleted::no);
    BOOST_REQUIRE(res.has_value());
    val = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(val.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(val.version, pps::schema_version{1});
    BOOST_REQUIRE_EQUAL(val.deleted, pps::is_deleted::no);
    BOOST_REQUIRE_EQUAL(val.schema.def(), string_def0.share());

    // Request bad version
    res = s.get_subject_schema(
      subject0, pps::schema_version{2}, pps::include_deleted::no);
    BOOST_REQUIRE(res.has_error());
    err = std::move(res).assume_error();
    BOOST_REQUIRE(err.code() == pps::error_code::subject_version_not_found);
}

BOOST_AUTO_TEST_CASE(test_store_get_versions) {
    pps::store s;

    // First insert, expect id{1}, version{1}
    s.insert({subject0, string_def0.share()});

    auto versions = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(versions.has_value());
    BOOST_REQUIRE_EQUAL(versions.value().size(), 1);
    BOOST_REQUIRE_EQUAL(versions.value().front(), pps::schema_version{1});

    // Insert duplicate, expect id{1}, versions{1}
    s.insert({subject0, string_def0.share()});

    versions = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(versions.has_value());
    BOOST_REQUIRE_EQUAL(versions.value().size(), 1);
    BOOST_REQUIRE_EQUAL(versions.value().front(), pps::schema_version{1});

    // Insert different schema, expect id{2}, version{2}
    s.insert({subject0, int_def0.share()});

    versions = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(versions.has_value());
    BOOST_REQUIRE_EQUAL(versions.value().size(), 2);
    BOOST_REQUIRE_EQUAL(versions.value().front(), pps::schema_version{1});
    BOOST_REQUIRE_EQUAL(versions.value().back(), pps::schema_version{2});
}

BOOST_AUTO_TEST_CASE(test_store_get_subjects) {
    auto is_equal = [](auto lhs) {
        return [lhs](auto rhs) { return lhs == rhs; };
    };

    pps::store s;

    auto subjects = s.get_subjects(pps::include_deleted::no);
    BOOST_REQUIRE(subjects.empty());

    // First insert
    s.insert({subject0, string_def0.share()});
    subjects = s.get_subjects(pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);

    // second insert
    s.insert({subject1, string_def0.share()});
    subjects = s.get_subjects(pps::include_deleted::no);
    BOOST_REQUIRE(subjects.size() == 2);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject1)), 1);

    // Delete subject0 version
    pps::seq_marker dummy_marker;
    s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{1},
      pps::schema_id{1},
      pps::is_deleted::yes);
    subjects = s.get_subjects(pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    subjects = s.get_subjects(pps::include_deleted::yes);
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    auto b = s.delete_subject_version(subject0, pps::schema_version{1});

    // Delete subject1
    auto versions = s.delete_subject(
      dummy_marker, subject1, pps::permanent_delete::no);
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::no).size(), 0);
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::yes).size(), 1);
    versions = s.delete_subject(
      dummy_marker, subject1, pps::permanent_delete::yes);

    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::no).size(), 0);
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::yes).size(), 0);

    // Ensure subjects with empty versions is not returned
    auto c = s.set_compatibility(
      dummy_marker, subject0, pps::compatibility_level::none);
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::no).size(), 0);
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::yes).size(), 0);
}

BOOST_AUTO_TEST_CASE(test_store_global_compat) {
    // Setting the retrieving global compatibility should be allowed multiple
    // times

    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::store s;
    BOOST_REQUIRE(s.get_compatibility().value() == expected);

    // duplicate should return false
    BOOST_REQUIRE(s.set_compatibility(expected).value() == false);
    BOOST_REQUIRE(s.get_compatibility().value() == expected);

    expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(s.set_compatibility(expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility().value() == expected);
}

BOOST_AUTO_TEST_CASE(test_store_subject_compat) {
    // Setting the retrieving a subject compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;

    pps::compatibility_level global_expected{
      pps::compatibility_level::backward};
    pps::store s;
    BOOST_REQUIRE(s.get_compatibility().value() == global_expected);
    s.insert({subject0, string_def0.share()});

    auto sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      s.set_compatibility(dummy_marker, subject0, sub_expected).value()
      == true);
    BOOST_REQUIRE(
      s.get_compatibility(subject0, fallback).value() == sub_expected);

    // duplicate should return false
    sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      s.set_compatibility(dummy_marker, subject0, sub_expected).value()
      == false);
    BOOST_REQUIRE(
      s.get_compatibility(subject0, fallback).value() == sub_expected);

    sub_expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      s.set_compatibility(dummy_marker, subject0, sub_expected).value()
      == true);
    BOOST_REQUIRE(
      s.get_compatibility(subject0, fallback).value() == sub_expected);
    BOOST_REQUIRE(s.get_compatibility().value() == global_expected);

    // Clearing compatibility should fallback to global
    BOOST_REQUIRE(
      s.clear_compatibility(dummy_marker, subject0).value() == true);
    BOOST_REQUIRE(
      s.get_compatibility(subject0, fallback).value() == global_expected);
}

BOOST_AUTO_TEST_CASE(test_store_subject_compat_fallback) {
    // A Subject should fallback to the current global setting
    auto fallback = pps::default_to_global::yes;

    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::store s;
    s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(s.get_compatibility(subject0, fallback).value() == expected);

    expected = pps::compatibility_level::forward;
    BOOST_REQUIRE(s.set_compatibility(expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility(subject0, fallback).value() == expected);
}

BOOST_AUTO_TEST_CASE(test_store_invalid_subject_compat) {
    // Setting and getting a compatibility for a non-existant subject should
    // fail
    auto fallback = pps::default_to_global::yes;

    pps::seq_marker dummy_marker;
    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::store s;

    BOOST_REQUIRE_EQUAL(
      s.get_compatibility(subject0, fallback).error().code(),
      pps::error_code::compatibility_not_found);

    expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      s.set_compatibility(dummy_marker, subject0, expected).value());
}

BOOST_AUTO_TEST_CASE(test_store_delete_subject) {
    const std::vector<pps::schema_version> expected_vers{
      {pps::schema_version{1}, pps::schema_version{2}}};

    pps::store s;
    s.set_compatibility(pps::compatibility_level::none).value();

    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EQUAL(
      s.delete_subject(dummy_marker, subject0, pps::permanent_delete::no)
        .error()
        .code(),
      pps::error_code::subject_not_found);

    BOOST_REQUIRE_EQUAL(
      s.delete_subject(dummy_marker, subject0, pps::permanent_delete::yes)
        .error()
        .code(),
      pps::error_code::subject_not_found);

    // First insert, expect id{1}, version{1}
    s.insert({subject0, string_def0.share()});
    s.insert({subject0, int_def0.share()});

    auto v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(v_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      v_res.value().cbegin(),
      v_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // permanent delete of not soft-deleted should fail
    auto d_res = s.delete_subject(
      dummy_marker, subject0, pps::permanent_delete::yes);
    BOOST_REQUIRE(d_res.has_error());
    BOOST_REQUIRE_EQUAL(
      d_res.error().code(), pps::error_code::subject_not_deleted);

    v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(v_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      v_res.value().cbegin(),
      v_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // expecting one subject
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::no).size(), 1);

    // soft delete should return versions
    d_res = s.delete_subject(dummy_marker, subject0, pps::permanent_delete::no);
    BOOST_REQUIRE(d_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      d_res.value().cbegin(),
      d_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // soft-deleted should return error
    v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(v_res.has_error());
    BOOST_REQUIRE_EQUAL(
      v_res.error().code(), pps::error_code::subject_not_found);

    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::no).size(), 0);
    BOOST_REQUIRE_EQUAL(s.get_subjects(pps::include_deleted::yes).size(), 1);

    // Second soft delete should fail
    d_res = s.delete_subject(dummy_marker, subject0, pps::permanent_delete::no);
    BOOST_REQUIRE(d_res.has_error());
    BOOST_REQUIRE_EQUAL(
      d_res.error().code(), pps::error_code::subject_soft_deleted);

    // Clearing the compatibility of a soft-deleted subject is allowed
    BOOST_REQUIRE(s.clear_compatibility(dummy_marker, subject0).has_value());

    v_res = s.get_versions(subject0, pps::include_deleted::yes);
    BOOST_REQUIRE(v_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      v_res.value().cbegin(),
      v_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // permanent delete should return versions
    d_res = s.delete_subject(
      dummy_marker, subject0, pps::permanent_delete::yes);
    BOOST_REQUIRE(d_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      d_res.value().cbegin(),
      d_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // permanently deleted should not find subject, even if include_deleted
    v_res = s.get_versions(subject0, pps::include_deleted::yes);
    BOOST_REQUIRE(v_res.has_error());
    BOOST_REQUIRE_EQUAL(
      v_res.error().code(), pps::error_code::subject_not_found);

    BOOST_REQUIRE(s.get_subjects(pps::include_deleted::no).empty());
    BOOST_REQUIRE(s.get_subjects(pps::include_deleted::yes).empty());

    // Second permanant delete should fail
    d_res = s.delete_subject(
      dummy_marker, subject0, pps::permanent_delete::yes);
    BOOST_REQUIRE(d_res.has_error());
    BOOST_REQUIRE_EQUAL(
      d_res.error().code(), pps::error_code::subject_not_found);

    // Clearing the compatibility of a hard-deleted subject should fail
    BOOST_REQUIRE(
      s.clear_compatibility(dummy_marker, subject0).error().code()
      == pps::error_code::subject_not_found);
}

BOOST_AUTO_TEST_CASE(test_store_delete_subject_version) {
    const std::vector<pps::schema_version> expected_vers{
      {pps::schema_version{1}, pps::schema_version{2}}};

    pps::seq_marker dummy_marker;
    pps::store s;
    s.set_compatibility(pps::compatibility_level::none).value();

    // Test unknown subject
    BOOST_REQUIRE_EQUAL(
      s.delete_subject_version(subject0, pps::schema_version{0}).error().code(),
      pps::error_code::subject_not_found);

    // First insert, expect id{1}, version{1}
    s.insert({subject0, string_def0.share()});
    s.insert({subject0, int_def0.share()});

    auto v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE(v_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      v_res.value().cbegin(),
      v_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // Test unknown versions
    BOOST_REQUIRE_EQUAL(
      s.delete_subject_version(subject0, pps::schema_version{42})
        .error()
        .code(),
      pps::error_code::subject_version_not_found);

    // perm-delete before soft-delete should fail
    BOOST_REQUIRE_EQUAL(
      s.delete_subject_version(subject0, pps::schema_version{1}).error().code(),
      pps::error_code::subject_version_not_deleted);

    // soft-delete version 1
    BOOST_REQUIRE_NO_THROW(s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{1},
      pps::schema_id{1},
      pps::is_deleted::yes));

    // expect [v2] for include_deleted::no
    v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(v_res.value().size(), 1);
    BOOST_REQUIRE_EQUAL(v_res.value().front(), pps::schema_version{2});

    // expect [v1,v2] for include_deleted::yes
    v_res = s.get_versions(subject0, pps::include_deleted::yes);
    BOOST_REQUIRE(v_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      v_res.value().cbegin(),
      v_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    // perm-delete version 1
    BOOST_REQUIRE(
      s.delete_subject_version(subject0, pps::schema_version{1}).value());

    // expect [v2] for include_deleted::no
    v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(v_res.value().size(), 1);
    BOOST_REQUIRE_EQUAL(v_res.value().front(), pps::schema_version{2});

    // expect [v2] for include_deleted::yes
    v_res = s.get_versions(subject0, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(v_res.value().size(), 1);
    BOOST_REQUIRE_EQUAL(v_res.value().front(), pps::schema_version{2});

    // perm-delete version 1, second time should fail
    BOOST_REQUIRE_EQUAL(
      s.delete_subject_version(subject0, pps::schema_version{1}).error().code(),
      pps::error_code::subject_version_not_found);
}

BOOST_AUTO_TEST_CASE(test_store_subject_version_latest) {
    pps::seq_marker dummy_marker;
    pps::store s;
    s.set_compatibility(pps::compatibility_level::none).value();

    // First insert, expect id{1}, version{1}
    s.insert({subject0, string_def0.share()});
    // First insert, expect id{2}, version{2}
    s.insert({subject0, int_def0.share()});

    // Test latest
    auto latest = s.get_subject_version_id(
      subject0, std::nullopt, pps::include_deleted::yes);
    BOOST_REQUIRE(latest.has_value());
    BOOST_REQUIRE_EQUAL(latest.assume_value().version, pps::schema_version{2});
    BOOST_REQUIRE_EQUAL(latest.assume_value().deleted, pps::is_deleted::no);

    latest = s.get_subject_version_id(
      subject0, std::nullopt, pps::include_deleted::no);
    BOOST_REQUIRE(latest.has_value());
    BOOST_REQUIRE_EQUAL(latest.assume_value().version, pps::schema_version{2});
    BOOST_REQUIRE_EQUAL(latest.assume_value().deleted, pps::is_deleted::no);

    // soft-delete version 2
    BOOST_REQUIRE_NO_THROW(s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{2},
      pps::schema_id{1},
      pps::is_deleted::yes));

    // Test latest
    latest = s.get_subject_version_id(
      subject0, std::nullopt, pps::include_deleted::yes);
    BOOST_REQUIRE(latest.has_value());
    BOOST_REQUIRE_EQUAL(latest.assume_value().version, pps::schema_version{2});
    BOOST_REQUIRE_EQUAL(latest.assume_value().deleted, pps::is_deleted::yes);

    latest = s.get_subject_version_id(
      subject0, std::nullopt, pps::include_deleted::no);
    BOOST_REQUIRE(latest.has_value());
    BOOST_REQUIRE_EQUAL(latest.assume_value().version, pps::schema_version{1});
    BOOST_REQUIRE_EQUAL(latest.assume_value().deleted, pps::is_deleted::no);
}

BOOST_AUTO_TEST_CASE(test_store_delete_subject_after_delete_version) {
    std::vector<pps::schema_version> expected_vers{{pps::schema_version{2}}};

    pps::store s;
    s.set_compatibility(pps::compatibility_level::none).value();

    pps::seq_marker dummy_marker;

    // First insert, expect id{1}, version{1}
    s.insert({subject0, string_def0.share()});
    s.insert({subject0, int_def0.share()});

    // delete version 1
    s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{1},
      pps::schema_id{1},
      pps::is_deleted::yes);

    auto del_res = s.delete_subject(
      dummy_marker, subject0, pps::permanent_delete::no);
    BOOST_REQUIRE(del_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      del_res.value().cbegin(),
      del_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());

    expected_vers = {{pps::schema_version{1}}, {pps::schema_version{2}}};
    del_res = s.delete_subject(
      dummy_marker, subject0, pps::permanent_delete::yes);
    BOOST_REQUIRE(del_res.has_value());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      del_res.value().cbegin(),
      del_res.value().cend(),
      expected_vers.cbegin(),
      expected_vers.cend());
}
