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
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/util.h"

#include <boost/test/unit_test.hpp>

#include <algorithm>

namespace pps = pandaproxy::schema_registry;

constexpr std::string_view sv_string_def0{R"({"type":"string"})"};
constexpr std::string_view sv_string_def1{R"({"type": "string"})"};
constexpr std::string_view sv_int_def0{R"({"type": "int"})"};
const pps::schema_definition string_def0{
  pps::make_schema_definition<json::UTF8<>>(sv_string_def0).value(),
  pps::schema_type::avro};
const pps::schema_definition string_def1{
  pps::make_schema_definition<json::UTF8<>>(sv_string_def1).value(),
  pps::schema_type::avro};
const pps::schema_definition int_def0{
  pps::make_schema_definition<json::UTF8<>>(sv_int_def0).value(),
  pps::schema_type::avro};
const auto subject0 = pps::context_subject::unqualified("subject0");
const auto subject1 = pps::context_subject::unqualified("subject1");
const auto subject2 = pps::context_subject::unqualified("subject2");

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
  pps::context_subject sub,
  pps::schema_definition def,
  pps::schema_type,
  pps::schema_id id,
  pps::schema_version version,
  pps::is_deleted deleted) {
    store.upsert_schema({sub.ctx, id}, std::move(def), false);
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

    auto res = s.get_schema_definition(
      {pps::default_context, pps::schema_id{1}});
    BOOST_REQUIRE(res.has_error());
    auto err = std::move(res).assume_error();
    BOOST_REQUIRE(err.code() == pps::error_code::schema_id_not_found);

    // First insert, expect id{1}
    auto ins_res = s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    res = s.get_schema_definition({pps::default_context, ins_res.id});
    BOOST_REQUIRE(res.has_value());

    auto def = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(def, string_def0.share());
}

BOOST_AUTO_TEST_CASE(test_store_get_schema_subject_versions) {
    pps::store s;

    pps::seq_marker dummy_marker;

    // First insert, expect id{1}
    auto ins_res = s.insert({subject0, schema1.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    auto versions = s.get_schema_subject_versions(
      {pps::default_context, pps::schema_id{1}});
    BOOST_REQUIRE_EQUAL(versions.size(), 1);
    BOOST_REQUIRE_EQUAL(versions[0].sub, subject0);
    BOOST_REQUIRE_EQUAL(versions[0].version, pps::schema_version{1});

    versions = s.get_schema_subject_versions(
      {pps::default_context, pps::schema_id{2}});
    BOOST_REQUIRE(versions.empty());

    // Second insert, expect id{2}
    ins_res = s.insert({subject0, schema2.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{2});

    // expect [{schema 2, version 2}]
    versions = s.get_schema_subject_versions(
      {pps::default_context, pps::schema_id{2}});
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
    versions = s.get_schema_subject_versions(
      {pps::default_context, pps::schema_id{2}});
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
    auto ins_res = s.insert({subject0, schema1.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    auto subjects = s.get_schema_subjects(
      {pps::default_context, pps::schema_id{1}}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject0)), 1);

    // Second insert, same schema, expect id{1}
    ins_res = s.insert({subject1, schema1.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert yet another schema associated with a different subject
    ins_res = s.insert({subject2, schema2.share()});
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    subjects = s.get_schema_subjects(
      {pps::default_context, pps::schema_id{1}}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject1)), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject2)), 0);

    subjects = s.get_schema_subjects(
      {pps::default_context, pps::schema_id{2}}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject0)), 0);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject1)), 0);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject2)), 1);

    // Test deletion

    s.upsert_subject(
      dummy_marker,
      subject0,
      pps::schema_version{1},
      pps::schema_id{1},
      pps::is_deleted::yes);

    subjects = s.get_schema_subjects(
      {pps::default_context, pps::schema_id{1}}, pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject1)), 1);

    subjects = s.get_schema_subjects(
      {pps::default_context, pps::schema_id{1}}, pps::include_deleted::yes);
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject1)), 1);
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
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject0)), 1);

    // second insert
    s.insert({subject1, string_def0.share()});
    subjects = s.get_subjects(pps::include_deleted::no);
    BOOST_REQUIRE(subjects.size() == 2);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(std::ranges::count_if(subjects, is_equal(subject1)), 1);

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

BOOST_AUTO_TEST_CASE(test_store_get_subjects_prefix) {
    pps::store s;

    const auto ctx_test = pps::context{".test"};
    const auto ctx_other = pps::context{".other"};

    // Insert subjects across multiple contexts
    s.insert(
      {pps::context_subject{pps::default_context, pps::subject{"apple"}},
       string_def0.share()});
    s.insert(
      {pps::context_subject{pps::default_context, pps::subject{"app"}},
       int_def0.share()});
    s.insert(
      {pps::context_subject{pps::default_context, pps::subject{"banana"}},
       string_def0.share()});
    s.insert(
      {pps::context_subject{ctx_test, pps::subject{"apple"}},
       string_def0.share()});
    s.insert(
      {pps::context_subject{ctx_test, pps::subject{"avocado"}},
       int_def0.share()});
    s.insert(
      {pps::context_subject{ctx_other, pps::subject{"apple"}},
       string_def0.share()});
    s.insert(
      {pps::context_subject{ctx_other, pps::subject{"banana"}},
       int_def0.share()});

    // No prefix returns all subjects across all contexts
    auto subjects = s.get_subjects(pps::include_deleted::no);
    BOOST_REQUIRE_EQUAL(subjects.size(), 7);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"app"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"banana"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"avocado"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_other, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_other, pps::subject{"banana"}}));

    // Explicit default context prefix ":.:" matches all default context
    // subjects
    subjects = s.get_subjects(pps::include_deleted::no, ":.:");
    BOOST_REQUIRE_EQUAL(subjects.size(), 3);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"app"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"banana"}}));

    // Prefix "app" matches default context "apple" and "app"
    subjects = s.get_subjects(pps::include_deleted::no, "app");
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"app"}}));

    // Prefix "apple" matches only default context "apple"
    subjects = s.get_subjects(pps::include_deleted::no, "apple");
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"apple"}}));

    // Prefix ":.test:" matches all subjects in the .test context
    subjects = s.get_subjects(pps::include_deleted::no, ":.test:");
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"avocado"}}));

    // Prefix ":.test:app" matches only .test context "apple"
    subjects = s.get_subjects(pps::include_deleted::no, ":.test:app");
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"apple"}}));

    // Prefix ":.other:" matches all subjects in the .other context
    subjects = s.get_subjects(pps::include_deleted::no, ":.other:");
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_other, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_other, pps::subject{"banana"}}));

    // Prefix with no matches
    subjects = s.get_subjects(pps::include_deleted::no, "zzz");
    BOOST_REQUIRE(subjects.empty());

    // Wildcard prefix ":*:app" matches subjects named "app*" in all contexts
    subjects = s.get_subjects(pps::include_deleted::no, ":*:app");
    BOOST_REQUIRE_EQUAL(subjects.size(), 4);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"app"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"apple"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_other, pps::subject{"apple"}}));

    // Wildcard prefix ":*:apple" matches exact name "apple" across contexts
    subjects = s.get_subjects(pps::include_deleted::no, ":*:apple");
    BOOST_REQUIRE_EQUAL(subjects.size(), 3);

    // Wildcard prefix ":*:banana" matches "banana" in default and .other
    subjects = s.get_subjects(pps::include_deleted::no, ":*:banana");
    BOOST_REQUIRE_EQUAL(subjects.size(), 2);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects,
        pps::context_subject{pps::default_context, pps::subject{"banana"}}));
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_other, pps::subject{"banana"}}));

    // Wildcard prefix ":*:avocado" matches only .test context
    subjects = s.get_subjects(pps::include_deleted::no, ":*:avocado");
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE(
      std::ranges::contains(
        subjects, pps::context_subject{ctx_test, pps::subject{"avocado"}}));

    // Wildcard prefix ":*:" with empty subject prefix matches all subjects
    subjects = s.get_subjects(pps::include_deleted::no, ":*:");
    BOOST_REQUIRE_EQUAL(subjects.size(), 7);

    // Wildcard prefix with no matches
    subjects = s.get_subjects(pps::include_deleted::no, ":*:zzz");
    BOOST_REQUIRE(subjects.empty());
}

BOOST_AUTO_TEST_CASE(test_store_global_compat) {
    // Setting the retrieving global compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::store s;
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, pps::default_to_global::yes)
        .value()
      == expected);

    // duplicate should return false
    BOOST_REQUIRE(s.clear_compatibility(pps::default_context).value() == false);
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, pps::default_to_global::yes)
        .value()
      == expected);

    expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      s.set_compatibility(dummy_marker, pps::default_context, expected).value()
      == true);
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, pps::default_to_global::yes)
        .value()
      == expected);
}

BOOST_AUTO_TEST_CASE(test_store_subject_compat) {
    // Setting the retrieving a subject compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;

    pps::compatibility_level global_expected{
      pps::compatibility_level::backward};
    pps::store s;
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, fallback).value()
      == global_expected);
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
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, fallback).value()
      == global_expected);

    // Clearing compatibility should fallback to global
    BOOST_REQUIRE(
      s.clear_compatibility(dummy_marker, subject0).value() == true);
    BOOST_REQUIRE(
      s.get_compatibility(subject0, fallback).value() == global_expected);
}

BOOST_AUTO_TEST_CASE(test_store_subject_compat_fallback) {
    // A Subject should fallback to the current global setting
    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;

    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::store s;
    s.insert({subject0, string_def0.share()});
    BOOST_REQUIRE(s.get_compatibility(subject0, fallback).value() == expected);

    expected = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      s.set_compatibility(dummy_marker, pps::default_context, expected).value()
      == true);
    BOOST_REQUIRE(s.get_compatibility(subject0, fallback).value() == expected);
}

BOOST_AUTO_TEST_CASE(test_store_invalid_subject_compat) {
    // Setting and getting a compatibility for a non-existant subject should
    // fail
    auto fallback = pps::default_to_global::no;

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

    pps::seq_marker dummy_marker;
    pps::store s;
    s.set_compatibility(
       dummy_marker, pps::default_context, pps::compatibility_level::none)
      .value();

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
    s.set_compatibility(
       dummy_marker, pps::default_context, pps::compatibility_level::none)
      .value();

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
    s.set_compatibility(
       dummy_marker, pps::default_context, pps::compatibility_level::none)
      .value();

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

    pps::seq_marker dummy_marker;
    pps::store s;
    s.set_compatibility(
       dummy_marker, pps::default_context, pps::compatibility_level::none)
      .value();

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

BOOST_AUTO_TEST_CASE(test_store_context_mode) {
    // Test setting and getting mode at the context level
    auto test_ctx = pps::context{".test"};
    pps::seq_marker dummy_marker;
    auto s = pps::store{pps::is_mutable::yes};
    auto fallback = pps::default_to_global::yes;

    // Default mode is read_write
    BOOST_REQUIRE(
      s.get_mode(pps::default_context, fallback).value()
      == pps::mode::read_write);
    BOOST_REQUIRE(
      s.get_mode(test_ctx, fallback).value() == pps::mode::read_write);

    // Set mode on default context
    BOOST_REQUIRE(s.set_mode(
                     dummy_marker,
                     pps::default_context,
                     pps::mode::read_only,
                     pps::force::no)
                    .value());
    BOOST_REQUIRE(
      s.get_mode(pps::default_context, fallback).value()
      == pps::mode::read_only);
    BOOST_REQUIRE(
      s.get_mode(test_ctx, fallback).value() == pps::mode::read_write);

    // Set different mode on test context
    BOOST_REQUIRE(
      s.set_mode(dummy_marker, test_ctx, pps::mode::import, pps::force::no)
        .value());
    BOOST_REQUIRE(
      s.get_mode(pps::default_context, fallback).value()
      == pps::mode::read_only);
    BOOST_REQUIRE(s.get_mode(test_ctx, fallback).value() == pps::mode::import);

    // Clear mode returns to default
    BOOST_REQUIRE(s.clear_mode(test_ctx, pps::force::no).value());
    BOOST_REQUIRE(
      s.get_mode(test_ctx, fallback).value() == pps::mode::read_write);
}

BOOST_AUTO_TEST_CASE(test_store_context_mode_written_at) {
    // Test that mode write markers are tracked correctly at context level
    auto test_ctx = pps::context{".test"};
    auto s = pps::store{pps::is_mutable::yes};

    // Initially context doesn't exist
    BOOST_REQUIRE(s.get_context_mode_written_at(test_ctx).has_error());
    BOOST_REQUIRE(
      s.get_context_mode_written_at(pps::default_context).has_error());

    // Create distinct markers
    auto marker1 = pps::seq_marker{
      .seq = model::offset{1},
      .node = model::node_id{0},
      .version = pps::schema_version{0},
      .key_type = pps::seq_marker_key_type::mode};
    auto marker2 = pps::seq_marker{
      .seq = model::offset{2},
      .node = model::node_id{0},
      .version = pps::schema_version{0},
      .key_type = pps::seq_marker_key_type::mode};

    // Set mode on test context, verify marker is tracked
    BOOST_REQUIRE(
      s.set_mode(marker1, test_ctx, pps::mode::read_only, pps::force::no)
        .value());
    auto markers = s.get_context_mode_written_at(test_ctx).value();
    BOOST_REQUIRE_EQUAL(markers.size(), 1);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);

    // Set mode again, second marker is added
    BOOST_REQUIRE(
      s.set_mode(marker2, test_ctx, pps::mode::import, pps::force::no).value());
    markers = s.get_context_mode_written_at(test_ctx).value();
    BOOST_REQUIRE_EQUAL(markers.size(), 2);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);
    BOOST_REQUIRE_EQUAL(markers[1], marker2);

    // Default context should still not exist
    BOOST_REQUIRE(
      s.get_context_mode_written_at(pps::default_context).has_error());

    // Clear mode clears all markers
    BOOST_REQUIRE(s.clear_mode(test_ctx, pps::force::no).value());
    BOOST_REQUIRE(s.get_context_mode_written_at(test_ctx).has_error());
}

BOOST_AUTO_TEST_CASE(test_store_context_config) {
    // Test setting and getting compatibility (config) at the context level
    auto test_ctx = pps::context{".test"};
    pps::seq_marker dummy_marker;
    auto s = pps::store{pps::is_mutable::yes};
    auto fallback = pps::default_to_global::yes;

    // Default config is backward compatibility
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, fallback).value()
      == pps::compatibility_level::backward);
    BOOST_REQUIRE(
      s.get_compatibility(test_ctx, fallback).value()
      == pps::compatibility_level::backward);

    // Set config on default context
    BOOST_REQUIRE(
      s.set_compatibility(
         dummy_marker, pps::default_context, pps::compatibility_level::full)
        .value());
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, fallback).value()
      == pps::compatibility_level::full);
    BOOST_REQUIRE(
      s.get_compatibility(test_ctx, fallback).value()
      == pps::compatibility_level::backward);

    // Set different config on test context
    BOOST_REQUIRE(s.set_compatibility(
                     dummy_marker, test_ctx, pps::compatibility_level::none)
                    .value());
    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, fallback).value()
      == pps::compatibility_level::full);
    BOOST_REQUIRE(
      s.get_compatibility(test_ctx, fallback).value()
      == pps::compatibility_level::none);

    // Clear config returns to default
    BOOST_REQUIRE(s.clear_compatibility(test_ctx).value());
    BOOST_REQUIRE(
      s.get_compatibility(test_ctx, fallback).value()
      == pps::compatibility_level::backward);
}

BOOST_AUTO_TEST_CASE(test_store_context_config_written_at) {
    // Test that config (compatibility) write markers are tracked correctly
    auto test_ctx = pps::context{".test"};
    pps::store s;

    // Initially context doesn't exist
    BOOST_REQUIRE(s.get_context_config_written_at(test_ctx).has_error());

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
      s.set_compatibility(marker1, test_ctx, pps::compatibility_level::full)
        .value());
    auto markers = s.get_context_config_written_at(test_ctx).value();
    BOOST_REQUIRE_EQUAL(markers.size(), 1);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);

    // Set compatibility again, second marker is added
    BOOST_REQUIRE(
      s.set_compatibility(marker2, test_ctx, pps::compatibility_level::none)
        .value());
    markers = s.get_context_config_written_at(test_ctx).value();
    BOOST_REQUIRE_EQUAL(markers.size(), 2);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);
    BOOST_REQUIRE_EQUAL(markers[1], marker2);

    // Default context should still not exist
    BOOST_REQUIRE(
      s.get_context_config_written_at(pps::default_context).has_error());

    // Clear compatibility clears all markers
    BOOST_REQUIRE(s.clear_compatibility(test_ctx).value());
    BOOST_REQUIRE(s.get_context_config_written_at(test_ctx).has_error());
}

BOOST_AUTO_TEST_CASE(test_store_context_materialized) {
    pps::store s;
    auto test_ctx = pps::context{".test"};

    // Empty state
    BOOST_REQUIRE(s.is_context_materialized(pps::default_context));
    BOOST_REQUIRE(!s.is_context_materialized(test_ctx));
    auto contexts = s.get_materialized_contexts();
    BOOST_REQUIRE_EQUAL(contexts.size(), 1);
    BOOST_REQUIRE_EQUAL(contexts[0], pps::default_context);

    // Set .test context as materialized
    s.set_context_materialized(test_ctx, true);
    BOOST_REQUIRE(s.is_context_materialized(test_ctx));
    contexts = s.get_materialized_contexts();
    BOOST_REQUIRE_EQUAL(contexts.size(), 2);
    BOOST_REQUIRE(
      std::ranges::find(contexts, pps::default_context) != contexts.end());
    BOOST_REQUIRE(std::ranges::find(contexts, test_ctx) != contexts.end());

    // Set .test context as not materialized (tombstone)
    s.set_context_materialized(test_ctx, false);
    BOOST_REQUIRE(!s.is_context_materialized(test_ctx));
    contexts = s.get_materialized_contexts();
    BOOST_REQUIRE_EQUAL(contexts.size(), 1);
    BOOST_REQUIRE_EQUAL(contexts[0], pps::default_context);
}
