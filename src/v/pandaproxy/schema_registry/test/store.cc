// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/store.h"

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/util.h"

#include <absl/algorithm/container.h>
#include <boost/test/unit_test.hpp>

namespace pps = pandaproxy::schema_registry;

constexpr std::string_view sv_string_def0{R"({"type":"string"})"};
constexpr std::string_view sv_string_def1{R"({"type": "string"})"};
constexpr std::string_view sv_int_def0{R"({"type": "int"})"};
const pps::schema_definition string_def0{
  pps::make_schema_definition<rapidjson::UTF8<>>(sv_string_def0).value()};
const pps::schema_definition string_def1{
  pps::make_schema_definition<rapidjson::UTF8<>>(sv_string_def1).value()};
const pps::schema_definition int_def0{
  pps::make_schema_definition<rapidjson::UTF8<>>(sv_int_def0).value()};
const pps::subject subject0{"subject0"};
const pps::subject subject1{"subject1"};

BOOST_AUTO_TEST_CASE(test_store_insert) {
    pps::store s;

    // First insert, expect id{1}, version{1}
    auto ins_res = s.insert(subject0, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert duplicate, expect id{1}, versions{1}
    ins_res = s.insert(subject0, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(!ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert duplicate, with spaces, expect id{1}, versions{1}
    ins_res = s.insert(subject0, string_def1, pps::schema_type::avro);
    BOOST_REQUIRE(!ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert on different subject, expect id{1}, version{1}
    ins_res = s.insert(subject1, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Insert different schema, expect id{2}, version{2}
    ins_res = s.insert(subject0, int_def0, pps::schema_type::avro);
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{2});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{2});
}

BOOST_AUTO_TEST_CASE(test_store_get_schema) {
    pps::store s;

    auto res = s.get_schema(pps::schema_id{1});
    BOOST_REQUIRE(res.has_error());
    auto err = std::move(res).assume_error();
    BOOST_REQUIRE(err == pps::error_code::schema_id_not_found);

    // First insert, expect id{1}
    auto ins_res = s.insert(subject0, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    res = s.get_schema(ins_res.id);
    BOOST_REQUIRE(res.has_value());

    auto val = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(val.id, ins_res.id);
    BOOST_REQUIRE_EQUAL(val.definition(), string_def0());
    BOOST_REQUIRE(val.type == pps::schema_type::avro);
}

BOOST_AUTO_TEST_CASE(test_store_get_subject_schema) {
    pps::store s;

    auto res = s.get_subject_schema(subject0, pps::schema_version{1});
    BOOST_REQUIRE(res.has_error());
    auto err = std::move(res).assume_error();
    BOOST_REQUIRE(err == pps::error_code::subject_not_found);

    // First insert, expect id{1}, version{1}
    auto ins_res = s.insert(subject0, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(ins_res.version, pps::schema_version{1});

    // Request good version
    res = s.get_subject_schema(subject0, pps::schema_version{1});
    BOOST_REQUIRE(res.has_value());
    auto val = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(val.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(val.version, pps::schema_version{1});
    BOOST_REQUIRE_EQUAL(val.deleted, false);
    BOOST_REQUIRE_EQUAL(val.definition, string_def0);

    // Second insert, expect id{1}, version{1}
    ins_res = s.insert(subject0, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(!ins_res.inserted);
    BOOST_REQUIRE_EQUAL(ins_res.id, pps::schema_id{1});

    // Request good version
    res = s.get_subject_schema(subject0, pps::schema_version{1});
    BOOST_REQUIRE(res.has_value());
    val = std::move(res).assume_value();
    BOOST_REQUIRE_EQUAL(val.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(val.version, pps::schema_version{1});
    BOOST_REQUIRE_EQUAL(val.deleted, false);
    BOOST_REQUIRE_EQUAL(val.definition, string_def0);

    // Request bad version
    res = s.get_subject_schema(subject0, pps::schema_version{2});
    BOOST_REQUIRE(res.has_error());
    err = std::move(res).assume_error();
    BOOST_REQUIRE(err == pps::error_code::subject_version_not_found);
}

BOOST_AUTO_TEST_CASE(test_store_get_versions) {
    pps::store s;

    // First insert, expect id{1}, version{1}
    s.insert(subject0, string_def0, pps::schema_type::avro);

    auto versions = s.get_versions(subject0);
    BOOST_REQUIRE(versions.has_value());
    BOOST_REQUIRE_EQUAL(versions.value().size(), 1);
    BOOST_REQUIRE_EQUAL(versions.value().front(), pps::schema_version{1});

    // Insert duplicate, expect id{1}, versions{1}
    s.insert(subject0, string_def0, pps::schema_type::avro);

    versions = s.get_versions(subject0);
    BOOST_REQUIRE(versions.has_value());
    BOOST_REQUIRE_EQUAL(versions.value().size(), 1);
    BOOST_REQUIRE_EQUAL(versions.value().front(), pps::schema_version{1});

    // Insert different schema, expect id{2}, version{2}
    s.insert(subject0, int_def0, pps::schema_type::avro);

    versions = s.get_versions(subject0);
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

    auto subjects = s.get_subjects();
    BOOST_REQUIRE(subjects.empty());

    // First insert
    s.insert(subject0, string_def0, pps::schema_type::avro);
    subjects = s.get_subjects();
    BOOST_REQUIRE_EQUAL(subjects.size(), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);

    // second insert
    s.insert(subject1, string_def0, pps::schema_type::avro);
    subjects = s.get_subjects();
    BOOST_REQUIRE(subjects.size() == 2);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject0)), 1);
    BOOST_REQUIRE_EQUAL(absl::c_count_if(subjects, is_equal(subject1)), 1);
}

BOOST_AUTO_TEST_CASE(test_store_global_compat) {
    // Setting the retrieving global compatibility should be allowed multiple
    // times

    pps::compatibility_level expected{pps::compatibility_level::none};
    pps::store s;
    BOOST_REQUIRE(s.get_compatibility().value() == expected);

    expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(s.set_compatibility(expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility().value() == expected);

    // duplicate should return false
    expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(s.set_compatibility(expected).value() == false);
    BOOST_REQUIRE(s.get_compatibility().value() == expected);

    expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(s.set_compatibility(expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility().value() == expected);
}

BOOST_AUTO_TEST_CASE(test_store_subject_compat) {
    // Setting the retrieving a subject compatibility should be allowed multiple
    // times

    pps::compatibility_level global_expected{pps::compatibility_level::none};
    pps::store s;
    BOOST_REQUIRE(s.get_compatibility().value() == global_expected);
    s.insert(subject0, string_def0, pps::schema_type::avro);

    auto sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(s.set_compatibility(subject0, sub_expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility(subject0).value() == sub_expected);

    // duplicate should return false
    sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(s.set_compatibility(subject0, sub_expected).value() == false);
    BOOST_REQUIRE(s.get_compatibility(subject0).value() == sub_expected);

    sub_expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(s.set_compatibility(subject0, sub_expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility(subject0).value() == sub_expected);
    BOOST_REQUIRE(s.get_compatibility().value() == global_expected);
}

BOOST_AUTO_TEST_CASE(test_store_subject_compat_fallback) {
    // A Subject should fallback to the current global setting

    pps::compatibility_level expected{pps::compatibility_level::none};
    pps::store s;
    s.insert(subject0, string_def0, pps::schema_type::avro);
    BOOST_REQUIRE(s.get_compatibility(subject0).value() == expected);

    expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(s.set_compatibility(expected).value() == true);
    BOOST_REQUIRE(s.get_compatibility(subject0).value() == expected);
}

BOOST_AUTO_TEST_CASE(test_store_invalid_subject_compat) {
    // Setting and getting a compatibility for a non-existant subject should
    // fail

    pps::compatibility_level expected{pps::compatibility_level::none};
    pps::store s;

    BOOST_REQUIRE_EQUAL(
      s.get_compatibility(subject0).error(),
      pps::error_code::subject_not_found);

    expected = pps::compatibility_level::backward;
    BOOST_REQUIRE_EQUAL(
      s.set_compatibility(subject0, expected).error(),
      pps::error_code::subject_not_found);
}
