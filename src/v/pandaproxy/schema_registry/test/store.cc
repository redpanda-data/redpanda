// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/store.h"

#include "pandaproxy/schema_registry/util.h"

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
