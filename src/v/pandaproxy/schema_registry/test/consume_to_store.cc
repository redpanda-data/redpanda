// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/util.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>

namespace pps = pandaproxy::schema_registry;

constexpr std::string_view sv_string_def0{R"({"type":"string"})"};
constexpr std::string_view sv_int_def0{R"({"type": "int"})"};
const pps::schema_definition string_def0{
  pps::make_schema_definition<rapidjson::UTF8<>>(sv_string_def0).value()};
const pps::schema_definition int_def0{
  pps::make_schema_definition<rapidjson::UTF8<>>(sv_int_def0).value()};
const pps::subject subject0{"subject0"};
constexpr pps::topic_key_magic magic0{0};
constexpr pps::topic_key_magic magic1{1};
constexpr pps::topic_key_magic magic2{2};
constexpr pps::schema_version version0{0};
constexpr pps::schema_version version1{1};
constexpr pps::schema_id id0{0};
constexpr pps::schema_id id1{1};

SEASTAR_THREAD_TEST_CASE(test_consume_to_store) {
    pps::store s;
    auto c = pps::consume_to_store(s);

    auto good_schema_1 = pps::as_record_batch(
      pps::schema_key{subject0, version0, magic1},
      pps::schema_value{
        subject0, version0, pps::schema_type::avro, id0, string_def0});
    BOOST_REQUIRE_NO_THROW(c(std::move(good_schema_1)).get());

    auto s_res = s.get_subject_schema(
      subject0, version0, pps::include_deleted::no);
    BOOST_REQUIRE(s_res.has_value());
    BOOST_REQUIRE(s_res.value().definition == string_def0);

    auto bad_schema_magic = pps::as_record_batch(
      pps::schema_key{subject0, version0, magic2},
      pps::schema_value{
        subject0, version0, pps::schema_type::avro, id0, string_def0});
    BOOST_REQUIRE_THROW(c(std::move(bad_schema_magic)).get(), pps::exception);

    BOOST_REQUIRE(
      s.get_compatibility().value() == pps::compatibility_level::none);
    BOOST_REQUIRE(
      s.get_compatibility(subject0).value() == pps::compatibility_level::none);

    auto good_config = pps::as_record_batch(
      pps::config_key{subject0, magic0},
      pps::config_value{pps::compatibility_level::full});
    BOOST_REQUIRE_NO_THROW(c(std::move(good_config)).get());

    BOOST_REQUIRE(
      s.get_compatibility(subject0).value() == pps::compatibility_level::full);

    auto bad_config_magic = pps::as_record_batch(
      pps::config_key{subject0, magic1},
      pps::config_value{pps::compatibility_level::full});
    BOOST_REQUIRE_THROW(c(std::move(bad_config_magic)).get(), pps::exception);
}
