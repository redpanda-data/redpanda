// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/storage.h"

#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/util.h"

#include <absl/algorithm/container.h>
#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

namespace ppj = pandaproxy::json;
namespace pps = pandaproxy::schema_registry;

constexpr std::string_view avro_schema_key_sv{
  R"({
  "keytype": "SCHEMA",
  "subject": "my-kafka-value",
  "version": 1,
  "magic": 1
})"};
const pps::schema_key avro_schema_key{
  .sub{pps::subject{"my-kafka-value"}},
  .version{pps::schema_version{1}},
  .magic{pps::topic_key_magic{1}}};

constexpr std::string_view avro_schema_value_sv{
  R"({
  "subject": "my-kafka-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"string\"}",
  "deleted": true
})"};
const pps::schema_value avro_schema_value{
  .sub{pps::subject{"my-kafka-value"}},
  .version{pps::schema_version{1}},
  .type = pps::schema_type::avro,
  .id{pps::schema_id{1}},
  .schema{pps::schema_definition{R"({"type":"string"})"}},
  .deleted = true};

BOOST_AUTO_TEST_CASE(test_storage_serde) {
    {
        auto key = ppj::rjson_parse(
          avro_schema_key_sv.data(), pps::schema_key_handler<>{});
        BOOST_CHECK_EQUAL(avro_schema_key, key);

        auto str = ppj::rjson_serialize(avro_schema_key);
        BOOST_CHECK_EQUAL(str, ppj::minify(avro_schema_key_sv));
    }

    {
        auto val = ppj::rjson_parse(
          avro_schema_value_sv.data(), pps::schema_value_handler<>{});
        BOOST_CHECK_EQUAL(avro_schema_value, val);

        auto str = ppj::rjson_serialize(avro_schema_value);
        BOOST_CHECK_EQUAL(str, ppj::minify(avro_schema_value_sv));
    }
}
