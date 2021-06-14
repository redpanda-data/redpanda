/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/api/wasm_list.h"
#include "coproc/tests/fixtures/coproc_slim_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "test_utils/fixture.h"
#include "utils/type_traits.h"

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <numeric>
#include <type_traits>

using copro_typeid = coproc::registry::type_identifier;

template<typename T>
class rapidjson_value_comparator {
public:
    explicit rapidjson_value_comparator(T&& value)
      : _value(std::forward<T>(value)) {}

    bool operator()(const rapidjson::Value& v) const {
        if constexpr (std::is_integral_v<T>) {
            return v.GetInt() == _value;
        } else if constexpr (
          std::is_same_v<T, std::string> || std::is_same_v<T, ss::sstring>) {
            return v.GetString() == _value;
        } else if constexpr (std::is_same_v<T, bool>) {
            return v.GetBool() == _value;
        } else if constexpr (std::is_floating_point_v<T>) {
            return v.GetDouble() == _value;
        } else {
            static_assert(dependent_false<T>::value, "Unsupported type");
        }
        return false;
    }

private:
    T _value;
};

FIXTURE_TEST(test_wasm_status_report, coproc_slim_fixture) {
    model::node_id nid(5);
    model::topic foo("foo");
    model::topic bar("bar");
    model::topic baz("baz");
    setup({{foo, 2}, {bar, 8}, {baz, 3}}).get();
    enable_coprocessors(
      {{.id = 22220,
        .data{
          .tid = copro_typeid::null_coprocessor, .topics = {{bar, tp_stored}}}},
       {.id = 77778,
        .data{
          .tid = copro_typeid::null_coprocessor,
          .topics = {{foo, tp_stored}, {baz, tp_latest}}}}})
      .get();
    absl::btree_map<coproc::script_id, coproc::wasm::deploy_attributes> layout;
    layout[coproc::script_id(22220)] = coproc::wasm::deploy_attributes{
      .description = "1 hello world!", .name = "22220-name"};
    layout[coproc::script_id(77778)] = coproc::wasm::deploy_attributes{
      .description = "2 hello world!", .name = "77778-name"};
    auto result
      = coproc::wasm::current_status(nid, get_pacemaker(), layout).get();
    auto data = model::consume_reader_to_memory(
                  std::move(result), model::no_timeout)
                  .get();
    BOOST_CHECK_EQUAL(data.size(), 1);
    auto record = std::move(data[0].copy_records()[0]);
    auto record_key = record.release_key();
    auto record_iobuf = record.release_value();
    auto key_node_id
      = iobuf_const_parser(record_key).consume_type<model::node_id::type>();
    auto value_as_string
      = iobuf_const_parser(record_iobuf).read_string(record_iobuf.size_bytes());

    info("Raw Json value: {}", value_as_string);
    rapidjson::Document doc;
    doc.Parse(std::move(value_as_string));
    BOOST_CHECK(!doc.HasParseError() && doc.IsObject());
    BOOST_CHECK(
      doc.HasMember("node_id") && doc.HasMember("status")
      && doc.HasMember("coprocessors"));
    BOOST_CHECK_EQUAL(std::distance(doc.MemberBegin(), doc.MemberEnd()), 3);
    BOOST_CHECK_EQUAL(doc["node_id"].GetInt(), nid());
    BOOST_CHECK_EQUAL(doc["node_id"].GetInt(), key_node_id);
    BOOST_CHECK_EQUAL(ss::sstring(doc["status"].GetString()), "up");
    BOOST_CHECK(doc["coprocessors"].IsObject());
    const auto& copros = doc["coprocessors"];
    BOOST_CHECK(copros.HasMember("22220-name"));
    BOOST_CHECK(copros.HasMember("77778-name"));
    BOOST_CHECK_EQUAL(
      std::distance(copros.MemberBegin(), copros.MemberEnd()), 2);
    const auto& two_obj = copros["22220-name"].GetObject();
    const auto& seven_obj = copros["77778-name"].GetObject();
    BOOST_CHECK(two_obj.HasMember("input_topics"));
    BOOST_CHECK(two_obj.HasMember("description"));
    BOOST_CHECK(seven_obj.HasMember("input_topics"));
    BOOST_CHECK(seven_obj.HasMember("description"));
    BOOST_CHECK_EQUAL(seven_obj["description"].GetString(), "2 hello world!");
    BOOST_CHECK_EQUAL(two_obj["description"].GetString(), "1 hello world!");
    const auto& two_arr = two_obj["input_topics"].GetArray();
    const auto& seven_arr = seven_obj["input_topics"].GetArray();
    BOOST_CHECK_EQUAL(two_arr.Size(), 1);
    BOOST_CHECK_EQUAL(ss::sstring(two_arr[0].GetString()), "bar");
    BOOST_CHECK_EQUAL(seven_arr.Size(), 2);

    BOOST_CHECK_NE(
      std::find_if(
        seven_arr.Begin(),
        seven_arr.End(),
        rapidjson_value_comparator<std::string>("foo")),
      seven_arr.End());
    BOOST_CHECK_NE(
      std::find_if(
        seven_arr.Begin(),
        seven_arr.End(),
        rapidjson_value_comparator<std::string>("baz")),
      seven_arr.End());
}
