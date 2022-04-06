// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/config_store.h"
#include "json/document.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <cstdint>
#include <iostream>
#include <random>
#include <utility>

namespace {

ss::logger lg("config test"); // NOLINT

struct test_config : public config::config_store {
    config::property<int> optional_int;
    config::property<ss::sstring> required_string;
    config::property<int64_t> an_int64_t;
    config::property<custom_aggregate> an_aggregate;
    config::property<std::vector<ss::sstring>> strings;
    config::property<std::optional<int16_t>> nullable_int;
    config::property<std::optional<ss::sstring>> nullable_string;
    config::property<bool> boolean;
    config::property<std::chrono::seconds> seconds;
    config::property<std::optional<std::chrono::seconds>> optional_seconds;
    config::property<std::chrono::milliseconds> milliseconds;

    test_config()
      : optional_int(
        *this,
        "optional_int",
        "An optional int value",
        {.visibility = config::visibility::tunable},
        100)
      , required_string(
          *this,
          "required_string",
          "Required string value",
          {.needs_restart = config::needs_restart::no,
           .visibility = config::visibility::user})
      , an_int64_t(*this, "an_int64_t", "Some other int type", {}, 200)
      , an_aggregate(
          *this,
          "an_aggregate",
          "Aggregate type",
          {},
          custom_aggregate{"str", 10})
      , strings(
          *this,
          "strings",
          "Required strings vector",
          config::base_property::metadata{})
      , nullable_int(
          *this,
          "nullable_int",
          "A nullable (std::optional) int value",
          {},
          std::nullopt)
      , nullable_string(
          *this,
          "optional_string",
          "An optional string value",
          {},
          std::nullopt)
      , boolean(
          *this,
          "boolean",
          "Plain boolean property",
          config::base_property::metadata{
            .needs_restart = config::needs_restart::no},
          false)
      , seconds(*this, "seconds", "Plain seconds")
      , optional_seconds(*this, "optional_seconds", "Optional seconds")
      , milliseconds(*this, "milliseconds", "Plain milliseconds") {}
};

YAML::Node minimal_valid_configuration() {
    return YAML::Load("required_string: test_value_1\n"
                      "strings:\n"
                      " - first\n"
                      " - second\n"
                      " - third\n");
}

YAML::Node valid_configuration() {
    return YAML::Load("optional_int: 3\n"
                      "required_string: test_value_2\n"
                      "an_int64_t: 55\n"
                      "an_aggregate:\n"
                      "  string_value: some_value\n"
                      "  int_value: 88\n"
                      "strings:\n"
                      " - one\n"
                      " - two\n"
                      " - three\n"
                      "nullable_int: 111\n");
}

} // namespace

namespace std {
static inline ostream& operator<<(ostream& o, const custom_aggregate& c) {
    o << "int_value=" << c.int_value << ", string_value=" << c.string_value;
    return o;
}

static inline std::ostream&
operator<<(std::ostream& ostr, const std::optional<int16_t>& rhs) {
    if (rhs.has_value()) {
        ostr << rhs.value();
    } else {
        ostr << "~";
    }
    return ostr;
}

} // namespace std

namespace YAML {
template<>
struct convert<custom_aggregate> {
    using type = custom_aggregate;
    static Node encode(const type& rhs) {
        Node node;
        node["string_value"] = rhs.string_value;
        node["int_value"] = rhs.int_value;
        return node;
    }
    static bool decode(const Node& node, type& rhs) {
        // Required fields
        for (auto s : {"string_value", "int_value"}) {
            if (!node[s]) {
                return false;
            }
        }
        rhs.int_value = node["int_value"].as<int>();
        rhs.string_value = node["string_value"].as<ss::sstring>();
        return true;
    }
};
} // namespace YAML

SEASTAR_THREAD_TEST_CASE(read_minimal_valid_configuration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(minimal_valid_configuration());
    BOOST_TEST(errors.size() == 0);

    BOOST_TEST(cfg.optional_int() == 100);
    BOOST_TEST(cfg.required_string() == "test_value_1");
    BOOST_TEST(cfg.an_int64_t() == 200);
    BOOST_TEST(cfg.an_aggregate().string_value == "str");
    BOOST_TEST(cfg.an_aggregate().int_value == 10);
    BOOST_TEST(cfg.strings().at(0) == "first");
    BOOST_TEST(cfg.strings().at(1) == "second");
    BOOST_TEST(cfg.strings().at(2) == "third");
    BOOST_TEST(cfg.nullable_int() == std::nullopt);
};

SEASTAR_THREAD_TEST_CASE(read_valid_configuration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    BOOST_TEST(errors.size() == 0);

    BOOST_TEST(cfg.optional_int() == 3);
    BOOST_TEST(cfg.required_string() == "test_value_2");
    BOOST_TEST(cfg.an_int64_t() == 55);
    BOOST_TEST(cfg.an_aggregate().string_value == "some_value");
    BOOST_TEST(cfg.an_aggregate().int_value == 88);
    BOOST_TEST(cfg.strings().at(0) == "one");
    BOOST_TEST(cfg.strings().at(1) == "two");
    BOOST_TEST(cfg.strings().at(2) == "three");
    BOOST_TEST(cfg.nullable_int() == std::make_optional(111));
};

SEASTAR_THREAD_TEST_CASE(update_property_value) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(minimal_valid_configuration());
    BOOST_TEST(errors.size() == 0);

    BOOST_TEST(cfg.required_string() == "test_value_1");
    cfg.get("required_string").set_value(ss::sstring("new_string_value"));
    BOOST_TEST(cfg.required_string() == "new_string_value");
};

SEASTAR_THREAD_TEST_CASE(validate_valid_configuration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    BOOST_TEST(errors.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(validate_invalid_configuration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    BOOST_TEST(errors.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(config_json_serialization) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    BOOST_TEST(errors.size() == 0);
    lg.info("Config: {}", cfg);
    // json data
    const char* expected_result = "{"
                                  "\"strings\": [\"one\", \"two\", \"three\"],"
                                  "\"an_int64_t\": 55,"
                                  "\"optional_int\": 3,"
                                  "\"an_aggregate\": {"
                                  "\"string_value\": \"some_value\","
                                  "\"int_value\": 88"
                                  "},"
                                  "\"required_string\": \"test_value_2\","
                                  "\"nullable_int\": 111"
                                  "}";

    // cfg -> json string
    json::StringBuffer cfg_sb;
    json::Writer<json::StringBuffer> cfg_writer(cfg_sb);
    cfg.to_json(cfg_writer);
    auto jstr = cfg_sb.GetString();

    // json string -> rapidjson doc
    json::Document res_doc;
    res_doc.Parse(jstr);

    // json string -> rapidjson doc
    json::Document exp_doc;
    exp_doc.Parse(expected_result);

    // test equivalence
    BOOST_TEST(res_doc["required_string"].IsString());
    BOOST_TEST(
      res_doc["required_string"].GetString()
      == exp_doc["required_string"].GetString());

    BOOST_TEST(res_doc["optional_int"].IsInt());
    BOOST_TEST(
      res_doc["optional_int"].GetInt() == exp_doc["optional_int"].GetInt());

    BOOST_TEST(res_doc["an_int64_t"].IsInt64());
    BOOST_TEST(
      res_doc["an_int64_t"].GetInt64() == exp_doc["an_int64_t"].GetInt64());

    BOOST_TEST(res_doc["an_aggregate"].IsObject());

    BOOST_TEST(res_doc["an_aggregate"]["int_value"].IsInt());
    BOOST_TEST(
      res_doc["an_aggregate"]["int_value"].GetInt()
      == exp_doc["an_aggregate"]["int_value"].GetInt());

    BOOST_TEST(res_doc["an_aggregate"]["string_value"].IsString());
    BOOST_TEST(
      res_doc["an_aggregate"]["string_value"].GetString()
      == exp_doc["an_aggregate"]["string_value"].GetString());

    BOOST_TEST(res_doc["strings"].IsArray());

    BOOST_TEST(res_doc["nullable_int"].IsInt());
    BOOST_TEST(
      res_doc["nullable_int"].GetInt() == exp_doc["nullable_int"].GetInt());
}

/// Test that unset std::optional options are decoded correctly
/// when given as 'null', not just when absent.
SEASTAR_THREAD_TEST_CASE(deserialize_explicit_null) {
    auto with_null = YAML::Load("required_string: test_value_1\n"
                                "strings:\n"
                                " - first\n"
                                " - second\n"
                                " - third\n"
                                "nullable_int: ~\n");

    auto cfg = test_config();
    auto errors = cfg.read_yaml(with_null);
    BOOST_TEST(errors.size() == 0);
    BOOST_TEST(cfg.nullable_int() == std::nullopt);
}

SEASTAR_THREAD_TEST_CASE(property_metadata) {
    auto cfg = test_config();
    BOOST_TEST(cfg.optional_int.type_name() == "integer");
    BOOST_TEST(
      config::to_string_view(cfg.optional_int.get_visibility()) == "tunable");

    BOOST_TEST(cfg.boolean.is_nullable() == false);
    BOOST_TEST(cfg.nullable_string.is_array() == false);

    BOOST_TEST(cfg.required_string.type_name() == "string");
    BOOST_TEST(cfg.required_string.is_array() == false);
    BOOST_TEST(
      config::to_string_view(cfg.required_string.get_visibility()) == "user");

    BOOST_TEST(cfg.boolean.is_nullable() == false);

    BOOST_TEST(cfg.an_int64_t.type_name() == "integer");
    BOOST_TEST(cfg.boolean.is_nullable() == false);
    BOOST_TEST(cfg.nullable_string.is_array() == false);

    BOOST_TEST(cfg.an_aggregate.type_name() == "custom_aggregate");
    BOOST_TEST(cfg.boolean.is_nullable() == false);
    BOOST_TEST(cfg.nullable_string.is_array() == false);

    BOOST_TEST(cfg.strings.type_name() == "string");
    BOOST_TEST(cfg.strings.is_array() == true);
    BOOST_TEST(cfg.strings.is_nullable() == false);

    BOOST_TEST(cfg.nullable_string.type_name() == "string");
    BOOST_TEST(cfg.nullable_string.is_nullable() == true);
    BOOST_TEST(cfg.nullable_string.is_array() == false);

    BOOST_TEST(cfg.nullable_int.type_name() == "integer");
    BOOST_TEST(cfg.nullable_int.is_nullable() == true);
    BOOST_TEST(cfg.nullable_int.is_array() == false);

    BOOST_TEST(cfg.boolean.type_name() == "boolean");
    BOOST_TEST(cfg.boolean.is_nullable() == false);
    BOOST_TEST(cfg.boolean.is_array() == false);

    BOOST_TEST(cfg.seconds.type_name() == "integer");
    BOOST_TEST(cfg.seconds.units_name() == "s");
    BOOST_TEST(cfg.seconds.is_nullable() == false);
    BOOST_TEST(cfg.seconds.is_array() == false);

    BOOST_TEST(cfg.optional_seconds.type_name() == "integer");
    BOOST_TEST(cfg.optional_seconds.units_name() == "s");
    BOOST_TEST(cfg.optional_seconds.is_nullable() == true);
    BOOST_TEST(cfg.optional_seconds.is_array() == false);

    BOOST_TEST(cfg.milliseconds.type_name() == "integer");
    BOOST_TEST(cfg.milliseconds.units_name() == "ms");
    BOOST_TEST(cfg.milliseconds.is_nullable() == false);
    BOOST_TEST(cfg.milliseconds.is_array() == false);
};

SEASTAR_THREAD_TEST_CASE(property_bind) {
    auto cfg = test_config();
    BOOST_TEST(cfg.boolean() == false);
    auto binding = cfg.boolean.bind();
    BOOST_TEST(binding() == false);
    cfg.boolean.set_value(true);
    BOOST_TEST(cfg.boolean() == true);
    BOOST_TEST(binding() == true);

    int watch_count = 0;

    BOOST_TEST(cfg.required_string() == cfg.required_string.default_value());
    auto str_binding = cfg.required_string.bind();
    BOOST_TEST(str_binding() == cfg.required_string.default_value());
    str_binding.watch([&watch_count]() { ++watch_count; });

    cfg.required_string.set_value(ss::sstring("newvalue"));
    BOOST_TEST(cfg.required_string() == "newvalue");
    BOOST_TEST(str_binding() == "newvalue");
    BOOST_TEST(watch_count == 1);

    // Check that bindings are safe to use after move
    config::binding<ss::sstring> bind2 = std::move(str_binding);
    cfg.required_string.set_value(ss::sstring("newvalue2"));
    BOOST_TEST(bind2() == "newvalue2");
    BOOST_TEST(watch_count == 2);

    // Check that bindings are safe to use after copy
    config::binding<ss::sstring> bind3 = bind2;
    cfg.required_string.set_value(ss::sstring("newvalue3"));
    BOOST_TEST(bind2() == "newvalue3");
    BOOST_TEST(bind3() == "newvalue3");
    BOOST_TEST(watch_count == 4);
}
