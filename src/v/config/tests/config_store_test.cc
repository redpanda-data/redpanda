// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/config_store.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <rapidjson/document.h>

#include <array>
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

    test_config()
      : optional_int(
        *this,
        "optional_int",
        "An optional int value",
        config::required::no,
        100)
      , required_string(
          *this,
          "required_string",
          "Required string value",
          config::base_property::metadata{})
      , an_int64_t(
          *this, "an_int64_t", "Some other int type", config::required::no, 200)
      , an_aggregate(
          *this,
          "an_aggregate",
          "Aggregate type",
          config::required::no,
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
          config::required::no,
          std::nullopt) {}
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
    cfg.read_yaml(minimal_valid_configuration());

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
    cfg.read_yaml(valid_configuration());

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
    cfg.read_yaml(minimal_valid_configuration());

    BOOST_TEST(cfg.required_string() == "test_value_1");
    cfg.get("required_string").set_value(ss::sstring("new_string_value"));
    BOOST_TEST(cfg.required_string() == "new_string_value");
};

SEASTAR_THREAD_TEST_CASE(validate_valid_configuration) {
    auto cfg = test_config();
    cfg.read_yaml(valid_configuration());
    auto errors = cfg.validate();
    BOOST_TEST(errors.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(validate_invalid_configuration) {
    auto cfg = test_config();
    cfg.read_yaml(valid_configuration());
    auto errors = cfg.validate();
    BOOST_TEST(errors.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(config_json_serialization) {
    auto cfg = test_config();
    cfg.read_yaml(valid_configuration());
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
    rapidjson::StringBuffer cfg_sb;
    rapidjson::Writer<rapidjson::StringBuffer> cfg_writer(cfg_sb);
    cfg.to_json(cfg_writer);
    auto jstr = cfg_sb.GetString();

    // json string -> rapidjson doc
    rapidjson::Document res_doc;
    res_doc.Parse(jstr);

    // json string -> rapidjson doc
    rapidjson::Document exp_doc;
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
    cfg.read_yaml(with_null);
    auto errors = cfg.validate();
    BOOST_TEST(errors.size() == 0);
    BOOST_TEST(cfg.nullable_int() == std::nullopt);
}
