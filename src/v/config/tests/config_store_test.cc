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

#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>
#include <iterator>
#include <random>
#include <string>
#include <utility>

namespace {

ss::logger lg("config_test"); // NOLINT

struct test_config : public config::config_store {
    config::property<int> optional_int;
    config::property<ss::sstring> required_string;
    config::property<int64_t> an_int64_t;
    config::property<testing::custom_aggregate> an_aggregate;
    config::property<std::vector<ss::sstring>> strings;
    config::property<std::optional<int16_t>> nullable_int;
    config::property<std::optional<ss::sstring>> nullable_string;
    config::property<std::optional<std::vector<ss::sstring>>> nullable_strings;
    config::property<bool> boolean;
    config::property<std::chrono::seconds> seconds;
    config::property<std::optional<std::chrono::seconds>> optional_seconds;
    config::property<std::chrono::milliseconds> milliseconds;
    config::property<ss::sstring> default_secret_string;
    config::property<ss::sstring> secret_string;
    config::property<bool> aliased_bool;

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
          testing::custom_aggregate{"str", 10})
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
      , nullable_strings(
          *this,
          "optional_strings",
          "An optional strings vector",
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
      , milliseconds(*this, "milliseconds", "Plain milliseconds")
      , default_secret_string(
          *this,
          "default_secret_string",
          "Secret string value set to the default",
          {.secret = config::is_secret::yes})
      , secret_string(
          *this,
          "secret_string",
          "Secret string value",
          {.secret = config::is_secret::yes})
      , aliased_bool(
          *this,
          "aliased_bool",
          "Property with a compat alias",
          {.aliases = {"aliased_bool_legacy"}},
          true) {}
};

struct noop_config : public config::config_store {};

YAML::Node minimal_valid_configuration() {
    return YAML::Load(
      "required_string: test_value_1\n"
      "strings:\n"
      " - first\n"
      " - second\n"
      " - third\n");
}

YAML::Node valid_configuration() {
    return YAML::Load(
      "optional_int: 3\n"
      "required_string: test_value_2\n"
      "an_int64_t: 55\n"
      "an_aggregate:\n"
      "  string_value: some_value\n"
      "  int_value: 88\n"
      "strings:\n"
      " - one\n"
      " - two\n"
      " - three\n"
      "nullable_int: 111\n"
      "secret_string: actual_secret\n"
      "aliased_bool_legacy: false\n");
}

} // namespace

namespace std {
static inline ostream&
operator<<(ostream& o, const testing::custom_aggregate& c) {
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
struct convert<testing::custom_aggregate> {
    using type = testing::custom_aggregate;
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

TEST(ConfigStoreTest, ReadMinimalValidConfiguration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(minimal_valid_configuration());
    EXPECT_EQ(errors.size(), 0);

    EXPECT_EQ(cfg.optional_int(), 100);
    EXPECT_EQ(cfg.required_string(), "test_value_1");
    EXPECT_EQ(cfg.an_int64_t(), 200);
    EXPECT_EQ(cfg.an_aggregate().string_value, "str");
    EXPECT_EQ(cfg.an_aggregate().int_value, 10);
    EXPECT_EQ(cfg.strings().at(0), "first");
    EXPECT_EQ(cfg.strings().at(1), "second");
    EXPECT_EQ(cfg.strings().at(2), "third");
    EXPECT_EQ(cfg.nullable_int(), std::nullopt);
}

TEST(ConfigStoreTest, ReadValidConfiguration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    EXPECT_EQ(errors.size(), 0);

    EXPECT_EQ(cfg.optional_int(), 3);
    EXPECT_EQ(cfg.required_string(), "test_value_2");
    EXPECT_EQ(cfg.an_int64_t(), 55);
    EXPECT_EQ(cfg.an_aggregate().string_value, "some_value");
    EXPECT_EQ(cfg.an_aggregate().int_value, 88);
    EXPECT_EQ(cfg.strings().at(0), "one");
    EXPECT_EQ(cfg.strings().at(1), "two");
    EXPECT_EQ(cfg.strings().at(2), "three");
    EXPECT_EQ(cfg.nullable_int(), std::make_optional(111));
    EXPECT_EQ(cfg.secret_string(), "actual_secret");
    EXPECT_EQ(cfg.aliased_bool(), false);
}

TEST(ConfigStoreTest, UpdatePropertyValue) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(minimal_valid_configuration());
    EXPECT_EQ(errors.size(), 0);

    EXPECT_EQ(cfg.required_string(), "test_value_1");
    cfg.get("required_string").set_value(ss::sstring("new_string_value"));
    EXPECT_EQ(cfg.required_string(), "new_string_value");
}

TEST(ConfigStoreTest, TrackSetState) {
    auto cfg = test_config();

    EXPECT_EQ(cfg.optional_int(), 100);
    EXPECT_TRUE(cfg.optional_int.is_default());
    EXPECT_FALSE(cfg.optional_int.is_set());

    // set to default value
    cfg.get("required_string").set_value(ss::sstring{});
    EXPECT_TRUE(cfg.required_string.is_default());
    EXPECT_TRUE(cfg.required_string.is_set());

    // set to non-default value
    cfg.get("an_int64_t").set_value(int64_t{100});
    EXPECT_FALSE(cfg.an_int64_t.is_default());
    EXPECT_TRUE(cfg.an_int64_t.is_set());
}

TEST(ConfigStoreTest, ValidateValidConfiguration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    EXPECT_EQ(errors.size(), 0);
}

TEST(ConfigStoreTest, ValidateAnotherValidConfiguration) {
    auto cfg = test_config();
    auto errors = cfg.read_yaml(valid_configuration());
    EXPECT_EQ(errors.size(), 0);
}

TEST(ConfigStoreTest, ConfigJsonSerialization) {
    const auto test_with_redaction = [](config::redact_secrets redact) {
        auto cfg = test_config();
        auto errors = cfg.read_yaml(valid_configuration());
        EXPECT_EQ(errors.size(), 0);
        lg.info("Config: {}", cfg);
        // json data
        auto expected_result = redact == config::redact_secrets::yes
                                 ? "{"
                                   "\"strings\": [\"one\", \"two\", \"three\"],"
                                   "\"an_int64_t\": 55,"
                                   "\"optional_int\": 3,"
                                   "\"an_aggregate\": {"
                                   "\"string_value\": \"some_value\","
                                   "\"int_value\": 88"
                                   "},"
                                   "\"required_string\": \"test_value_2\","
                                   "\"nullable_int\": 111,"
                                   "\"default_secret_string\": \"\","
                                   "\"secret_string\": \"[secret]\""
                                   "}"
                                 : "{"
                                   "\"strings\": [\"one\", \"two\", \"three\"],"
                                   "\"an_int64_t\": 55,"
                                   "\"optional_int\": 3,"
                                   "\"an_aggregate\": {"
                                   "\"string_value\": \"some_value\","
                                   "\"int_value\": 88"
                                   "},"
                                   "\"required_string\": \"test_value_2\","
                                   "\"nullable_int\": 111,"
                                   "\"default_secret_string\": \"\","
                                   "\"secret_string\": \"actual_secret\""
                                   "}";

        // cfg -> json string
        json::StringBuffer cfg_sb;
        json::Writer<json::StringBuffer> cfg_writer(cfg_sb);
        cfg.to_json(cfg_writer, redact);
        auto jstr = cfg_sb.GetString();

        // json string -> rapidjson doc
        json::Document res_doc;
        res_doc.Parse(jstr);

        // json string -> rapidjson doc
        json::Document exp_doc;
        exp_doc.Parse(expected_result);

        // test equivalence
        EXPECT_TRUE(res_doc["required_string"].IsString());
        EXPECT_STREQ(
          res_doc["required_string"].GetString(),
          exp_doc["required_string"].GetString());

        EXPECT_TRUE(res_doc["optional_int"].IsInt());
        EXPECT_EQ(
          res_doc["optional_int"].GetInt(), exp_doc["optional_int"].GetInt());

        EXPECT_TRUE(res_doc["an_int64_t"].IsInt64());
        EXPECT_EQ(
          res_doc["an_int64_t"].GetInt64(), exp_doc["an_int64_t"].GetInt64());

        EXPECT_TRUE(res_doc["an_aggregate"].IsObject());

        EXPECT_TRUE(res_doc["an_aggregate"]["int_value"].IsInt());
        EXPECT_EQ(
          res_doc["an_aggregate"]["int_value"].GetInt(),
          exp_doc["an_aggregate"]["int_value"].GetInt());

        EXPECT_TRUE(res_doc["an_aggregate"]["string_value"].IsString());
        EXPECT_STREQ(
          res_doc["an_aggregate"]["string_value"].GetString(),
          exp_doc["an_aggregate"]["string_value"].GetString());

        EXPECT_TRUE(res_doc["strings"].IsArray());

        EXPECT_TRUE(res_doc["nullable_int"].IsInt());
        EXPECT_EQ(
          res_doc["nullable_int"].GetInt(), exp_doc["nullable_int"].GetInt());
        EXPECT_STREQ(
          res_doc["secret_string"].GetString(),
          exp_doc["secret_string"].GetString());
    };
    test_with_redaction(config::redact_secrets::yes);
    test_with_redaction(config::redact_secrets::no);
}

TEST(ConfigStoreTest, DeserializeExplicitNull) {
    auto with_null = YAML::Load(
      "required_string: test_value_1\n"
      "strings:\n"
      " - first\n"
      " - second\n"
      " - third\n"
      "nullable_int: ~\n");

    auto cfg = test_config();
    auto errors = cfg.read_yaml(with_null);
    EXPECT_EQ(errors.size(), 0);
    EXPECT_EQ(cfg.nullable_int(), std::nullopt);
}

TEST(ConfigStoreTest, PropertyMetadata) {
    auto cfg = test_config();
    EXPECT_EQ(cfg.optional_int.type_name(), "integer");
    EXPECT_EQ(
      config::to_string_view(cfg.optional_int.get_visibility()), "tunable");

    EXPECT_FALSE(cfg.boolean.is_nullable());
    EXPECT_FALSE(cfg.nullable_string.is_array());

    EXPECT_EQ(cfg.required_string.type_name(), "string");
    EXPECT_FALSE(cfg.required_string.is_array());
    EXPECT_EQ(
      config::to_string_view(cfg.required_string.get_visibility()), "user");

    EXPECT_FALSE(cfg.boolean.is_nullable());

    EXPECT_EQ(cfg.an_int64_t.type_name(), "integer");
    EXPECT_FALSE(cfg.boolean.is_nullable());
    EXPECT_FALSE(cfg.nullable_string.is_array());

    EXPECT_EQ(cfg.an_aggregate.type_name(), "custom_aggregate");
    EXPECT_FALSE(cfg.boolean.is_nullable());
    EXPECT_FALSE(cfg.nullable_string.is_array());

    EXPECT_EQ(cfg.strings.type_name(), "string");
    EXPECT_TRUE(cfg.strings.is_array());
    EXPECT_FALSE(cfg.strings.is_nullable());

    EXPECT_EQ(cfg.nullable_string.type_name(), "string");
    EXPECT_TRUE(cfg.nullable_string.is_nullable());
    EXPECT_FALSE(cfg.nullable_string.is_array());

    EXPECT_EQ(cfg.nullable_strings.type_name(), "string");
    EXPECT_TRUE(cfg.nullable_strings.is_nullable());
    EXPECT_TRUE(cfg.nullable_strings.is_array());

    EXPECT_EQ(cfg.nullable_int.type_name(), "integer");
    EXPECT_TRUE(cfg.nullable_int.is_nullable());
    EXPECT_FALSE(cfg.nullable_int.is_array());

    EXPECT_EQ(cfg.boolean.type_name(), "boolean");
    EXPECT_FALSE(cfg.boolean.is_nullable());
    EXPECT_FALSE(cfg.boolean.is_array());

    EXPECT_EQ(cfg.seconds.type_name(), "integer");
    EXPECT_EQ(cfg.seconds.units_name(), "s");
    EXPECT_FALSE(cfg.seconds.is_nullable());
    EXPECT_FALSE(cfg.seconds.is_array());

    EXPECT_EQ(cfg.optional_seconds.type_name(), "integer");
    EXPECT_EQ(cfg.optional_seconds.units_name(), "s");
    EXPECT_TRUE(cfg.optional_seconds.is_nullable());
    EXPECT_FALSE(cfg.optional_seconds.is_array());

    EXPECT_EQ(cfg.milliseconds.type_name(), "integer");
    EXPECT_EQ(cfg.milliseconds.units_name(), "ms");
    EXPECT_FALSE(cfg.milliseconds.is_nullable());
    EXPECT_FALSE(cfg.milliseconds.is_array());
}

TEST(ConfigStoreTest, PropertyBind) {
    auto cfg = test_config();
    EXPECT_FALSE(cfg.boolean());
    auto binding = cfg.boolean.bind();
    EXPECT_FALSE(binding());
    cfg.boolean.set_value(true);
    EXPECT_TRUE(cfg.boolean());
    EXPECT_TRUE(binding());

    int watch_count = 0;

    EXPECT_EQ(cfg.required_string(), cfg.required_string.default_value());
    auto str_binding = cfg.required_string.bind();
    EXPECT_EQ(str_binding(), cfg.required_string.default_value());
    str_binding.watch([&watch_count]() { ++watch_count; });

    cfg.required_string.set_value(ss::sstring("newvalue"));
    EXPECT_EQ(cfg.required_string(), "newvalue");
    EXPECT_EQ(str_binding(), "newvalue");
    EXPECT_EQ(watch_count, 1);

    // Check that bindings are safe to use after move
    config::binding<ss::sstring> bind2 = std::move(str_binding);
    cfg.required_string.set_value(ss::sstring("newvalue2"));
    EXPECT_EQ(bind2(), "newvalue2");
    EXPECT_EQ(watch_count, 2);

    // Check that bindings are safe to use after copy
    config::binding<ss::sstring> bind3 = bind2;
    cfg.required_string.set_value(ss::sstring("newvalue3"));
    EXPECT_EQ(bind2(), "newvalue3");
    EXPECT_EQ(bind3(), "newvalue3");
    EXPECT_EQ(watch_count, 4);

    // Check the bindings are bound to the moved-to properties, not to the
    // moved-from ones
    auto cfg2 = std::move(cfg);
    cfg2.required_string.set_value(ss::sstring("newvalue4"));
    // NOLINTNEXTLINE
    cfg.required_string.set_value(ss::sstring("badvalue"));
    EXPECT_EQ(bind2(), "newvalue4");
    EXPECT_EQ(bind3(), "newvalue4");
    EXPECT_EQ(watch_count, 6);

    // Check that the bindings are updated when the property is reset to its
    // default value.
    cfg2.required_string.reset();
    EXPECT_EQ(bind2(), "");
    EXPECT_EQ(bind3(), "");
    EXPECT_EQ(watch_count, 8);
}

TEST(ConfigStoreTest, PropertyConversionBind) {
    auto cfg = test_config();
    EXPECT_FALSE(cfg.boolean());
    auto binding = cfg.boolean.bind<ss::sstring>(
      [](bool v) { return v ? "true" : "false"; });
    EXPECT_EQ(binding(), "false");
    cfg.boolean.set_value(true);
    EXPECT_TRUE(cfg.boolean());
    EXPECT_EQ(binding(), "true");

    int watch_count = 0;

    EXPECT_EQ(cfg.required_string(), cfg.required_string.default_value());
    auto str_binding = cfg.required_string.bind<std::string>(
      [](const ss::sstring& s) {
          return std::string(
            std::make_reverse_iterator(s.cend()),
            std::make_reverse_iterator(s.cbegin()));
      });
    str_binding.watch([&watch_count]() { ++watch_count; });

    cfg.required_string.set_value(ss::sstring("newvalue"));
    EXPECT_EQ(cfg.required_string(), "newvalue");
    EXPECT_EQ(str_binding(), "eulavwen");
    EXPECT_EQ(watch_count, 1);

    // Check that bindings are safe to use after move
    config::conversion_binding<std::string, ss::sstring> bind2 = std::move(
      str_binding);
    cfg.required_string.set_value(ss::sstring("newvalue2"));
    EXPECT_EQ(bind2(), "2eulavwen");
    EXPECT_EQ(watch_count, 2);

    // Check that bindings are safe to use after copy
    config::conversion_binding<std::string, ss::sstring> bind3 = bind2;
    cfg.required_string.set_value(ss::sstring("newvalue3"));
    EXPECT_EQ(bind2(), "3eulavwen");
    EXPECT_EQ(bind3(), "3eulavwen");
    EXPECT_EQ(watch_count, 4);

    // Check the bindings are bound to the moved-to properties, not to the
    // moved-from ones
    auto cfg2 = std::move(cfg);
    cfg2.required_string.set_value(ss::sstring("newvalue4"));
    // NOLINTNEXTLINE
    cfg.required_string.set_value(ss::sstring("badvalue"));
    EXPECT_EQ(bind2(), "4eulavwen");
    EXPECT_EQ(bind3(), "4eulavwen");
    EXPECT_EQ(watch_count, 6);
}

TEST(ConfigStoreTest, PropertyBindWithMultipleConfigStores) {
    // check that a copy of a configuration store, created for validating an
    // incoming value, does not propagate the value to the bindings to the
    // original configuration store

    // main config store
    auto cfg = test_config();
    // set a property to a defined value
    constexpr auto boolean_expected_value = false;
    cfg.boolean.set_value(boolean_expected_value);
    // create a binding like it's done in the codebase
    auto boolean_bind = cfg.boolean.bind();
    boolean_bind.watch([&] {
        ADD_FAILURE() << "watcher should not be called in this test";
        EXPECT_EQ(cfg.boolean.value(), boolean_expected_value);
    });

    EXPECT_EQ(cfg.boolean.value(), boolean_expected_value);
    {
        SCOPED_TRACE("simulating patch_cluster_config");
        // tmp copy meant to validate an incoming value (see
        // admin/server.cc::patch_cluster_config)
        auto cfg_tmp = test_config();
        // Populate the temporary config object with existing values
        cfg.for_each([&](const auto& p) {
            auto& tmp_p = cfg_tmp.get(p.name());
            tmp_p = p;
        });

        EXPECT_EQ(cfg_tmp.boolean.value(), boolean_expected_value);
        auto anti_value = YAML::Load(
          fmt::format("{}", !boolean_expected_value));
        auto& boolean_prop = cfg_tmp.get("boolean");
        EXPECT_EQ(boolean_prop.validate(anti_value), std::nullopt)
          << "sanity check: the test should pass validation";

        // this is expected not to trigger the boolean_bind watcher
        boolean_prop.set_value(anti_value);
    }
}

TEST(ConfigStoreTest, PropertyAliasing) {
    auto cfg = test_config();

    // Aliases should work when retrieving a property with get()
    EXPECT_EQ(cfg.get("aliased_bool").name(), "aliased_bool");
    EXPECT_EQ(cfg.get("aliased_bool_legacy").name(), "aliased_bool");

    // Aliases should not show up when iterating through properties
    bool seen_primary = false;
    bool seen_secondary = false;
    cfg.for_each([&](config::base_property& p) {
        if (p.name() == "aliased_bool") {
            seen_primary = true;
        } else if (p.name() == "aliased_bool_legacy") {
            seen_secondary = true;
        }
    });
    EXPECT_TRUE(seen_primary);
    EXPECT_FALSE(seen_secondary);

    // Aliases should not show up when getting the list of all properties
    auto property_names = cfg.property_names();
    EXPECT_TRUE(property_names.contains("aliased_bool"));
    EXPECT_FALSE(property_names.contains("aliased_bool_legacy"));

    EXPECT_TRUE(cfg.property_names_and_aliases().contains("aliased_bool"));
}

TEST(ConfigStoreTest, IgnoredKeys) {
    auto yaml_with_unknown_properties = YAML::Load(R"yaml(
secret_string: terces
aliased_bool_legacy: false
    )yaml");

    {
        SCOPED_TRACE(
          "smoke test: check that the properties are valid for test_config");
        auto cfg = test_config{};
        EXPECT_EQ(cfg.secret_string.value(), "");
        EXPECT_EQ(cfg.aliased_bool.value(), true);
        EXPECT_NO_THROW(cfg.read_yaml(yaml_with_unknown_properties));
        EXPECT_EQ(cfg.secret_string.value(), "terces");
        EXPECT_EQ(cfg.aliased_bool.value(), false);
    }
    {
        SCOPED_TRACE(
          "if a key is managed by the config, it will be set and "
          "the ignored_missing list does not matter");
        auto cfg = test_config{};
        EXPECT_NO_THROW(cfg.read_yaml(yaml_with_unknown_properties));
        EXPECT_EQ(cfg.secret_string.value(), "terces");
        EXPECT_EQ(cfg.aliased_bool.value(), false);
    }
    {
        SCOPED_TRACE("an unknown key will not generate an exception");
        auto noop_cfg = noop_config{};
        EXPECT_NO_THROW(noop_cfg.read_yaml(yaml_with_unknown_properties));
    }
}
