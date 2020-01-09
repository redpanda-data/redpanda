#include "config/config_store.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>
#include <utility>

namespace {
struct custom_aggregate {
    ss::sstring string_value;
    int int_value;
};

struct test_config : public config::config_store {
    config::property<int> optional_int;
    config::property<ss::sstring> required_string;
    config::property<int64_t> an_int64_t;
    config::property<custom_aggregate> an_aggregate;
    config::property<std::vector<ss::sstring>> strings;

    test_config()
      : optional_int(
        *this,
        "optional_int",
        "An optional int value",
        config::required::no,
        100)
      , required_string(*this, "required_string", "Required string value")
      , an_int64_t(
          *this, "an_int64_t", "Some other int type", config::required::no, 200)
      , an_aggregate(
          *this,
          "an_aggregate",
          "Aggregate type",
          config::required::no,
          custom_aggregate{"str", 10})
      , strings(*this, "strings", "Required strings vector") {}
};

YAML::Node minimal_valid_cofniguration() {
    return YAML::Load("required_string: test_value_1\n"
                      "strings:\n"
                      " - first\n"
                      " - second\n"
                      " - third\n");
}

YAML::Node valid_cofniguration() {
    return YAML::Load("optional_int: 3\n"
                      "required_string: test_value_2\n"
                      "an_int64_t: 55\n"
                      "an_aggregate:\n"
                      "  string_value: some_value\n"
                      "  int_value: 88\n"
                      "strings:\n"
                      " - one\n"
                      " - two\n"
                      " - three\n");
}

static void to_json(nlohmann::json& j, const custom_aggregate& v) {
    j = {{"string_value", v.string_value}, {"int_value", v.int_value}};
}

} // namespace

namespace std {
static inline ostream& operator<<(ostream& o, const custom_aggregate& c) {
    o << "int_value=" << c.int_value << ", string_value=" << c.string_value;
    return o;
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
    cfg.read_yaml(minimal_valid_cofniguration());

    BOOST_TEST(cfg.optional_int() == 100);
    BOOST_TEST(cfg.required_string() == "test_value_1");
    BOOST_TEST(cfg.an_int64_t() == 200);
    BOOST_TEST(cfg.an_aggregate().string_value == "str");
    BOOST_TEST(cfg.an_aggregate().int_value == 10);
    BOOST_TEST(cfg.strings().at(0) == "first");
    BOOST_TEST(cfg.strings().at(1) == "second");
    BOOST_TEST(cfg.strings().at(2) == "third");
};

SEASTAR_THREAD_TEST_CASE(read_valid_configuration) {
    auto cfg = test_config();
    cfg.read_yaml(valid_cofniguration());

    BOOST_TEST(cfg.optional_int() == 3);
    BOOST_TEST(cfg.required_string() == "test_value_2");
    BOOST_TEST(cfg.an_int64_t() == 55);
    BOOST_TEST(cfg.an_aggregate().string_value == "some_value");
    BOOST_TEST(cfg.an_aggregate().int_value == 88);
    BOOST_TEST(cfg.strings().at(0) == "one");
    BOOST_TEST(cfg.strings().at(1) == "two");
    BOOST_TEST(cfg.strings().at(2) == "three");
};

SEASTAR_THREAD_TEST_CASE(update_property_value) {
    auto cfg = test_config();
    cfg.read_yaml(minimal_valid_cofniguration());

    BOOST_TEST(cfg.required_string() == "test_value_1");
    cfg.get("required_string").set_value(ss::sstring("new_string_value"));
    BOOST_TEST(cfg.required_string() == "new_string_value");
};

SEASTAR_THREAD_TEST_CASE(validate_valid_configuration) {
    auto cfg = test_config();
    cfg.read_yaml(valid_cofniguration());
    auto errors = cfg.validate();
    BOOST_TEST(errors.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(validate_invalid_configuration) {
    auto cfg = test_config();
    cfg.read_yaml(valid_cofniguration());
    auto errors = cfg.validate();
    BOOST_TEST(errors.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(json_serialization) {
    auto cfg = test_config();
    cfg.read_yaml(valid_cofniguration());

    // cfg -> json object -> json string
    nlohmann::json j;
    cfg.to_json(j);
    auto jstr = j.dump();

    // json string -> json object
    auto j2 = nlohmann::json::parse(jstr);

    // test equivalence
    BOOST_TEST(j2["required_string"] == "test_value_2");
}
