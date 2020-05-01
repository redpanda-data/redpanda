#include "json/json.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <rapidjson/document.h>

#include <optional>

namespace {

struct personne_t {
    struct nested {
        int x;
        int y;
        double z;
    };

    std::string full_name;
    ss::sstring nic; // national id card
    std::vector<ss::sstring> sons_names;
    int age;

    std::optional<float> height;

    nested obj;
};

} // namespace

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const personne_t::nested& obj) {
    w.StartObject();

    w.Key("x");
    json::rjson_serialize(w, obj.x);

    w.Key("y");
    json::rjson_serialize(w, obj.y);

    w.Key("z");
    json::rjson_serialize(w, obj.z);

    w.EndObject();
}

void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w, const personne_t& p) {
    w.StartObject();

    w.Key("full_name");
    json::rjson_serialize(w, p.full_name);

    w.Key("nic");
    json::rjson_serialize(w, p.nic);

    w.Key("age");
    json::rjson_serialize(w, p.age);

    w.Key("sons_names");
    json::rjson_serialize(w, p.sons_names);

    w.Key("height");
    json::rjson_serialize(w, p.height);

    w.Key("obj");
    rjson_serialize(w, p.obj);

    w.EndObject();
}

SEASTAR_THREAD_TEST_CASE(json_serialization_test) {
    const char* expected_result = "{"
                                  "\"full_name\" : \"foo bar\","
                                  "\"nic\" : \"981615823\","
                                  "\"age\" : 51,"
                                  "\"sons_names\" :[\"foo_derived "
                                  "bar\",\"boo_by_far bar\",\"lolipop bar\"],"
                                  "\"height\":1.7799999713897706,"
                                  "\"obj\":{\"x\":98,\"y\":78,\"z\":13.369}"
                                  "}";

    personne_t p1;
    p1.full_name = "foo bar";
    p1.sons_names = {ss::sstring{"foo_derived bar"},
                     ss::sstring{"boo_by_far bar"},
                     ss::sstring{"lolipop bar"}};
    p1.age = 51;
    p1.nic = ss::sstring{"981615823"};
    p1.height = 1.78;
    p1.obj = personne_t::nested{98, 78, 13.369};

    rapidjson::StringBuffer cfg_sb;
    rapidjson::Writer<rapidjson::StringBuffer> cfg_writer(cfg_sb);
    rjson_serialize(cfg_writer, p1);
    auto jstr = cfg_sb.GetString();

    // json string -> rapidjson doc - result
    rapidjson::Document res_doc;
    res_doc.Parse(jstr);

    // json string -> rapidjson doc - expectation
    rapidjson::Document exp_doc;
    exp_doc.Parse(expected_result);

    BOOST_TEST(res_doc["full_name"].IsString());
    BOOST_TEST(
      res_doc["full_name"].GetString() == exp_doc["full_name"].GetString());

    BOOST_TEST(res_doc["nic"].IsString());
    BOOST_TEST(res_doc["nic"].GetString() == exp_doc["nic"].GetString());

    BOOST_TEST(res_doc["age"].IsInt());
    BOOST_TEST(res_doc["age"].GetInt() == exp_doc["age"].GetInt());

    BOOST_TEST(res_doc["height"].IsDouble());
    BOOST_TEST(res_doc["height"].GetDouble() == exp_doc["height"].GetDouble());

    BOOST_TEST(res_doc["sons_names"].IsArray());

    BOOST_TEST(res_doc["obj"].IsObject());
}
