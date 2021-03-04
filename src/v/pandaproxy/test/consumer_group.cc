// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "kafka/protocol/join_group.h"
#include "kafka/types.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/json/requests/create_consumer.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"
#include "utils/unresolved_address.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace kc = kafka::client;

namespace {

auto get_consumer_offsets(
  http::client& client,
  const kafka::group_id& g_id,
  const kafka::member_id& m_id) {
    const ss::sstring get_offsets_body(R"({
   "partitions":[
      {
         "topic":"t",
         "partition":0
      }
   ]
})");
    auto body = iobuf();
    body.append(get_offsets_body.data(), get_offsets_body.size());
    auto res = http_request(
      client,
      fmt::format("/consumers/{}/instances/{}/offsets", g_id(), m_id()),
      std::move(body),
      boost::beast::http::verb::get);
    return res;
};

} // namespace

FIXTURE_TEST(pandaproxy_consumer_group, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_client();

    auto advertised_address{unresolved_address{"proxy.example.com", 8080}};
    pp::shard_local_cfg().advertised_pandaproxy_api.set_value(
      advertised_address);

    kafka::group_id group_id{"test_group"};
    kafka::member_id member_id{kafka::no_member};
    {
        info("Create consumer");
        ss::sstring req_body(R"({
  "format": "binary",
  "auto.offset.reset": "earliest",
  "auto.commit.enable": "false",
  "fetch.min.bytes": "1",
  "consumer.request.timeout.ms": "10000"
})");
        iobuf req_body_buf;
        req_body_buf.append(req_body.data(), req_body.size());
        auto res = http_request(
          client,
          fmt::format("/consumers/{})", group_id()),
          std::move(req_body_buf));
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);

        auto res_data = ppj::rjson_parse(
          res.body.data(), ppj::create_consumer_response_handler());
        BOOST_REQUIRE(res_data.instance_id != kafka::no_member);
        member_id = res_data.instance_id;
        BOOST_REQUIRE_EQUAL(
          res_data.base_uri,
          fmt::format(
            "http://{}:{}/consumers/{}",
            advertised_address.host(),
            advertised_address.port(),
            member_id()));
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          "application/vnd.kafka.binary.v2+json");
    }
    info("Member id: {}", member_id);

    info("Adding known topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        info("List known topics");
        auto res = http_request(client, "/topics");
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(res.body, R"(["t"])");
    }
    {
        info("Subscribe consumer");
        ss::sstring req_body(R"(
{
  "topics": [
    "t"
  ]
})");
        iobuf req_body_buf;
        req_body_buf.append(req_body.data(), req_body.size());
        auto res = http_request(
          client,
          fmt::format(
            "/consumers/{}/instances/{}/subscription", group_id(), member_id()),
          std::move(req_body_buf));
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }

    {
        info("Produce to topic");
        set_client_config("retries", size_t(5));
        const ss::sstring produce_body(R"({
   "records":[
      {
         "value":"dmVjdG9yaXplZA==",
         "partition":0
      },
      {
         "value":"cGFuZGFwcm94eQ==",
         "partition":0
      },
      {
         "value":"bXVsdGlicm9rZXI=",
         "partition":0
      }
   ]
})");
        auto body = iobuf();
        body.append(produce_body.data(), produce_body.size());
        auto res = http_request(client, "/topics/t", std::move(body));

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":1}]})");
    }
    {
        info("Consume from topic");
        auto res = http_request(
          client,
          fmt::format(
            "/consumers/{}/instances/{}/records?timeout={}&max_bytes={}",
            group_id(),
            member_id(),
            "1000",
            "1000000"));
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":"AAD//w==","value":"","partition":0,"offset":0},{"topic":"t","key":"","value":"dmVjdG9yaXplZA==","partition":0,"offset":1},{"topic":"t","key":"","value":"cGFuZGFwcm94eQ==","partition":0,"offset":2},{"topic":"t","key":"","value":"bXVsdGlicm9rZXI=","partition":0,"offset":3}])");
    }

    {
        info("Get consumer offsets (expect -1)");
        auto res = get_consumer_offsets(client, group_id, member_id);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"offsets":[{"topic":"t","partition":0,"offset":-1,"metadata":""}]})");
    }

    {
        info("Commit 1 consumer offset");
        const ss::sstring commit_offsets_body(R"({
   "partitions":[
      {
         "topic":"t",
         "partition":0,
         "offset": 1
      }
   ]
})");
        auto body = iobuf();
        body.append(commit_offsets_body.data(), commit_offsets_body.size());
        auto res = http_request(
          client,
          fmt::format(
            "/consumers/{}/instances/{}/offsets", group_id(), member_id()),
          std::move(body),
          boost::beast::http::verb::post);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::no_content);
    }

    {
        info("Get consumer offsets (expect 1)");
        auto res = get_consumer_offsets(client, group_id, member_id);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"offsets":[{"topic":"t","partition":0,"offset":1,"metadata":""}]})");
    }

    {
        info("Commit all consumer offsets");
        auto res = http_request(
          client,
          fmt::format(
            "/consumers/{}/instances/{}/offsets", group_id(), member_id()),
          boost::beast::http::verb::post);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::no_content);
    }

    {
        info("Get consumer offsets (expect 3)");
        auto res = get_consumer_offsets(client, group_id, member_id);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"offsets":[{"topic":"t","partition":0,"offset":3,"metadata":""}]})");
    }

    {
        info("Remove consumer (expect no_content)");
        auto res = http_request(
          client,
          fmt::format("/consumers/{}/instances/{}", group_id(), member_id()),
          boost::beast::http::verb::delete_);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::no_content);
    }

    {
        info("Remove consumer (expect not_found)");
        auto res = http_request(
          client,
          fmt::format("/consumers/{}/instances/{}", group_id(), member_id()),
          boost::beast::http::verb::delete_);
        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::not_found);
    }
}
