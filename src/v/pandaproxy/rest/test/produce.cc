// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace ppj = pandaproxy::json;

FIXTURE_TEST(pandaproxy_produce, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    set_client_config("retry_base_backoff_ms", 10ms);
    set_client_config("produce_batch_delay_ms", 0ms);

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_proxy_client();
    const ss::sstring produce_binary_body(
      R"({
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

    const ss::sstring produce_json_body(
      R"({
   "records":[
      {
         "value":{"text":"vectorized"},
         "partition":0
      },
      {
         "value":{"text":"pandaproxy"},
         "partition":0
      },
      {
         "value":{"text":"multibroker"},
         "partition":0
      }
   ]
})");

    {
        info("Produce without topic");
        set_client_config("retries", size_t(0));
        auto body = iobuf();
        body.append(produce_binary_body.data(), produce_binary_body.size());
        auto res = http_request(
          client,
          "/topics/t",
          std::move(body),
          boost::beast::http::verb::post,
          ppj::serialization_format::binary_v2,
          ppj::serialization_format::v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          to_header_value(serialization_format::v2));
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"offsets":[{"partition":0,"error_code":3,"offset":-1}]})");
    }

    info("Adding known topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        info("Produce binary to known topic");
        // Will require a metadata update
        set_client_config("retries", size_t(5));
        auto body = iobuf();
        body.append(produce_binary_body.data(), produce_binary_body.size());
        auto res = http_request(
          client,
          "/topics/t",
          std::move(body),
          boost::beast::http::verb::post,
          ppj::serialization_format::binary_v2,
          ppj::serialization_format::v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":0}]})");
    }

    {
        info("Produce json to known topic");
        set_client_config("retries", size_t(0));
        auto body = iobuf();
        body.append(produce_json_body.data(), produce_json_body.size());
        auto res = http_request(
          client,
          "/topics/t",
          std::move(body),
          boost::beast::http::verb::post,
          ppj::serialization_format::json_v2,
          ppj::serialization_format::v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":3}]})");
    }
}
