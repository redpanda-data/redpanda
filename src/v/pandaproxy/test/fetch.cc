// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace kc = kafka::client;

FIXTURE_TEST(pandaproxy_fetch, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    kc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    kc::shard_local_cfg().produce_batch_delay.set_value(0ms);

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_client();
    const ss::sstring batch_1_body(
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

    const ss::sstring batch_2_body(
      R"({
   "records":[
      {
         "value":"bXVsdGliYXRjaA==",
         "partition":0
      }
   ]
})");

    {
        info("Fetch from unknown topic");
        kc::shard_local_cfg().retries.set_value(size_t(0));
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=0&max_bytes=1024&timeout=5000");

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"error_code":404,"message":"unknown_topic_or_partition"})");
    }

    info("Adding known topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        info("Produce to known topic - offsets 1-3");
        // Will require a metadata update
        kc::shard_local_cfg().retries.set_value(size_t(5));
        auto body = iobuf();
        body.append(batch_1_body.data(), batch_1_body.size());
        auto res = http_request(client, "/topics/t", std::move(body));

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":1}]})");
    }

    {
        info("Fetch offset 0 - expect offsets 1-3");
        kc::shard_local_cfg().retries.set_value(size_t(0));
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=0&max_bytes=1024&timeout=5000");

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":"","value":"dmVjdG9yaXplZA==","partition":0,"offset":1},{"topic":"t","key":"","value":"cGFuZGFwcm94eQ==","partition":0,"offset":2},{"topic":"t","key":"","value":"bXVsdGlicm9rZXI=","partition":0,"offset":3}])");
    }

    {
        info("Produce to known topic - offset 4");
        kc::shard_local_cfg().retries.set_value(size_t(0));
        auto body = iobuf();
        body.append(batch_2_body.data(), batch_2_body.size());
        auto res = http_request(client, "/topics/t", std::move(body));

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":4}]})");
    }

    {
        info("Fetch offset 4 - expect offset 4");
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=4&max_bytes=1024&timeout=5000");

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":"","value":"bXVsdGliYXRjaA==","partition":0,"offset":4}])");
    }

    {
        info("Fetch offset 2 - expect offsets 1-4");
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=2&max_bytes=1024&timeout=5000");

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":"","value":"dmVjdG9yaXplZA==","partition":0,"offset":1},{"topic":"t","key":"","value":"cGFuZGFwcm94eQ==","partition":0,"offset":2},{"topic":"t","key":"","value":"bXVsdGlicm9rZXI=","partition":0,"offset":3},{"topic":"t","key":"","value":"bXVsdGliYXRjaA==","partition":0,"offset":4}])");
    }
}
