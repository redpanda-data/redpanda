// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace ppj = pandaproxy::json;

FIXTURE_TEST(pandaproxy_fetch, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    set_client_config("retry_base_backoff_ms", 10ms);
    set_client_config("produce_batch_delay_ms", 0ms);

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_proxy_client();
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
        info("Fetch with missing request parameter 'offset'");
        set_client_config("retries", size_t(0));
        auto res = http_request(
          client,
          "/topics//partitions/0/"
          "records?max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::bad_request);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"error_code":40002,"message":"Missing mandatory parameter 'offset'"})");
    }

    {
        info("Fetch with missing path parameter 'topic_name'");
        set_client_config("retries", size_t(0));
        auto res = http_request(
          client,
          "/topics//partitions/0/"
          "records?offset=0&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::bad_request);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"error_code":40002,"message":"Missing mandatory parameter 'topic_name'"})");
    }

    {
        info("Fetch from unknown topic");
        set_client_config("retries", size_t(0));
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=0&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::not_found);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"error_code":40402,"message":"unknown_topic_or_partition"})");
    }

    info("Adding known topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        info("Produce to known topic - offsets 1-3");
        // Will require a metadata update
        set_client_config("retries", size_t(5));
        auto body = iobuf();
        body.append(batch_1_body.data(), batch_1_body.size());
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
        info("Fetch offset 0 - expect offsets 0-2");
        set_client_config("retries", size_t(0));
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=0&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":null,"value":"dmVjdG9yaXplZA==","partition":0,"offset":0},{"topic":"t","key":null,"value":"cGFuZGFwcm94eQ==","partition":0,"offset":1},{"topic":"t","key":null,"value":"bXVsdGlicm9rZXI=","partition":0,"offset":2}])");
    }

    {
        info("Produce to known topic - offset 3");
        set_client_config("retries", size_t(0));
        auto body = iobuf();
        body.append(batch_2_body.data(), batch_2_body.size());
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
          res.body, R"({"offsets":[{"partition":0,"offset":3}]})");
    }

    {
        info("Fetch offset 3 - expect offset 3");
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=3&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":null,"value":"bXVsdGliYXRjaA==","partition":0,"offset":3}])");
    }

    {
        info("Fetch offset 2 - expect offsets 0-3");
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=2&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":null,"value":"dmVjdG9yaXplZA==","partition":0,"offset":0},{"topic":"t","key":null,"value":"cGFuZGFwcm94eQ==","partition":0,"offset":1},{"topic":"t","key":null,"value":"bXVsdGlicm9rZXI=","partition":0,"offset":2},{"topic":"t","key":null,"value":"bXVsdGliYXRjaA==","partition":0,"offset":3}])");
    }
}

FIXTURE_TEST(pandaproxy_fetch_json_v2, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    set_client_config("retry_base_backoff_ms", 10ms);
    set_client_config("produce_batch_delay_ms", 0ms);

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_proxy_client();
    const ss::sstring batch_body(
      R"({
   "records":[
      {
         "key": null,
         "value":{"object":["vectorized"]},
         "partition":0
      },
      {
         "value":{"object":["pandaproxy"]},
         "partition":0
      }
   ]
})");

    info("Adding known topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        info("Produce to known topic - offsets 1-3");
        // Will require a metadata update
        set_client_config("retries", size_t(5));
        auto body = iobuf();
        body.append(batch_body.data(), batch_body.size());
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
          res.body, R"({"offsets":[{"partition":0,"offset":0}]})");
    }

    {
        info("Fetch offset 1 as json - expect offsets 0-1");
        auto res = http_request(
          client,
          "/topics/t/partitions/0/"
          "records?offset=1&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::json_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"([{"topic":"t","key":null,"value":{"object":["vectorized"]},"partition":0,"offset":0},{"topic":"t","key":null,"value":{"object":["pandaproxy"]},"partition":0,"offset":1}])");
    }
}

FIXTURE_TEST(pandaproxy_fetch_binary_with_json2, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    set_client_config("retry_base_backoff_ms", 10ms);
    set_client_config("produce_batch_delay_ms", 0ms);

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Conecting clinet");
    auto client = make_proxy_client();
    // Sends the string "pandaproxy" (without quotes) base64 encoded

    info("Adding know topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        const ss::sstring post_body(
          R"({
"records":[
    {
        "key": "cGFuZGFwcm94eQ==",
        "value": "IlZhbHVlIg=="
    }
]
})");
        info("Producing binary key to known topic");
        set_client_config("retries", size_t(5));
        auto body = iobuf();
        body.append(post_body.data(), post_body.size());
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
        info("Fetching binary data using json ACCEPT header");
        set_client_config("retries", size_t(0));
        auto res = http_request(
          client,
          "/topics/t/partitions/0/records?offset=0&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::json_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::request_timeout);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"error_code":40801,"message":"Unable to serialize key of record at offset 0 in topic:partition t:0"})");
    }

    {
        const ss::sstring post_body(
          R"({
"records":[
    {
        "value": "cGFuZGFwcm94eQ=="
    }
]
})");
        info("Producing binary value to known topic");
        set_client_config("retries", size_t(5));
        auto body = iobuf();
        body.append(post_body.data(), post_body.size());
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
          res.body, R"({"offsets":[{"partition":0,"offset":1}]})");
    }

    {
        info("Fetching binary data using json ACCEPT header");
        set_client_config("retries", size_t(0));
        auto res = http_request(
          client,
          "/topics/t/partitions/0/records?offset=1&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::json_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::request_timeout);
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"error_code":40801,"message":"Unable to serialize value of record at offset 1 in topic:partition t:0"})");
    }
}

FIXTURE_TEST(pandaproxy_fetch_compressed, pandaproxy_test_fixture) {
    wait_for_controller_leadership().get();

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    auto log_config = make_default_config();
    {
        using namespace storage;
        storage::disk_log_builder builder(log_config);
        storage::ntp_config ntp_cfg(
          ntp,
          log_config.base_dir,
          nullptr,
          get_next_partition_revision_id().get());
        builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
          | add_random_batch(model::offset(0), 10, maybe_compress_batches::yes)
          | stop();
    }

    add_topic(model::topic_namespace_view(ntp)).get();
    auto shard = app.shard_table.local().shard_for(ntp);

    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, ntp] {
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) {
              auto partition = mgr.get(ntp);
              return partition
                     && partition->committed_offset() >= model::offset(1);
          });
    }).get();

    auto client = make_proxy_client();
    {
        info("Fetching offset 0 from topic {}", tp);
        auto res = http_request(
          client,
          "/topics/t/partitions/0/records?offset=0&max_bytes=1024&timeout=5000",
          boost::beast::http::verb::get,
          ppj::serialization_format::v2,
          ppj::serialization_format::binary_v2);

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
    }
}
