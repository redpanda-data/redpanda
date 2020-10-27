#include "http/client.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/test/pandaproxy_fixture.h"
#include "pandaproxy/test/utils.h"

#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/test/tools/old/interface.hpp>

namespace ppc = pandaproxy::client;

FIXTURE_TEST(pandaproxy_produce, pandaproxy_test_fixture) {
    using namespace std::chrono_literals;

    ppc::shard_local_cfg().retry_base_backoff.set_value(10ms);
    ppc::shard_local_cfg().produce_batch_delay.set_value(0ms);

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Connecting client");
    auto client = make_client();
    const ss::sstring produce_body(
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

    {
        info("Produce without topic");
        ppc::shard_local_cfg().retries.set_value(size_t(0));
        auto body = iobuf();
        body.append(produce_body.data(), produce_body.size());
        auto res = http_request(client, "/topics/t", std::move(body));

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.headers.at(boost::beast::http::field::content_type),
          "application/vnd.kafka.binary.v2+json");
        BOOST_REQUIRE_EQUAL(
          res.body,
          R"({"offsets":[{"partition":0,"error_code":3,"offset":-1}]})");
    }

    info("Adding known topic");
    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto ntp = make_default_ntp(tp.topic, tp.partition);
    add_topic(model::topic_namespace_view(ntp)).get();

    {
        info("Produce to known topic");
        // Will require a metadata update
        ppc::shard_local_cfg().retries.set_value(size_t(5));
        auto body = iobuf();
        body.append(produce_body.data(), produce_body.size());
        auto res = http_request(client, "/topics/t", std::move(body));

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":1}]})");
    }

    {
        info("Produce to known topic");
        ppc::shard_local_cfg().retries.set_value(size_t(0));
        auto body = iobuf();
        body.append(produce_body.data(), produce_body.size());
        auto res = http_request(client, "/topics/t", std::move(body));

        BOOST_REQUIRE_EQUAL(
          res.headers.result(), boost::beast::http::status::ok);
        BOOST_REQUIRE_EQUAL(
          res.body, R"({"offsets":[{"partition":0,"offset":4}]})");
    }
}
