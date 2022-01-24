/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "rpc/dns.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "s3/client.h"
#include "s3/error.h"
#include "s3/signature.h"
#include "seastarx.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/routes.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tcp.hh>
#include <seastar/net/tls.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/algorithm/string.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

static const uint16_t httpd_port_number = 4434;
static constexpr const char* httpd_host_name = "127.0.0.1";
static constexpr const char* expected_payload
  = "Amazon Simple Storage Service (Amazon S3) is storage for the internet. "
    "You can use Amazon S3 to store and retrieve any amount of data at any "
    "time, from anywhere on the web. You can accomplish these tasks using the "
    "simple and intuitive web interface of the AWS Management Console.";
static const size_t expected_payload_size = std::strlen(expected_payload);
static constexpr const char* error_payload
  = "<?xml version=\"1.0\" "
    "encoding=\"UTF-8\"?><Error><Code>InternalError</"
    "Code><Message>Error.Message</"
    "Message><Resource>Error.Resource</Resource><RequestId>Error.RequestId</"
    "RequestId></Error>";
static constexpr const char* list_objects_payload = R"xml(
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>test-prefix</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>test-key1</Key>
    <LastModified>2021-01-10T01:00:00.000Z</LastModified>
    <ETag>"test-etag-1"</ETag>
    <Size>111</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>test-key2</Key>
    <LastModified>2021-01-10T02:00:00.000Z</LastModified>
    <ETag>"test-etag-2"</ETag>
    <Size>222</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <CommonPrefixes>
    <Prefix>test-prefix</Prefix>
  </CommonPrefixes>
</ListBucketResult>)xml";

void set_routes(ss::httpd::routes& r) {
    using namespace ss::httpd;
    auto empty_put_response = new function_handler(
      [](const_req req) {
          BOOST_REQUIRE(!req.get_header("x-amz-content-sha256").empty());
          BOOST_REQUIRE(req.content == expected_payload);
          return "";
      },
      "txt");
    auto erroneous_put_response = new function_handler(
      []([[maybe_unused]] const_req req, reply& reply) {
          reply.set_status(reply::status_type::internal_server_error);
          return error_payload;
      },
      "txt");
    auto get_response = new function_handler(
      [](const_req req) {
          BOOST_REQUIRE(!req.get_header("x-amz-content-sha256").empty());
          return ss::sstring(expected_payload, expected_payload_size);
      },
      "txt");
    auto erroneous_get_response = new function_handler(
      []([[maybe_unused]] const_req req, reply& reply) {
          reply.set_status(reply::status_type::internal_server_error);
          return error_payload;
      },
      "txt");
    auto empty_delete_response = new function_handler(
      [](const_req req, reply& reply) {
          BOOST_REQUIRE(!req.get_header("x-amz-content-sha256").empty());
          reply.set_status(reply::status_type::no_content);
          return "";
      },
      "txt");
    auto erroneous_delete_response = new function_handler(
      []([[maybe_unused]] const_req req, reply& reply) {
          reply.set_status(reply::status_type::internal_server_error);
          return error_payload;
      },
      "txt");
    auto list_objects_response = new function_handler(
      [](const_req req, reply& reply) {
          BOOST_REQUIRE(!req.get_header("x-amz-content-sha256").empty());
          auto prefix = req.get_header("prefix");
          if (prefix == "test") {
              // normal response
              return list_objects_payload;
          } else if (prefix == "test-error") {
              // error
              reply.set_status(reply::status_type::internal_server_error);
              return error_payload;
          }
          return "";
      },
      "txt");
    r.add(operation_type::PUT, url("/test"), empty_put_response);
    r.add(operation_type::PUT, url("/test-error"), erroneous_put_response);
    r.add(operation_type::GET, url("/test"), get_response);
    r.add(operation_type::GET, url("/test-error"), erroneous_get_response);
    r.add(operation_type::DELETE, url("/test"), empty_delete_response);
    r.add(
      operation_type::DELETE, url("/test-error"), erroneous_delete_response);
    r.add(operation_type::GET, url("/"), list_objects_response);
}

/// Http server and client
struct configured_test_pair {
    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<s3::client> client;
};

s3::configuration transport_configuration() {
    unresolved_address server_addr(httpd_host_name, httpd_port_number);
    s3::configuration conf{
      .uri = s3::access_point_uri(httpd_host_name),
      .access_key = s3::public_key_str("acess-key"),
      .secret_key = s3::private_key_str("secret-key"),
      .region = s3::aws_region_name("us-east-1"),
    };
    conf.server_addr = server_addr;
    conf._probe = ss::make_shared<s3::client_probe>(
      rpc::metrics_disabled::yes, "region", "endpoint");
    return conf;
}

/// Create server and client, server is initialized with default
/// testing paths and listening.
configured_test_pair started_client_and_server(const s3::configuration& conf) {
    auto client = ss::make_shared<s3::client>(conf);
    auto server = ss::make_shared<ss::httpd::http_server_control>();
    server->start().get();
    server->set_routes(set_routes).get();
    auto resolved = rpc::resolve_dns(conf.server_addr).get();
    server->listen(resolved).get();
    return {
      .server = server,
      .client = client,
    };
}

SEASTAR_TEST_CASE(test_put_object_success) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        payload.append(expected_payload, expected_payload_size);
        auto payload_stream = make_iobuf_input_stream(std::move(payload));
        client
          ->put_object(
            s3::bucket_name("test-bucket"),
            s3::object_key("test"),
            expected_payload_size,
            std::move(payload_stream),
            {},
            100ms)
          .get();
        // shouldn't throw
        // the request is verified by the server
        client->shutdown().get();
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_put_object_failure) {
    return ss::async([] {
        bool error_triggered = false;
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        payload.append(expected_payload, expected_payload_size);
        auto payload_stream = make_iobuf_input_stream(std::move(payload));
        try {
            client
              ->put_object(
                s3::bucket_name("test-bucket"),
                s3::object_key("test-error"),
                expected_payload_size,
                std::move(payload_stream),
                {},
                100ms)
              .get();
        } catch (const s3::rest_error_response& err) {
            BOOST_REQUIRE_EQUAL(err.code(), s3::s3_error_code::internal_error);
            BOOST_REQUIRE_EQUAL(err.message(), "Error.Message");
            BOOST_REQUIRE_EQUAL(err.request_id(), "Error.RequestId");
            BOOST_REQUIRE_EQUAL(err.resource(), "Error.Resource");
            error_triggered = true;
        }
        BOOST_REQUIRE(error_triggered);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_get_object_success) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        auto payload_stream = make_iobuf_ref_output_stream(payload);
        auto http_response = client
                               ->get_object(
                                 s3::bucket_name("test-bucket"),
                                 s3::object_key("test"),
                                 100ms)
                               .get0();
        auto input_stream = http_response->as_input_stream();
        ss::copy(input_stream, payload_stream).get0();
        iobuf_parser p(std::move(payload));
        auto actual_payload = p.read_string(p.bytes_left());
        BOOST_REQUIRE_EQUAL(actual_payload, expected_payload);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_get_object_failure) {
    return ss::async([] {
        bool error_triggered = false;
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        try {
            auto http_response = client
                                   ->get_object(
                                     s3::bucket_name("test-bucket"),
                                     s3::object_key("test-error"),
                                     100ms)
                                   .get0();
        } catch (const s3::rest_error_response& err) {
            BOOST_REQUIRE_EQUAL(err.code(), s3::s3_error_code::internal_error);
            BOOST_REQUIRE_EQUAL(err.message(), "Error.Message");
            BOOST_REQUIRE_EQUAL(err.request_id(), "Error.RequestId");
            BOOST_REQUIRE_EQUAL(err.resource(), "Error.Resource");
            error_triggered = true;
        }
        BOOST_REQUIRE(error_triggered);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_object_success) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        client
          ->delete_object(
            s3::bucket_name("test-bucket"), s3::object_key("test"), 100ms)
          .get0();
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_object_failure) {
    return ss::async([] {
        bool error_triggered = false;
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        try {
            client
              ->delete_object(
                s3::bucket_name("test-bucket"),
                s3::object_key("test-error"),
                100ms)
              .get0();
        } catch (const s3::rest_error_response& err) {
            BOOST_REQUIRE_EQUAL(err.code(), s3::s3_error_code::internal_error);
            BOOST_REQUIRE_EQUAL(err.message(), "Error.Message");
            BOOST_REQUIRE_EQUAL(err.request_id(), "Error.RequestId");
            BOOST_REQUIRE_EQUAL(err.resource(), "Error.Resource");
            error_triggered = true;
        }
        BOOST_REQUIRE(error_triggered);
        server->stop().get();
    });
}

static ss::sstring strtime(const std::chrono::system_clock::time_point& ts) {
    auto tt = std::chrono::system_clock::to_time_t(ts);
    auto tm = *std::gmtime(&tt);
    std::stringstream s;
    s << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S.000Z");
    return s.str();
}

SEASTAR_TEST_CASE(test_list_objects_success) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        auto payload_stream = make_iobuf_ref_output_stream(payload);
        auto lst = client
                     ->list_objects_v2(
                       s3::bucket_name("test-bucket"), s3::object_key("test"))
                     .get0();
        BOOST_REQUIRE_EQUAL(lst.is_truncated, false);
        BOOST_REQUIRE_EQUAL(lst.prefix, "test-prefix");
        // item 0
        BOOST_REQUIRE_EQUAL(lst.contents[0].key, "test-key1");
        BOOST_REQUIRE_EQUAL(lst.contents[0].size_bytes, 111);
        BOOST_REQUIRE_EQUAL(
          strtime(lst.contents[0].last_modified), "2021-01-10T01:00:00.000Z");
        // item 1
        BOOST_REQUIRE_EQUAL(lst.contents[1].key, "test-key2");
        BOOST_REQUIRE_EQUAL(lst.contents[1].size_bytes, 222);
        BOOST_REQUIRE_EQUAL(
          strtime(lst.contents[1].last_modified), "2021-01-10T02:00:00.000Z");
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_list_objects_failure) {
    return ss::async([] {
        bool error_triggered = false;
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        try {
            auto lst = client
                         ->list_objects_v2(
                           s3::bucket_name("test-bucket"),
                           s3::object_key("test-error"))
                         .get0();
        } catch (const s3::rest_error_response& err) {
            BOOST_REQUIRE_EQUAL(err.code(), s3::s3_error_code::internal_error);
            BOOST_REQUIRE_EQUAL(err.message(), "Error.Message");
            BOOST_REQUIRE_EQUAL(err.request_id(), "Error.RequestId");
            BOOST_REQUIRE_EQUAL(err.resource(), "Error.Resource");
            error_triggered = true;
        }
        BOOST_REQUIRE(error_triggered);
        server->stop().get();
    });
}

/// Http server and client
struct configured_server_and_client_pool {
    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<s3::client_pool> pool;
};
/// Create server and client connection pool, server is initialized with default
/// testing paths and listening.
configured_server_and_client_pool started_pool_and_server(
  size_t size,
  s3::client_pool_overdraft_policy policy,
  const s3::configuration& conf) {
    auto client = ss::make_shared<s3::client_pool>(size, conf, policy);
    auto server = ss::make_shared<ss::httpd::http_server_control>();
    server->start().get();
    server->set_routes(set_routes).get();
    auto resolved = rpc::resolve_dns(conf.server_addr).get0();
    server->listen(resolved).get();
    return {
      .server = server,
      .pool = client,
    };
}

void test_client_pool(s3::client_pool_overdraft_policy policy) {
    auto conf = transport_configuration();
    auto [server, pool] = started_pool_and_server(2, policy, conf);
    std::vector<ss::future<>> fut;
    for (size_t i = 0; i < 20; i++) {
        auto f = pool->acquire().then(
          [_server = server](
            s3::client_pool::client_lease lease) -> ss::future<> {
              auto server = _server;
              auto& [client, _] = lease;
              iobuf payload;
              auto payload_stream = make_iobuf_ref_output_stream(payload);
              auto http_response = co_await client->get_object(
                s3::bucket_name("test-bucket"), s3::object_key("test"), 100ms);
              auto input_stream = http_response->as_input_stream();
              co_await ss::copy(input_stream, payload_stream);
              iobuf_parser p(std::move(payload));
              auto actual_payload = p.read_string(p.bytes_left());
              BOOST_REQUIRE_EQUAL(actual_payload, expected_payload);
          });
        fut.emplace_back(std::move(f));
    }
    ss::when_all_succeed(fut.begin(), fut.end()).get0();
    BOOST_REQUIRE(pool->size() == 2);
    server->stop().get();
}

SEASTAR_TEST_CASE(test_client_pool_wait_strategy) {
    return ss::async([] {
        test_client_pool(s3::client_pool_overdraft_policy::wait_if_empty);
    });
}

SEASTAR_TEST_CASE(test_client_pool_create_new_strategy) {
    return ss::async([] {
        test_client_pool(s3::client_pool_overdraft_policy::create_new_if_empty);
    });
}

SEASTAR_TEST_CASE(test_client_pool_reconnect) {
    return ss::async([] {
        using namespace std::chrono_literals;
        auto conf = transport_configuration();
        auto [server, pool] = started_pool_and_server(
          2, s3::client_pool_overdraft_policy::wait_if_empty, conf);

        std::vector<ss::future<bool>> fut;
        for (size_t i = 0; i < 20; i++) {
            auto f = pool->acquire().then(
              [_server = server](
                s3::client_pool::client_lease lease) -> ss::future<bool> {
                  auto server = _server;
                  co_await ss::sleep(100ms);
                  auto& [client, _] = lease;
                  iobuf payload;
                  auto payload_stream = make_iobuf_ref_output_stream(payload);
                  try {
                      auto http_response = co_await client->get_object(
                        s3::bucket_name("test-bucket"),
                        s3::object_key("test"),
                        100ms);
                      auto input_stream = http_response->as_input_stream();
                      co_await ss::copy(input_stream, payload_stream);
                      iobuf_parser p(std::move(payload));
                      auto actual_payload = p.read_string(p.bytes_left());
                      BOOST_REQUIRE_EQUAL(actual_payload, expected_payload);
                      co_await client->shutdown();
                  } catch (const ss::abort_requested_exception&) {
                      co_return false;
                  }
                  co_return true;
              });
            fut.emplace_back(std::move(f));
        }
        auto result = ss::when_all_succeed(fut.begin(), fut.end()).get0();
        auto count = std::count(result.begin(), result.end(), true);
        BOOST_REQUIRE(count == 20);
        server->stop().get();
    });
}
