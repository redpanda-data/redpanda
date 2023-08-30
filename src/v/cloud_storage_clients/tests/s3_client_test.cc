/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_roles/signature.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/s3_client.h"
#include "hashing/secure.h"
#include "net/dns.h"
#include "net/types.h"
#include "net/unresolved_address.h"
#include "seastarx.h"
#include "test_utils/fixture.h"
#include "utils/base64.h"

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
#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/property_tree/xml_parser.hpp>
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
static constexpr const char* no_such_key_payload = R"xml(
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchKey</Code>
    <Message>Object not found</Message>
</Error>
)xml";
static constexpr const char* no_such_bucket_payload = R"xml(
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchBucket</Code>
    <Message>Bucket not found</Message>
</Error>
)xml";
static constexpr const char* list_objects_payload = R"xml(
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>test-prefix</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <NextContinuationToken>next</NextContinuationToken>
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

constexpr auto delete_objects_payload_result = R"xml(
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{}</DeleteResult>
)xml";

constexpr auto delete_objects_payload_error = R"xml(
    <Error>
        <Key>{}</Key>
        <Code>{}</Code>
    </Error>
)xml";

void set_routes(ss::httpd::routes& r) {
    using namespace ss::httpd;
    using reply = ss::http::reply;
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
          BOOST_REQUIRE_EQUAL(req.get_query_param("list-type"), "2");
          auto prefix = req.get_header("prefix");
          if (prefix == "test") {
              // normal response
              return list_objects_payload;
          } else if (prefix == "test-error") {
              // error
              reply.set_status(reply::status_type::internal_server_error);
              return error_payload;
          } else if (prefix == "test-cont") {
              BOOST_REQUIRE_EQUAL(req.get_header("continuation-token"), "ctok");
              return list_objects_payload;
          }
          return "";
      },
      "txt");
    auto unexpected_error_response = new function_handler(
      []([[maybe_unused]] const_req req, reply& reply) {
          reply.set_status(reply::status_type::internal_server_error);
          return "unexpected!";
      },
      "txt");
    auto key_not_found_response = new function_handler(
      []([[maybe_unused]] const_req req, reply& reply) {
          reply.set_status(reply::status_type::not_found);
          return no_such_key_payload;
      },
      "txt");
    auto bucket_not_found_response = new function_handler(
      []([[maybe_unused]] const_req req, reply& reply) {
          reply.set_status(reply::status_type::not_found);
          return no_such_bucket_payload;
      },
      "txt");
    auto delete_objects_response = new function_handler(
      [](const_req req, reply& reply) -> std::string {
          if (!req.query_parameters.contains("delete")) {
              reply.set_status(reply::status_type::bad_request);
              return "wrong query_parameter";
          }

          if (auto computed_md5base64 =
                [&] {
                    auto hash = internal::hash<GNUTLS_DIG_MD5, 16>{};
                    hash.update(req.content);
                    auto digest = hash.reset();

                    return bytes_to_base64(
                      {reinterpret_cast<const uint8_t*>(digest.data()),
                       digest.size()});
                }();
              computed_md5base64 != req.get_header("Content-MD5")) {
              reply.set_status(reply::status_type::bad_request);
              return fmt::format(
                "bad Content-MD5, expected:[{}] got:[{}] body:[{}]",
                computed_md5base64,
                req.get_header("Content-MD5"),
                req.content);
          }

          auto host = std::string{req.get_header("Host")};
          if (host.starts_with("oknoerror.")) {
              // this bucket accepts the delete request
              return fmt::format(delete_objects_payload_result, "");
          } else if (host.starts_with("okerror.")) {
              // this bucket accepts the delete request but fails to delete the
              // files
              auto req_root = [&] {
                  auto buffer_stream = std::istringstream{
                    std::string{req.content}};
                  auto tree = boost::property_tree::ptree{};
                  boost::property_tree::read_xml(buffer_stream, tree);
                  return tree;
              }();

              auto errors_xml = std::string{};

              // partially validate the request xml and construct the response
              // with the provided keys
              for (auto const& [tag, value] : req_root.get_child("Delete")) {
                  if (tag == "Quiet") {
                      continue;
                  }
                  if (tag != "Object") {
                      reply.set_status(reply::status_type::bad_request);
                      return fmt::format("bad xml, unexpected <{}>", tag);
                  }
                  auto key = value.find("Key");
                  if (key == value.not_found()) {
                      reply.set_status(reply::status_type::bad_request);
                      return "missing <Key>";
                  }
                  fmt::format_to(
                    std::back_inserter(errors_xml),
                    delete_objects_payload_error,
                    value.get<std::string>("Key"),
                    "InvalidPayer");
              }

              return fmt::format(delete_objects_payload_result, errors_xml);
          } else if (host.starts_with("empty-body")) {
              reply.set_status(reply::status_type::internal_server_error);
              return "";
          }

          reply.set_status(reply::status_type::internal_server_error);
          return "unexpected";
      },
      "txt");
    r.add(operation_type::PUT, url("/test"), empty_put_response);
    r.add(operation_type::PUT, url("/test-error"), erroneous_put_response);
    r.add(operation_type::GET, url("/test"), get_response);
    r.add(operation_type::GET, url("/test-error"), erroneous_get_response);
    r.add(operation_type::DELETE, url("/test"), empty_delete_response);
    r.add(
      operation_type::DELETE, url("/test-error"), erroneous_delete_response);
    r.add(
      operation_type::GET, url("/test-unexpected"), unexpected_error_response);
    r.add(operation_type::GET, url("/"), list_objects_response);
    r.add(
      operation_type::DELETE,
      url("/test-key-not-found"),
      key_not_found_response);
    r.add(
      operation_type::DELETE,
      url("/test-bucket-not-found"),
      bucket_not_found_response);
    r.add(operation_type::POST, url("/"), delete_objects_response);
}

/// Http server and client
struct configured_test_pair {
    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<cloud_storage_clients::s3_client> client;
};

cloud_storage_clients::s3_configuration transport_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("acess-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.server_addr = server_addr;
    conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
      net::metrics_disabled::yes,
      net::public_metrics_disabled::yes,
      cloud_roles::aws_region_name{"region"},
      cloud_storage_clients::endpoint_url{"endpoint"});
    return conf;
}

static ss::lw_shared_ptr<cloud_roles::apply_credentials>
make_credentials(const cloud_storage_clients::s3_configuration& cfg) {
    return ss::make_lw_shared(
      cloud_roles::make_credentials_applier(cloud_roles::aws_credentials{
        cfg.access_key.value(),
        cfg.secret_key.value(),
        std::nullopt,
        cfg.region}));
}

/// Create server and client, server is initialized with default
/// testing paths and listening.
configured_test_pair
started_client_and_server(const cloud_storage_clients::s3_configuration& conf) {
    auto client = ss::make_shared<cloud_storage_clients::s3_client>(
      conf, make_credentials(conf));
    auto server = ss::make_shared<ss::httpd::http_server_control>();
    server->start().get();
    server->set_routes(set_routes).get();
    auto resolved = net::resolve_dns(conf.server_addr).get();
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
            cloud_storage_clients::bucket_name("test-bucket"),
            cloud_storage_clients::object_key("test"),
            expected_payload_size,
            std::move(payload_stream),
            100ms)
          .get();
        // shouldn't throw
        // the request is verified by the server
        client->shutdown();
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_put_object_failure) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        payload.append(expected_payload, expected_payload_size);
        auto payload_stream = make_iobuf_input_stream(std::move(payload));
        const auto result = client
                              ->put_object(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test-error"),
                                expected_payload_size,
                                std::move(payload_stream),
                                100ms)
                              .get();
        BOOST_REQUIRE(!result);
        BOOST_REQUIRE_EQUAL(
          result.error(), cloud_storage_clients::error_outcome::retry);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_get_object_success) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        auto payload_stream = make_iobuf_ref_output_stream(payload);
        const auto result = client
                              ->get_object(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test"),
                                100ms)
                              .get0();

        BOOST_REQUIRE(result);

        auto input_stream = result.value()->as_input_stream();
        ss::copy(input_stream, payload_stream).get0();
        iobuf_parser p(std::move(payload));
        auto actual_payload = p.read_string(p.bytes_left());
        BOOST_REQUIRE_EQUAL(actual_payload, expected_payload);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_get_object_failure) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        const auto result = client
                              ->get_object(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test-error"),
                                100ms)
                              .get0();
        BOOST_REQUIRE(!result);
        BOOST_REQUIRE_EQUAL(
          result.error(), cloud_storage_clients::error_outcome::retry);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_object_success) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        const auto result = client
                              ->delete_object(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test"),
                                100ms)
                              .get0();

        BOOST_REQUIRE(result);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_object_failure) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);

        const auto result = client
                              ->delete_object(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test-error"),
                                100ms)
                              .get0();

        BOOST_REQUIRE(!result);
        BOOST_REQUIRE_EQUAL(
          result.error(), cloud_storage_clients::error_outcome::retry);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_object_not_found) {
    /*
     * Google Cloud Storage returns a 404 Not Found response when
     * attempting to delete an object that does not exist. In order
     * to mimic the AWS S3 behaviour (where no error is returned),
     * the error is ignored and logged by the client.
     */
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);

        const auto result
          = client
              ->delete_object(
                cloud_storage_clients::bucket_name("test-bucket"),
                cloud_storage_clients::object_key("test-key-not-found"),
                100ms)
              .get0();

        BOOST_REQUIRE(result);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_bucket_not_found) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);

        const auto result
          = client
              ->delete_object(
                cloud_storage_clients::bucket_name("test-bucket"),
                cloud_storage_clients::object_key("test-bucket-not-found"),
                100ms)
              .get0();

        BOOST_REQUIRE(!result);
        BOOST_REQUIRE(
          result.error() == cloud_storage_clients::error_outcome::fail);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_unexpected_error_message) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        const auto result
          = client
              ->get_object(
                cloud_storage_clients::bucket_name("test-bucket"),
                cloud_storage_clients::object_key("test-unexpected"),
                100ms)
              .get0();
        BOOST_REQUIRE(!result);
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
        const auto result = client
                              ->list_objects(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test"))
                              .get0();

        BOOST_REQUIRE(result);
        const auto& lst = result.value();

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

        BOOST_REQUIRE(!result.value().is_truncated);
        BOOST_REQUIRE_EQUAL(result.value().next_continuation_token, "next");
        std::vector<ss::sstring> common_prefixes{"test-prefix"};
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
          common_prefixes.begin(),
          common_prefixes.end(),
          result.value().common_prefixes.begin(),
          result.value().common_prefixes.end());
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_list_objects_with_filter) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        iobuf payload;
        auto payload_stream = make_iobuf_ref_output_stream(payload);
        const auto result
          = client
              ->list_objects(
                cloud_storage_clients::bucket_name("test-bucket"),
                cloud_storage_clients::object_key("test"),
                std::nullopt,
                std::nullopt,
                std::nullopt,
                http::default_connect_timeout,
                std::nullopt,
                [](const auto& item) { return item.key == "test-key2"; })
              .get0();

        BOOST_REQUIRE(result);
        const auto& lst = result.value();

        BOOST_REQUIRE_EQUAL(lst.is_truncated, false);

        BOOST_REQUIRE_EQUAL(lst.contents.size(), 1);

        BOOST_REQUIRE_EQUAL(lst.prefix, "test-prefix");
        BOOST_REQUIRE_EQUAL(lst.contents[0].key, "test-key2");
        BOOST_REQUIRE_EQUAL(lst.contents[0].size_bytes, 222);
        BOOST_REQUIRE_EQUAL(
          strtime(lst.contents[0].last_modified), "2021-01-10T02:00:00.000Z");

        BOOST_REQUIRE(!result.value().is_truncated);
        BOOST_REQUIRE_EQUAL(result.value().next_continuation_token, "next");
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_list_objects_failure) {
    return ss::async([] {
        auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        const auto result = client
                              ->list_objects(
                                cloud_storage_clients::bucket_name(
                                  "test-bucket"),
                                cloud_storage_clients::object_key("test-error"))
                              .get0();

        BOOST_REQUIRE(!result);
        BOOST_REQUIRE_EQUAL(
          result.error(), cloud_storage_clients::error_outcome::retry);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_list_objects_with_continuation) {
    return ss::async([] {
        const auto conf = transport_configuration();
        auto [server, client] = started_client_and_server(conf);
        const auto result = client
                              ->list_objects(
                                cloud_storage_clients::bucket_name{
                                  "test-bucket"},
                                cloud_storage_clients::object_key{"test-cont"},
                                {},
                                {},
                                "ctok")
                              .get0();
        BOOST_REQUIRE(result);
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_objects_success) {
    return ss::async([] {
        auto [server, client] = started_client_and_server(
          transport_configuration());
        auto result = client
                        ->delete_objects(
                          cloud_storage_clients::bucket_name{"oknoerror"},
                          {cloud_storage_clients::object_key{"key1"},
                           cloud_storage_clients::object_key{"key2"},
                           cloud_storage_clients::object_key{"key3"}},
                          http::default_connect_timeout)
                        .get0();
        BOOST_REQUIRE(result);
        BOOST_REQUIRE(result.value().undeleted_keys.empty());
        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_objects_errors) {
    return ss::async([] {
        auto [server, client] = started_client_and_server(
          transport_configuration());
        auto keys = std::array{
          cloud_storage_clients::object_key{"key1"},
          cloud_storage_clients::object_key{"key2"},
          cloud_storage_clients::object_key{"key3"}};

        auto result = client
                        ->delete_objects(
                          cloud_storage_clients::bucket_name{"okerror"},
                          {keys.begin(), keys.end()},
                          http::default_connect_timeout)
                        .get0();
        auto u_keys = std::vector<cloud_storage_clients::object_key>{};
        BOOST_REQUIRE(result);
        std::transform(
          result.value().undeleted_keys.begin(),
          result.value().undeleted_keys.end(),
          std::back_inserter(u_keys),
          [](auto const& kr) { return kr.key; });
        std::sort(u_keys.begin(), u_keys.end());
        BOOST_REQUIRE(
          std::equal(keys.begin(), keys.end(), u_keys.begin(), u_keys.end()));

        server->stop().get();
    });
}

SEASTAR_TEST_CASE(test_delete_object_retry) {
    return ss::async([] {
        auto [server, client] = started_client_and_server(
          transport_configuration());
        auto result = client
                        ->delete_objects(
                          cloud_storage_clients::bucket_name{"empty-body"},
                          {cloud_storage_clients::object_key{"key1"}},
                          http::default_connect_timeout)
                        .get();
        BOOST_REQUIRE(!result);
        BOOST_REQUIRE(
          result.error() == cloud_storage_clients::error_outcome::retry);

        server->stop().get();
    });
}

class client_pool_fixture {
public:
    client_pool_fixture()
      : s3_conf(transport_configuration())
      , server(ss::make_shared<ss::httpd::http_server_control>()) {
        pool
          .start(
            2,
            ss::sharded_parameter([] { return transport_configuration(); }),
            cloud_storage_clients::client_pool_overdraft_policy::wait_if_empty)
          .get();

        auto credentials = cloud_roles::aws_credentials{
          s3_conf.access_key.value(),
          s3_conf.secret_key.value(),
          std::nullopt,
          s3_conf.region};

        pool.local().load_credentials(std::move(credentials));

        server->start().get();
        server->set_routes(set_routes).get();
        auto resolved = net::resolve_dns(s3_conf.server_addr).get0();
        server->listen(resolved).get();
    }

    ~client_pool_fixture() {
        pool.stop().get();
        server->stop().get();
    }

    cloud_storage_clients::s3_configuration s3_conf;
    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::sharded<cloud_storage_clients::client_pool> pool;
};

static ss::future<> test_client_pool_payload(
  ss::shared_ptr<ss::httpd::http_server_control> server,
  cloud_storage_clients::client_pool::client_lease lease) {
    auto client = lease.client;
    iobuf payload;
    auto payload_stream = make_iobuf_ref_output_stream(payload);
    auto result = co_await client->get_object(
      cloud_storage_clients::bucket_name("test-bucket"),
      cloud_storage_clients::object_key("test"),
      100ms);
    BOOST_REQUIRE(result);

    auto input_stream = result.value()->as_input_stream();
    co_await ss::copy(input_stream, payload_stream);
    iobuf_parser p(std::move(payload));
    auto actual_payload = p.read_string(p.bytes_left());
    BOOST_REQUIRE_EQUAL(actual_payload, expected_payload);
}

FIXTURE_TEST(test_client_pool_wait_strategy, client_pool_fixture) {
    ss::abort_source never_abort;
    std::vector<ss::future<>> fut;
    for (size_t i = 0; i < 20; i++) {
        auto f
          = pool.local()
              .acquire(never_abort)
              .then([server = server](
                      cloud_storage_clients::client_pool::client_lease lease) {
                  return test_client_pool_payload(server, std::move(lease));
              });
        fut.emplace_back(std::move(f));
    }
    ss::when_all_succeed(fut.begin(), fut.end()).get0();
    BOOST_REQUIRE(pool.local().size() == 2);
}

static ss::future<bool> test_client_pool_reconnect_helper(
  ss::shared_ptr<ss::httpd::http_server_control> server,
  cloud_storage_clients::client_pool::client_lease lease) {
    auto client = lease.client;
    co_await ss::sleep(100ms);
    iobuf payload;
    auto payload_stream = make_iobuf_ref_output_stream(payload);
    try {
        auto result = co_await client->get_object(
          cloud_storage_clients::bucket_name("test-bucket"),
          cloud_storage_clients::object_key("test"),
          100ms);
        BOOST_REQUIRE(result);

        auto input_stream = result.value()->as_input_stream();
        co_await ss::copy(input_stream, payload_stream);
        iobuf_parser p(std::move(payload));
        auto actual_payload = p.read_string(p.bytes_left());
        BOOST_REQUIRE_EQUAL(actual_payload, expected_payload);
        client->shutdown();
    } catch (const ss::abort_requested_exception&) {
        co_return false;
    }
    co_return true;
}

FIXTURE_TEST(test_client_pool_reconnect, client_pool_fixture) {
    using namespace std::chrono_literals;
    ss::abort_source never_abort;
    std::vector<ss::future<bool>> fut;
    for (size_t i = 0; i < 20; i++) {
        auto f = pool.local()
                   .acquire(never_abort)
                   .then(
                     [server = server](
                       cloud_storage_clients::client_pool::client_lease lease) {
                         return test_client_pool_reconnect_helper(
                           server, std::move(lease));
                     });
        fut.emplace_back(std::move(f));
    }
    auto result = ss::when_all_succeed(fut.begin(), fut.end()).get0();
    auto count = std::count(result.begin(), result.end(), true);
    BOOST_REQUIRE(count == 20);
}
