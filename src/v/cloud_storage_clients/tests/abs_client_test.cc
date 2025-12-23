/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/abs_client.h"
#include "cloud_storage_clients/configuration.h"
#include "http/tests/utils.h"
#include "net/dns.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/request.hh>
#include <seastar/http/routes.hh>
#include <seastar/net/api.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;
using namespace cloud_storage_clients;

static const uint16_t httpd_port_number = 4434;
static constexpr const char* httpd_host_name = "localhost";

namespace {

/// Mock multipart/mixed response for batch delete success
/// Returns a response with all deletes successful (202 Accepted)
ss::sstring make_batch_delete_success_response(
  const ss::sstring& boundary, size_t num_keys) {
    ss::sstring response;
    for (size_t i = 0; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: {}\r\n\r\n"
          "HTTP/1.1 202 Accepted\r\n"
          "x-ms-request-id: test-request-id-{}\r\n"
          "x-ms-version: 2023-01-03\r\n\r\n",
          boundary,
          i,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

/// Mock multipart/mixed response for batch delete with errors
/// Returns a response with all deletes failed
ss::sstring make_batch_delete_error_response(
  const ss::sstring& boundary, size_t num_keys, const ss::sstring& error_code) {
    ss::sstring response;
    for (size_t i = 0; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: {}\r\n\r\n"
          "HTTP/1.1 403 Forbidden\r\n"
          "x-ms-error-code: {}\r\n"
          "x-ms-request-id: test-request-id-{}\r\n"
          "x-ms-version: 2023-01-03\r\n\r\n",
          boundary,
          i,
          error_code,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

/// Mock multipart/mixed response for batch delete with partial errors
/// First key succeeds, rest fail
ss::sstring make_batch_delete_partial_error_response(
  const ss::sstring& boundary, size_t num_keys, const ss::sstring& error_code) {
    ss::sstring response;
    // First key succeeds
    response += fmt::format(
      "--{}\r\n"
      "Content-Type: application/http\r\n"
      "Content-ID: 0\r\n\r\n"
      "HTTP/1.1 202 Accepted\r\n"
      "x-ms-request-id: test-request-id-0\r\n"
      "x-ms-version: 2023-01-03\r\n\r\n",
      boundary);

    // Rest fail
    for (size_t i = 1; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: {}\r\n\r\n"
          "HTTP/1.1 403 Forbidden\r\n"
          "x-ms-error-code: {}\r\n"
          "x-ms-request-id: test-request-id-{}\r\n"
          "x-ms-version: 2023-01-03\r\n\r\n",
          boundary,
          i,
          error_code,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

/// Mock multipart/mixed response for batch delete with 404s (not found)
/// Returns 404 for all deletes (which should be treated as success)
ss::sstring make_batch_delete_not_found_response(
  const ss::sstring& boundary, size_t num_keys) {
    ss::sstring response;
    for (size_t i = 0; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: {}\r\n\r\n"
          "HTTP/1.1 404 Not Found\r\n"
          "x-ms-error-code: BlobNotFound\r\n"
          "x-ms-request-id: test-request-id-{}\r\n"
          "x-ms-version: 2023-01-03\r\n\r\n",
          boundary,
          i,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

void set_routes(ss::httpd::routes& r) {
    using namespace ss::httpd;
    using reply = ss::http::reply;
    using flexible_function_handler
      = http::test_utils::flexible_function_handler;

    // Create a universal handler that dispatches based on Host header
    // Path format: /?comp=batch
    // The container name is embedded in the Host header
    auto dispatch_handler = new flexible_function_handler(
      [](
        const_req req, reply& reply, ss::sstring& content_type) -> std::string {
          auto host = std::string{req.get_header("Host")};

          if (
            !req.has_query_param("comp")
            || req.get_query_param("comp") != "batch") {
              reply.set_status(reply::status_type::bad_request);
              content_type = "text/plain";
              return "missing comp=batch query parameter";
          }

          // Count number of Content-ID entries in request body
          size_t num_keys = 0;
          size_t pos = 0;
          while ((pos = req.content.find("Content-ID:", pos))
                 != seastar::sstring::npos) {
              ++num_keys;
              pos += 11;
          }

          auto response_boundary = ss::sstring{"batch_response_boundary"};
          std::string response_body;

          // Dispatch based on host
          if (host.starts_with("test-success.")) {
              response_body = make_batch_delete_success_response(
                                response_boundary, num_keys)
                                .c_str();
              reply.set_status(reply::status_type::accepted);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (host.starts_with("test-errors.")) {
              response_body = make_batch_delete_error_response(
                                response_boundary,
                                num_keys,
                                "InvalidAuthenticationInfo")
                                .c_str();
              reply.set_status(reply::status_type::accepted);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (host.starts_with("test-partial.")) {
              response_body = make_batch_delete_partial_error_response(
                                response_boundary,
                                num_keys,
                                "AuthenticationFailed")
                                .c_str();
              reply.set_status(reply::status_type::accepted);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (host.starts_with("test-notfound.")) {
              response_body = make_batch_delete_not_found_response(
                                response_boundary, num_keys)
                                .c_str();
              reply.set_status(reply::status_type::accepted);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (host.starts_with("test-servererror.")) {
              reply.set_status(reply::status_type::internal_server_error);
              content_type = "application/xml";
              return R"xml(<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>InternalError</Code>
  <Message>The server encountered an internal error.</Message>
</Error>)xml";
          } else {
              reply.set_status(reply::status_type::bad_request);
              content_type = "text/plain";
              return "unknown test host";
          }

          return response_body;
      },
      "txt");

    r.add(operation_type::POST, url("/"), dispatch_handler);
}

abs_configuration make_test_configuration(std::string_view account) {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    abs_configuration conf;
    conf.storage_account_name = cloud_roles::storage_account(account);
    conf.uri = access_point_uri(
      fmt::format("{}.{}", conf.storage_account_name(), httpd_host_name));
    // Use a valid base64-encoded test key (88 characters, typical Azure key
    // length) This is a randomly generated test key
    conf.shared_key = cloud_roles::private_key_str(
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
      "K1SZFPTOtr/KBHBeksoGMGw==");
    conf.is_hns_enabled = false;
    conf.server_addr = server_addr;
    return conf;
}

ss::lw_shared_ptr<const cloud_roles::apply_credentials>
make_test_credentials(const abs_configuration& cfg) {
    return ss::make_lw_shared(
      cloud_roles::make_credentials_applier(
        cloud_roles::abs_credentials{
          cfg.storage_account_name, cfg.shared_key.value()}));
}

class abs_client_fixture : public ::testing::Test {
public:
    void set_up(std::string_view account = "test-success") {
        server = ss::make_shared<ss::httpd::http_server_control>();
        server->start().get();
        server->set_routes(set_routes).get();

        auto conf = make_test_configuration(account);
        auto resolved = net::resolve_dns(conf.server_addr).get();
        server->listen(resolved).get();

        auto transport_conf = build_transport_configuration(conf).get();
        client = ss::make_shared<abs_client>(
          nullptr,
          conf,
          transport_conf,
          conf.make_probe(),
          make_test_credentials(conf));
    }

    void TearDown() override {
        client->shutdown();
        server->stop().get();
    }

    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<abs_client> client;
};

} // anonymous namespace

TEST_F(abs_client_fixture, test_batch_delete_success) {
    set_up("test-success");
    // Test deleting 3 objects successfully
    auto keys = chunked_vector<object_key>{
      object_key{"key1"}, object_key{"key2"}, object_key{"key3"}};

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-success"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(abs_client_fixture, test_batch_delete_single_key) {
    set_up("test-success");
    // Test deleting a single object
    auto keys = chunked_vector<object_key>{object_key{"single-key"}};

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-success"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(abs_client_fixture, test_batch_delete_many_keys) {
    set_up("test-success");
    // Test deleting many objects (simulate batch behavior)
    chunked_vector<object_key> keys;
    for (int i = 0; i < 10; ++i) {
        keys.push_back(object_key{fmt::format("key-{}", i)});
    }

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-success"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(abs_client_fixture, test_batch_delete_all_errors) {
    set_up("test-errors");
    auto keys = chunked_vector<object_key>{
      object_key{"key1"}, object_key{"key2"}, object_key{"key3"}};

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-errors"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    // All keys should have failed
    EXPECT_EQ(result.value().undeleted_keys.size(), 3);

    // Verify all keys are in undeleted_keys
    for (size_t i = 0; i < keys.size(); ++i) {
        bool found = false;
        for (const auto& undeleted : result.value().undeleted_keys) {
            if (undeleted.key == keys[i]) {
                found = true;
                EXPECT_FALSE(undeleted.reason.empty());
                break;
            }
        }
        EXPECT_TRUE(found) << "Key " << keys[i]()
                           << " not found in undeleted_keys";
    }
}

TEST_F(abs_client_fixture, test_batch_delete_partial_errors) {
    set_up("test-partial");
    auto keys = chunked_vector<object_key>{
      object_key{"key1"}, object_key{"key2"}, object_key{"key3"}};

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-partial"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    // First key succeeds, rest fail
    EXPECT_EQ(result.value().undeleted_keys.size(), 2);

    // Verify the failed keys are key2 and key3
    for (const auto& undeleted : result.value().undeleted_keys) {
        EXPECT_TRUE(undeleted.key == keys[1] || undeleted.key == keys[2]);
        EXPECT_FALSE(undeleted.reason.empty());
    }
}

TEST_F(abs_client_fixture, test_batch_delete_not_found) {
    set_up("test-notfound");
    // Test deleting non-existent objects (404 should be treated as success)
    auto keys = chunked_vector<object_key>{
      object_key{"missing1"}, object_key{"missing2"}};

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-notfound"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    // 404 is treated as success for delete operations
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(abs_client_fixture, test_batch_delete_server_error) {
    set_up("test-servererror");
    auto keys = chunked_vector<object_key>{object_key{"key1"}};

    auto result = client
                    ->delete_objects(
                      bucket_name{"test-servererror"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    // Server error should result in error_outcome::retry
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), error_outcome::retry);
}
