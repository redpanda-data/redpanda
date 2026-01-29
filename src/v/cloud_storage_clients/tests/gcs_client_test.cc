/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/s3_client.h"
#include "gmock/gmock.h"
#include "http/tests/utils.h"
#include "net/dns.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/request.hh>
#include <seastar/http/routes.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace cloud_storage_clients;

static const uint16_t httpd_port_number = 14435;
static constexpr const char* httpd_host_name = "127.0.0.1";

namespace {

/// Mock multipart/mixed response for GCS batch delete success
/// Returns a response with all deletes successful (204 No Content)
ss::sstring
make_batch_delete_success_response(std::string_view boundary, size_t num_keys) {
    ss::sstring response;
    for (size_t i = 0; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: response-{}\r\n\r\n"
          "HTTP/1.1 204 No Content\r\n"
          "\r\n\r\n",
          boundary,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

/// Mock multipart/mixed response for GCS batch delete with errors
/// Returns a response with all deletes failed (403 Forbidden)
ss::sstring
make_batch_delete_error_response(std::string_view boundary, size_t num_keys) {
    ss::sstring response;
    for (size_t i = 0; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: response-{}\r\n\r\n"
          "HTTP/1.1 403 Forbidden\r\n"
          "Content-Type: application/json; charset=UTF-8\r\n"
          "Content-Length: 150\r\n\r\n"
          R"json({{
  "error": {{
    "code": 403,
    "message": "Access denied",
    "errors": [{{
      "domain": "global",
      "reason": "forbidden"
    }}]
  }}
}})json"
          "\r\n\r\n",
          boundary,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

/// Mock multipart/mixed response for GCS batch delete with partial errors
/// - First key succeeds
/// - Second key has a malformed content-id
/// - Third has out of range content ID
/// - And the rest fail
ss::sstring make_batch_delete_partial_error_response(
  std::string_view boundary, size_t num_keys) {
    ss::sstring response;
    // First key succeeds
    response += fmt::format(
      "--{}\r\n"
      "Content-Type: application/http\r\n"
      "Content-ID: response-0\r\n\r\n"
      "HTTP/1.1 204 No Content\r\n"
      "\r\n\r\n",
      boundary);

    response += fmt::format(
      "--{}\r\n"
      "Content-Type: application/http\r\n"
      "Content-ID: response-DEAD BEEF\r\n\r\n"
      "HTTP/1.1 204 No Content\r\n"
      "\r\n\r\n",
      boundary);

    response += fmt::format(
      "--{}\r\n"
      "Content-Type: application/http\r\n"
      "Content-ID: response-{}\r\n\r\n"
      "HTTP/1.1 204 No Content\r\n"
      "\r\n\r\n",
      boundary,
      num_keys * 2);

    // Rest fail
    for (size_t i = 3; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: response-{}\r\n\r\n"
          "HTTP/1.1 403 Forbidden\r\n"
          "Content-Type: application/json; charset=UTF-8\r\n\r\n"
          R"json({{
  "error": {{
    "code": 403,
    "message": "Forbidden"
  }}
}})json"
          "\r\n\r\n",
          boundary,
          i);
    }
    response += fmt::format("--{}--\r\n", boundary);
    return response;
}

/// Mock multipart/mixed response for GCS batch delete with 404s (not found)
/// Returns 404 for all deletes (which should be treated as success)
ss::sstring make_batch_delete_not_found_response(
  std::string_view boundary, size_t num_keys) {
    ss::sstring response;
    for (size_t i = 0; i < num_keys; ++i) {
        response += fmt::format(
          "--{}\r\n"
          "Content-Type: application/http\r\n"
          "Content-ID: response-{}\r\n\r\n"
          "HTTP/1.1 404 Not Found\r\n"
          "Content-Type: application/json; charset=UTF-8\r\n\r\n"
          R"json({{
  "error": {{
    "code": 404,
    "message": "Not Found"
  }}
}})json"
          "\r\n\r\n",
          boundary,
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

    // GCS batch API endpoint: POST /batch/storage/v1
    // Dispatch based on Authorization header to determine test scenario
    auto dispatch_handler = new flexible_function_handler(
      [](
        const_req req, reply& reply, ss::sstring& content_type) -> std::string {
          // Check that this is a batch request
          if (req._url != "/batch/storage/v1") {
              reply.set_status(reply::status_type::bad_request);
              content_type = "application/json";
              return R"json({"error": {"code": 400, "message": "Invalid endpoint"}})json";
          }

          // Count number of Content-ID entries in request body
          size_t num_keys = 0;
          size_t pos = 0;
          while ((pos = req.content.find("Content-ID:", pos))
                 != seastar::sstring::npos) {
              ++num_keys;
              pos += 11;
          }

          constexpr std::string_view response_boundary{
            "batch_response_boundary"};
          std::string response_body;

          // Dispatch based on Authorization header (test scenarios)
          auto auth = std::string{req.get_header("Authorization")};
          if (auth.contains("test-success")) {
              response_body = make_batch_delete_success_response(
                                response_boundary, num_keys)
                                .c_str();
              reply.set_status(reply::status_type::ok);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (auth.contains("test-errors")) {
              response_body = make_batch_delete_error_response(
                                response_boundary, num_keys)
                                .c_str();
              reply.set_status(reply::status_type::ok);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (auth.contains("test-partial")) {
              response_body = make_batch_delete_partial_error_response(
                                response_boundary, num_keys)
                                .c_str();
              reply.set_status(reply::status_type::ok);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (auth.contains("test-notfound")) {
              response_body = make_batch_delete_not_found_response(
                                response_boundary, num_keys)
                                .c_str();
              reply.set_status(reply::status_type::ok);
              content_type = fmt::format(
                "multipart/mixed; boundary={}", response_boundary);
          } else if (auth.contains("test-servererror")) {
              reply.set_status(reply::status_type::internal_server_error);
              content_type = "application/json";
              return R"json({"error": {"code": 500, "message": "Internal Server Error"}})json";
          } else {
              reply.set_status(reply::status_type::unauthorized);
              content_type = "application/json";
              return R"json({"error": {"code": 401, "message": "Unauthorized"}})json";
          }

          return response_body;
      },
      "multipart/mixed");

    r.add(operation_type::POST, url("/batch/storage/v1"), dispatch_handler);
}

s3_configuration make_test_configuration(std::string_view scenario) {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    s3_configuration conf;
    // Use a test access key that encodes the scenario name for dispatch
    conf.access_key = cloud_roles::public_key_str(
      fmt::format("AKIAIOSFODNN7EXAMPLE-{}", scenario));
    conf.secret_key = cloud_roles::private_key_str(
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    conf.region = cloud_roles::aws_region_name("auto");
    // Critical: Set URI to match GCS pattern so backend inference works
    conf.uri = access_point_uri("storage.googleapis.com");
    conf.server_addr = server_addr;
    conf.url_style = s3_url_style::path;
    conf.is_gcs = true;
    return conf;
}

ss::lw_shared_ptr<const cloud_roles::apply_credentials>
make_test_credentials(const s3_configuration& cfg) {
    return ss::make_lw_shared(
      cloud_roles::make_credentials_applier(
        cloud_roles::aws_credentials{
          cfg.access_key.value(),
          cfg.secret_key.value(),
          std::nullopt,
          cfg.region}));
}

class gcs_client_fixture : public ::testing::Test {
public:
    void set_up(std::string_view scenario = "test-success") {
        server = ss::make_shared<ss::httpd::http_server_control>();
        server->start().get();
        server->set_routes(set_routes).get();

        auto conf = make_test_configuration(scenario);
        auto resolved = net::resolve_dns(conf.server_addr).get();
        server->listen(resolved).get();

        auto transport_conf = build_transport_configuration(conf, nullptr);
        client = ss::make_shared<gcs_client>(
          nullptr,
          conf,
          transport_conf,
          conf.make_probe(),
          make_test_credentials(conf));
    }

    void TearDown() override {
        if (client) {
            client->shutdown();
            client->stop().get();
        }
        if (server) {
            server->stop().get();
        }
    }

    ss::shared_ptr<ss::httpd::http_server_control> server;
    ss::shared_ptr<s3_client> client;
};

} // anonymous namespace

TEST_F(gcs_client_fixture, test_gcs_batch_delete_success) {
    set_up("test-success");
    // Test deleting 3 objects successfully
    auto keys = chunked_vector<object_key>{
      object_key{"key1"}, object_key{"key2"}, object_key{"key3"}};

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_single_key) {
    set_up("test-success");
    // Test deleting a single object
    auto keys = chunked_vector<object_key>{object_key{"single-key"}};

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_many_keys) {
    set_up("test-success");
    // Test deleting many objects (simulate batch behavior)
    chunked_vector<object_key> keys;
    for (int i = 0; i < 10; ++i) {
        keys.push_back(object_key{fmt::format("key-{}", i)});
    }

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_all_errors) {
    set_up("test-errors");
    auto keys = chunked_vector<object_key>{
      object_key{"key1"}, object_key{"key2"}, object_key{"key3"}};

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value()) << result.error();
    // All keys should have failed
    EXPECT_EQ(result.value().undeleted_keys.size(), 3);

    auto undeleted
      = result.value().undeleted_keys
        | std::views::transform(
          [](const auto& uk) { return std::make_pair(uk.key, uk.reason); })
        | std::ranges::to<std::unordered_map<object_key, ss::sstring>>();

    // Verify all keys are in undeleted_keys
    for (const auto& k : keys) {
        ASSERT_TRUE(undeleted.contains(k))
          << fmt::format("Key {} not found in undeleted_keys", k);
        EXPECT_THAT(undeleted[k], testing::HasSubstr("Access denied"))
          << undeleted[k];
    }
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_partial_errors) {
    set_up("test-partial");
    auto keys = chunked_vector<object_key>{
      object_key{"key0"},
      object_key{"key1"},
      object_key{"key2"},
      object_key{"key3"},
      object_key{"key4"},
    };

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value())
      << make_error_code(result.error()).message();
    // First key succeeds, rest fail
    EXPECT_EQ(result.value().undeleted_keys.size(), keys.size() - 1);

    auto undeleted
      = result.value().undeleted_keys
        | std::views::transform(
          [](const auto& uk) { return std::make_pair(uk.key, uk.reason); })
        | std::ranges::to<std::unordered_map<object_key, ss::sstring>>();

    // Verify the failed keys are all but key0
    EXPECT_FALSE(undeleted.contains(keys[0])) << fmt::format(
      "Key {} unexpected undeleted: {}", keys[0], undeleted[keys[0]]);
    for (auto i : std::views::iota(1ul, keys.size())) {
        const auto& k = keys[i];
        ASSERT_TRUE(undeleted.contains(k))
          << fmt::format("Key {} not found in undeleted_keys", k);
        if (i == 1 || i == 2) {
            // Missing content ID, so we have no way to map the sub-response
            // back to the input key. Since corresponding content ID didn't
            // appear in the response, count this key as undeleted.
            EXPECT_THAT(
              undeleted[k],
              testing::HasSubstr("Object missing from batch response"))
              << undeleted[k];
        } else {
            EXPECT_THAT(undeleted[k], testing::HasSubstr("Forbidden"))
              << undeleted[k];
        }
    }
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_not_found) {
    set_up("test-notfound");
    // Test deleting non-existent objects (404 should be treated as success)
    auto keys = chunked_vector<object_key>{
      object_key{"missing1"}, object_key{"missing2"}};

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    ASSERT_TRUE(result.has_value())
      << make_error_code(result.error()).message();
    // 404 is treated as success for delete operations
    EXPECT_TRUE(result.value().undeleted_keys.empty());
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_server_error) {
    set_up("test-servererror");
    auto keys = chunked_vector<object_key>{object_key{"key1"}};

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    // Server error should result in error_outcome::retry
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), error_outcome::retry);
}

TEST_F(gcs_client_fixture, test_gcs_batch_delete_unauthorized) {
    set_up("test-unauthorized");
    auto keys = chunked_vector<object_key>{object_key{"key1"}};

    auto result = client
                    ->delete_objects(
                      plain_bucket_name{"test-bucket"},
                      keys,
                      http::default_connect_timeout)
                    .get();

    // Server error should result in error_outcome::retry
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), error_outcome::fail);
}
