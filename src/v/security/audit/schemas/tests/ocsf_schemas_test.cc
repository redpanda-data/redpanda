/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "json/json.h"
#include "security/audit/schemas/types.h"
#include "security/audit/schemas/utils.h"
#include "security/request_auth.h"
#include "security/types.h"

#include <seastar/net/socket_defs.hh>

#include <optional>
#define BOOST_TEST_MODULE security_audit

#include "json/reader.h"
#include "json/stream.h"
#include "json/stringbuffer.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/schemas.h"

#include <seastar/http/request.hh>

#include <boost/test/unit_test.hpp>

namespace sa = security::audit;

static const sa::user default_user{
  .domain = "redpanda.com",
  .name = "redpanda-user",
  .type_id = sa::user::type::user,
  .uid = "none"};

static const ss::sstring default_user_ser{
  R"(
{
"domain": "redpanda.com",
"name": "redpanda-user",
"type_id": 1,
"uid": "none"
}
)"};

static const sa::user default_user_with_role{
  .domain = "redpanda.com",
  .name = "redpanda-user",
  .type_id = sa::user::type::user,
  .uid = "none",
  .groups = std::vector<sa::group>{
    {sa::group{.type = sa::group::type_id::role, .name = "redpanda-group"}}}};

static const ss::sstring default_user_with_role_ser{
  R"(
{
"domain": "redpanda.com",
"name": "redpanda-user",
"type_id": 1,
"uid": "none",
"groups" : [{"type": "role", "name": "redpanda-group"}]
}
)"};

static const sa::authorization_result authz_success{
  .decision = "authorized",
  .policy = sa::policy{
    .desc = "some description", .name = "acl_authorization"}};

static const ss::sstring authz_success_ser{
  R"(
{
  "decision": "authorized",
  "policy": {
    "desc": "some description",
    "name": "acl_authorization"
  }
})"};

static const sa::api api_create_topic{
  .operation = "create_topic", .service = {.name = "kafka rpc"}};

static const ss::sstring api_create_topic_ser{
  R"(
{
  "operation": "create_topic",
  "service": {
    "name": "kafka rpc"
  }
})"};

static const sa::network_endpoint rp_kafka_endpoint{
  .addr{"1.1.1.1", 9092}, .svc_name = "kafka", .uid = "cluster1"};

static const ss::sstring rp_kafka_endpoint_ser{
  R"(
{
  "ip": "1.1.1.1",
  "port": 9092,
  "svc_name": "kafka",
  "uid": "cluster1"
}
)"};

static const sa::resource_detail resource_detail{
  .name = "topic1", .type = "topic"};

static const ss::sstring resource_detail_ser{
  R"(
{
  "name": "topic1",
  "type": "topic"
}
)"};

static const sa::network_endpoint client_kafka_endpoint{
  .intermediate_ips = {"2.2.2.2", "3.3.3.3"},
  .addr{"1.1.1.2", 9092},
  .name = "rpk",
};

static const ss::sstring client_kafka_endpoint_ser{
  R"(
{
  "intermediate_ips": ["2.2.2.2", "3.3.3.3"],
  "ip": "1.1.1.2",
  "name": "rpk",
  "port": 9092
}
)"};

static const sa::api_activity_unmapped unmapped {
  .authorization_metadata = sa::authorization_metadata {
    .acl_authorization = {
      .host = "*",
      .op = "CREATE",
      .permission_type = "ALLOW",
      .principal = "User:redpanda-user",
    },
    .resource = {
      .name = "topic1",
      .pattern = "LITERAL",
      .type = "topic",
    }
  }
};

static const ss::sstring unmapped_ser{
  R"(
{
  "authorization_metadata": {
    "acl_authorization": {
      "host": "*",
      "op": "CREATE",
      "permission_type": "ALLOW",
      "principal": "User:redpanda-user"
    },
    "resource": {
      "name": "topic1",
      "pattern": "LITERAL",
      "type": "topic"
    }
  }
}
)"};

static const ss::sstring metadata_ser{
  R"(
{
  "product": {
    "name": "Redpanda",
    "uid": "0",
    "vendor_name": "Redpanda Data, Inc.",
    "version": ")"
  + ss::sstring{redpanda_git_version()} + R"("
  },
  "version": "1.0.0"
}
)"};

static const ss::sstring metadata_cloud_profile_ser{
  R"(
{
  "product": {
    "name": "Redpanda",
    "uid": "0",
    "vendor_name": "Redpanda Data, Inc.",
    "version": ")"
  + ss::sstring{redpanda_git_version()} + R"("
  },
  "profiles": ["cloud"],
  "version": "1.0.0"
}
  )"};

static const sa::http_header test_header{
  .name = "Accept-Encoding", .value = "application/json"};

static inline sa::http_request test_http_request() {
    return {
      .http_headers = std::vector<sa::http_header>{test_header},
      .http_method = "GET",
      .url
      = {.hostname = "127.0.0.1:9644", .path = "/v1/cluster_config", .port = sa::port_t{9644}, .scheme = "http", .url_string = "http://127.0.0.1:9644/v1/cluster_config"},
      .user_agent = "netscape",
      .version = "1.1"};
}

static const ss::sstring test_http_request_ser{
  R"(
{
  "http_headers": [ { "name": "Accept-Encoding", "value": "application/json" } ],
  "http_method" : "GET",
  "url": {
    "hostname": "127.0.0.1:9644",
    "path": "/v1/cluster_config",
    "port": 9644,
    "scheme": "http",
    "url_string": "http://127.0.0.1:9644/v1/cluster_config"
  },
  "user_agent": "netscape",
  "version": "1.1"
}
)"};

static const sa::product test_product{
  .name = "test-product",
  .uid = "0",
  .vendor_name = ss::sstring{sa::vendor_name},
  .version = ss::sstring{redpanda_git_version()},
  .feature = sa::feature{.name = "test-feature"}};

static const ss::sstring test_product_ser{
  R"(
{
  "feature": {
    "name": "test-feature"
  },
  "name": "test-product",
  "uid": "0",
  "vendor_name": ")"
  + ss::sstring{sa::vendor_name} + R"(",
  "version": ")"
  + ss::sstring{redpanda_git_version()} + R"("
}
)"};

static const sa::service test_service{.name = "test"};

static const ss::sstring test_service_ser{
  R"(
{
  "name": "test"
}
  )"};

BOOST_AUTO_TEST_CASE(validate_api_activity) {
    auto dst_endpoint = rp_kafka_endpoint;
    auto src_endpoint = client_kafka_endpoint;
    auto now = sa::timestamp_t{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count()};
    auto api_act = sa::api_activity{
      sa::api_activity::activity_id::create,
      sa::actor{
        .authorizations = {authz_success}, .user = default_user_with_role},
      sa::api{api_create_topic},
      std::move(dst_endpoint),
      test_http_request(),
      {resource_detail},
      sa::severity_id::informational,
      std::move(src_endpoint),
      sa::api_activity::status_id::success,
      now,
      sa::api_activity_unmapped{unmapped}};

    auto ser = sa::rjson_serialize(api_act);

    ss::sstring expected{
      R"(
{
    "category_uid": 6,
    "class_uid": 6003,
    "metadata": )"
      + metadata_cloud_profile_ser + R"(,
    "severity_id": 1,
    "time": )"
      + ss::to_sstring(now) + R"(,
    "type_uid": 600301,
    "activity_id": 1,
    "actor": {
        "authorizations": [)"
      + authz_success_ser + R"(],
        "user": )"
      + default_user_with_role_ser + R"(
    },
    "api": )"
      + api_create_topic_ser + R"(,
    "cloud": { "provider": "" },
    "dst_endpoint": )"
      + rp_kafka_endpoint_ser + R"(,
    "http_request": )"
      + test_http_request_ser + R"(,
    "resources": [)"
      + resource_detail_ser + R"(],
    "src_endpoint": )"
      + client_kafka_endpoint_ser + R"(,
    "status_id": 1,
    "unmapped": )"
      + unmapped_ser + R"(
})"};

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(validate_authentication_sasl_scram) {
    auto dst_endpoint = rp_kafka_endpoint;
    auto src_endpoint = client_kafka_endpoint;
    auto now = sa::timestamp_t{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count()};
    auto authn = sa::authentication(
      sa::authentication::activity_id::logon,
      "SCRAM-SHA256",
      std::move(dst_endpoint),
      sa::authentication::used_cleartext::no,
      sa::authentication::used_mfa ::yes,
      std::move(src_endpoint),
      test_service,
      sa::severity_id::informational,
      sa::authentication::status_id::success,
      std::nullopt,
      now,
      sa::user{default_user});

    auto ser = sa::rjson_serialize(authn);

    const ss::sstring expected{
      R"(
{
"category_uid": 3,
"class_uid": 3002,
"metadata": )"
      + metadata_ser + R"(,
"severity_id": 1,
"time": )"
      + ss::to_sstring(now) + R"(,
"type_uid": 300201,
"activity_id": 1,
"auth_protocol": "SCRAM-SHA256",
"auth_protocol_id": 99,
"dst_endpoint": )"
      + rp_kafka_endpoint_ser + R"(,
"is_cleartext": false,
"is_mfa": true,
"service": )"
      + test_service_ser + R"(,
"src_endpoint": )"
      + client_kafka_endpoint_ser + R"(,
"status_id": 1,
"user": )"
      + default_user_ser + R"(
}
)"};

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(validate_authentication_kerberos) {
    auto dst_endpoint = rp_kafka_endpoint;
    auto src_endpoint = client_kafka_endpoint;
    auto now = sa::timestamp_t{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count()};
    auto authn = sa::authentication(
      sa::authentication::activity_id::logon,
      sa::authentication::auth_protocol_id::kerberos,
      std::move(dst_endpoint),
      sa::authentication::used_cleartext::yes,
      sa::authentication::used_mfa ::no,
      std::move(src_endpoint),
      test_service,
      sa::severity_id::informational,
      sa::authentication::status_id::failure,
      "Failure",
      now,
      sa::user{default_user});

    auto ser = sa::rjson_serialize(authn);

    const ss::sstring expected{
      R"(
{
"category_uid": 3,
"class_uid": 3002,
"metadata": )"
      + metadata_ser + R"(,
"severity_id": 1,
"time": )"
      + ss::to_sstring(now) + R"(,
"type_uid": 300201,
"activity_id": 1,
"auth_protocol_id": 2,
"dst_endpoint": )"
      + rp_kafka_endpoint_ser + R"(,
"is_cleartext": true,
"is_mfa": false,
"service": )"
      + test_service_ser + R"(,
"src_endpoint": )"
      + client_kafka_endpoint_ser + R"(,
"status_id": 2,
"status_detail": "Failure",
"user": )"
      + default_user_ser + R"(
}
)"};

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(validate_application_lifecycle) {
    auto now = sa::timestamp_t{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count()};
    auto app_lifecycle = sa::application_lifecycle(
      sa::application_lifecycle::activity_id::start,
      sa::product{test_product},
      sa::severity_id::informational,
      now);

    auto ser = sa::rjson_serialize(app_lifecycle);

    const auto expected = fmt::format(
      R"(
{{
  "category_uid": 6,
  "class_uid": 6002,
  "metadata": {metadata},
  "severity_id": 1,
  "time": {time},
  "type_uid": 600203,
  "activity_id": 3,
  "app": {product}
}}
)",
      fmt::arg("metadata", metadata_ser),
      fmt::arg("time", ss::to_sstring(app_lifecycle.get_time())),
      fmt::arg("product", test_product_ser));

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(validate_increment) {
    auto now = sa::timestamp_t{1};
    auto app_lifecycle = sa::application_lifecycle(
      sa::application_lifecycle::activity_id::start,
      sa::product{test_product},
      sa::severity_id::informational,
      now);

    auto increment_time = sa::timestamp_t{2};

    app_lifecycle.increment(increment_time);

    auto increment_time2 = sa::timestamp_t{3};

    app_lifecycle.increment(increment_time2);

    auto ser = sa::rjson_serialize(app_lifecycle);

    const ss::sstring expected{
      R"(
{
  "category_uid": 6,
  "class_uid": 6002,
  "count": 3,
  "end_time": )"
      + ss::to_sstring(increment_time2) + R"(,
  "metadata": )"
      + metadata_ser + R"(,
  "severity_id": 1,
  "start_time": )"
      + ss::to_sstring(now) + R"(,
  "time": )"
      + ss::to_sstring(now) + R"(,
  "type_uid": 600203,
  "activity_id": 3,
  "app": )"
      + test_product_ser + R"(
}
)"};

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(make_api_activity_event_authorized) {
    const ss::socket_address client_addr{ss::ipv4_addr("10.0.0.1", 12345)};
    const ss::socket_address server_addr{ss::ipv4_addr("10.1.1.1", 23456)};
    const ss::sstring url = "/v1/test?param=val";
    const ss::sstring method = "GET";
    const ss::sstring protocol_name = "https";
    const ss::sstring host_name = "local";
    const ss::sstring version = "1.1";
    const ss::sstring user_agent = "Mozilla";
    const ss::sstring username = "test";

    auto req = ss::http::request::make(method, host_name, url);
    req.parse_query_param();
    req._client_address = client_addr;
    req._server_address = server_addr;
    req._version = version;
    req._headers["User-Agent"] = user_agent;
    req._headers["Authorization"] = "You shouldn't see this at all";
    req.protocol_name = protocol_name;

    request_auth_result auth_result{
      security::credential_user{username},
      security::credential_password{"password"},
      "sasl",
      request_auth_result::superuser::no};

    auto api_activity = sa::api_activity::construct(
      req, auth_result, "http", true, std::nullopt);

    auth_result.pass();

    const auto expected = fmt::format(
      R"(
{{
 "category_uid": 6,
  "class_uid": 6003,
  "metadata": {metadata},
  "severity_id": 1,
  "time": {time},
  "type_uid": 600302,
  "activity_id": 2,
  "actor": {{
    "authorizations": [
      {{
        "decision": "authorized",
        "policy": {{
          "desc": "",
          "name": "http"
        }}
      }}
    ],
    "user": {{
      "name": "{username}",
      "type_id": 1
    }}
  }},
  "api": {{
    "operation": "{method}",
    "service": {{
      "name": "http"
    }}
  }},
  "cloud": {{ "provider": "" }},
  "dst_endpoint": {{
    "ip": "10.1.1.1",
    "port": 23456,
    "svc_name": "http"
  }},
  "http_request": {{
    "http_headers": [
      {{
        "name": "User-Agent",
        "value": "{user_agent}"
      }},
      {{
        "name": "Authorization",
        "value": "******"
      }},
      {{
        "name": "Host",
        "value": "{host_name}"
      }}
    ],
    "http_method": "{method}",
    "url": {{
      "hostname": "{host_name}",
      "path": "{url}",
      "port": 23456,
      "scheme": "{protocol_name}",
      "url_string": "{url_string}"
    }},
    "user_agent": "{user_agent}",
    "version": "{version}"
  }},
  "src_endpoint": {{
    "ip": "10.0.0.1",
    "port": 12345
  }},
  "status_id": 1,
  "unmapped": {{}}
}}
    )",
      fmt::arg("metadata", metadata_cloud_profile_ser),
      fmt::arg("time", ss::to_sstring(api_activity.get_time())),
      fmt::arg("username", username),
      fmt::arg("method", method),
      fmt::arg("user_agent", user_agent),
      fmt::arg("host_name", host_name),
      fmt::arg("url", url),
      fmt::arg("protocol_name", protocol_name),
      fmt::arg("url_string", protocol_name + "://" + host_name + url),
      fmt::arg("version", version));

    auto ser = sa::rjson_serialize(api_activity);

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(make_authentication_event_success) {
    const ss::socket_address client_addr{ss::ipv4_addr("10.0.0.1", 12345)};
    const ss::socket_address server_addr{ss::ipv4_addr("10.1.1.1", 23456)};
    const ss::sstring url = "/v1/test?param=val";
    const ss::sstring method = "GET";
    const ss::sstring protocol_name = "https";
    const ss::sstring host_name = "local";
    const ss::sstring version = "1.1";
    const ss::sstring user_agent = "Mozilla";
    const ss::sstring username = "test";
    const ss::sstring service_name = "http";

    auto req = ss::http::request::make(method, host_name, url);
    req.parse_query_param();
    req._client_address = client_addr;
    req._server_address = server_addr;
    req._version = version;
    req._headers["User-Agent"] = user_agent;
    req._headers["Authorization"] = "You shouldn't see this at all";
    req.protocol_name = protocol_name;

    request_auth_result auth_result{
      security::credential_user{username},
      security::credential_password{"password"},
      "sasl",
      request_auth_result::superuser::no};

    auto authn = sa::authentication::construct(sa::authentication_event_options {
      .auth_protocol = "sasl",
      .server_addr = {fmt::format("{}", req.get_server_address().addr()), req.get_server_address().port(), req.get_server_address().addr().in_family()},
      .svc_name = service_name,
      .client_addr = {fmt::format("{}", req.get_client_address().addr()), req.get_client_address().port(), req.get_client_address().addr().in_family()},
      .is_cleartext = sa::authentication::used_cleartext::no,
      .user = {
        .name = username,
        .type_id = sa::user::type::user,
      },
    });

    auth_result.pass();

    const auto expected = fmt::format(
      R"(
{{
  "category_uid": 3,
  "class_uid": 3002,
  "metadata": {metadata},
  "severity_id": 1,
  "time": {time},
  "type_uid": 300201,
  "activity_id": 1,
  "auth_protocol": "sasl",
  "auth_protocol_id": 99,
  "dst_endpoint": {{
    "ip": "10.1.1.1",
    "port": 23456,
    "svc_name": "{service_name}"
  }},
  "is_cleartext": false,
  "is_mfa": false,
  "service": {{
    "name": "{service_name}"
  }},
  "src_endpoint": {{
    "ip": "10.0.0.1",
    "port": 12345
  }},
  "status_id": 1,
  "user": {{
    "name": "{username}",
    "type_id": 1
  }}
}}
    ))",
      fmt::arg("metadata", metadata_ser),
      fmt::arg("service_name", service_name),
      fmt::arg("time", ss::to_sstring(authn.get_time())),
      fmt::arg("username", username));

    auto ser = sa::rjson_serialize(authn);
    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(make_api_activity_event_authorized_authn_disabled) {
    const ss::socket_address client_addr{ss::ipv4_addr("10.0.0.1", 12345)};
    const ss::socket_address server_addr{ss::ipv4_addr("10.1.1.1", 23456)};
    const ss::sstring url = "/v1/test?param=val";
    const ss::sstring method = "GET";
    const ss::sstring protocol_name = "https";
    const ss::sstring host_name = "local";
    const ss::sstring version = "1.1";
    const ss::sstring user_agent = "Mozilla";
    const ss::sstring username = "test";

    auto req = ss::http::request::make(method, host_name, url);
    req.parse_query_param();
    req._client_address = client_addr;
    req._server_address = server_addr;
    req._version = version;
    req._headers["User-Agent"] = user_agent;
    req._headers["Authorization"] = "You shouldn't see this at all";
    req.protocol_name = protocol_name;

    request_auth_result auth_result{
      request_auth_result::authenticated::yes,
      request_auth_result::superuser::yes,
      request_auth_result::auth_required::no};

    auto api_activity = sa::api_activity::construct(
      req, auth_result, "http", true, std::nullopt);

    auth_result.pass();

    const auto expected = fmt::format(
      R"(
{{
  "category_uid": 6,
  "class_uid": 6003,
  "metadata": {metadata},
  "severity_id": 1,
  "time": {time},
  "type_uid": 600302,
  "activity_id": 2,
  "actor": {{
    "authorizations": [
      {{
        "decision": "authorized",
        "policy": {{
          "desc": "Auth Disabled",
          "name": "http"
        }}
      }}
    ],
    "user": {{
      "name": "{{{{anonymous}}}}",
      "type_id": 2
    }}
  }},
  "api": {{
    "operation": "{method}",
    "service": {{
      "name": "http"
    }}
  }},
  "cloud": {{ "provider": "" }},
  "dst_endpoint": {{
    "ip": "10.1.1.1",
    "port": 23456,
    "svc_name": "http"
  }},
  "http_request": {{
    "http_headers": [
      {{
        "name": "User-Agent",
        "value": "{user_agent}"
      }},
      {{
        "name": "Authorization",
        "value": "******"
      }},
      {{
        "name": "Host",
        "value": "{host_name}"
      }}
    ],
    "http_method": "{method}",
    "url": {{
      "hostname": "{host_name}",
      "path": "{url}",
      "port": 23456,
      "scheme": "{protocol_name}",
      "url_string": "{url_string}"
    }},
    "user_agent": "{user_agent}",
    "version": "{version}"
  }},
  "src_endpoint": {{
    "ip": "10.0.0.1",
    "port": 12345
  }},
  "status_id": 1,
  "unmapped": {{}}
}}
)",
      fmt::arg("metadata", metadata_cloud_profile_ser),
      fmt::arg("time", ss::to_sstring(api_activity.get_time())),
      fmt::arg("method", method),
      fmt::arg("user_agent", user_agent),
      fmt::arg("host_name", host_name),
      fmt::arg("url", url),
      fmt::arg("protocol_name", protocol_name),
      fmt::arg("url_string", protocol_name + "://" + host_name + url),
      fmt::arg("version", version));

    auto ser = sa::rjson_serialize(api_activity);

    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(make_authn_event_failure) {
    const ss::socket_address client_addr{ss::ipv4_addr("10.0.0.1", 12345)};
    const ss::socket_address server_addr{ss::ipv4_addr("10.1.1.1", 23456)};
    const ss::sstring url = "/v1/test?param=val";
    const ss::sstring method = "GET";
    const ss::sstring protocol_name = "https";
    const ss::sstring host_name = "local";
    const ss::sstring version = "1.1";
    const ss::sstring user_agent = "Mozilla";
    const ss::sstring username = "test";
    const ss::sstring service_name = "test-service";

    auto req = ss::http::request::make(method, host_name, url);
    req.parse_query_param();
    req._client_address = client_addr;
    req._server_address = server_addr;
    req._version = version;
    req._headers["User-Agent"] = user_agent;
    req._headers["Authorization"] = "You shouldn't see this at all";
    req.protocol_name = protocol_name;

    auto authn = sa::authentication::construct(sa::authentication_event_options {
      .server_addr = {fmt::format("{}", req.get_server_address().addr()), req.get_server_address().port(), req.get_server_address().addr().in_family()},
      .svc_name = service_name,
      .client_addr = {fmt::format("{}", req.get_client_address().addr()), req.get_client_address().port(), req.get_client_address().addr().in_family()},
      .is_cleartext = sa::authentication::used_cleartext::no,
      .user = {
        .name = username,
        .type_id = sa::user::type::unknown,
      },
      .error_reason = "FAILURE"
    });

    const auto expected = fmt::format(
      R"(
{{
  "category_uid": 3,
  "class_uid": 3002,
  "metadata": {metadata},
  "severity_id": 1,
  "time": {time},
  "type_uid": 300201,
  "activity_id": 1,
  "auth_protocol_id": 0,
  "dst_endpoint": {{
    "ip": "10.1.1.1",
    "port": 23456,
    "svc_name": "{service_name}"
  }},
  "is_cleartext": false,
  "is_mfa": false,
  "service": {{
    "name": "{service_name}"
  }},
  "src_endpoint": {{
    "ip": "10.0.0.1",
    "port": 12345
  }},
  "status_id": 2,
  "status_detail": "FAILURE",
  "user": {{
    "name": "{username}",
    "type_id": 0
  }}
}}
)",
      fmt::arg("metadata", metadata_ser),
      fmt::arg("service_name", service_name),
      fmt::arg("time", ss::to_sstring(authn.get_time())),
      fmt::arg("username", username));

    auto ser = sa::rjson_serialize(authn);
    BOOST_REQUIRE_EQUAL(ser, ::json::minify(expected));
}

BOOST_AUTO_TEST_CASE(test_ocsf_size) {
    auto req = test_http_request();
    req.http_headers.push_back(req.http_headers[0]); // Add another element

    const ss::socket_address client_addr{ss::ipv4_addr("10.0.0.1", 12345)};
    const ss::socket_address server_addr{ss::ipv4_addr("10.1.1.1", 23456)};
    auto http_req = ss::http::request::make(
      req.http_method, req.url.hostname, req.url.url_string);
    http_req.parse_query_param();
    http_req._client_address = client_addr;
    http_req._server_address = server_addr;
    http_req._version = req.version;
    http_req._headers["User-Agent"] = req.user_agent;
    http_req._headers["Authorization"] = "You shouldn't see this at all";
    http_req.protocol_name = req.url.scheme;

    const ss::sstring username = "test";
    const ss::sstring service_name = "test-service";
    auto authn = sa::authentication::construct(sa::authentication_event_options {
      .server_addr = {fmt::format("{}", http_req.get_server_address().addr()), http_req.get_server_address().port(), http_req.get_server_address().addr().in_family()},
      .svc_name = service_name,
      .client_addr = {fmt::format("{}", http_req.get_client_address().addr()), http_req.get_client_address().port(), http_req.get_client_address().addr().in_family()},
      .is_cleartext = sa::authentication::used_cleartext::no,
      .user = {
        .name = username,
        .type_id = sa::user::type::unknown,
      },
      .error_reason = "FAILURE"
    });

    size_t estimated_size = sizeof(sa::ocsf_base_event<sa::authentication>);
    const auto& [activity_id, auth_protocol, auth_protocol_id, dest_endpoint_host, is_cleartext, is_mfa, service, src_endpoint_host, status_id, status_detail, user]
      = authn.equality_fields();
    estimated_size += sizeof(activity_id);
    estimated_size += sizeof(ss::sstring) + auth_protocol.size();
    estimated_size += sizeof(auth_protocol_id);
    estimated_size += sizeof(ss::sstring) + dest_endpoint_host.size();
    estimated_size += sizeof(is_cleartext);
    estimated_size += sizeof(is_mfa);
    estimated_size += sizeof(ss::sstring) + service.name.size();
    estimated_size += sizeof(ss::sstring) + src_endpoint_host.size();
    estimated_size += sizeof(status_id);
    estimated_size
      += sizeof(std::optional<ss::sstring>)
         + (status_detail.has_value() ? sizeof(ss::sstring) + status_detail->size() : 0);
    estimated_size += user.domain.size() + user.name.size() + user.uid.size()
                      + sizeof(user.type_id) + (sizeof(ss::sstring) * 3)
                      + sizeof(user.groups); // NOLINT bugprone-sizeof-container
    if (user.groups.size() == 1) {
        estimated_size += sizeof(user.type_id) + sizeof(user.groups[0].type)
                          + sizeof(ss::sstring) + user.groups[0].name.size();
    }

    BOOST_CHECK_EQUAL(estimated_size, 416);
    BOOST_CHECK_EQUAL(authn.estimated_size(), 416);
}
