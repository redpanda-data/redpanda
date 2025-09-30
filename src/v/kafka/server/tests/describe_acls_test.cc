// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/security_frontend.h"
#include "kafka/protocol/describe_acls.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "test_utils/boost_fixture.h"

using namespace std::chrono_literals;

class describe_acls_fixture : public redpanda_thread_fixture {
protected:
    void create_acl(security::acl_binding binding) {
        app.controller->get_security_frontend()
          .local()
          .create_acls({std::move(binding)}, 5s)
          .get();
    }
};

FIXTURE_TEST(describe_acls_any_kafka, describe_acls_fixture) {
    // This test verifies that when requesting any ACL and
    // "describe_registry_acls" is false, only receive back Kafka resource ACLs
    wait_for_controller_leadership().get();

    create_acl(
      security::acl_binding{
        security::resource_pattern{
          security::resource_type::topic,
          "test-topic",
          security::pattern_type::literal},
        security::acl_entry{
          security::acl_principal{
            security::principal_type::user, "User:test-user"},
          security::acl_host::wildcard_host(),
          security::acl_operation::read,
          security::acl_permission::allow}});

    create_acl(
      security::acl_binding{
        security::resource_pattern{
          security::resource_type::sr_subject,
          "test-topic-value",
          security::pattern_type::literal},
        security::acl_entry{
          security::acl_principal{
            security::principal_type::user, "User:test-user"},
          security::acl_host::wildcard_host(),
          security::acl_operation::read,
          security::acl_permission::allow}});

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_acls_request req;
    req.data.describe_registry_acls = false;
    req.data.resource_type = 1;
    req.data.resource_pattern_type = 1;
    req.data.operation = 1;
    req.data.permission_type = 1;

    auto resp = client.dispatch(std::move(req), kafka::api_version(2)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.resources.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.resources[0].type, 2);
    BOOST_REQUIRE_EQUAL(resp.data.resources[0].name, "test-topic");
    BOOST_REQUIRE(!resp.data.resources[0].registry_resource);
}

FIXTURE_TEST(describe_acls_any_sr, describe_acls_fixture) {
    // This test verifies that when requesting any ACL and
    // "describe_registry_acls" is true, only receive back Schema Registry
    // resource ACLs
    wait_for_controller_leadership().get();

    create_acl(
      security::acl_binding{
        security::resource_pattern{
          security::resource_type::topic,
          "test-topic",
          security::pattern_type::literal},
        security::acl_entry{
          security::acl_principal{
            security::principal_type::user, "User:test-user"},
          security::acl_host::wildcard_host(),
          security::acl_operation::read,
          security::acl_permission::allow}});

    create_acl(
      security::acl_binding{
        security::resource_pattern{
          security::resource_type::sr_subject,
          "test-topic-value",
          security::pattern_type::literal},
        security::acl_entry{
          security::acl_principal{
            security::principal_type::user, "User:test-user"},
          security::acl_host::wildcard_host(),
          security::acl_operation::read,
          security::acl_permission::allow}});

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_acls_request req;
    req.data.describe_registry_acls = true;
    req.data.resource_type = 1;
    req.data.resource_pattern_type = 1;
    req.data.operation = 1;
    req.data.permission_type = 1;

    auto resp = client.dispatch(std::move(req), kafka::api_version(2)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.resources.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.resources[0].type, 100); // 100 is SR_SUBJECT
    BOOST_REQUIRE_EQUAL(resp.data.resources[0].name, "test-topic-value");
    BOOST_REQUIRE(resp.data.resources[0].registry_resource);
}

FIXTURE_TEST(describe_acls_invalid_for_kafka, describe_acls_fixture) {
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_acls_request req;
    req.data.describe_registry_acls = false;
    // Request SR_SUBJECT resource type, which is invalid if
    // describe_registry_acls is false
    req.data.resource_type = 100;
    req.data.resource_pattern_type = 1;
    req.data.operation = 1;
    req.data.permission_type = 1;

    auto resp = client.dispatch(std::move(req), kafka::api_version(2)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(
      resp.data.error_code, kafka::error_code::invalid_request);
}

FIXTURE_TEST(describe_acls_invalid_for_sr, describe_acls_fixture) {
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_acls_request req;
    req.data.describe_registry_acls = true;
    // Request TOPIC resource type, which is invalid if describe_registry_acls
    // is true
    req.data.resource_type = 2;
    req.data.resource_pattern_type = 1;
    req.data.operation = 1;
    req.data.permission_type = 1;

    auto resp = client.dispatch(std::move(req), kafka::api_version(2)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(
      resp.data.error_code, kafka::error_code::invalid_request);
}
