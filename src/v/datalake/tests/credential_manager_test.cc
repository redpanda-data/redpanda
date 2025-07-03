/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/apply_credentials.h"
#include "cloud_roles/types.h"
#include "config/types.h"
#include "datalake/credential_manager.h"
#include "hashing/secure.h"
#include "http/client.h"
#include "test_utils/scoped_config.h"

#include <boost/beast/http/message.hpp>
#include <gtest/gtest.h>

#include <string_view>

namespace datalake {

class mock_apply_credentials_impl
  : public cloud_roles::apply_credentials::impl {
public:
    explicit mock_apply_credentials_impl(std::error_code result = {})
      : _result(result) {}

    std::error_code
    add_auth(http::client::request_header& /*header*/) const override {
        _add_auth_called = true;
        return _result;
    }

    void reset_creds(cloud_roles::credentials /*creds*/) override {
        _reset_creds_called = true;
    }

    std::ostream& print(std::ostream& os) const override {
        return os << "mock_apply_credentials_impl";
    }

    bool is_oauth() const override { return false; }

    bool add_auth_called() const { return _add_auth_called; }
    bool reset_creds_called() const { return _reset_creds_called; }

private:
    std::error_code _result;
    mutable bool _add_auth_called = false;
    bool _reset_creds_called = false;
};

cloud_roles::apply_credentials make_mock_applier(std::error_code result = {}) {
    return cloud_roles::apply_credentials(
      std::make_unique<mock_apply_credentials_impl>(result));
}

class credential_manager_tester {
public:
    explicit credential_manager_tester(credential_manager& mgr)
      : _mgr(mgr) {}

    void set_apply_credentials(
      ss::lw_shared_ptr<cloud_roles::apply_credentials> applier) {
        _mgr.apply_credentials_ = std::move(applier);
    }

    cloud_roles::apply_credentials* get_apply_credentials() {
        return _mgr.apply_credentials_.get();
    }

private:
    credential_manager& _mgr;
};

class CredentialManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _manager = std::make_unique<credential_manager>();
        _tester = std::make_unique<credential_manager_tester>(*_manager);
        _test_impl = nullptr;
    }

    void TearDown() override {
        _test_impl = nullptr;
        _tester.reset();
        _manager.reset();
    }

    iobuf create_test_payload(std::string_view content) {
        iobuf buf;
        buf.append(content.data(), content.size());
        return buf;
    }

    boost::beast::http::request_header<>
    create_test_request(bool with_auth_header = false) {
        boost::beast::http::request_header<> req;
        req.method(boost::beast::http::verb::post);
        req.target("/v1/namespaces/test/tables");
        req.set(boost::beast::http::field::host, "test.catalog.com");
        if (with_auth_header) {
            req.set(
              boost::beast::http::field::authorization, "Bearer old-token");
        }
        return req;
    }

    void setup_test_credentials(std::error_code mock_result = {}) {
        auto mock_impl = std::make_unique<mock_apply_credentials_impl>(
          mock_result);
        _test_impl = mock_impl.get(); // Store raw pointer for test access
        auto mock_applier = cloud_roles::apply_credentials(
          std::move(mock_impl));
        auto shared_applier
          = ss::make_lw_shared<cloud_roles::apply_credentials>(
            std::move(mock_applier));
        _tester->set_apply_credentials(std::move(shared_applier));
    }

    ss::sstring compute_expected_sha256(std::string_view content) {
        hash_sha256 hasher;
        hasher.update({content.data(), content.size()});
        auto hash = hasher.reset();
        return to_hex(hash);
    }

    std::unique_ptr<credential_manager> _manager;
    std::unique_ptr<credential_manager_tester> _tester;
    mock_apply_credentials_impl* _test_impl;
};

// Test case 1: Non-SigV4 authentication modes should return immediately
TEST_F(CredentialManagerTest, NonSigV4AuthModesReturnImmediately) {
    // Test with 'none' auth mode
    {
        scoped_config cfg;
        cfg.get("iceberg_rest_catalog_authentication_mode")
          .set_value(config::datalake_catalog_auth_mode::none);

        auto req = create_test_request();
        auto payload = create_test_payload("test data");

        auto result
          = _manager->maybe_sign(std::make_optional(std::move(payload)), req)
              .get();
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), std::monostate{});

        // Request should be unchanged
        EXPECT_FALSE(req.count(boost::beast::http::field::authorization));
        EXPECT_FALSE(req.count("x-amz-content-sha256"));
    }

    // Test with 'bearer' auth mode
    {
        scoped_config cfg;
        cfg.get("iceberg_rest_catalog_authentication_mode")
          .set_value(config::datalake_catalog_auth_mode::bearer);

        auto req = create_test_request();
        auto payload = create_test_payload("test data");

        auto result
          = _manager->maybe_sign(std::make_optional(std::move(payload)), req)
              .get();
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), std::monostate{});

        // Request should be unchanged
        EXPECT_FALSE(req.count(boost::beast::http::field::authorization));
        EXPECT_FALSE(req.count("x-amz-content-sha256"));
    }

    // Test with 'oauth2' auth mode
    {
        scoped_config cfg;
        cfg.get("iceberg_rest_catalog_authentication_mode")
          .set_value(config::datalake_catalog_auth_mode::oauth2);

        auto req = create_test_request();
        auto payload = create_test_payload("test data");

        auto result
          = _manager->maybe_sign(std::make_optional(std::move(payload)), req)
              .get();
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), std::monostate{});

        // Request should be unchanged
        EXPECT_FALSE(req.count(boost::beast::http::field::authorization));
        EXPECT_FALSE(req.count("x-amz-content-sha256"));
    }
}

// Test case 2: SHA256 hash computation with payload
TEST_F(CredentialManagerTest, SHA256HashWithPayload) {
    scoped_config cfg;
    cfg.get("iceberg_rest_catalog_authentication_mode")
      .set_value(config::datalake_catalog_auth_mode::aws_sigv4);
    setup_test_credentials();

    auto req = create_test_request();
    std::string_view test_content = "test payload content";
    auto payload = create_test_payload(test_content);

    // Compute expected SHA256
    auto expected_sha = compute_expected_sha256(test_content);

    auto result
      = _manager->maybe_sign(std::make_optional(std::move(payload)), req).get();
    EXPECT_TRUE(result.has_value());

    // Verify SHA256 header is set correctly
    EXPECT_TRUE(req.count("x-amz-content-sha256"));
    std::string header_value(req.at("x-amz-content-sha256"));
    EXPECT_EQ(header_value, expected_sha);

    // Verify credential applier was called
    EXPECT_TRUE(_test_impl->add_auth_called());
}

// Test case 3: SHA256 hash computation without payload (empty string hash)
TEST_F(CredentialManagerTest, SHA256HashWithoutPayload) {
    scoped_config cfg;
    cfg.get("iceberg_rest_catalog_authentication_mode")
      .set_value(config::datalake_catalog_auth_mode::aws_sigv4);
    setup_test_credentials();

    auto req = create_test_request();

    auto result = _manager->maybe_sign(std::nullopt, req).get();
    EXPECT_TRUE(result.has_value());

    // The manager should not set the content sha header.
    // Signing does this in the empty payload case, when
    // using a non-test impl.
    // This should probably be improved? The flow is weird.
    EXPECT_FALSE(req.count("x-amz-content-sha256"));

    // Verify credential applier was called
    EXPECT_TRUE(_test_impl->add_auth_called());
}

// Test case 4: Authorization header clearing
TEST_F(CredentialManagerTest, AuthorizationHeaderClearing) {
    scoped_config cfg;
    cfg.get("iceberg_rest_catalog_authentication_mode")
      .set_value(config::datalake_catalog_auth_mode::aws_sigv4);
    setup_test_credentials();

    auto req = create_test_request(true); // with existing auth header

    // Verify the header exists before signing
    EXPECT_TRUE(req.count(boost::beast::http::field::authorization));
    std::string initial_auth(req.at(boost::beast::http::field::authorization));
    EXPECT_EQ(initial_auth, "Bearer old-token");

    auto result = _manager->maybe_sign(std::nullopt, req).get();
    EXPECT_TRUE(result.has_value());

    // Verify the old authorization header was cleared
    // The mock applier doesn't add a new one, so it should be absent
    EXPECT_FALSE(req.count(boost::beast::http::field::authorization));

    // Verify credential applier was called
    EXPECT_TRUE(_test_impl->add_auth_called());
}

// Test case 5: Signing error propagation
TEST_F(CredentialManagerTest, SigningErrorPropagation) {
    scoped_config cfg;
    cfg.get("iceberg_rest_catalog_authentication_mode")
      .set_value(config::datalake_catalog_auth_mode::aws_sigv4);

    // Set up credentials that return an error
    std::error_code test_error = std::make_error_code(
      std::errc::permission_denied);
    setup_test_credentials(test_error);

    auto req = create_test_request();

    auto result = _manager->maybe_sign(std::nullopt, req).get();
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), test_error);

    // Verify credential applier was called
    EXPECT_TRUE(_test_impl->add_auth_called());
}

} // namespace datalake
