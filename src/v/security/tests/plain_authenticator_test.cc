// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/errc.h"
#include "security/plain_authenticator.h"
#include "security/scram_algorithm.h"
#include "security/scram_credential.h"
#include "test_utils/test.h"

class sasl_plain_test_fixture : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        authn = std::make_unique<security::plain_authenticator>(store);
        ASSERT_FALSE_CORO(authn->complete());
        ASSERT_FALSE_CORO(authn->failed());
    }

    ss::future<> TearDownAsync() override {
        authn.reset();
        co_return;
    }

    security::credential_store store;
    std::unique_ptr<security::plain_authenticator> authn;

    enum class scram_type_t {
        scram_sha256,
        scram_sha512,
    };

    security::scram_credential make_scram_credential(
      scram_type_t scram_type,
      const ss::sstring& username,
      const ss::sstring& password) {
        auto iterations = [scram_type]() {
            if (scram_type == scram_type_t::scram_sha256) {
                return security::scram_sha256::min_iterations;
            }
            return security::scram_sha512::min_iterations;
        }();

        security::acl_principal principal(
          security::principal_type::user, username);

        return scram_type == scram_type_t::scram_sha256
                 ? security::scram_sha256::make_credentials(
                     std::move(principal), password, iterations)
                 : security::scram_sha512::make_credentials(
                     std::move(principal), password, iterations);
    }

    bytes create_authn_message(
      const ss::sstring& username, const ss::sstring& password) {
        bytes rv;
        rv.push_back('\0');
        std::ranges::move(bytes::from_string(username), std::back_inserter(rv));
        rv.push_back('\0');
        std::ranges::move(bytes::from_string(password), std::back_inserter(rv));
        return rv;
    }
};

TEST_F_CORO(sasl_plain_test_fixture, test_success_scram256) {
    ss::sstring username("user");
    ss::sstring password("password");

    auto creds = make_scram_credential(
      scram_type_t::scram_sha256, username, password);
    store.put(security::credential_user{username}, creds);

    auto res = co_await authn->authenticate(
      create_authn_message(username, password));
    ASSERT_TRUE_CORO(res.has_value())
      << "Error during authenticate: " << res.assume_error();
    EXPECT_TRUE(authn->complete());
    EXPECT_FALSE(authn->failed());
    EXPECT_EQ(authn->principal().name(), username);
}

TEST_F_CORO(sasl_plain_test_fixture, test_success_scram512) {
    ss::sstring username("user");
    ss::sstring password("password");

    auto creds = make_scram_credential(
      scram_type_t::scram_sha512, username, password);
    store.put(security::credential_user{username}, creds);

    auto res = co_await authn->authenticate(
      create_authn_message(username, password));
    ASSERT_TRUE_CORO(res.has_value())
      << "Error during authenticate: " << res.assume_error();
    EXPECT_TRUE(authn->complete());
    EXPECT_FALSE(authn->failed());
    EXPECT_EQ(authn->principal().name(), username);
}

TEST_F_CORO(sasl_plain_test_fixture, test_invalid_password) {
    ss::sstring username("user");
    ss::sstring password("password");
    ss::sstring wrong_password("wrong_password");
    auto creds = make_scram_credential(
      scram_type_t::scram_sha256, username, password);
    store.put(security::credential_user{username}, creds);

    auto res = co_await authn->authenticate(
      create_authn_message(username, wrong_password));
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}

TEST_F_CORO(sasl_plain_test_fixture, test_invalid_user) {
    ss::sstring username("user");
    ss::sstring password("password");
    ss::sstring wrong_username("wrong_user");

    auto creds = make_scram_credential(
      scram_type_t::scram_sha256, username, password);
    store.put(security::credential_user{username}, creds);

    auto res = co_await authn->authenticate(
      create_authn_message(wrong_username, password));
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}

TEST_F_CORO(sasl_plain_test_fixture, test_resuse_authn) {
    ss::sstring username("user");
    ss::sstring password("password");

    auto creds = make_scram_credential(
      scram_type_t::scram_sha256, username, password);
    store.put(security::credential_user{username}, creds);
    {
        auto res = co_await authn->authenticate(
          create_authn_message(username, password));
        ASSERT_TRUE_CORO(res.has_value())
          << "Error during authenticate: " << res.assume_error();
        EXPECT_TRUE(authn->complete());
        EXPECT_FALSE(authn->failed());
    }

    {
        auto res = co_await authn->authenticate(
          create_authn_message(username, password));
        ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
        EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
        EXPECT_FALSE(authn->complete());
        EXPECT_TRUE(authn->failed());
    }
}

TEST_F_CORO(sasl_plain_test_fixture, test_authz_id) {
    ss::sstring username("user");
    ss::sstring password("password");
    auto authn_msg = bytes::from_string("authz");
    auto authn_msg_tmp = create_authn_message(username, password);
    std::ranges::move(authn_msg_tmp, std::back_inserter(authn_msg));

    auto res = co_await authn->authenticate(authn_msg);
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}

TEST_F_CORO(sasl_plain_test_fixture, no_username) {
    ss::sstring password("password");
    auto res = co_await authn->authenticate(create_authn_message("", password));
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}

TEST_F_CORO(sasl_plain_test_fixture, no_password) {
    ss::sstring username("user");
    auto res = co_await authn->authenticate(create_authn_message(username, ""));
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}

TEST_F_CORO(sasl_plain_test_fixture, massive_username) {
    ss::sstring username(1024, 'a');
    ss::sstring password("password");

    auto creds = make_scram_credential(
      scram_type_t::scram_sha256, username, password);
    store.put(security::credential_user{username}, creds);

    auto res = co_await authn->authenticate(
      create_authn_message(username, password));
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}

TEST_F_CORO(sasl_plain_test_fixture, massive_password) {
    ss::sstring username("user");
    ss::sstring password(1024, 'a');

    auto creds = make_scram_credential(
      scram_type_t::scram_sha256, username, password);
    store.put(security::credential_user{username}, creds);

    auto res = co_await authn->authenticate(
      create_authn_message(username, password));
    ASSERT_FALSE_CORO(res.has_value()) << "Should not have authenticated";
    EXPECT_EQ(res.assume_error(), security::errc::invalid_credentials);
    EXPECT_FALSE(authn->complete());
    EXPECT_TRUE(authn->failed());
}
