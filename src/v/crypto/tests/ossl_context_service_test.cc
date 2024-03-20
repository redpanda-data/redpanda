/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "../ssl_utils.h"
#include "base/seastarx.h"
#include "bytes/bytes.h"
#include "crypto/crypto.h"
#include "crypto/exceptions.h"
#include "crypto/ossl_context_service.h"
#include "crypto/tests/test_values.h"
#include "crypto/types.h"
#include "ossl_context_service_test_base.h"
#include "ssx/thread_worker.h"
#include "test_utils/test.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#include <openssl/ssl.h>
#include <openssl/types.h>

#include <cstdlib>

using namespace test_values;

using EVP_MD_ptr = crypto::internal::handle<EVP_MD, EVP_MD_free>;

class ossl_context_test_framework : public ossl_context_base_test_framework {};

struct context_framework_tests {
    crypto::is_fips_mode fips_mode;
    ss::sstring always_there_md_test_name;
    ss::sstring provider_name;
    ss::sstring maybe_there_md_test_name;
    bool should_be_there;
};

class ossl_context_test_framework_param
  : public ossl_context_test_framework
  , public ::testing::WithParamInterface<context_framework_tests> {
public:
    ss::future<> SetUpAsync() override {
        co_await ossl_context_test_framework::SetUpAsync();
        auto& param = GetParam();

#ifndef FIPS_MODULE_REQUIRED
        // If we do not expect the module to be present, skip
        // the test if it isn't present.  If the module does happen
        // to be present, then execute the test
        auto fips_mod_present = co_await fips_module_present();
        if (param.fips_mode && !fips_mod_present) {
            GTEST_SKIP_CORO()
              << "Skipping FIPS test because module is not present";
        }
#endif

        ASSERT_NO_THROW_CORO(co_await svc.start(
          std::ref(*thread_worker()),
          get_config_file_path(),
          ::getenv("MODULE_DIR"),
          param.fips_mode));

        ASSERT_NO_THROW_CORO(
          co_await svc.invoke_on_all(&crypto::ossl_context_service::start));
    }

    ss::future<> TearDownAsync() override {
        co_await svc.stop();
        co_await ossl_context_test_framework::TearDownAsync();
    }

protected:
    ss::sharded<crypto::ossl_context_service> svc;
};

TEST_P_CORO(ossl_context_test_framework_param, validate_always_there_md) {
    auto& param = GetParam();

    const auto test_func = [param]() {
        auto md = EVP_MD_ptr(EVP_MD_fetch(
          nullptr, param.always_there_md_test_name.c_str(), nullptr));
        ASSERT_TRUE(md) << "Was unable to fetch "
                        << param.always_there_md_test_name << " on shard "
                        << ss::this_shard_id();
        auto provider_name = ss::sstring(
          OSSL_PROVIDER_get0_name(EVP_MD_get0_provider(md.get())));
        EXPECT_EQ(provider_name, param.provider_name);
    };

    co_await ss::smp::invoke_on_all(test_func);

    co_await thread_worker()->submit(test_func);
}

TEST_P_CORO(ossl_context_test_framework_param, validate_maybe_there_md) {
    auto& param = GetParam();

    const auto test_func = [param]() {
        auto md = EVP_MD_ptr(EVP_MD_fetch(
          nullptr, param.maybe_there_md_test_name.c_str(), nullptr));
        EXPECT_TRUE((md.get() != nullptr) == param.should_be_there);
    };

    co_await ss::smp::invoke_on_all(test_func);

    co_await thread_worker()->submit(test_func);
}

TEST_P_CORO(ossl_context_test_framework_param, validate_global_default) {
    auto& param = GetParam();

    const auto test_func
      = [param]() {
            auto md = EVP_MD_ptr(EVP_MD_fetch(
              OSSL_LIB_CTX_get0_global_default(),
              param.always_there_md_test_name.c_str(),
              nullptr));
            EXPECT_FALSE(md)
              << "Should not have received any MD from global default context";
        };

    co_await ss::smp::invoke_on_all(test_func);
    co_await thread_worker()->submit(test_func);
}

TEST_P_CORO(ossl_context_test_framework_param, digests) {
    EXPECT_NO_THROW(EXPECT_EQ(
      md5_expected_val,
      crypto::digest(crypto::digest_type::MD5, md5_test_val)));
    EXPECT_NO_THROW(EXPECT_EQ(
      sha256_expected_val,
      crypto::digest(crypto::digest_type::SHA256, sha256_test_val)));
    EXPECT_NO_THROW(EXPECT_EQ(
      sha512_expected_val,
      crypto::digest(crypto::digest_type::SHA512, sha512_test_val)));
    return ss::make_ready_future();
}

TEST_P_CORO(ossl_context_test_framework_param, hmac) {
    EXPECT_NO_THROW(EXPECT_EQ(
      hmac_sha256_expected,
      crypto::hmac(
        crypto::digest_type::SHA256, hmac_sha256_key, hmac_sha256_msg)));
    EXPECT_NO_THROW(EXPECT_EQ(
      hmac_sha512_expected,
      crypto::hmac(
        crypto::digest_type::SHA512, hmac_sha512_key, hmac_sha512_msg)));
    return ss::make_ready_future();
}

TEST_P_CORO(ossl_context_test_framework_param, keys) {
    auto key = crypto::key::load_key(
      example_pem_rsa_private_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::yes);
    EXPECT_EQ(key.get_type(), crypto::key_type::RSA);
    EXPECT_TRUE(key.is_private_key());

    key = crypto::key::load_key(
      example_pem_rsa_public_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::no);

    EXPECT_EQ(key.get_type(), crypto::key_type::RSA);
    EXPECT_FALSE(key.is_private_key());

    EXPECT_THROW(
      crypto::key::load_key(
        example_pem_ec_public_key,
        crypto::format_type::PEM,
        crypto::is_private_key_t::no),
      crypto::exception);

    EXPECT_NO_THROW(
      crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e));
    return ss::make_ready_future();
}

TEST_P_CORO(ossl_context_test_framework_param, sigver) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);
    EXPECT_NO_THROW(EXPECT_TRUE(crypto::verify_signature(
      crypto::digest_type::SHA256, key, sig_good_msg, sig_good_sig)));
    EXPECT_NO_THROW(EXPECT_FALSE(crypto::verify_signature(
      crypto::digest_type::SHA256, key, sig_bad_msg, sig_bad_sig)));
    return ss::make_ready_future();
}

INSTANTIATE_TEST_SUITE_P(
  ossl_context_tests,
  ossl_context_test_framework_param,
  ::testing::Values(
    context_framework_tests{
      .fips_mode = crypto::is_fips_mode::yes,
      .always_there_md_test_name = "SHA256",
      .provider_name = "fips",
      .maybe_there_md_test_name = "BLAKE2S-256",
      .should_be_there = false},
    context_framework_tests{
      .fips_mode = crypto::is_fips_mode::no,
      .always_there_md_test_name = "SHA256",
      .provider_name = "default",
      .maybe_there_md_test_name = "BLAKE2S-256",
      .should_be_there = true}));
