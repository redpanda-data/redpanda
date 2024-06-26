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

#include "crypto/crypto.h"
#include "crypto/ossl_context_service.h"
#include "random/generators.h"
#include "ssx/thread_worker.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/perf_tests.hh>

static constexpr size_t inner_iters = 1000;

template<typename F>
static size_t test_body(size_t msg_len, F n) {
    auto buffer = random_generators::gen_alphanum_string(msg_len);
    for (auto i = inner_iters; i--;) {
        auto s = n(buffer);
        perf_tests::do_not_optimize(s);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * msg_len;
}

struct openssl_perf {
public:
    openssl_perf()
      : _thread_worker{std::make_unique<ssx::singleton_thread_worker>()} {
#ifdef PERF_FIPS_MODE
        auto fips_mode = crypto::is_fips_mode::yes;
#else
        auto fips_mode = crypto::is_fips_mode::no;
#endif
        _thread_worker->start({.name = "worker"}).get();
        _svc
          .start(
            std::ref(*_thread_worker),
            get_config_file_path(),
            ::getenv("MODULE_DIR"),
            fips_mode)
          .get();
        _svc.invoke_on_all(&crypto::ossl_context_service::start).get();
    }

    ~openssl_perf() {
        _svc.stop().get();
        _thread_worker->stop().get();
        _thread_worker.reset();
    }

private:
    std::unique_ptr<ssx::singleton_thread_worker> _thread_worker{nullptr};
    ss::sharded<crypto::ossl_context_service> _svc;

    static std::string get_config_file_path() {
        auto conf_file = ::getenv("OPENSSL_CONF");
        if (conf_file) {
            return conf_file;
        } else {
            return "";
        }
    }
};

PERF_TEST_F(openssl_perf, md5_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        return crypto::digest(crypto::digest_type::MD5, buffer);
    });
}

PERF_TEST_F(openssl_perf, sha256_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        return crypto::digest(crypto::digest_type::SHA256, buffer);
    });
}

PERF_TEST_F(openssl_perf, sha512_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        return crypto::digest(crypto::digest_type::SHA512, buffer);
    });
}

PERF_TEST_F(openssl_perf, hmac_sha256_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        auto key = random_generators::gen_alphanum_string(32);
        return crypto::hmac(crypto::digest_type::SHA256, key, buffer);
    });
}

PERF_TEST_F(openssl_perf, hmac_sha512_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        auto key = random_generators::gen_alphanum_string(32);
        return crypto::hmac(crypto::digest_type::SHA512, key, buffer);
    });
}
