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

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/perf_tests.hh>

#include <gnutls/crypto.h>
#include <gnutls/gnutls.h>

static constexpr size_t inner_iters = 1000;

template<gnutls_mac_algorithm_t Algo, size_t DigestSize>
class hmac {
    static_assert(DigestSize > 0, "digest cannot be zero length");

public:
    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    explicit hmac(std::string_view key)
      : hmac(key.data(), key.size()) {}

    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    explicit hmac(bytes_view key)
      : hmac(key.data(), key.size()) {}

    hmac(const hmac&) = delete;
    hmac& operator=(const hmac&) = delete;
    hmac(hmac&&) = delete;
    hmac& operator=(hmac&&) = delete;

    ~hmac() noexcept { gnutls_hmac_deinit(_handle, nullptr); }

    void update(std::string_view data) { update(data.data(), data.size()); }
    void update(bytes_view data) { update(data.data(), data.size()); }

    template<std::size_t Size>
    void update(const std::array<char, Size>& data) {
        update(data.data(), Size);
    }

    /**
     * Return the current output and reset.
     */
    std::array<char, DigestSize> reset() {
        std::array<char, DigestSize> digest;
        gnutls_hmac_output(_handle, digest.data());
        return digest;
    }

private:
    // silence clang-tidy about _handle being uninitialized
    // NOLINTNEXTLINE(hicpp-member-init, cppcoreguidelines-pro-type-member-init)
    hmac(const void* key, size_t size) {
        int ret = gnutls_hmac_init(&_handle, Algo, key, size);
        if (unlikely(ret)) {
            throw std::runtime_error(gnutls_strerror(ret));
        }

        ret = gnutls_hmac_get_len(Algo);
        if (unlikely(ret != DigestSize)) {
            throw std::runtime_error("invalid digest length");
        }
    }

    void update(const void* data, size_t size) {
        int ret = gnutls_hmac(_handle, data, size);
        if (unlikely(ret)) {
            throw std::runtime_error(gnutls_strerror(ret));
        }
    }

    gnutls_hmac_hd_t _handle;
};

template<gnutls_digest_algorithm_t Algo, size_t DigestSize>
class hash {
public:
    static constexpr auto digest_size = DigestSize;
    using digest_type = std::array<char, DigestSize>;

    hash() {
        int ret = gnutls_hash_init(&_handle, Algo);
        if (unlikely(ret)) {
            throw std::runtime_error("hash init failed");
        }

        ret = gnutls_hash_get_len(Algo);
        if (unlikely(ret != DigestSize)) {
            throw std::runtime_error("BOO");
        }
    }

    hash(const hash&) = delete;
    hash& operator=(const hash&) = delete;
    hash(hash&&) = delete;
    hash& operator=(hash&&) = delete;

    ~hash() noexcept { gnutls_hash_deinit(_handle, nullptr); }

    void update(std::string_view data) { update(data.data(), data.size()); }
    void update(bytes_view data) { update(data.data(), data.size()); }

    /**
     * Return the current output and reset.
     */
    digest_type reset() {
        std::array<char, DigestSize> digest;
        gnutls_hash_output(_handle, digest.data());
        return digest;
    }

private:
    void update(const void* data, size_t size) {
        int ret = gnutls_hash(_handle, data, size);
        if (unlikely(ret)) {
            throw std::runtime_error("blah update");
        }
    }

    gnutls_hash_hd_t _handle;
};

using hmac_sha256 = hmac<GNUTLS_MAC_SHA256, 32>;
using hmac_sha512 = hmac<GNUTLS_MAC_SHA512, 64>;
using hash_sha256 = hash<GNUTLS_DIG_SHA256, 32>;
using hash_sha512 = hash<GNUTLS_DIG_SHA512, 64>;
using hash_md5 = hash<GNUTLS_DIG_MD5, 16>;

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

    ss::future<> stop() {
        co_await _svc.stop();
        co_await _thread_worker->stop();
    }

    ~openssl_perf() = default;

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

static std::unique_ptr<openssl_perf> global_perf{nullptr};

struct openssl_perf_test {
    openssl_perf_test() {
        if (!global_perf) {
            global_perf = std::make_unique<openssl_perf>();
            ss::engine().at_exit([]() -> ss::future<> {
                co_await global_perf->stop();
                global_perf.reset();
            });
        }
    }

    ~openssl_perf_test() = default;
};

PERF_TEST_F(openssl_perf_test, md5_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        return crypto::digest(crypto::digest_type::MD5, buffer);
    });
}

PERF_TEST_F(openssl_perf_test, sha256_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        return crypto::digest(crypto::digest_type::SHA256, buffer);
    });
}

PERF_TEST_F(openssl_perf_test, sha512_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        return crypto::digest(crypto::digest_type::SHA512, buffer);
    });
}

PERF_TEST(gnutls, md5_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        hash_md5 md5{};
        md5.update(buffer);
        return md5.reset();
    });
}

PERF_TEST(gnutls, sha256_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        hash_sha256 sha256{};
        sha256.update(buffer);
        return sha256.reset();
    });
}

PERF_TEST(gnutls, sha512_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        hash_sha512 sha512{};
        sha512.update(buffer);
        return sha512.reset();
    });
}

PERF_TEST_F(openssl_perf_test, hmac_sha256_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        auto key = random_generators::gen_alphanum_string(32);
        return crypto::hmac(crypto::digest_type::SHA256, key, buffer);
    });
}

PERF_TEST_F(openssl_perf_test, hmac_sha512_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        auto key = random_generators::gen_alphanum_string(32);
        return crypto::hmac(crypto::digest_type::SHA512, key, buffer);
    });
}

PERF_TEST(gnutls, hmac_sha256_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        auto key = random_generators::gen_alphanum_string(32);
        hmac_sha256 hmac{key};
        hmac.update(buffer);
        return hmac.reset();
    });
}

PERF_TEST(gnutls, hmac_sha512_1k) {
    return test_body(1024, [](const ss::sstring& buffer) {
        auto key = random_generators::gen_alphanum_string(32);
        hmac_sha512 hmac{key};
        hmac.update(buffer);
        return hmac.reset();
    });
}
