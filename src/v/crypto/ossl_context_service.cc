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

#include "crypto/ossl_context_service.h"

#include "base/outcome.h"
#include "base/vassert.h"
#include "ssl_utils.h"
#include "ssx/thread_worker.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#include <openssl/ssl.h>

#include <utility>

namespace crypto {

namespace {

template<class R>
using initialize_result = result<R, std::string>;

ss::logger lg("ossl-library-context-service");

using OSSL_PROVIDER_ptr = internal::handle<OSSL_PROVIDER, [](OSSL_PROVIDER* p) {
    if (1 != OSSL_PROVIDER_unload(p)) {
        lg.warn(
          "Failed to unload OSSL provider: {}",
          internal::ossl_error::build_error());
    }
}>;

struct initialize_return {
    OSSL_PROVIDER_ptr crypto_provider;
    OSSL_PROVIDER_ptr base_provider;
};

std::string make_ssl_error_response(const std::string& msg) {
    return msg + internal::ossl_error::build_error();
}

initialize_result<initialize_return> initialize_openssl(
  OSSL_LIB_CTX* ctx,
  std::string_view module_path,
  std::string_view conf_file,
  is_fips_mode fips_mode) noexcept {
    if (fips_mode && (conf_file.empty() || module_path.empty())) {
        return "Configuration file and module path required in FIPS mode";
    }

    if (!conf_file.empty()) {
        lg.debug("Attempting to load OpenSSL config file {}", conf_file);
        if (!OSSL_LIB_CTX_load_config(ctx, conf_file.data())) {
            return make_ssl_error_response(
              fmt::format("Failed to load config file {}", conf_file));
        }
    }

    if (!module_path.empty()) {
        lg.debug("Attempting to set OpenSSL module path to {}", module_path);
        if (!OSSL_PROVIDER_set_default_search_path(ctx, module_path.data())) {
            return make_ssl_error_response(
              fmt::format("Failed to set module path to {}", module_path));
        }
    }

    auto provider_name = fips_mode ? "fips" : "default";
    lg.debug("Crypto provider: {}", provider_name);

    auto crypto_provider = OSSL_PROVIDER_ptr(
      OSSL_PROVIDER_load(ctx, provider_name));

    if (!crypto_provider) {
        return make_ssl_error_response(
          fmt::format("Failed to load '{}' provider", provider_name));
    }

    auto base_provider = OSSL_PROVIDER_ptr(OSSL_PROVIDER_load(ctx, "base"));
    if (!base_provider) {
        return make_ssl_error_response("Failed to load base provider");
    }

    if (fips_mode) {
        if (!EVP_set_default_properties(ctx, "fips=yes")) {
            return make_ssl_error_response(
              "Failed to set default properties to 'fips=yes'");
        }
    }

    if (!OPENSSL_init_ssl(
          OPENSSL_INIT_LOAD_SSL_STRINGS | OPENSSL_INIT_NO_LOAD_CONFIG,
          nullptr)) {
        return make_ssl_error_response("Failed to initialize OpenSSL");
    }

    return {std::move(crypto_provider), std::move(base_provider)};
}

struct initialize_thread_return {
    initialize_return init_ret;
    OSSL_LIB_CTX* orig_ctx{nullptr};
};

initialize_result<initialize_thread_return> initialize_worker_thread(
  OSSL_LIB_CTX* ctx,
  std::string_view module_path,
  std::string_view conf_file,
  is_fips_mode fips_mode) noexcept {
    // Here, we assign the provided library context to the thread worker's
    // thread instance so any use of OpenSSL within the thread worker (krb5)
    // uses the appropriately initialiazed context
    auto old_context = OSSL_LIB_CTX_set0_default(ctx);
    lg.debug(
      "thread worker context: {}, replacing {}",
      fmt::ptr(ctx),
      fmt::ptr(old_context));
    auto init_return = initialize_openssl(
      ctx, module_path, conf_file, fips_mode);

    if (init_return.has_failure()) {
        return init_return.as_failure();
    }

    return initialize_thread_return{
      .init_ret = std::move(init_return.assume_value()),
      .orig_ctx = old_context};
}

void finalize_worker_thread(OSSL_LIB_CTX* orig_ctx) {
    OSSL_LIB_CTX_set0_default(orig_ctx);
}
} // namespace

ossl_context_service::~ossl_context_service() noexcept = default;

class ossl_context_service::impl final {
    friend class ossl_context_test_class;

public:
    impl(
      ssx::singleton_thread_worker& thread_worker,
      ss::sstring config_file,
      ss::sstring module_path,
      is_fips_mode fips_mode)
      : _thread_worker(thread_worker)
      , _config_file(std::move(config_file))
      , _module_path(std::move(module_path))
      , _fips_mode(fips_mode) {}

    ~impl() noexcept {
        vassert(
          _shard_ctx == nullptr && _old_context == nullptr,
          "OpenSSL context service being destructed without being properly "
          "shutdown");
    }

    impl(const impl&) = delete;
    impl& operator=(const impl&) = delete;
    impl(impl&&) noexcept = delete;
    impl& operator=(impl&&) noexcept = delete;

    ss::future<> start() {
        lg.debug("Starting OpenSSL Context service...");
        if (ss::this_shard_id() == 0) {
            // On main shard, load the 'null' provider to the default context
            // This prevents the default context from performing any
            // cryptographic operation
            _defctxnull = OSSL_PROVIDER_ptr(
              OSSL_PROVIDER_load(nullptr, "null"));
            lg.debug("Loaded null into global provider");
            // We also need to create a library contxt and load it in just
            // within the thread worker's thread.  This is so the krb5 library
            // will use this loaded context for any operations it needs to
            // perform
            _thread_worker_ctx = internal::OSSL_LIB_CTX_ptr(OSSL_LIB_CTX_new());
            auto init_resp = co_await _thread_worker.submit([this] {
                return initialize_worker_thread(
                  _thread_worker_ctx.get(),
                  _module_path,
                  _config_file,
                  _fips_mode);
            });

            if (init_resp.has_error()) {
                throw exception(init_resp.assume_error());
            }

            _initialize_thread_worker_holder = std::move(
              init_resp.assume_value());
        }

        _shard_ctx = internal::OSSL_LIB_CTX_ptr(OSSL_LIB_CTX_new());
        // This call assigns the created library context to the thread local
        // (shard) variable within OpenSSL.  This allows OpenSSL API calls that
        // supply nullptr to the OSSL_LIB_CTX parameter to use the thread local
        // context
        _old_context = OSSL_LIB_CTX_set0_default(_shard_ctx.get());
        lg.debug(
          "Created new shard context for {} replacing {}",
          fmt::ptr(_shard_ctx.get()),
          fmt::ptr(_old_context));
        auto init_resp = co_await _thread_worker.submit([this] {
            return initialize_openssl(
              _shard_ctx.get(), _module_path, _config_file, _fips_mode);
        });

        if (init_resp.has_failure()) {
            throw exception(init_resp.assume_error());
        }

        _crypto_provider = std::move(init_resp.assume_value().crypto_provider);
        _base_provider = std::move(init_resp.assume_value().base_provider);
    }

    ss::future<> stop() {
        lg.trace("Stopping service...");
        _base_provider.reset();
        _crypto_provider.reset();
        if (_old_context != nullptr) {
            auto replaced_context = OSSL_LIB_CTX_set0_default(_old_context);
            lg.debug(
              "Reverted to old context {} and received back {} (expecting {})",
              fmt::ptr(_old_context),
              fmt::ptr(replaced_context),
              fmt::ptr(_shard_ctx.get()));
            vassert(
              replaced_context == _shard_ctx.get(),
              "Replacing original context returns unexpected library context");
        } else {
            lg.warn("Original context is null... startup failed?");
        }
        _old_context = nullptr;
        _shard_ctx.reset();

        if (ss::this_shard_id() == 0) {
            _initialize_thread_worker_holder.init_ret.base_provider.reset();
            _initialize_thread_worker_holder.init_ret.crypto_provider.reset();
            co_await _thread_worker.submit([this] {
                return finalize_worker_thread(
                  _initialize_thread_worker_holder.orig_ctx);
            });
            _thread_worker_ctx.reset();
            _defctxnull.reset();
        }
    }

    is_fips_mode fips_mode() const { return _fips_mode; }

private:
    ssx::singleton_thread_worker& _thread_worker;
    ss::sstring _config_file;
    ss::sstring _module_path;
    is_fips_mode _fips_mode;
    // Only relevant on shard0 - holds the null provider on the global default
    // context
    OSSL_PROVIDER_ptr _defctxnull{nullptr};
    // Loaded provider on shard local context that provides cryptographic
    // support.  Could be default or FIPS
    OSSL_PROVIDER_ptr _crypto_provider{nullptr};
    // Base provider that provides support for non cryptographic operations.
    // Always present regardless of FIPS or non-FIPS mode
    OSSL_PROVIDER_ptr _base_provider{nullptr};
    // Only relevant on shard0 - holds the thread worker's context
    initialize_thread_return _initialize_thread_worker_holder;
    // Only relevant on shard0 - this is the OpenSSL library context for the
    // worker thread
    internal::OSSL_LIB_CTX_ptr _thread_worker_ctx{nullptr};
    // The shard local context
    internal::OSSL_LIB_CTX_ptr _shard_ctx{nullptr};
    // The original context - must be held so it can be returned to OpenSSL when
    // the shard local context is cleaned up
    OSSL_LIB_CTX* _old_context{nullptr};
};

ossl_context_service::ossl_context_service(
  ssx::singleton_thread_worker& thread_worker,
  ss::sstring config_file,
  ss::sstring module_path,
  is_fips_mode fips_mode)
  : _impl(std::make_unique<impl>(
    thread_worker, std::move(config_file), std::move(module_path), fips_mode)) {
}

ss::future<> ossl_context_service::start() { return _impl->start(); }

ss::future<> ossl_context_service::stop() { return _impl->stop(); }

is_fips_mode ossl_context_service::fips_mode() const {
    return _impl->fips_mode();
}

} // namespace crypto
