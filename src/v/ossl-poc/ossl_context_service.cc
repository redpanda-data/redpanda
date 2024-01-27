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

#include "ossl_context_service.h"

#include "ossl-poc/ssl_utils.h"

#include <seastar/core/future.hh>

namespace {
using initialize_return = std::tuple<OSSL_PROVIDER_ptr, OSSL_PROVIDER_ptr>;

initialize_return
initialize_openssl(OSSL_LIB_CTX* libctx, const ss::sstring& module_path) {
    // Grab the path to the config file and feed it to the library context
    auto conf_file = ::getenv("OPENSSL_CONF");
    if (!OSSL_LIB_CTX_load_config(libctx, conf_file)) {
        throw ossl_error(
          ss::sstring("Failed to load config file ") + conf_file);
    }

    // Tell OpenSSL where to find modules
    if (!OSSL_PROVIDER_set_default_search_path(libctx, module_path.c_str())) {
        throw ossl_error("Failed to set default search path to " + module_path);
    }

    // Loads the FIPS module, found in the default search path, set above
    auto fips = OSSL_PROVIDER_ptr(OSSL_PROVIDER_load(libctx, "fips"));
    if (!fips) {
        throw ossl_error("Failed to load FIPS provider");
    }

    auto base = OSSL_PROVIDER_ptr(OSSL_PROVIDER_load(libctx, "base"));

    if (!base) {
        throw ossl_error("Failed to load base provider");
    }

    if (!OSSL_PROVIDER_available(libctx, "fips")) {
        throw ossl_error("FIPS NOT UNAVAILBEL");
    }

    // Make sure to initialize OpenSSL so it doesn't get re-initialized
    if (!OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, nullptr)) {
        throw ossl_error("Failed to initialize OpenSSL");
    }

    return {std::move(fips), std::move(base)};
}
} // namespace

ossl_context_service::ossl_context_service(
  ssx::singleton_thread_worker& thread_worker, ss::sstring module_path)
  : _thread_worker(thread_worker)
  , _module_path(std::move(module_path)) {}

ss::future<> ossl_context_service::start() {
    // Creates a new context
    _cur_context = OSSL_LIB_CTX_new();
    lg.info("lib_ctx: {}", fmt::ptr(_cur_context));

    // Replaces the current context with the newly created one
    _old_context = OSSL_LIB_CTX_set0_default(_cur_context);
    lg.info("_old_context: {}", fmt::ptr(_old_context));

    lg.info("Initializing library...");
    // Due to OpenSSL using File I/O and other nasties, run the initialization
    // within an alien thread, providng the created OpenSSL library context
    std::tie(_fips_provider, _base_provider) = co_await _thread_worker.submit(
      [this] { return initialize_openssl(_cur_context, _module_path); });

    lg.info("Successfully initialized");
}

ss::future<> ossl_context_service::stop() {
    lg.info("Deallocating init ptr");
    _base_provider.reset();
    _fips_provider.reset();
    lg.info("Setting back old context {}", fmt::ptr(_old_context));
    auto replaced_context = OSSL_LIB_CTX_set0_default(_old_context);
    lg.info("Replaced context {}", fmt::ptr(replaced_context));
    OSSL_LIB_CTX_free(replaced_context);

    return ss::make_ready_future();
}
