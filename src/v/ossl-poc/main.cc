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

#include "logger.h"
#include "ossl_tls_service.h"
#include "ssl_utils.h"
#include "ssx/thread_worker.h"
#include "utils/stop_signal.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <deque>
#include <functional>

namespace {
struct initialize_return {
    OSSL_PROVIDER_ptr fips_ptr;
    OSSL_PROVIDER_ptr base_ptr;

    initialize_return()
      : fips_ptr(nullptr, OSSL_PROVIDER_unload)
      , base_ptr(nullptr, OSSL_PROVIDER_unload) {}

    initialize_return(
      OSSL_PROVIDER_ptr&& fips_ptr, OSSL_PROVIDER_ptr&& base_ptr)
      : fips_ptr(std::move(fips_ptr))
      , base_ptr(std::move(base_ptr)) {}
};
initialize_return initialize_openssl(const ss::sstring& module_path) {
    if (!OSSL_PROVIDER_set_default_search_path(nullptr, module_path.c_str())) {
        throw ossl_error("Failed to set default search path to " + module_path);
    }

    auto fips = OSSL_PROVIDER_load(nullptr, "fips");
    if (!fips) {
        throw ossl_error("Failed to load FIPS provider");
    }

    auto fips_ptr = OSSL_PROVIDER_ptr(fips, OSSL_PROVIDER_unload);

    auto base = OSSL_PROVIDER_load(nullptr, "base");

    if (!base) {
        throw ossl_error("Failed to load base provider");
    }

    auto base_ptr = OSSL_PROVIDER_ptr(base, OSSL_PROVIDER_unload);

    if (!OSSL_PROVIDER_available(nullptr, "fips")) {
        throw ossl_error("FIPS NOT UNAVAILBEL");
    }

    if (!OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, nullptr)) {
        throw ossl_error("Failed to initialize OpenSSL");
    }

    return {std::move(fips_ptr), std::move(base_ptr)};
}
} // namespace

int main(int argc, char* argv[]) {
    ss::app_template app;
    {
        namespace po = boost::program_options;
        app.add_options()(
          "key", po::value<ss::sstring>(), "Path to the key file")(
          "cert", po::value<ss::sstring>(), "path to the cert file")(
          "port", po::value<uint16_t>()->default_value(4567), "Port to use")(
          "module-path", po::value<ss::sstring>(), "Path to the modules");
    }

    ss::sharded<ossl_tls_service> ssl_service;
    std::unique_ptr<ssx::singleton_thread_worker> thread_worker;
    return app.run(argc, argv, [&] {
        return ss::async([&]() {
            auto& opts = app.configuration();
            auto module_path = opts["module-path"].as<ss::sstring>();
            auto key_path = opts["key"].as<ss::sstring>();
            auto cert_path = opts["cert"].as<ss::sstring>();
            auto port = opts["port"].as<uint16_t>();

            stop_signal sg;
            std::deque<ss::deferred_action<std::function<void()>>> deferred;
            thread_worker = std::make_unique<ssx::singleton_thread_worker>();
            deferred.emplace_back([&thread_worker] {
                thread_worker->stop().get();
                thread_worker.reset();
            });

            thread_worker->start({.name = "worker"}).get();

            lg.info("Initializing OpenSSL");
            initialize_return init_values{};
            try {
                init_values = thread_worker
                                ->submit([&module_path] {
                                    return initialize_openssl(module_path);
                                })
                                .get();
            } catch (ossl_error& e) {
                lg.error(e.what());
                return 1;
            }

            lg.info("OpenSSL successfully initialized");
            lg.info("Starting service, opening port on {}", port);

            ss::socket_address addr{port};
            ssl_service.start(addr, std::move(key_path), std::move(cert_path))
              .get();
            deferred.emplace_back([&ssl_service] { ssl_service.stop().get(); });
            ssl_service.invoke_on_all(&ossl_tls_service::start).get();
            lg.info("Started service now waiting...");
            sg.wait().get();
            lg.info("Exiting...");
            return 0;
        });
    });
}
