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
#include "ossl_context_service.h"
#include "ossl_tls_service.h"
#include "ssl_utils.h"
#include "ssx/thread_worker.h"
#include "utils/stop_signal.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <openssl/crypto.h>

#include <deque>
#include <functional>

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
    ss::sharded<ossl_context_service> ossl_context;
    std::unique_ptr<ssx::singleton_thread_worker> thread_worker;
    return app.run(argc, argv, [&] {
        return ss::async([&]() {
            std::deque<ss::deferred_action<std::function<void()>>> deferred;

            auto& opts = app.configuration();
            auto module_path = opts["module-path"].as<ss::sstring>();
            auto key_path = opts["key"].as<ss::sstring>();
            auto cert_path = opts["cert"].as<ss::sstring>();
            auto port = opts["port"].as<uint16_t>();

            stop_signal sg;

            thread_worker = std::make_unique<ssx::singleton_thread_worker>();
            deferred.emplace_back([&thread_worker] {
                thread_worker->stop().get();
                thread_worker.reset();
            });

            thread_worker->start({.name = "worker"}).get();

            lg.info("Creating ossl context");
            ossl_context.start(std::ref(*thread_worker), module_path).get();
            deferred.emplace_back(
              [&ossl_context] { ossl_context.stop().get(); });
            lg.info("Invoking start on ossl_context");
            ossl_context.invoke_on_all(&ossl_context_service::start).get();
            lg.info("FInished invoking start");

            lg.info("Starting service, opening port on {}", port);

            ss::socket_address addr{port};
            ssl_service
              .start(
                std::ref(ossl_context),
                addr,
                std::move(key_path),
                std::move(cert_path))
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
