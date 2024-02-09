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
#include "ssl_utils.h"
#include "ssx/thread_worker.h"
#include "utils/stop_signal.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <openssl/x509.h>
#include <openssl/x509_vfy.h>

#include <deque>
#include <functional>
#include <memory>

int main(int argc, char* argv[]) {
    ss::app_template app;
    {
        namespace po = boost::program_options;
        app.add_options()(
          "key", po::value<ss::sstring>(), "Path to the key file")(
          "cert", po::value<ss::sstring>(), "Path to the certificate file")(
          "chain", po::value<ss::sstring>(), "Path to the chain file")(
          "module-path", po::value<ss::sstring>(), "Path to the modules dir")(
          "crl", po::value<ss::sstring>(), "Path to the CRL")(
          "conf-file",
          po::value<ss::sstring>(),
          "Path to the configuration file");
    }

    ss::sharded<ossl_context_service> ossl_context;
    std::unique_ptr<ssx::singleton_thread_worker> thread_worker;

    return app.run(argc, argv, [&] {
        return ss::async([&]() {
            std::deque<ss::deferred_action<std::function<void()>>> deferred;
            auto& opts = app.configuration();
            auto module_path = opts["module-path"].as<ss::sstring>();
            auto key_path = opts["key"].as<ss::sstring>();
            auto cert_path = opts["cert"].as<ss::sstring>();
            auto chain_path = opts["chain"].as<ss::sstring>();
            auto conf_file = opts["conf-file"].as<ss::sstring>();

            std::optional<X509_CRL_ptr> crl;

            stop_signal sg;

            thread_worker = std::make_unique<ssx::singleton_thread_worker>();
            deferred.emplace_back([&thread_worker] {
                thread_worker->stop().get();
                thread_worker.reset();
            });

            thread_worker->start({.name = "worker"}).get();

            lg.info("Creating ossl context");
            ossl_context.start(std::ref(*thread_worker), module_path, conf_file)
              .get();
            deferred.emplace_back(
              [&ossl_context] { ossl_context.stop().get(); });
            lg.info("Invoking start on ossl_context");
            ossl_context.invoke_on_all(&ossl_context_service::start).get();
            lg.info("Finished invoking start on OSSL context");

            auto cert = load_x509_from_file(
                          cert_path, ossl_context.local().get_ossl_context())
                          .get();

            auto stack = load_cert_chain_from_file(
                           chain_path, ossl_context.local().get_ossl_context())
                           .get();

            lg.info("Got {} certs in stack", sk_X509_num(stack.get()));

            auto x509_store_ctx = X509_STORE_CTX_ptr(X509_STORE_CTX_new_ex(
              ossl_context.local().get_ossl_context(), nullptr));
            if (!X509_STORE_CTX_init(
                  x509_store_ctx.get(), nullptr, cert.get(), nullptr)) {
                throw ossl_error("Failed to initialize X509 STORE Context");
            }
            lg.info("Successfully initialized X509 context");
            X509_STORE_CTX_set0_trusted_stack(
              x509_store_ctx.get(), stack.get());
            lg.info("Set trusted stack");

            if (opts.contains("crl")) {
                lg.info("Loading CRL");
                crl.emplace(load_crl_from_file(
                              opts["crl"].as<ss::sstring>(),
                              ossl_context.local().get_ossl_context())
                              .get());
                lg.info("Loaded CRL");
            }

            std::optional<X509_CRL_STACK_ptr> crl_stack;

            if (crl) {
                crl_stack = X509_CRL_STACK_ptr(sk_X509_CRL_new_null());
                X509_CRL_up_ref(crl->get());
                sk_X509_CRL_push(crl_stack->get(), crl->get());
                X509_STORE_CTX_set0_crls(
                  x509_store_ctx.get(), crl_stack->get());
                lg.info("Set stack of CRLs");
            }

            auto params = X509_STORE_CTX_get0_param(x509_store_ctx.get());
            X509_VERIFY_PARAM_set_flags(params, X509_V_FLAG_CRL_CHECK);

            try {
                throw ossl_error();
            } catch (ossl_error& e) {
                lg.warn("wtf...: {}", e.what());
            }

            if (1 != X509_verify_cert(x509_store_ctx.get())) {
                lg.error(
                  "Failed to verify cert: {}",
                  X509_verify_cert_error_string(
                    X509_STORE_CTX_get_error(x509_store_ctx.get())));

            } else {
                lg.info("Successfully verified the cert!");
            }
            return 0;
        });
    });
}
