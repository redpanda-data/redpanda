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

#pragma once

#include "base/seastarx.h"
#include "ssx/thread_worker.h"
#include "test_utils/runfiles.h"
#include "test_utils/test.h"

#include <seastar/util/log.hh>

#include <openssl/crypto.h>
#include <openssl/types.h>

#include <memory>

// XXX
inline std::string get_config_file_path() {
    char* var = std::getenv("OPENSSL_CONF");
    if (var != nullptr) {
        return var;
    }
    return test_utils::get_runfile_path("src/v/crypto/tests/openssl_conf.cnf");
}

class ossl_context_base_test_framework : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        ss::global_logger_registry().set_logger_level(
          "ossl-library-context-service", seastar::log_level::trace);

        _thread_worker = std::make_unique<ssx::singleton_thread_worker>();
        co_await _thread_worker->start({.name = "worker"});

        // Grab a copy of the global context.  This will be set on all shards at
        // clean up just in case a test fails and does not perform this action
        _global_context = OSSL_LIB_CTX_get0_global_default();
    }

    ss::future<> TearDownAsync() override {
        co_await ss::smp::invoke_on_all(
          [this]() { OSSL_LIB_CTX_set0_default(_global_context); });
        if (_thread_worker) {
            co_await _thread_worker->stop();
            _thread_worker.reset();
        }
    }

protected:
    std::unique_ptr<ssx::singleton_thread_worker>& thread_worker() {
        return _thread_worker;
    }

    ss::future<bool> fips_module_present() {
        auto module_dir = test_utils::get_runfile_path("src/v/crypto/tests");
        if (char* override = ::getenv("MODULE_DIR"); override != nullptr) {
            module_dir = override;
        }
        auto dir_type = co_await ss::file_type(module_dir);
        if (
          !dir_type
          || dir_type.value() != ss::directory_entry_type::directory) {
            co_return false;
        } else {
            auto fips_file = module_dir + "/fips.so";
            co_return co_await ss::file_exists(fips_file);
        }
    }

private:
    std::unique_ptr<ssx::singleton_thread_worker> _thread_worker{nullptr};
    OSSL_LIB_CTX* _global_context{nullptr};
};
