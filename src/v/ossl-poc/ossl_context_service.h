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
#include "logger.h"
#include "ssl_utils.h"
#include "ssx/thread_worker.h"

#include <openssl/crypto.h>
#include <openssl/provider.h>

class ossl_context_service final {
public:
    ossl_context_service(
      ssx::singleton_thread_worker& thread_worker, ss::sstring module_path);
    ~ossl_context_service() = default;

    ossl_context_service(const ossl_context_service&) = delete;
    ossl_context_service(ossl_context_service&&) = delete;
    ossl_context_service& operator=(const ossl_context_service&) = delete;
    ossl_context_service& operator=(ossl_context_service&&) = delete;

    ss::future<> start();

    ss::future<> stop();

    OSSL_LIB_CTX* get_ossl_context() { return _cur_context; }

private:
    OSSL_LIB_CTX* _cur_context{};
    OSSL_LIB_CTX* _old_context{};
    OSSL_PROVIDER_ptr _fips_provider{nullptr, OSSL_PROVIDER_unload};
    OSSL_PROVIDER_ptr _base_provider{nullptr, OSSL_PROVIDER_unload};
    ssx::singleton_thread_worker& _thread_worker;
    ss::sstring _module_path;
};
