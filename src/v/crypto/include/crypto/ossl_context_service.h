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

#include "ssx/thread_worker.h"

#include <memory>

namespace crypto {
using is_fips_mode = ss::bool_class<struct is_fips_mode_tag>;
/**
 * Service used to configure shard local OpenSSL library contexts.
 * These contexts will be fetched by the OpenSSL library when performing
 * cryptographic operations.
 */
class ossl_context_service final {
public:
    /**
     * @brief Construct a new ossl context service object
     *
     * @param thread_worker Thread worker used to perform I/O operations outside
     * of the Seastar context
     * @param config_file Path to the OpenSSL config file
     * @param module_path Path to the directory that contains the FIPS module
     * @param fips_mode Whether or not to start in FIPS mode
     */
    ossl_context_service(
      ssx::singleton_thread_worker& thread_worker,
      ss::sstring config_file,
      ss::sstring module_path,
      is_fips_mode fips_mode);
    ~ossl_context_service() noexcept;
    ossl_context_service(const ossl_context_service&) = delete;
    ossl_context_service(ossl_context_service&&) = delete;
    ossl_context_service& operator=(const ossl_context_service&) = delete;
    ossl_context_service& operator=(ossl_context_service&&) = delete;

    ss::future<> start();
    ss::future<> stop();

    is_fips_mode fips_mode() const;

private:
    class impl;
    std::unique_ptr<impl> _impl;

    bool in_rp_fixture_test() const;
};
} // namespace crypto
