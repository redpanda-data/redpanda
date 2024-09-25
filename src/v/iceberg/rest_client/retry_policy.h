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

#include "http/client.h"
#include "iceberg/rest_client/types.h"

namespace iceberg::rest_client {

// Repreents an http call which failed, encodes information about possible
// retriability and cause
struct failure {
    bool can_be_retried;
    http_call_error err;
};

struct retry_policy {
    using result_t = tl::expected<http::collected_response, failure>;

    // Given a ready future which will yield a collected_response, judges
    // whether it is:
    // 1. successful
    // 2. has failed and can be retried
    // 3. has failed and cannot be retried
    virtual result_t
    should_retry(ss::future<http::collected_response> response_f) const
      = 0;

    // If the ready future did not fail, this method checks the response http
    // status
    virtual result_t should_retry(http::collected_response response) const = 0;

    // Handles the case where the future failed with an exception
    virtual failure should_retry(std::exception_ptr ex) const = 0;
    virtual ~retry_policy() = default;
};

struct default_retry_policy : public retry_policy {
    result_t
    should_retry(ss::future<http::collected_response> response_f) const final;
    result_t should_retry(http::collected_response response) const final;
    failure should_retry(std::exception_ptr ex) const final;
};

} // namespace iceberg::rest_client
