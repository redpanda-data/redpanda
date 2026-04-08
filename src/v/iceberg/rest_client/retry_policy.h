/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "http/client.h"
#include "iceberg/rest_client/error.h"

namespace iceberg::rest_client {

struct request_error {
    error_kind kind;
    http_call_error err;
};

struct retry_policy {
    using result_t = tl::expected<http::downloaded_response, request_error>;

    // Given a ready future which will yield a downloaded_response, judges
    // whether it is:
    // 1. successful
    // 2. has failed and can be retried
    // 3. has failed and cannot be retried
    virtual result_t
    should_retry(ss::future<http::downloaded_response> response_f) const = 0;

    // If the ready future did not fail, this method checks the response http
    // status
    virtual result_t should_retry(http::downloaded_response response) const = 0;

    // Handles the case where the future failed with an exception. Shutdown
    // related errors are returned indicating so, all other errors are
    // classified into retriable or permanent_failure.
    virtual request_error should_retry(std::exception_ptr ex) const = 0;
    virtual ~retry_policy() = default;
};

struct default_retry_policy : public retry_policy {
    result_t
    should_retry(ss::future<http::downloaded_response> response_f) const final;
    result_t should_retry(http::downloaded_response response) const final;
    request_error should_retry(std::exception_ptr ex) const final;
};

} // namespace iceberg::rest_client
