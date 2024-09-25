/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/retry_policy.h"

#include "net/connection.h"

namespace {

using iceberg::rest_client::failure;

failure unretriable(std::string_view msg) {
    return failure{.can_be_retried = false, .err = ss::sstring{msg}};
}

failure retriable(std::string_view msg) {
    return failure{.can_be_retried = true, .err = ss::sstring{msg}};
}

using enum boost::beast::http::status;
constexpr auto retriable_statuses = std::to_array(
  {internal_server_error,
   bad_gateway,
   service_unavailable,
   gateway_timeout,
   request_timeout,
   too_many_requests});

bool is_abort_or_gate_close_exception(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const ss::abort_requested_exception&) {
        return true;
    } catch (const ss::gate_closed_exception&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace iceberg::rest_client {

retry_policy::result_t default_retry_policy::should_retry(
  ss::future<http::downloaded_response> response_f) const {
    vassert(response_f.available(), "future is not resolved");
    if (!response_f.failed()) {
        // future resolved successfully, check the status code
        return should_retry(response_f.get());
    } else {
        // future failed, check the exception kind
        return tl::unexpected(should_retry(response_f.get_exception()));
    }
}

retry_policy::result_t
default_retry_policy::should_retry(http::downloaded_response response) const {
    const auto status = response.status;
    // the successful status class contains all status codes starting with 2
    if (
      boost::beast::http::to_status_class(status)
      == boost::beast::http::status_class::successful) {
        return response;
    }

    const auto can_be_retried = std::ranges::find(retriable_statuses, status)
                                != retriable_statuses.end();
    return tl::unexpected(
      failure{.can_be_retried = can_be_retried, .err = status});
}

failure default_retry_policy::should_retry(std::exception_ptr ex) const {
    try {
        std::rethrow_exception(ex);
    } catch (const std::system_error& err) {
        if (net::is_reconnect_error(err)) {
            return retriable(err.what());
        }
        return unretriable(err.what());
    } catch (const ss::timed_out_error& err) {
        return retriable(err.what());
    } catch (const boost::system::system_error& err) {
        if (
          err.code() != boost::beast::http::error::end_of_stream
          && err.code() != boost::beast::http::error::partial_message) {
            return unretriable(err.what());
        }
        return retriable(err.what());
    } catch (const ss::gate_closed_exception&) {
        throw;
    } catch (const ss::abort_requested_exception&) {
        throw;
    } catch (const ss::nested_exception& nested) {
        if (
          is_abort_or_gate_close_exception(nested.inner)
          || is_abort_or_gate_close_exception(nested.outer)) {
            throw;
        };
        return unretriable(fmt::format(
          "{} [outer: {}, inner: {}]",
          nested.what(),
          nested.outer,
          nested.inner));
    } catch (...) {
        return unretriable(fmt::format("{}", std::current_exception()));
    }
}

bool failure::is_transport_error() const {
    return std::holds_alternative<ss::sstring>(err);
}

} // namespace iceberg::rest_client
