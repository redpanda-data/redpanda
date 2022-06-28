/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/logger.h"
#include "cloud_roles/probe.h"
#include "cloud_roles/request_response_helpers.h"
#include "model/metadata.h"
#include "s3/signature.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>

namespace cloud_roles {

struct retry_params {
    ss::lowres_clock::duration backoff_ms;
    uint8_t max_retries;
};

class refresh_credentials {
public:
    class impl {
    public:
        impl(
          ss::sstring api_host,
          uint16_t api_port,
          s3::aws_region_name region,
          ss::abort_source& as,
          retry_params retry_params);
        impl(impl&&) noexcept = default;

        impl& operator=(impl&&) noexcept = delete;

        impl(const impl&) = delete;
        impl& operator=(const impl&) = delete;

        virtual ~impl() noexcept = default;

        /// Fetches credentials from api, the result can be iobuf or an error
        /// encountered during the fetch operation
        virtual ss::future<api_response> fetch_credentials() = 0;

        /// Parses response from API into valid credentials. If errors were
        /// encountered during fetch, they are returned as is. If the response
        /// is a valid iobuf, it is parsed into a credentials object.
        /// Returns the credentials or the parse error
        api_response_parse_result handle_response(api_response resp);

        ss::future<> sleep_until_expiry() const;

        virtual std::ostream& print(std::ostream& os) const = 0;

    protected:
        /// Returns an http client with the API host and port applied
        http::client make_api_client() const;

        /// Helper to parse the iobuf returned from API into a credentials
        /// object, customized to API response structure
        virtual api_response_parse_result parse_response(iobuf resp) = 0;

        /// Sets the amount of seconds to sleep before making the next API call
        /// to fetch credentials. Depends on expiry time of current set of
        /// credentials.
        void next_sleep_duration(ss::lowres_clock::duration sd) {
            vlog(
              clrl_log.trace,
              "setting next sleep duration to {} seconds",
              std::chrono::duration_cast<std::chrono::seconds>(sd).count());
            _sleep_duration = sd;
        }

        /// Calculates sleep duration given a time point in future where the
        /// credentials will expire. Keeps a small buffer to allow for network
        /// calls
        ss::lowres_clock::duration calculate_sleep_duration(
          std::chrono::system_clock::time_point expires_at) const;

        /// Calculates sleep duration given the number of seconds when the
        /// credentials will expire. Keeps a small buffer to allow for network
        /// calls
        ss::lowres_clock::duration
        calculate_sleep_duration(uint32_t expiry_sec) const;

        /// When a retryable error is seen, increment retries and set a small
        /// backoff before attempting to fetch credentials again. If the retries
        /// go over the max limit, abort the operation by throwing an exception.
        void increment_retries();

        void reset_retries() {
            vlog(
              clrl_log.info, "resetting retry counter from {} to 0", _retries);
            _retries = 0;
        }

        uint8_t retries() const { return _retries; }

        const ss::sstring& api_host() const { return _api_host; }

        uint16_t api_port() const { return _api_port; }

        s3::aws_region_name region() const { return _region; }

    private:
        /// The hostname to query for credentials. Can be overridden using env
        /// variable `RP_SI_CREDS_API_HOST`
        ss::sstring _api_host;

        /// The port to query for credentials. Can be overridden using env
        /// variable `RP_SI_CREDS_API_PORT`
        uint16_t _api_port;
        s3::aws_region_name _region;
        uint8_t _retries{0};

        /// The duration to sleep before fetching credentials. Derived from
        /// credentials expiry time adjusted with a buffer to account for
        /// network calls.
        std::optional<ss::lowres_clock::duration> _sleep_duration{std::nullopt};
        ss::abort_source& _as;
        retry_params _retry_params;
    };

    refresh_credentials(
      std::unique_ptr<impl> impl,
      ss::gate& gate,
      ss::abort_source& as,
      credentials_update_cb_t creds_update,
      s3::aws_region_name region);

    void start();

    std::ostream& print(std::ostream& os) const { return _impl->print(os); }

    ss::future<api_response> fetch_credentials() {
        return _impl->fetch_credentials();
    }

private:
    ss::future<> do_start();

    /// Fetch the credentials from API using impl and trigger the callback
    /// function with valid credentials. If an error was encountered when
    /// fetching credentials, handle retry or abort based on the error.
    ss::future<> fetch_and_update_credentials();

    api_response_parse_result handle_response(api_response resp) {
        return _impl->handle_response(std::move(resp));
    }

    ss::future<> sleep_until_expiry() const {
        return _impl->sleep_until_expiry();
    }

private:
    std::unique_ptr<impl> _impl;
    ss::gate& _gate;
    ss::abort_source& _as;
    credentials_update_cb_t _credentials_update;
    s3::aws_region_name _region;
    auth_refresh_probe _probe;
};

std::ostream& operator<<(std::ostream& os, const refresh_credentials& rc);

struct endpoint {
    ss::sstring host;
    uint16_t port;
};

static constexpr retry_params default_retry_params{
  .backoff_ms = std::chrono::milliseconds{500}, .max_retries = 8};

template<typename CredentialsProvider>
refresh_credentials make_refresh_credentials(
  ss::gate& gate,
  ss::abort_source& as,
  credentials_update_cb_t creds_update_cb,
  s3::aws_region_name region,
  std::optional<endpoint> endpoint = std::nullopt,
  retry_params retry_params = default_retry_params) {
    auto host = endpoint ? endpoint->host : CredentialsProvider::default_host;
    auto port = endpoint ? endpoint->port : CredentialsProvider::default_port;
    auto impl = std::make_unique<CredentialsProvider>(
      host.data(), port, region, as, retry_params);
    return cloud_roles::refresh_credentials{
      std::move(impl), gate, as, std::move(creds_update_cb), std::move(region)};
}

/// Builds a refresh_credentials object based on the credentials source set in
/// configuration.
refresh_credentials make_refresh_credentials(
  model::cloud_credentials_source cloud_credentials_source,
  ss::gate& gate,
  ss::abort_source& as,
  credentials_update_cb_t creds_update_cb,
  s3::aws_region_name region,
  std::optional<endpoint> endpoint = std::nullopt,
  retry_params retry_params = default_retry_params);

} // namespace cloud_roles
