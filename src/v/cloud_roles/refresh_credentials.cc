/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/refresh_credentials.h"

#include "cloud_roles/aws_refresh_impl.h"
#include "cloud_roles/aws_sts_refresh_impl.h"
#include "cloud_roles/gcp_refresh_impl.h"
#include "cloud_roles/logger.h"
#include "config/node_config.h"
#include "model/metadata.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>

namespace cloud_roles {

/// These environment variables can be used to override the default hostname and
/// port for fetching temporary credentials for testing.
struct override_api_endpoint_env_vars {
    static constexpr std::string_view host = "RP_SI_CREDS_API_HOST";
    static constexpr std::string_view port = "RP_SI_CREDS_API_PORT";
};

/// Multiplier to derive sleep duration from expiry time. Leaves 0.1 * expiry
/// seconds as buffer to make API calls. For default expiry of 1 hour, this
/// results in a fetch after 54 minutes.
static constexpr float sleep_from_expiry_multiplier = 0.9;

refresh_credentials::refresh_credentials(
  std::unique_ptr<impl> impl,
  ss::gate& gate,
  ss::abort_source& as,
  credentials_update_cb_t creds_update,
  aws_region_name region)
  : _impl(std::move(impl))
  , _gate(gate)
  , _as(as)
  , _credentials_update(std::move(creds_update))
  , _region{std::move(region)} {}

void refresh_credentials::start() {
    ssx::background = ssx::spawn_with_gate_then(
      _gate, [this]() { return do_start(); });
}

ss::future<> refresh_credentials::do_start() {
    return ss::do_until(
      [this] { return _gate.is_closed() || _as.abort_requested(); },
      [this] { return fetch_and_update_credentials(); });
}

static std::optional<ss::sstring>
load_and_validate_env_var(std::string_view env_var) {
    char* override_maybe = std::getenv(env_var.data());
    if (override_maybe) {
        ss::sstring override{override_maybe};
        if (!override.empty()) {
            return override;
        }

        vlog(
          clrl_log.warn,
          "override environment variable {} is set but empty, ignoring",
          env_var);
    }
    return std::nullopt;
}

refresh_credentials::impl::impl(
  ss::sstring api_host,
  uint16_t api_port,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params)
  : _api_host{std::move(api_host)}
  , _api_port{api_port}
  , _region{std::move(region)}
  , _as{as}
  , _retry_params{retry_params} {
    if (auto host_override = load_and_validate_env_var(
          override_api_endpoint_env_vars::host);
        host_override) {
        vlog(
          clrl_log.debug,
          "api_host overridden from {} to {}",
          _api_host,
          *host_override);
        _api_host = {*host_override};
    }
    if (auto port_override = load_and_validate_env_var(
          override_api_endpoint_env_vars::port);
        port_override) {
        try {
            auto override_port = std::stoi(*port_override);
            vlog(
              clrl_log.debug,
              "api_port overridden from {} to {}",
              _api_port,
              override_port);
            _api_port = override_port;
        } catch (...) {
            vlog(
              clrl_log.error,
              "failed to convert port value {} from string to uint16_t",
              *port_override);
        }
    }
}

ss::future<> refresh_credentials::fetch_and_update_credentials() {
    // Before fetching new credentials, either:
    // 1. do not sleep - this is an initial call to API
    // 2. sleep until we are close to expiry of credentials
    // 3. sleep in case of retryable failure for a short duration

    co_await sleep_until_expiry();

    vlog(clrl_log.debug, "fetching credentials");
    auto fetch_response = co_await fetch_credentials();
    auto handle_result = handle_response(std::move(fetch_response));

    co_return co_await ss::visit(
      std::move(handle_result),
      [this](malformed_api_response_error err) {
          _impl->increment_retries();
          _probe.fetch_failed();
          vlog(
            clrl_log.error,
            "bad api response, missing fields: {}",
            err.missing_fields);
          return ss::now();
      },
      [this](api_response_parse_error err) {
          _impl->increment_retries();
          _probe.fetch_failed();
          vlog(clrl_log.error, "failed to parse api response: {}", err.reason);
          return ss::now();
      },
      [this](api_request_error err) {
          _impl->increment_retries();
          _probe.fetch_failed();
          vlog(
            clrl_log.error,
            "api request failed (retrying after cool-off period): {}",
            err.reason);
          return ss::now();
      },
      [this](credentials creds) {
          _impl->reset_retries();
          _probe.fetch_success();
          vlog(clrl_log.info, "fetched credentials {}", creds);
          return _credentials_update(std::move(creds));
      });
}

std::chrono::milliseconds
refresh_credentials::impl::calculate_sleep_duration(uint32_t expiry_sec) const {
    int sleep = std::floor(expiry_sec * sleep_from_expiry_multiplier);
    return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::seconds{sleep});
}

std::chrono::milliseconds refresh_credentials::impl::calculate_sleep_duration(
  std::chrono::system_clock::time_point expires_at) const {
    auto now = std::chrono::system_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(
                  expires_at - now)
                  .count();
    return calculate_sleep_duration(diff);
}

void refresh_credentials::impl::increment_retries() {
    _retries += 1;
    auto sleep_ms = _retry_params.backoff_ms * (2 * _retries);
    vlog(clrl_log.info, "retry after {} ms", sleep_ms);
    _sleep_duration = sleep_ms;
}

api_response_parse_result
refresh_credentials::impl::handle_response(api_response resp) {
    if (std::holds_alternative<iobuf>(resp)) {
        try {
            return parse_response(std::move(std::get<iobuf>(resp)));
        } catch (const std::exception& e) {
            // Parsing error: it may be a temporary issue with the API, or it
            // may be a permanent problem with our request.
            return api_response_parse_error{.reason = e.what()};
        }
    } else {
        vassert(
          std::holds_alternative<api_request_error>(resp),
          "unexpected response variant");
        return std::get<api_request_error>(resp);
    }
}

ss::future<> refresh_credentials::impl::sleep_until_expiry() const {
    if (_sleep_duration) {
        co_await ss::sleep_abortable(*_sleep_duration, _as);
    }
}

http::client refresh_credentials::impl::make_api_client() const {
    return http::client{
      net::base_transport::configuration{
        .server_addr = net::unresolved_address{_api_host, _api_port},
        .credentials = {},
        .disable_metrics = net::metrics_disabled::yes,
        .tls_sni_hostname = std::nullopt},
      _as};
}

refresh_credentials make_refresh_credentials(
  model::cloud_credentials_source cloud_credentials_source,
  ss::gate& gate,
  ss::abort_source& as,
  credentials_update_cb_t creds_update_cb,
  aws_region_name region,
  std::optional<net::unresolved_address> endpoint,
  retry_params retry_params) {
    switch (cloud_credentials_source) {
    case model::cloud_credentials_source::config_file:
        vlog(
          clrl_log.error,
          "invalid request to create refresh_credentials for static "
          "credentials");
        throw std::invalid_argument(fmt_with_ctx(
          fmt::format, "cannot generate refresh with static credentials"));
    case model::cloud_credentials_source::aws_instance_metadata:
        return make_refresh_credentials<aws_refresh_impl>(
          gate,
          as,
          std::move(creds_update_cb),
          std::move(region),
          std::move(endpoint),
          retry_params);
    case model::cloud_credentials_source::sts:
        return make_refresh_credentials<aws_sts_refresh_impl>(
          gate,
          as,
          std::move(creds_update_cb),
          std::move(region),
          std::move(endpoint),
          retry_params);
    case model::cloud_credentials_source::gcp_instance_metadata:
        return make_refresh_credentials<gcp_refresh_impl>(
          gate,
          as,
          std::move(creds_update_cb),
          std::move(region),
          std::move(endpoint),
          retry_params);
    }
}

std::ostream& operator<<(std::ostream& os, const refresh_credentials& rc) {
    return rc.print(os);
}

} // namespace cloud_roles
