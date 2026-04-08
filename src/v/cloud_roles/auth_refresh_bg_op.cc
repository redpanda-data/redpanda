/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_roles/auth_refresh_bg_op.h"

#include "cloud_roles/refresh_credentials.h"
#include "ssx/future-util.h"

#include <optional>

namespace cloud_roles {
static constexpr auto refresh_rate = std::chrono::seconds(10);

auth_refresh_bg_op::auth_refresh_bg_op(
  ss::logger& logger,
  ss::gate& gate,
  ss::abort_source& as,
  model::cloud_credentials_source cloud_credentials_source,
  credentials_source_config source_config)
  : _log(logger)
  , _gate(gate)
  , _as(as)
  , _cloud_credentials_source(cloud_credentials_source)
  , _source_config(std::move(source_config)) {}

void auth_refresh_bg_op::maybe_start_auth_refresh_op(
  cloud_roles::credentials_update_cb_t credentials_update_cb,
  ss::sstring metrics_tag) {
    if (ss::this_shard_id() == auth_refresh_shard_id) {
        do_start_auth_refresh_op(
          std::move(credentials_update_cb), std::move(metrics_tag));
    }
}

void auth_refresh_bg_op::do_start_auth_refresh_op(
  cloud_roles::credentials_update_cb_t credentials_update_cb,
  ss::sstring metrics_tag) {
    if (is_static_config()) {
        // If credentials are static IE not changing, we just need to set the
        // credential object once on all cores with static strings.
        vlog(_log.info, "creating static credentials");

        // Send the credentials to the client pool in a fiber
        ssx::spawn_with_gate(
          _gate,
          [creds = build_static_credentials(),
           fn = std::move(credentials_update_cb)] { return fn(creds); });
    } else {
        // Create an implementation of refresh_credentials based on the setting
        // cloud_credentials_source.
        try {
            cloud_roles::aws_service_name service_name;
            cloud_roles::aws_region_name region_name;

            ss::visit(
              _source_config,
              [](const cloud_roles::credentials&) {
                  throw std::runtime_error(
                    "non-static credentials source expected");
              },
              [&](
                const cloud_roles::auth_refresh_bg_op::s3_compat_config&
                  s3_cfg) {
                  service_name = s3_cfg.service;
                  region_name = s3_cfg.region;
              },
              [&](const cloud_roles::auth_refresh_bg_op::abs_config&) {});

            _refresh_credentials.emplace(
              cloud_roles::make_refresh_credentials(
                _cloud_credentials_source,
                _as,
                std::move(credentials_update_cb),
                service_name,
                region_name,
                std::nullopt,
                cloud_roles::default_retry_params,
                std::move(metrics_tag)));

            vlog(
              _log.info,
              "created credentials refresh implementation based on credentials "
              "source {}: {}",
              _cloud_credentials_source,
              *_refresh_credentials);
            _refresh_credentials->start();
        } catch (const std::exception& ex) {
            vlog(
              _log.error,
              "failed to initialize cloud storage authentication system: {}",
              ex.what());
        }
    }
}

bool auth_refresh_bg_op::is_static_config() const {
    return std::holds_alternative<cloud_roles::credentials>(_source_config);
}

void auth_refresh_bg_op::maybe_refresh_credentials() {
    _refresh_cnt++;
    auto refresh_allowed = [this]() {
        if (!_last_refresh_time.has_value()) {
            return true;
        }
        auto ts = _last_refresh_time.value();
        auto now = ss::lowres_clock::now();
        return ts < now ? now - ts > refresh_rate : false;
    }();
    if (refresh_allowed && _refresh_credentials.has_value()) {
        _refresh_credentials->refresh();
        _last_refresh_time = ss::lowres_clock::now();
    }
}

uint64_t auth_refresh_bg_op::token_refresh_count() const noexcept {
    return _refresh_cnt;
}

cloud_roles::credentials auth_refresh_bg_op::build_static_credentials() const {
    if (
      auto creds = std::get_if<cloud_roles::credentials>(&_source_config);
      creds) {
        return *creds;
    } else {
        throw std::runtime_error(
          "static credentials requested but not provided");
    }
}

ss::future<> auth_refresh_bg_op::stop() {
    if (
      ss::this_shard_id() == auth_refresh_shard_id
      && _refresh_credentials.has_value()) {
        co_await _refresh_credentials.value().stop();
    }
}

} // namespace cloud_roles
