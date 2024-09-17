/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/auth_refresh_bg_op.h"

#include "cloud_io/logger.h"
#include "ssx/future-util.h"

namespace cloud_io {

auth_refresh_bg_op::auth_refresh_bg_op(
  ss::gate& gate,
  ss::abort_source& as,
  cloud_storage_clients::client_configuration client_conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _gate(gate)
  , _as(as)
  , _client_conf(std::move(client_conf))
  , _cloud_credentials_source(cloud_credentials_source) {}

void auth_refresh_bg_op::maybe_start_auth_refresh_op(
  cloud_roles::credentials_update_cb_t credentials_update_cb) {
    if (ss::this_shard_id() == auth_refresh_shard_id) {
        do_start_auth_refresh_op(std::move(credentials_update_cb));
    }
}

cloud_storage_clients::client_configuration
auth_refresh_bg_op::get_client_config() const {
    return _client_conf;
}

void auth_refresh_bg_op::set_client_config(
  cloud_storage_clients::client_configuration conf) {
    _client_conf = std::move(conf);
}

void auth_refresh_bg_op::do_start_auth_refresh_op(
  cloud_roles::credentials_update_cb_t credentials_update_cb) {
    if (is_static_config()) {
        // If credentials are static IE not changing, we just need to set the
        // credential object once on all cores with static strings.
        vlog(
          log.info,
          "creating static credentials based on credentials source {}",
          _cloud_credentials_source);

        // Send the credentials to the client pool in a fiber
        ssx::spawn_with_gate(
          _gate,
          [creds = build_static_credentials(),
           fn = std::move(credentials_update_cb)] { return fn(creds); });
    } else {
        // Create an implementation of refresh_credentials based on the setting
        // cloud_credentials_source.
        try {
            auto region_name = ss::visit(
              _client_conf,
              [](const cloud_storage_clients::s3_configuration& cfg) {
                  // S3 needs a region name to compose requests, this extracts
                  // it from s3_configuration
                  return cloud_roles::aws_region_name{cfg.region};
              },
              [](const cloud_storage_clients::abs_configuration&) {
                  // Azure Blob Storage does not need a region name to compose
                  // the requests, so this value is defaulted since it's ignored
                  // downstream
                  return cloud_roles::aws_region_name{};
              });
            _refresh_credentials.emplace(cloud_roles::make_refresh_credentials(
              _cloud_credentials_source,
              _as,
              std::move(credentials_update_cb),
              region_name));

            vlog(
              log.info,
              "created credentials refresh implementation based on credentials "
              "source {}: {}",
              _cloud_credentials_source,
              *_refresh_credentials);
            _refresh_credentials->start();
        } catch (const std::exception& ex) {
            vlog(
              log.error,
              "failed to initialize cloud storage authentication system: {}",
              ex.what());
        }
    }
}

bool auth_refresh_bg_op::is_static_config() const {
    return _cloud_credentials_source
           == model::cloud_credentials_source::config_file;
}

cloud_roles::credentials auth_refresh_bg_op::build_static_credentials() const {
    return ss::visit(
      _client_conf,
      [](const cloud_storage_clients::s3_configuration& cfg)
        -> cloud_roles::credentials {
          return cloud_roles::aws_credentials{
            cfg.access_key.value(),
            cfg.secret_key.value(),
            std::nullopt,
            cfg.region};
      },
      [](const cloud_storage_clients::abs_configuration& cfg)
        -> cloud_roles::credentials {
          return cloud_roles::abs_credentials{
            cfg.storage_account_name, cfg.shared_key.value()};
      });
}

ss::future<> auth_refresh_bg_op::stop() {
    if (
      ss::this_shard_id() == auth_refresh_shard_id
      && _refresh_credentials.has_value()) {
        co_await _refresh_credentials.value().stop();
    }
}

} // namespace cloud_io
