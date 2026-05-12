/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/credential_manager.h"

#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/upstream.h"

namespace cloud_storage_clients {

credential_manager::credential_manager(
  upstream& upstream,
  cloud_storage_clients::client_configuration conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _upstream(upstream)
  , _client_conf(std::move(conf))
  , _auth_refresh_bg_op{
      pool_log,
      _gate,
      _as,
      cloud_credentials_source,
      cloud_storage_clients::build_refresh_credentials_source(
        _client_conf,
        cloud_credentials_source,
        config::shard_local_cfg().cloud_storage_credentials_host())}
  , _azure_shared_key_binding(
      config::shard_local_cfg().cloud_storage_azure_shared_key.bind()) {}

ss::future<> credential_manager::start() {
    // If the credentials source is from config file, bypass the background
    // op to refresh credentials periodically, and load pool with static
    // credentials right now.
    if (_auth_refresh_bg_op.is_static_config()) {
        _upstream.load_credentials(
          _auth_refresh_bg_op.build_static_credentials());
    } else {
        const ss::sstring metrics_tag = _upstream.key() == default_upstream_key
                                          ? ""
                                          : ssx::sformat("{}", _upstream.key());

        // Launch background operation to fetch credentials on
        // auth_refresh_shard_id, and copy them to other shards. We do not wait
        // for this operation here, the wait is done in client_pool::acquire to
        // avoid delaying application startup.
        _auth_refresh_bg_op.maybe_start_auth_refresh_op(
          [this](auto credentials) {
              return _upstream.container().invoke_on_all(
                [c = std::move(credentials)](upstream& svc) {
                    svc.load_credentials(std::move(c));
                });
          },
          metrics_tag);
    }

    _azure_shared_key_binding.watch([this] {
        if (!std::holds_alternative<cloud_storage_clients::abs_configuration>(
              _client_conf)) {
            vlog(
              pool_log.warn,
              "Attempt to set cloud_storage_azure_shared_key for cluster using "
              "S3 detected");
            return;
        }

        vlog(
          pool_log.info,
          "cloud_storage_azure_shared_key was updated. Refreshing "
          "credentials.");

        auto new_shared_key = _azure_shared_key_binding();
        if (!new_shared_key) {
            vlog(
              pool_log.info,
              "cloud_storage_azure_shared_key was unset. Will continue "
              "using the previous value until restart.");

            return;
        }

        auto& abs_config = std::get<cloud_storage_clients::abs_configuration>(
          _client_conf);
        abs_config.shared_key = cloud_roles::private_key_str{*new_shared_key};

        _auth_refresh_bg_op.set_source_config(
          cloud_storage_clients::build_refresh_credentials_source(
            _client_conf, model::cloud_credentials_source::config_file));

        _upstream.load_credentials(
          _auth_refresh_bg_op.build_static_credentials());
    });

    co_return;
}

ss::future<> credential_manager::stop() {
    _as.request_abort();
    co_await _gate.close();
    co_await _auth_refresh_bg_op.stop();
}

void credential_manager::maybe_refresh_credentials() {
    _auth_refresh_bg_op.maybe_refresh_credentials();
}

uint64_t credential_manager::token_refresh_count() const noexcept {
    return _auth_refresh_bg_op.token_refresh_count();
}

}; // namespace cloud_storage_clients
