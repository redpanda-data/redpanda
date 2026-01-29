/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/upstream.h"

#include "cloud_storage_clients/abs_client.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/s3_client.h"
#include "ssx/future-util.h"

namespace cloud_storage_clients {

namespace {
using namespace std::chrono_literals;

constexpr auto self_configure_attempts = 3;
constexpr auto self_configure_backoff = 1s;
} // namespace

upstream::upstream(
  client_configuration config,
  ss::shared_ptr<ss::tls::certificate_credentials> tls_credentials,
  ss::shared_ptr<client_probe> probe)
  : _config(std::move(config))
  , _transport_config(build_transport_configuration(_config, tls_credentials))
  , _probe(std::move(probe))
  , _credential_manager(
      *this, _config, ss::visit(_config, [](const common_configuration& c) {
          return c.cloud_credentials_source;
      })) {}

ss::future<> upstream::start() {
    vlog(pool_log.info, "Upstream starting {}", _config);

    std::exception_ptr e;
    try {
        co_await _credential_manager.start();
        vlog(pool_log.info, "Upstream credentials started {}", _config);

        if (ss::this_shard_id() == ss::shard_id{0}) {
            vlog(pool_log.info, "Starting client self-configuration...");
            co_await client_self_configure();
        } else {
            vlog(
              pool_log.info,
              "Upstream waiting on self config barrier {}",
              _config);
            auto u = co_await ss::get_units(_self_config_barrier, 1);
            u = {}; // release the unit
        }

        vlog(pool_log.info, "Upstream started {}", _config);
    } catch (...) {
        // Log as otherwise we're a bit blind.
        vlog(
          pool_log.warn,
          "Upstream failed to start {}: {}",
          _config,
          std::current_exception());

        // All peer-shards are waiting on _self_config_barrier, so we need to
        // break it to avoid deadlock.

        e = std::current_exception();
    }
    if (e) {
        // Other shards are waiting on the self-config barrier, break it so that
        // they can proceed to stop as well.
        co_await container().invoke_on_all(
          [](upstream& svc) { return svc._self_config_barrier.broken(); });
        std::rethrow_exception(e);
    }
}

ss::future<> upstream::stop() {
    _as.request_abort();
    _self_config_barrier.broken();
    _credentials_var.broken();

    co_await _gate.close();
    co_await _credential_manager.stop();

    _probe = nullptr;
}

void upstream::prepare_stop() {
    _as.request_abort();
    _self_config_barrier.broken();
    _credentials_var.broken();
}

upstream::client_ptr upstream::make_client() noexcept {
    return ss::visit(
      _config,
      [this](const s3_configuration& cfg) -> client_ptr {
          if (cfg.is_gcs) {
              return ss::make_shared<gcs_client>(
                weak_from_this(),
                cfg,
                _transport_config,
                _probe,
                _as,
                _apply_credentials);
          }
          return ss::make_shared<s3_client>(
            weak_from_this(),
            cfg,
            _transport_config,
            _probe,
            _as,
            _apply_credentials);
      },
      [this](const abs_configuration& cfg) -> client_ptr {
          return ss::make_shared<abs_client>(
            weak_from_this(),
            cfg,
            _transport_config,
            _probe,
            _as,
            _apply_credentials);
      });
}

void upstream::maybe_refresh_credentials() {
    if (ss::this_shard_id() == cloud_roles::auth_refresh_shard_id) {
        return _credential_manager.maybe_refresh_credentials();
    } else {
        return ssx::spawn_with_gate(_gate, [this] {
            return container().invoke_on(
              cloud_roles::auth_refresh_shard_id, [](upstream& upstream) {
                  upstream._credential_manager.maybe_refresh_credentials();
              });
        });
    }
}

uint64_t upstream::token_refresh_count() const noexcept {
    return _credential_manager.token_refresh_count();
}

void upstream::load_credentials(cloud_roles::credentials credentials) {
    if (unlikely(!_apply_credentials)) {
        _apply_credentials = ss::make_lw_shared(
          cloud_roles::make_credentials_applier(std::move(credentials)));
        _credentials_var.signal();
    } else {
        _apply_credentials->reset_creds(std::move(credentials));
    }
}

ss::future<> upstream::client_self_configure() {
    if (!_apply_credentials) {
        vlog(pool_log.trace, "Awaiting credentials ...");
        co_await wait_for_credentials();
    }

    std::optional<client_self_configuration_output> self_config_output;

    const bool requires_self_config = std::visit(
      [](const auto& cfg) -> bool { return cfg.requires_self_configuration; },
      _config);
    if (requires_self_config) {
        vlog(
          pool_log.info,
          "Client requires self configuration step. Proceeding ...");

        auto client = make_client();
        auto shutdown_on_abort = _as.subscribe(
          [client]() noexcept { client->shutdown(); });
        auto result = co_await do_client_self_configure(client);
        co_await client->stop();

        if (!result) {
            throw upstream_self_configuration_error(
              "Client self configuration failed");
        }

        self_config_output = *result;
        vlog(
          pool_log.info,
          "Client self configuration completed with result {}",
          *self_config_output);
    }

    co_await container().invoke_on_all([self_config_output](upstream& svc) {
        return svc.accept_self_configure_result(self_config_output)
          .handle_exception_type([](const ss::gate_closed_exception&) {})
          .handle_exception_type([](const ss::broken_condition_variable&) {})
          .handle_exception([](std::exception_ptr e) {
              vlog(
                pool_log.error,
                "Unexpected exception thrown while accepting self "
                "configuration: {}",
                e);
          });
    });
}

ss::future<
  std::optional<cloud_storage_clients::client_self_configuration_output>>
upstream::do_client_self_configure(client_ptr client) {
    try {
        for (auto attempt = 1; attempt <= self_configure_attempts; ++attempt) {
            auto result = co_await client->self_configure();
            if (result) {
                co_return result.value();
            }

            if (result.error() == cloud_storage_clients::error_outcome::retry) {
                vlog(
                  pool_log.warn,
                  "Self configuration attempt {}/{} failed with retryable "
                  "error. "
                  "Will retry in {}s.",
                  attempt,
                  self_configure_attempts,
                  self_configure_backoff.count());
                co_await ss::sleep_abortable(self_configure_backoff, _as);
            } else {
                break;
            }
        }
    } catch (...) {
        vlog(
          pool_log.warn,
          "Exception throw during client self configuration: {}",
          std::current_exception());
    }

    co_return std::nullopt;
}

ss::future<> upstream::accept_self_configure_result(
  std::optional<client_self_configuration_output> result) {
    if (!_apply_credentials) {
        vlog(pool_log.trace, "Awaiting credentials ...");
        co_await wait_for_credentials();
    }

    if (_gate.is_closed() || _as.abort_requested()) {
        throw ss::gate_closed_exception();
    }

    if (result) {
        cloud_storage_clients::apply_self_configuration_result(
          _config, *result);
    }

    // We signal the waiters only after the client pool is initialized, so
    // that any upload operations waiting are ready to proceed.
    _self_config_barrier.signal(_self_config_barrier.max_counter());
}

ss::future<> upstream::wait_for_credentials() {
    co_await _credentials_var.wait([this]() {
        return _gate.is_closed() || _as.abort_requested()
               || bool{_apply_credentials};
    });

    if (_gate.is_closed() || _as.abort_requested()) {
        throw ss::gate_closed_exception();
    }
    co_return;
}

} // namespace cloud_storage_clients
