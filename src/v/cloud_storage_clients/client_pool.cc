/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/client_pool.h"

#include "cloud_storage_clients/abs_client.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/s3_client.h"
#include "ssx/future-util.h"

#include <seastar/core/smp.hh>

#include <algorithm>
#include <random>

namespace {
constexpr auto self_configure_attempts = 3;
constexpr auto self_configure_backoff = 1s;
} // namespace

namespace cloud_storage_clients {

client_pool::client_pool(
  size_t size,
  client_configuration conf,
  client_pool_overdraft_policy policy,
  std::optional<std::reference_wrapper<stop_signal>> application_stop_signal)
  : _capacity(size)
  , _config(std::move(conf))
  , _probe(std::visit([](auto&& p) { return p._probe; }, _config))
  , _policy(policy) {
    if (ss::this_shard_id() == self_config_shard) {
        ssx::spawn_with_gate(
          _gate, [this, app_stop_signal = application_stop_signal]() {
              return client_self_configure(app_stop_signal);
          });
    }
}

ss::future<> client_pool::client_self_configure(
  std::optional<std::reference_wrapper<stop_signal>> application_stop_signal) {
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
        auto result = co_await do_client_self_configure(client);
        co_await client->stop();

        if (!result) {
            vlog(
              pool_log.error,
              "Self configuration of the cloud storage client failed. "
              "This indicates a misconfiguration of Redpanda. "
              "Aborting start-up ...");

            vassert(
              application_stop_signal.has_value(),
              "Application abort source not present in client pool");

            application_stop_signal->get().signaled();

            // Return in order to drop _gate which allows stop() to proceed.
            co_return;
        }

        self_config_output = *result;
        vlog(
          pool_log.info,
          "Client self configuration completed with result {} ",
          *self_config_output);
    }

    co_await container().invoke_on_all([self_config_output](client_pool& svc) {
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
client_pool::do_client_self_configure(http_client_ptr client) {
    try {
        for (auto attempt = 1; attempt <= self_configure_attempts; ++attempt) {
            auto result = co_await client->self_configure();
            if (result) {
                co_return result.value();
            }

            if (result.error() == cloud_storage_clients::error_outcome::retry) {
                vlog(
                  pool_log.error,
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

ss::future<> client_pool::accept_self_configure_result(
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

    populate_client_pool();

    // We signal the waiters only after the client pool is initialized, so
    // that any upload operations waiting are ready to proceed.
    _self_config_barrier.signal(_self_config_barrier.max_counter());
}

ss::future<> client_pool::stop() {
    vlog(pool_log.info, "Stopping client pool: {}", _pool.size());

    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    _cvar.broken();
    _self_config_barrier.broken();
    _credentials_var.broken();
    // Wait until all leased objects are returned
    co_await _gate.close();

    for (auto& it : _pool) {
        co_await it->stop();
    }

    vlog(pool_log.info, "Stopped client pool");
    _probe = nullptr;
}

void client_pool::shutdown_connections() {
    vlog(pool_log.info, "Shutting down client pool: {}", _pool.size());

    _as.request_abort();
    _cvar.broken();
    _self_config_barrier.broken();
    _credentials_var.broken();

    for (auto& it : _leased) {
        it.client->shutdown();
    }
    for (auto& it : _pool) {
        it->shutdown();
    }

    vlog(pool_log.info, "Shut down of client pool complete");
}

bool client_pool::shutdown_initiated() { return _as.abort_requested(); }

std::tuple<unsigned int, unsigned int> pick_two_random_shards() {
    static thread_local std::vector<unsigned> shards = [] {
        std::vector<unsigned> res;
        for (auto i = 0UL; i < ss::smp::count; i++) {
            if (i != ss::this_shard_id()) {
                res.push_back(i);
            }
        }
        return res;
    }();
    vassert(ss::smp::count > 1, "At least two shards are required");
    if (shards.size() == 1) {
        return std::tie(shards.at(0), shards.at(0));
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(shards.begin(), shards.end(), gen);
    return std::tie(shards.at(0), shards.at(1));
}

/// \brief Acquire http client from the pool.
///
/// as: An abort source which must outlive the lease, that will
///     be used to shutdown the client's connections when it fires.
///
/// \note it's guaranteed that the client can only be acquired once
///       before it gets released (release happens implicitly, when
///       the lifetime of the pointer ends).
/// \return client pointer (via future that can wait if all clients
///         are in use)
ss::future<client_pool::client_lease>
client_pool::acquire(ss::abort_source& as) {
    gate_guard guard(_gate);
    std::optional<unsigned int> source_sid;
    try {
        // If credentials have not yet been acquired, wait for them. It is
        // possible that credentials are not initialized right after remote
        // starts, and we have not had a response from the credentials API yet,
        // but we have scheduled an upload. This wait ensures that when we call
        // the storage API we have a set of valid credentials.
        if (std::optional<ssx::semaphore_units> u = ss::try_get_units(
              _self_config_barrier, 1);
            !u.has_value()) {
            u = co_await ss::get_units(_self_config_barrier, 1);
        }

        while (unlikely(
          _pool.empty() && !_gate.is_closed() && !_as.abort_requested())) {
            if (
              ss::smp::count == 1
              || _policy == client_pool_overdraft_policy::wait_if_empty
              || _leased.size() >= _capacity * 2) {
                // If borrowing is disabled or this shard borrowed '_capacity'
                // client connections then wait util one of the clients is
                // freed.
                co_await _cvar.wait();
                vlog(
                  pool_log.debug,
                  "cvar triggered, pool size: {}",
                  _pool.size());
            } else {
                auto clients_in_use = [](client_pool& other) {
                    return std::clamp(
                      other._capacity - other._pool.size(),
                      0UL,
                      other._capacity);
                };
                // Borrow from random shard. Use 2-random approach. Pick 2
                // random shards
                auto [sid1, sid2] = pick_two_random_shards();
                auto cnt1 = co_await container().invoke_on(
                  sid1, clients_in_use);
                // sid1 == sid2 if we have only two shards
                auto cnt2 = sid1 == sid2 ? cnt1
                                         : co_await container().invoke_on(
                                           sid2, clients_in_use);
                auto [sid, cnt] = cnt1 < cnt2 ? std::tie(sid1, cnt1)
                                              : std::tie(sid2, cnt2);
                vlog(
                  pool_log.debug,
                  "Going to borrow from {} which has {} clients in use out of "
                  "{}",
                  sid,
                  cnt,
                  _capacity);
                bool success = false;
                if (cnt < _capacity) {
                    success = co_await container().invoke_on(
                      sid, [my_sid = ss::this_shard_id()](client_pool& other) {
                          return other.borrow_one(my_sid);
                      });
                }
                // Depending on the result either wait or create new connection
                if (success) {
                    vlog(pool_log.debug, "successfuly borrowed from {}", sid);
                    if (_probe) {
                        _probe->register_borrow();
                    }
                    source_sid = sid;
                    _pool.emplace_back(make_client());
                } else {
                    vlog(pool_log.debug, "can't borrow connection, waiting");
                    co_await _cvar.wait();
                    vlog(
                      pool_log.debug,
                      "cvar triggered, pool size: {}",
                      _pool.size());
                }
            }
        }
    } catch (const ss::broken_condition_variable&) {
    }
    if (_gate.is_closed() || _as.abort_requested()) {
        throw ss::gate_closed_exception();
    }
    vassert(!_pool.empty(), "'acquire' invariant is broken");
    auto client = _pool.back();
    _pool.pop_back();

    update_usage_stats();
    vlog(
      pool_log.debug,
      "client lease is acquired, own usage stat: {}, is-borrowed: {}",
      normalized_num_clients_in_use(),
      source_sid.has_value());

    std::unique_ptr<client_probe::hist_t::measurement> measurement;
    if (_probe) {
        measurement = _probe->register_lease_duration();
    }

    client_lease lease(
      client,
      as,
      ss::make_deleter([pool = weak_from_this(),
                        client,
                        g = std::move(guard),
                        source_sid]() mutable {
          if (pool) {
              if (source_sid.has_value()) {
                  vlog(
                    pool_log.debug, "disposing the borrowed client connection");
                  // Since the client was borrowed we can't just add it back to
                  // the pool. This will lead to a situation when the connection
                  // simultaneously exists on two different shards.
                  client->shutdown();
                  ssx::spawn_with_gate(pool->_gate, [client] {
                      return client->stop().finally([client] {});
                  });
                  // In the background return the client to the connection pool
                  // of the source shard. The lifetime is guaranteed by the gate
                  // guard.
                  ssx::spawn_with_gate(pool->_gate, [&pool, source_sid] {
                      return pool->container().invoke_on(
                        source_sid.value(),
                        [my_sid = ss::this_shard_id()](client_pool& other) {
                            other.return_one(my_sid);
                        });
                  });
              } else {
                  pool->release(client);
              }
          }
      }),
      std::move(measurement));
    _leased.push_back(lease);

    co_return lease;
}

void client_pool::update_usage_stats() {
    if (_probe) {
        _probe->register_utilization(normalized_num_clients_in_use());
    }
}

size_t client_pool::normalized_num_clients_in_use() const {
    // Here we won't be showing that some clients are available if previously
    // the pool was depleted. This is needed to prevent borrowing from
    // overloaded shards.
    auto current = _capacity - std::clamp(_pool.size(), 0UL, _capacity);
    auto normalized = static_cast<int>(
      100.0 * double(current) / static_cast<double>(_capacity));
    return normalized;
}

bool client_pool::borrow_one(unsigned other) {
    if (_pool.empty()) {
        vlog(pool_log.debug, "declining borrow by {}", other);
        return false;
    }
    vlog(
      pool_log.debug,
      "approving borrow by {}, pool size {}/{}",
      other,
      _pool.size(),
      _capacity);
    // TODO: do not use the topmost element. Find the one
    // with expired connection.
    auto c = _pool.back();
    _pool.pop_back();
    update_usage_stats();
    c->shutdown();
    ssx::spawn_with_gate(_gate, [c] { return c->stop().finally([c] {}); });
    return true;
}

void client_pool::return_one(unsigned other) {
    vlog(pool_log.debug, "shard {} returns a client", other);
    if (_pool.size() + _leased.size() < _capacity) {
        // The _pool has fewer elements than it should have because it was
        // borrowed from previously.
        _pool.emplace_back(make_client());
        update_usage_stats();
        vlog(
          pool_log.debug,
          "creating new client, current usage is {}/{}",
          normalized_num_clients_in_use(),
          _capacity);
        _cvar.signal();
    }
}

size_t client_pool::size() const noexcept { return _pool.size(); }

size_t client_pool::max_size() const noexcept { return _capacity; }

void client_pool::populate_client_pool() {
    for (size_t i = 0; i < _capacity; i++) {
        _pool.emplace_back(make_client());
    }

    _cvar.signal();
}

client_pool::http_client_ptr client_pool::make_client() const {
    return std::visit(
      [this](const auto& cfg) -> http_client_ptr {
          using cfg_type = std::decay_t<decltype(cfg)>;
          if constexpr (std::is_same_v<s3_configuration, cfg_type>) {
              return ss::make_shared<s3_client>(cfg, _as, _apply_credentials);
          } else if constexpr (std::is_same_v<abs_configuration, cfg_type>) {
              return ss::make_shared<abs_client>(cfg, _as, _apply_credentials);
          } else {
              static_assert(always_false_v<cfg_type>, "Unknown client type");
          }
      },
      _config);
}

void client_pool::release(http_client_ptr leased) {
    vlog(
      pool_log.debug,
      "releasing a client, pool size: {}, capacity: {}",
      _pool.size(),
      _capacity);
    if (_pool.size() < _capacity) {
        _pool.emplace_back(std::move(leased));
    }
    _cvar.signal();
}

void client_pool::load_credentials(cloud_roles::credentials credentials) {
    if (unlikely(!_apply_credentials)) {
        _apply_credentials = ss::make_lw_shared(
          cloud_roles::make_credentials_applier(std::move(credentials)));
        _credentials_var.signal();
    } else {
        _apply_credentials->reset_creds(std::move(credentials));
    }
}

ss::future<> client_pool::wait_for_credentials() {
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
