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

#include "cloud_storage_clients/logger.h"
#include "crash_tracker/recorder.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/smp.hh>
#include <seastar/core/timed_out_error.hh>

#include <algorithm>
#include <chrono>
#include <optional>
#include <random>
#include <stdexcept>
#include <utility>

using namespace std::chrono_literals;

namespace {

constexpr auto pool_ready_timeout = 15s;
} // namespace

namespace cloud_storage_clients {

client_pool::client_pool(
  upstream_registry& upstream_registry,
  size_t size,
  client_configuration conf,
  client_pool_overdraft_policy policy)
  : _capacity(size)
  , _config(std::move(conf))
  , _probe(std::visit([](auto&& p) { return p.make_probe(); }, _config))
  , _policy(policy)
  , _upstream_registry(upstream_registry) {}

ss::future<> client_pool::start(
  std::optional<std::reference_wrapper<stop_signal>> application_stop_signal) {
    ssx::spawn_with_gate(_gate, [this, application_stop_signal]() {
        return _upstream_registry.get(default_upstream_key)
          .then([this](const upstream_registry::handle&) {
              vlog(
                pool_log.info,
                "Starting client pool with capacity {} clients on shard {}",
                _capacity,
                ss::this_shard_id());
              // Signal that self-configuration is complete.
              _pool_ready_barrier.signal(_pool_ready_barrier.max_counter());
          })
          .handle_exception([application_stop_signal](
                              const std::exception_ptr&) {
              vassert(
                application_stop_signal.has_value() || ss::this_shard_id() != 0,
                "Application abort source not present in client pool");

              crash_tracker::get_recorder().record_crash_exception(
                std::make_exception_ptr(
                  std::runtime_error(
                    "Cloud storage client self-configuration failed. Check "
                    "your cloud storage credentials and configuration.")));

              if (application_stop_signal.has_value()) {
                  vlog(
                    pool_log.trace,
                    "Signaling application stop due to failure");
                  application_stop_signal->get().signaled();
              }
          });
    });
    co_return;
}

ss::future<> client_pool::stop() {
    vlog(pool_log.info, "Stopping client pool: {}", _idle_clients.size());

    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    _cvar.broken();
    _pool_ready_barrier.broken();
    // Wait for all background operations to complete.
    co_await _bg_gate.close();
    // Wait until all leased objects are returned
    co_await _gate.close();

    std::vector<ss::future<>> stops;
    stops.reserve(_idle_clients.size());

    for (auto& it : _idle_clients) {
        stops.emplace_back(it.second.ptr->stop());
    }

    co_await ss::when_all_succeed(stops.begin(), stops.end());

    vlog(pool_log.info, "Stopped client pool");
    _probe = nullptr;
}

void client_pool::shutdown_connections() {
    vlog(
      pool_log.info,
      "Shutting down client pool: {} ({} connections leased)",
      _idle_clients.size(),
      _leased.size());

    _as.request_abort();
    _cvar.broken();
    _pool_ready_barrier.broken();

    // TODO: ask upstream to "shutdown connections" too.

    for (auto& it : _leased) {
        it.client->shutdown();
    }
    for (auto& it : _idle_clients) {
        it.second.ptr->shutdown();
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
ss::future<client_pool::client_lease> client_pool::acquire(
  const bucket_params& bucket,
  ss::abort_source& as,
  std::optional<ss::lowres_clock::time_point> deadline) {
    auto guard = _gate.hold();

    const upstream_key key = create_upstream_key(_config, bucket);
    auto h = co_await _upstream_registry.get(key);

    auto lru_idle_list_per_upstream_count = [this](const upstream_key& k) {
        auto it = _lru_idle_list_per_upstream.find(k);
        if (it == _lru_idle_list_per_upstream.end()) {
            return 0UL;
        }
        return it->second.size();
    };

    vlog(
      pool_log.debug,
      "got upstream handle for key {}, upstream idle clients {}, total idle "
      "{}, capacity {}",
      key,
      lru_idle_list_per_upstream_count(key),
      _idle_clients.size(),

      _capacity);

    std::optional<unsigned int> source_sid;
    std::optional<client_ptr> client;

    auto deadline_reached = [&deadline] {
        return deadline.has_value()
               && ss::lowres_clock::now() >= deadline.value();
    };

    try {
        // If credentials have not yet been acquired, wait for them. It is
        // possible that credentials are not initialized right after remote
        // starts, and we have not had a response from the credentials API yet,
        // but we have scheduled an upload. This wait ensures that when we call
        // the storage API we have a set of valid credentials.
        if (std::optional<ssx::semaphore_units> u = ss::try_get_units(
              _pool_ready_barrier, 1);
            !u.has_value()) {
            // Timeout exception will be thrown if the credentials are not
            // refreshed yet. The code in the 'remote' class handles this
            // exception. Most of the time this exception means that the
            // IAM-roles (or other source of credentials) is not configured
            // properly.
            try {
                u = co_await ss::get_units(
                  _pool_ready_barrier, 1, pool_ready_timeout);
            } catch (const ss::timed_out_error&) {
                vlog(
                  pool_log.error,
                  "Failed to acquire credentials within timeout");
                throw;
            }
        }

        while (!client.has_value() && !deadline_reached() && !_gate.is_closed()
               && !_as.abort_requested()) {
            fmt::print(
              "own leased: {}, idle: {}\n",
              _num_own_leased,
              _idle_clients.size());

            auto up_list_it = _lru_idle_list_per_upstream.find(key);
            if (up_list_it == _lru_idle_list_per_upstream.end()) {
                up_list_it = _lru_idle_list_per_upstream
                               .emplace(key, upstream_list{})
                               .first;
            }

            if (likely(!up_list_it->second.empty())) {
                // Return an idle connection if available.
                client = up_list_it->second.back().ptr;
                ++_num_own_leased;
                // Will auto unlink from LRU lists.
                _idle_clients.erase(client->get());

                fmt::print("reusing idle client\n");
            } else if (_num_own_leased + _lru_idle_list.size() < _capacity) {
                fmt::print("below capacity -> creating client \n");
                // Create a new connection if we are under capacity.
                ++_num_own_leased;
                client = h->make_client();
            } else if (!_lru_idle_list.empty()) {
                // Drop the oldest client regardless of upstream.
                fmt::print("replacing oldest client\n");

                // Auto unlinks from LRU lists.
                _idle_clients.erase(_lru_idle_list.front().ptr.get());

                // Give the new connection to the caller.
                client = h->make_client();
                ++_num_own_leased;

            } else if (
              ss::smp::count == 1
              || _policy == client_pool_overdraft_policy::wait_if_empty
              || _leased.size() >= _capacity * 2) {
                // If borrowing is disabled or this shard borrowed '_capacity'
                // client connections then wait util one of the clients is
                // freed.
                co_await ssx::with_timeout_abortable(
                  _cvar.wait(), deadline.value_or(model::no_timeout), as);

                vlog(
                  pool_log.debug,
                  "cvar triggered, idle size: {}, leased size: {}",
                  _idle_clients.size(),
                  _leased.size());
            } else {
                // Try borrowing from peer shard.
                auto clients_in_use = [](client_pool& other) {
                    return other._num_own_leased;
                };
                // Use 2-random approach. Pick 2 random shards
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
                    vlog(pool_log.debug, "successfully borrowed from {}", sid);
                    if (_probe) {
                        _probe->register_borrow();
                    }
                    source_sid = sid;
                    client = h->make_client();
                } else {
                    vlog(pool_log.debug, "can't borrow connection, waiting");
                    // In-between failing to borrow from local pool and failing
                    // to borrow from a remote pool (co_await/async-operation),
                    // local pool may have gotten a client back. There is no
                    // need to wait in such case.
                    if (_idle_clients.empty() || _num_own_leased >= _capacity) {
                        co_await ssx::with_timeout_abortable(
                          _cvar.wait(), model::no_timeout, as);
                        vlog(
                          pool_log.debug,
                          "cvar triggered, pool size: {}",
                          _idle_clients.size());
                    }
                }
            }
        }
    } catch (const ss::broken_condition_variable&) {
    } catch (const ss::broken_named_semaphore&) {
        // this is thrown at shutdown_connections/stop if we are waiting on
        // _self_config_barrier
    }
    if (_gate.is_closed() || _as.abort_requested()) {
        throw ss::gate_closed_exception();
    } else if (!client.has_value() && deadline_reached()) {
        throw ss::timed_out_error();
    }
    vassert(client.has_value(), "'acquire' invariant is broken");

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
      client.value(),
      as,
      ss::make_deleter([pool = weak_from_this(),
                        client = client.value(),
                        g = std::move(guard),
                        source_sid,
                        key]() mutable {
          if (pool) {
              if (source_sid.has_value()) {
                  // If all clients from the local pool are in-use we will
                  // shutdown the borrowed one and return the "accounting unit"
                  // to the source shard.
                  // Otherwise, we replace the oldest client in the
                  // pool to improve connection reuse.
                  if (!pool->_idle_clients.empty()) {
                      vlog(
                        pool_log.debug,
                        "disposing the oldest client connection and "
                        "replacing it with the borrowed one");
                      auto [it, inserted] = pool->_idle_clients.emplace(
                        client.get(), client_wrapper{std::move(client)});
                      vassert(
                        inserted,
                        "borrowed client is already in the idle list");
                      pool->_lru_idle_list.push_back(it->second);
                      client = pool->_lru_idle_list.front().ptr;
                      // Auto unlink from LRU lists.
                      pool->_idle_clients.erase(client.get());
                  } else {
                      vlog(
                        pool_log.debug,
                        "disposing the borrowed client connection");
                  }

                  client->shutdown();
                  ssx::spawn_with_gate(pool->_bg_gate, [client] {
                      return client->stop().finally([client] {});
                  });
                  // In the background return the client to the connection pool
                  // of the source shard. The lifetime is guaranteed by the gate
                  // guard.
                  ssx::spawn_with_gate(pool->_bg_gate, [&pool, source_sid] {
                      return pool->container().invoke_on(
                        source_sid.value(),
                        [my_sid = ss::this_shard_id()](client_pool& other) {
                            other.return_one(my_sid);
                        });
                  });
              } else {
                  pool->release(client, key);
              }
          }
      }),
      std::move(measurement));
    _leased.push_back(lease);

    co_return lease;
}

auto client_pool::acquire_with_timeout(
  const bucket_params& key,
  ss::abort_source& as,
  ss::lowres_clock::duration timeout,
  std::optional<ss::sstring> ctx) -> ss::future<client_lease> {
    auto lease = co_await acquire(key, as);
    if (timeout < ss::lowres_clock::duration::max()) {
        // take a copy of the shared_ptr held by the lease to avoid racing with
        // client_pool teardown
        lease._wd = std::make_unique<ssx::watchdog>(
          timeout,
          [probe = _probe,
           client = lease.client,
           timeout,
           ctx = std::move(ctx)]() mutable {
              if (ctx.has_value()) {
                  vlog(
                    pool_log.warn,
                    "{} - Lease expired after {}ms. Shutting down client...",
                    ctx.value(),
                    timeout / 1ms);
              } else {
                  vlog(
                    pool_log.warn,
                    "Lease expired after {}ms. Shutting down client...",
                    timeout / 1ms);
              }
              if (probe) {
                  probe->register_timeout();
              }
              if (client) {
                  client->shutdown();
              }
          });
    }
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
    auto normalized = static_cast<int>(
      100.0 * double(_num_own_leased) / static_cast<double>(_capacity));
    return normalized;
}

bool client_pool::borrow_one(unsigned other) {
    if (_num_own_leased >= _capacity) {
        vlog(pool_log.debug, "declining borrow by {}; all leased", other);
        return false;
    }
    vlog(
      pool_log.debug,
      "approving borrow by {}, pool size {}/{}, owned leased: {}",
      other,
      _idle_clients.size(),
      _capacity,
      _num_own_leased);

    if (_idle_clients.size() + _num_own_leased < _capacity) {
        // Virtual borrow if we are not at capacity yet.
        ++_num_own_leased;
        update_usage_stats();
        return true;
    }

    // TODO: do not use the bottommost (oldest) element. Find the one
    // with expired connection.
    auto c = _lru_idle_list.front().ptr;
    // Will auto unlink from LRU list.
    _idle_clients.erase(c.get());
    ++_num_own_leased;
    update_usage_stats();
    c->shutdown();
    ssx::spawn_with_gate(_bg_gate, [c] { return c->stop().finally([c] {}); });
    return true;
}

void client_pool::return_one(unsigned other) {
    vlog(pool_log.debug, "shard {} returns a client", other);
    vassert(
      _num_own_leased > 0,
      "invariant broken: trying to return a borrowed client but none are "
      "leased");
    --_num_own_leased;
    update_usage_stats();
    vlog(
      pool_log.debug,
      "creating new client, current usage is {}/{}",
      normalized_num_clients_in_use(),
      _capacity);
    _cvar.signal();
}

size_t client_pool::idle_count() const noexcept { return _idle_clients.size(); }

size_t client_pool::capacity() const noexcept { return _capacity; }

void client_pool::release(client_ptr leased, upstream_key key) {
    vlog(
      pool_log.debug,
      "releasing a client, pool size: {}, capacity: {}",
      _idle_clients.size(),
      _capacity);
    vassert(
      _idle_clients.size() < _capacity,
      "tried to release a client but the pool is at capacity");
    vassert(
      _num_own_leased > 0,
      "invariant broken: trying to release a client when none are leased");
    --_num_own_leased;

    auto [it, inserted] = _idle_clients.emplace(
      leased.get(), client_wrapper{std::move(leased)});
    vassert(inserted, "client being released is already in the idle list");

    _lru_idle_list.push_back(it->second);
    auto up_list_it = _lru_idle_list_per_upstream.find(key);
    if (up_list_it == _lru_idle_list_per_upstream.end()) {
        up_list_it
          = _lru_idle_list_per_upstream.emplace(key, upstream_list{}).first;
    }
    up_list_it->second.push_back(it->second);

    _cvar.signal();
}

ss::future<uint64_t> client_pool::token_refresh_count() const {
    return _upstream_registry
      .get(default_upstream_key)

      .then(
        [](upstream_registry::handle h) { return (*h).token_refresh_count(); });
}

} // namespace cloud_storage_clients
