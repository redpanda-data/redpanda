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
#include "random/generators.h"
#include "ssx/abort_source.h"
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

/// Picks two distinct random shards, excluding self. Used for "power of two
/// choices" load balancing.
///
/// Uses rejection-free sampling: instead of picking from [0,n) and retrying on
/// excluded values, we pick from a smaller range [0,n-k) and map to valid
/// values by skipping over excluded ones. This guarantees O(1) with exactly one
/// random draw per selection.
[[nodiscard]] std::pair<ss::shard_id, ss::shard_id> pick_two_random_shards() {
    using dist_t = std::uniform_int_distribution<ss::shard_id>;

    const ss::shard_id n = ss::smp::count;
    const ss::shard_id self = ss::this_shard_id();

    vassert(n > 1, "At least two shards are required");

    if (n == 2) {
        ss::shard_id other = (self == 0) ? 1 : 0;
        return {other, other};
    }

    auto& eng = random_generators::global().engine();

    // Pick cpu_a from [0, n), excluding self
    dist_t dist(0, n - 2);
    auto r = dist(eng);
    const ss::shard_id cpu_a = r < self ? r : r + 1;

    // Pick cpu_b from [0, n), excluding self and cpu_a
    dist.param(dist_t::param_type{0, n - 3});
    auto cpu_b = dist(eng);
    const auto [lo, hi] = std::minmax(self, cpu_a);
    if (cpu_b >= lo) {
        ++cpu_b;
    }
    if (cpu_b >= hi) {
        ++cpu_b;
    }

    return {cpu_a, cpu_b};
}
} // namespace

namespace cloud_storage_clients {

client_pool::client_pool(
  upstream_registry& registry,
  size_t size,
  client_configuration conf,
  client_pool_overdraft_policy policy)
  : _upstreams(registry)
  , _capacity(size)
  , _config(std::move(conf))
  , _probe(registry.probe())
  , _policy(policy) {}

ss::future<> client_pool::start(
  std::optional<std::reference_wrapper<stop_signal>> application_stop_signal) {
    ssx::spawn_with_gate(_gate, [this, application_stop_signal]() {
        // Eagerly attempt to start the default upstream and trigger stop on
        // any failure.
        return _upstreams.get(default_upstream_key)
          .then([this](upstream_registry::handle up) {
              _default_upstream.emplace(std::move(up));
              populate_client_pool(*_default_upstream);
          })
          .handle_exception([application_stop_signal](std::exception_ptr e) {
              try {
                  std::rethrow_exception(e);
              } catch (const upstream_self_configuration_error& ex) {
                  // Fallthrough to the logic below.
                  std::ignore = ex;
              } catch (...) {
                  vlog(
                    pool_log.warn,
                    "Failed to get upstream for client pool: {}",
                    e);

                  // Ignore other exceptions. We get here only when shutdown
                  // happens before start completes.
                  return ss::now();
              }

              if (ss::this_shard_id() == self_config_shard) {
                  vlog(
                    pool_log.error,
                    "Self configuration of the cloud storage client failed. "
                    "This indicates a misconfiguration of Redpanda. "
                    "Aborting start-up ...");

                  vassert(
                    application_stop_signal.has_value(),
                    "Application abort source not present in client pool");

                  crash_tracker::get_recorder().record_crash_exception(
                    std::make_exception_ptr(
                      std::runtime_error(
                        "Cloud storage client self-configuration failed. "
                        "Check your cloud storage credentials and "
                        "configuration.")));

                  application_stop_signal->get().signaled();
              }

              return ss::now();
          });
    });

    co_return;
}

ss::future<> client_pool::stop() {
    vlog(
      pool_log.info,
      "Stopping client pool: {} ({} connections leased)",
      _idle_clients.size(),
      _leased.size());

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

    for (auto& [_, entry] : _idle_clients) {
        stops.emplace_back(entry.ptr->stop());
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

    for (auto& it : _leased) {
        it.client->shutdown();
    }
    for (auto& [_, entry] : _idle_clients) {
        entry.ptr->shutdown();
    }

    vlog(pool_log.info, "Shut down of client pool complete");
}

bool client_pool::shutdown_initiated() { return _as.abort_requested(); }

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
  const bucket_name_parts& bucket,
  ss::abort_source& as,
  std::optional<ss::lowres_clock::time_point> deadline) {
    auto guard = _gate.hold();

    auto up_key = make_upstream_key(_config, bucket);

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
        if (
          std::optional<ssx::semaphore_units> u = ss::try_get_units(
            _pool_ready_barrier, 1);
          !u.has_value()) {
            const auto ready_deadline = std::min(
              deadline.value_or(ss::lowres_clock::time_point::max()),
              ss::lowres_clock::now() + pool_ready_timeout);
            auto timeout_as = ss::abort_on_expiry(ready_deadline);
            auto wait_as = ssx::composite_abort_source(
              as, timeout_as.abort_source());

            // Timeout exception will be thrown if the credentials are not
            // refreshed yet. The code in the 'remote' class handles this
            // exception. Most of the time this exception means that the
            // IAM-roles (or other source of credentials) is not configured
            // properly.
            try {
                u = co_await ss::get_units(
                  _pool_ready_barrier, 1, wait_as.as());
            } catch (const ss::timed_out_error&) {
                vlog(
                  pool_log.warn,
                  "Timed out waiting for client pool to be ready");
                throw;
            }
        }

        std::optional<upstream_registry::handle> dynamic_up_holder;
        upstream_registry::handle* up_ptr = nullptr;
        if (up_key == default_upstream_key) {
            up_ptr = &_default_upstream.value();
        } else {
            dynamic_up_holder.emplace(co_await _upstreams.get(up_key));
            up_ptr = &*dynamic_up_holder;
        }
        auto& up = *up_ptr;

        while (!deadline_reached() && !_gate.is_closed()
               && !_as.abort_requested()) {
            if (up_key != default_upstream_key) {
                // For now, we don't implement client re-use for non-default
                // upstreams. Just create a new client every time.
                // But we still need to respect capacity limits by consuming
                // a slot from the pool.
                if (_idle_clients.empty()) {
                    co_await ssx::with_timeout_abortable(
                      _cvar.wait(), deadline.value_or(model::no_timeout), as);
                    continue;
                }
                // Consume a slot from the pool.
                auto slot_client = pop_least_recently_used();
                slot_client->shutdown();
                ssx::spawn_with_gate(_bg_gate, [slot_client] {
                    return slot_client->stop().finally([slot_client] {});
                });
                client = up->make_client(_as);
                break;
            }

            if (client.has_value()) {
                if (!(*client)->is_valid()) {
                    vlog(
                      pool_log.debug, "Ignoring invalid client from the pool");

                    [this, &client, &up]() noexcept {
                        _idle_clients.erase(client->get());
                        (*client)->shutdown();
                        ssx::spawn_with_gate(_bg_gate, [c = *client] {
                            return c->stop().finally([c] {});
                        });
                        client.reset();
                        emplace_idle(up);
                    }();
                } else {
                    break;
                }
            }

            if (likely(!_idle_clients.empty())) {
                client = pop_most_recently_used();
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
                  "cvar triggered, pool size: {}",
                  _idle_clients.size());
            } else {
                // Try borrowing from peer shard.
                auto clients_in_use = [](client_pool& other) {
                    return ss::get_units(other._pool_ready_barrier, 1)
                      .then([&other](ssx::semaphore_units) {
                          return std::clamp(
                            other._capacity - other._idle_clients.size(),
                            0UL,
                            other._capacity);
                      });
                };
                // Use 2-random approach. Pick 2 random shards
                auto [sid1, sid2] = pick_two_random_shards();
                size_t cnt1 = _capacity;
                size_t cnt2 = _capacity;
                try {
                    cnt1 = co_await container().invoke_on(sid1, clients_in_use);
                    cnt2 = sid1 == sid2 ? cnt1
                                        : co_await container().invoke_on(
                                            sid2, clients_in_use);
                } catch (const ss::broken_named_semaphore&) {
                    // Remote shard is shutting down, treat as fully
                    // utilized so we skip borrowing.
                }
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
                    client = up->make_client(_as);
                } else {
                    vlog(pool_log.debug, "can't borrow connection, waiting");
                    // In-between failing to borrow from local pool and failing
                    // to borrow from a remote pool (co_await/async-operation),
                    // local pool may have gotten a client back. There is no
                    // need to wait in such case.
                    if (_idle_clients.empty()) {
                        co_await ssx::with_timeout_abortable(
                          _cvar.wait(),
                          deadline.value_or(model::no_timeout),
                          as);
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
                        up_key]() mutable {
          if (pool) {
              if (up_key != default_upstream_key) {
                  // For now, we don't implement client re-use for non-default
                  // upstreams. Just shutdown the client and return the slot
                  // to the pool.
                  pool->emplace_idle(pool->_default_upstream.value());
                  pool->_cvar.signal();
                  client->shutdown();
                  ssx::spawn_with_gate(pool->_bg_gate, [client] {
                      return client->stop().finally([client] {});
                  });
                  return;
              }

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
                      client = pool->replace_least_recently_used(client);
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
                  ssx::spawn_with_gate(
                    pool->_bg_gate, [pool, source_sid] noexcept {
                        return pool->container().invoke_on(
                          source_sid.value(),
                          [my_sid = ss::this_shard_id()](client_pool& other) {
                              if (other._as.abort_requested()) {
                                  // We are shutting down, ok to skip returning
                                  // the borrowed connection.
                                  return ss::now();
                              }
                              auto h = other._bg_gate.hold();
                              return ss::get_units(other._pool_ready_barrier, 1)
                                .then([&](ssx::semaphore_units) {
                                    other.return_one(
                                      other._default_upstream.value(), my_sid);
                                })
                                .finally([h = std::move(h)] {});
                          });
                    });
              } else {
                  pool->release_most_recently_used(client);
              }
          }
      }),
      std::move(measurement));

    _as.check();
    _leased.push_back(lease);

    co_return lease;
}

auto client_pool::acquire_with_timeout(
  const bucket_name_parts& bucket,
  ss::abort_source& as,
  ss::lowres_clock::duration timeout,
  std::optional<ss::sstring> ctx) -> ss::future<client_lease> {
    auto lease = co_await acquire(bucket, as);
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
    auto current = _capacity - std::clamp(_idle_clients.size(), 0UL, _capacity);
    auto normalized = static_cast<int>(
      100.0 * double(current) / static_cast<double>(_capacity));
    return normalized;
}

bool client_pool::borrow_one(unsigned other) noexcept {
    if (_idle_clients.empty()) {
        vlog(pool_log.debug, "declining borrow by {}", other);
        return false;
    }
    vlog(
      pool_log.debug,
      "approving borrow by {}, pool size {}/{}",
      other,
      _idle_clients.size(),
      _capacity);
    // TODO: do not use the bottommost (oldest) element. Find the one
    // with expired connection.
    auto c = pop_least_recently_used();
    update_usage_stats();
    c->shutdown();
    ssx::spawn_with_gate(_bg_gate, [c] { return c->stop().finally([c] {}); });
    return true;
}

void client_pool::return_one(
  upstream_registry::handle& up, unsigned other) noexcept {
    vlog(pool_log.debug, "shard {} returns a client", other);
    vassert(
      _idle_clients.size() < _capacity,
      "tried to return a borrowed client but the pool is full");
    emplace_idle(up);
    update_usage_stats();
    vlog(
      pool_log.debug,
      "creating new client, current usage is {}/{}",
      normalized_num_clients_in_use(),
      _capacity);
    _cvar.signal();
}

void client_pool::emplace_idle(upstream_registry::handle& up) noexcept {
    auto new_client = up->make_client(_as);
    const client* raw_ptr = new_client.get();
    auto [it, inserted] = _idle_clients.emplace(
      raw_ptr, idle_entry(std::move(new_client)));
    // Cold clients are at the front. Hot clients are at the back.
    _idle_clients_lru.push_front(it->second);
}

client_pool::client_ptr client_pool::pop_least_recently_used() noexcept {
    vassert(
      !_idle_clients_lru.empty(),
      "tried to pop from LRU idle list when it's empty");
    auto& entry = _idle_clients_lru.front();
    auto client = std::move(entry.ptr);
    // Automatically removes the entry from the intrusive LRU list.
    _idle_clients.erase(client.get());
    return client;
}

client_pool::client_ptr client_pool::pop_most_recently_used() noexcept {
    vassert(
      !_idle_clients_lru.empty(),
      "tried to pop from LRU idle list when it's empty");
    auto& entry = _idle_clients_lru.back();
    auto client = std::move(entry.ptr);
    // Automatically removes the entry from the intrusive LRU list.
    _idle_clients.erase(client.get());
    return client;
}

void client_pool::release_most_recently_used(client_ptr leased) noexcept {
    vlog(
      pool_log.debug,
      "releasing a client, pool size: {}, capacity: {}",
      _idle_clients.size(),
      _capacity);
    vassert(
      _idle_clients.size() < _capacity,
      "tried to release a client but the pool is at capacity");
    const client* raw_ptr = leased.get();
    auto [it, inserted] = _idle_clients.emplace(
      raw_ptr, idle_entry(std::move(leased)));
    vassert(
      inserted,
      "tried to release a client but the client is already in idle clients");
    // Cold clients are at the front. Hot clients are at the back.
    _idle_clients_lru.push_back(it->second);
    _cvar.signal();
}

client_pool::client_ptr
client_pool::replace_least_recently_used(client_ptr leased) noexcept {
    const client* raw_ptr = leased.get();
    auto [it, inserted] = _idle_clients.emplace(
      raw_ptr, idle_entry(std::move(leased)));
    vassert(
      inserted,
      "tried to replace LRU client but the client is already in idle clients");
    _idle_clients_lru.push_back(it->second);

    // Pop the oldest client
    auto& lru_entry = _idle_clients_lru.front();
    auto result = std::move(lru_entry.ptr);
    _idle_clients.erase(result.get());
    return result;
}

size_t client_pool::idle_count() const noexcept { return _idle_clients.size(); }

size_t client_pool::capacity() const noexcept { return _capacity; }

void client_pool::populate_client_pool(upstream_registry::handle& up) {
    vlog(pool_log.info, "Populating client pool with {} clients", _capacity);

    _idle_clients.reserve(_capacity);
    for (size_t i = 0; i < _capacity; i++) {
        emplace_idle(up);
    }

    // Be defensive in checking that we properly synchronized access to
    // `_idle_clients` and `_cvar`. Before populate_client_pool() is called, we
    // do not expect anyone to check the size of the pool or wait on the
    // condition variable.
    vassert(
      !_cvar.has_waiters(),
      "This is a bug: _cvar is not expected to have waiters at this point. "
      "Missing synchronization?");

    _pool_ready_barrier.signal(_pool_ready_barrier.max_counter());
}

uint64_t client_pool::token_refresh_count() const {
    return _default_upstream.value()->token_refresh_count();
}

} // namespace cloud_storage_clients
