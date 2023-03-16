#include "cloud_storage_clients/client_pool.h"

#include "cloud_storage_clients/abs_client.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/s3_client.h"
#include "ssx/future-util.h"

#include <seastar/core/smp.hh>

namespace cloud_storage_clients {

client_pool::client_pool(
  size_t size, client_configuration conf, client_pool_overdraft_policy policy)
  : _capacity(size)
  , _config(std::move(conf))
  , _probe(std::visit([](auto&& p) { return p._probe; }, _config))
  , _policy(policy) {}

ss::future<> client_pool::stop() {
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    _cvar.broken();
    // Wait until all leased objects are returned
    co_await _gate.close();
}

void client_pool::shutdown_connections() {
    _as.request_abort();
    _cvar.broken();
    for (auto& it : _leased) {
        it.client->shutdown();
    }
    for (auto& it : _pool) {
        it->shutdown();
    }
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
        if (unlikely(!_apply_credentials)) {
            co_await wait_for_credentials();
        }

        while (unlikely(
          _pool.empty() && !_gate.is_closed() && !_as.abort_requested())) {
            if (_policy == client_pool_overdraft_policy::wait_if_empty) {
                co_await _cvar.wait();
                vlog(
                  pool_log.debug,
                  "cvar triggered, pool size: {}",
                  _pool.size());
            } else {
                // Ask other shards for allowance
                auto resources = co_await container().map(
                  [](client_pool& other) {
                      auto sid = ss::this_shard_id();
                      return std::make_tuple(
                        std::clamp(
                          other._capacity - other._pool.size(),
                          0UL,
                          other._capacity),
                        sid);
                  });
                std::sort(resources.begin(), resources.end());
                auto [cnt, sid] = resources.front();
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

    std::unique_ptr<hdr_hist::measurement> measurement;
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
    if (_pool.size() == _capacity) {
        return;
    }
    _pool.emplace_back(std::move(leased));
    _cvar.signal();
}

void client_pool::load_credentials(cloud_roles::credentials credentials) {
    if (unlikely(!_apply_credentials)) {
        _apply_credentials = ss::make_lw_shared(
          cloud_roles::make_credentials_applier(std::move(credentials)));
        populate_client_pool();
        // We signal the waiter only after the client pool is initialized, so
        // that any upload operations waiting are ready to proceed.
        _credentials_var.signal();
    } else {
        _apply_credentials->reset_creds(std::move(credentials));
    }
}

ss::future<> client_pool::wait_for_credentials() {
    co_await _credentials_var.wait([this]() {
        return _gate.is_closed() || _as.abort_requested()
               || (bool{_apply_credentials} && !_pool.empty());
    });

    if (_gate.is_closed() || _as.abort_requested()) {
        throw ss::gate_closed_exception();
    }
    co_return;
}

} // namespace cloud_storage_clients
