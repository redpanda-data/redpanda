#include "cloud_storage_clients/client_pool.h"

#include "cloud_storage_clients/abs_client.h"
#include "cloud_storage_clients/s3_client.h"

namespace cloud_storage_clients {

client_pool::client_pool(
  size_t size, client_configuration conf, client_pool_overdraft_policy policy)
  : _max_size(size)
  , _config(std::move(conf))
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
    try {
        // If credentials have not yet been acquired, wait for them. It is
        // possible that credentials are not initialized right after remote
        // starts, and we have not had a response from the credentials API yet,
        // but we have scheduled an upload. This wait ensures that when we call
        // the storage API we have a set of valid credentials.
        if (unlikely(!_apply_credentials)) {
            co_await wait_for_credentials();
        }

        while (_pool.empty() && !_gate.is_closed() && !_as.abort_requested()) {
            if (_policy == client_pool_overdraft_policy::wait_if_empty) {
                co_await _cvar.wait();
            } else {
                _pool.emplace_back(make_client());
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
    client_lease lease(
      client,
      as,
      ss::make_deleter([pool = weak_from_this(), client, g = std::move(guard)] {
          if (pool) {
              pool->release(client);
          }
      }));
    _leased.push_back(lease);
    co_return lease;
}

size_t client_pool::size() const noexcept { return _pool.size(); }

size_t client_pool::max_size() const noexcept { return _max_size; }

void client_pool::populate_client_pool() {
    for (size_t i = 0; i < _max_size; i++) {
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
    if (_pool.size() == _max_size) {
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
