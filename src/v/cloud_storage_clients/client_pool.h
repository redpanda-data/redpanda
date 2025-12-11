/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage_clients/bucket_params.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_probe.h"
#include "cloud_storage_clients/upstream_registry.h"
#include "container/intrusive_list_helpers.h"
#include "ssx/watchdog.h"
#include "utils/stop_signal.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

namespace cloud_storage_clients {

/// Policy that controls behaviour of the client pool
/// in situation when number of requested client connections
/// exceeds pool capacity
enum class client_pool_overdraft_policy {
    /// Client pool should wait unitl any existing lease will be canceled
    wait_if_empty,
    /// Client pool should try to borrow connection from another shard
    borrow_if_empty
};

inline constexpr ss::shard_id self_config_shard = ss::shard_id{0};

/// Connection pool implementation
/// All connections share the same configuration
class client_pool
  : public ss::weakly_referencable<client_pool>
  , public ss::peering_sharded_service<client_pool> {
public:
    using client_ptr = ss::shared_ptr<client>;
    struct client_lease {
        client_ptr client;
        ss::deleter deleter;
        ss::abort_source::subscription as_sub;
        intrusive_list_hook _hook;
        std::unique_ptr<client_probe::hist_t::measurement> _track_duration;
        std::unique_ptr<ssx::watchdog> _wd;

        client_lease(
          client_ptr p,
          ss::abort_source& as,
          ss::deleter deleter,
          std::unique_ptr<client_probe::hist_t::measurement> m)
          : client(std::move(p))
          , deleter(std::move(deleter))
          , _track_duration(std::move(m)) {
            auto as_sub_opt = as.subscribe(
              // Lifetimes:
              // - Object referred to by `client` must stay alive until this
              //   lease is dropped.  This is guaranteed because lease carries
              //   a shared_ptr reference to it.
              // - Abort source must stay alive until this lease is dropped.
              // This
              //   is by convention, that Redpanda subsystems shut down their
              //   inner objects first before the enclosing parent (and its
              //   abort source) are destroyed.
              [client = &(*client)]() noexcept { client->shutdown(); });
            if (as_sub_opt) {
                as_sub = std::move(*as_sub_opt);
            }
        }

        client_lease(client_lease&& other) noexcept
          : client(std::move(other.client))
          , deleter(std::move(other.deleter))
          , as_sub(std::move(other.as_sub))
          , _wd(std::move(other._wd)) {
            _hook.swap_nodes(other._hook);
        }

        client_lease& operator=(client_lease&& other) noexcept {
            client = std::move(other.client);
            deleter = std::move(other.deleter);
            as_sub = std::move(other.as_sub);
            _hook.swap_nodes(other._hook);
            _wd = std::move(other._wd);
            return *this;
        }

        client_lease(const client_lease&) = delete;
        client_lease& operator=(const client_lease&) = delete;
    };

    /// C-tor
    ///
    /// \param size is a size of the pool
    /// \param conf is a client configuration
    /// \param policy controls what happens when the pool is empty (wait or try
    ///               to borrow from another shard)
    /// \param application_abort_source abort source which can be used to stop
    /// Redpanda gracefully
    client_pool(
      upstream_registry& upstream_registry,
      size_t size,
      client_configuration conf,
      client_pool_overdraft_policy policy
      = client_pool_overdraft_policy::wait_if_empty);

    ss::future<> start(
      std::optional<std::reference_wrapper<stop_signal>> application_stop_signal
      = std::nullopt);
    ss::future<> stop();

    void shutdown_connections();

    bool shutdown_initiated();

    ss::future<uint64_t> token_refresh_count() const;

    /// \brief Acquire http client from the pool.
    ///
    /// \note it's guaranteed that the client can only be acquired once
    ///       before it gets released (release happens implicitly, when
    ///       the lifetime of the pointer ends).
    /// \param as
    /// \param deadline - Optional timeout. If deadline is reached before a
    ///                   client becomes available, throw ss::timed_out_error
    /// \return client pointer (via future that can wait if all clients
    ///         are in use)
    ss::future<client_lease> acquire(
      const bucket_params& params,
      ss::abort_source& as,
      std::optional<ss::lowres_clock::time_point> deadline = std::nullopt);

    /// \brief Acquire http client from the pool for a specified duration.
    ///
    /// Same invariants as ::acquire apply (see above).
    /// The provided timeout is applied in two distinct ways:
    ///   - Fed through to a watchdog timer governing the lifetime of the lease.
    ///     If it fires before the lease is returned, immediately calls shutdown
    ///     on the enclosed client.
    ///   - Passed to client_pool::acquire, which throws if we reach the
    ///     deadline before a client becomes available.
    ///
    /// \param as
    /// \param deadline - Lease expiration time, after which the client is
    ///                   forcibly shut down.
    /// \param ctx - Optional context for the log message. e.g. the string
    ///              representation of a retry_chain_node.
    ss::future<client_lease> acquire_with_timeout(
      const bucket_params& params,
      ss::abort_source& as,
      ss::lowres_clock::duration deadline,
      std::optional<ss::sstring> ctx = std::nullopt);

    /// Idle client connections count. Even if this is 0, the pool may
    /// still be able to serve acquire().
    size_t idle_count() const noexcept;

    /// \brief Configured capacity of the pool.
    size_t capacity() const noexcept;

    bool has_background_operations() const noexcept {
        return _bg_gate.get_count() > 0;
    }

    bool has_waiters() const noexcept { return _cvar.has_waiters(); }

private:
    void release(client_ptr leased, upstream_key key);

    /// Return number of clients which wasn't utilized
    size_t normalized_num_clients_in_use() const;
    bool borrow_one(unsigned other);
    void return_one(unsigned other);

    void update_usage_stats();

    /// Configured capacity per shard
    const size_t _capacity;

    client_configuration _config;
    net::base_transport::configuration _transport_config;

    ss::shared_ptr<client_probe> _probe;
    client_pool_overdraft_policy _policy;

    struct client_wrapper {
        explicit client_wrapper(client_ptr p)
          : ptr(std::move(p)) {}
        client_wrapper(const client_wrapper& other) = delete;
        client_wrapper& operator=(client_wrapper& other) = delete;
        client_wrapper(client_wrapper&& other) noexcept
          : ptr(std::move(other.ptr)) {
            _hook.swap_nodes(other._hook);
            _upstream_hook.swap_nodes(other._upstream_hook);
        }
        client_wrapper& operator=(client_wrapper&& other) = delete;
        ~client_wrapper() = default;

        client_ptr ptr;
        intrusive_list_hook _hook;
        intrusive_list_hook _upstream_hook;
    };

    // Authoritative ownership of all idle clients.
    std::unordered_map<client*, client_wrapper> _idle_clients;

    // Global LRU list of idle clients.
    intrusive_list<client_wrapper, &client_wrapper::_hook> _lru_idle_list;

    // Per-upstream LRU lists of idle clients.
    using upstream_list
      = intrusive_list<client_wrapper, &client_wrapper::_upstream_hook>;
    std::map<upstream_key, upstream_list> _lru_idle_list_per_upstream;

    size_t _num_own_leased{0};

    // List of all connections currently used by clients
    intrusive_list<client_lease, &client_lease::_hook> _leased;
    ss::condition_variable _cvar;
    ss::abort_source _as;
    ss::gate _gate;
    // A gate for background operations. Most useful in testing where we want
    // to wait all async housekeeping to complete before asserting state
    // invariants.
    ss::gate _bg_gate;

    ssx::semaphore _pool_ready_barrier{0, "pool_barrier"};

    upstream_registry& _upstream_registry;
};

} // namespace cloud_storage_clients
