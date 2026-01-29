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

#include "cloud_storage_clients/bucket_name_parts.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_probe.h"
#include "cloud_storage_clients/upstream_registry.h"
#include "container/intrusive_list_helpers.h"
#include "ssx/watchdog.h"
#include "utils/stop_signal.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <unordered_map>

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
class client_pool final
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
          , _track_duration(std::move(other._track_duration))
          , _wd(std::move(other._wd)) {
            _hook.swap_nodes(other._hook);
        }

        client_lease& operator=(client_lease&& other) noexcept {
            client = std::move(other.client);
            deleter = std::move(other.deleter);
            as_sub = std::move(other.as_sub);
            _hook.swap_nodes(other._hook);
            _track_duration = std::move(other._track_duration);
            _wd = std::move(other._wd);
            return *this;
        }

        client_lease(const client_lease&) = delete;
        client_lease& operator=(const client_lease&) = delete;
    };

    /// C-tor
    ///
    /// \param registry upstream registry to obtain upstreams from for creating
    /// clients
    /// \param size is a size of the pool
    /// \param conf is a client configuration
    /// \param policy controls what happens when the pool is empty (wait or try
    ///               to borrow from another shard)
    /// \param application_abort_source abort source which can be used to stop
    /// Redpanda gracefully
    client_pool(
      upstream_registry& registry,
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

    uint64_t token_refresh_count() const;

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
      const bucket_name_parts& bucket,
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
      const bucket_name_parts& bucket,
      ss::abort_source& as,
      ss::lowres_clock::duration deadline,
      std::optional<ss::sstring> ctx = std::nullopt);

    /// \brief Idle clients waiting in the pool. If this number is less than
    /// capacity, some clients are currently leased out, borrowed, or shutdown.
    size_t idle_count() const noexcept;

    /// \brief Configured capacity of the pool. Does not take into account
    /// connections that may be borrowed from other shards, i.e. when
    /// `client_pool_overdraft_policy::borrow_if_empty` is used.
    size_t capacity() const noexcept;

    bool has_background_operations() const noexcept {
        return _bg_gate.get_count() > 0;
    }

    bool has_waiters() const noexcept {
        return _cvar.has_waiters() || _pool_ready_barrier.waiters() > 0;
    }

private:
    /// An entry in the idle clients collection, combining ownership with
    /// intrusive list membership for LRU ordering.
    struct idle_entry {
        explicit idle_entry(client_ptr p)
          : ptr(std::move(p)) {}
        idle_entry(const idle_entry&) = delete;
        idle_entry& operator=(const idle_entry&) = delete;
        idle_entry(idle_entry&& other) noexcept
          : ptr(std::move(other.ptr)) {
            _hook.swap_nodes(other._hook);
        }
        idle_entry& operator=(idle_entry&&) = delete;
        ~idle_entry() = default;

        client_ptr ptr;
        intrusive_list_hook _hook;
    };

    void populate_client_pool(upstream_registry::handle& up);

    /// Return [0, 100] normalized number of clients currently in use by this
    /// pool (leased locally or lent to other shards) .
    size_t normalized_num_clients_in_use() const;
    bool borrow_one(unsigned other) noexcept;
    void return_one(upstream_registry::handle& up, unsigned other) noexcept;

    /// Add a new idle client to the pool.
    void emplace_idle(upstream_registry::handle& up) noexcept;

    /// Pop the most recently used idle client from the pool.
    [[nodiscard]]
    client_ptr pop_most_recently_used() noexcept;

    /// Pop the least recently used idle client from the pool.
    [[nodiscard]]
    client_ptr pop_least_recently_used() noexcept;

    /// Release a leased client back to the pool as most recently used.
    void release_most_recently_used(client_ptr leased) noexcept;

    /// Replace the least recently used idle client with the provided one.
    [[nodiscard]] client_ptr
    replace_least_recently_used(client_ptr leased) noexcept;

    void update_usage_stats();

    upstream_registry& _upstreams;
    std::optional<upstream_registry::handle> _default_upstream;

    /// Configured capacity per shard
    const size_t _capacity;

    client_configuration _config;

    ss::shared_ptr<client_probe> _probe;
    client_pool_overdraft_policy _policy;

    // Authoritative ownership of all idle clients.
    std::unordered_map<const client*, idle_entry> _idle_clients;

    // Approximately LRU list of idle clients for reuse.
    // Cold clients are at the front. Hot clients are at the back.
    intrusive_list<idle_entry, &idle_entry::_hook> _idle_clients_lru;

    // List of all connections currently used by clients, includes borrowed
    // connections.
    intrusive_list<client_lease, &client_lease::_hook> _leased;

    ss::condition_variable _cvar;
    ss::abort_source _as;
    ss::gate _gate;
    // A gate for background operations. Most useful in testing where we want
    // to wait all async housekeeping to complete before asserting state
    // invariants.
    ss::gate _bg_gate;

    ssx::semaphore _pool_ready_barrier{0, "pool_barrier"};
};

} // namespace cloud_storage_clients
