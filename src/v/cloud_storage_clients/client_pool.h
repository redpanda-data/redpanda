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

#include "cloud_roles/apply_credentials.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_probe.h"
#include "utils/gate_guard.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/condition-variable.hh>

namespace cloud_storage_clients {

/// Policy that controls behaviour of the client pool
/// in situation when number of requested client connections
/// exceeds pool capacity
enum class client_pool_overdraft_policy {
    /// Client pool should wait unitl any existing lease will be canceled
    wait_if_empty,
    /// Client pool should create transient client connection to serve the
    /// request
    create_new_if_empty
};

/// Connection pool implementation
/// All connections share the same configuration
class client_pool : public ss::weakly_referencable<client_pool> {
public:
    using http_client_ptr = ss::shared_ptr<client>;
    struct client_lease {
        http_client_ptr client;
        ss::deleter deleter;
        ss::abort_source::subscription as_sub;
        intrusive_list_hook _hook;

        client_lease(
          http_client_ptr p, ss::abort_source& as, ss::deleter deleter)
          : client(std::move(p))
          , deleter(std::move(deleter)) {
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
          , as_sub(std::move(other.as_sub)) {
            _hook.swap_nodes(other._hook);
        }

        client_lease& operator=(client_lease&& other) noexcept {
            client = std::move(other.client);
            deleter = std::move(other.deleter);
            as_sub = std::move(other.as_sub);
            _hook.swap_nodes(other._hook);
            return *this;
        }

        client_lease(const client_lease&) = delete;
        client_lease& operator=(const client_lease&) = delete;
    };

    client_pool(
      size_t size,
      client_configuration conf,
      client_pool_overdraft_policy policy
      = client_pool_overdraft_policy::wait_if_empty);

    ss::future<> stop();

    void shutdown_connections();

    /// Performs the dual functions of loading refreshed credentials into
    /// apply_credentials object, as well as initializing the client pool
    /// the first time this function is called.
    void load_credentials(cloud_roles::credentials credentials);

    /// \brief Acquire http client from the pool.
    ///
    /// \note it's guaranteed that the client can only be acquired once
    ///       before it gets released (release happens implicitly, when
    ///       the lifetime of the pointer ends).
    /// \return client pointer (via future that can wait if all clients
    ///         are in use)
    ss::future<client_lease> acquire(ss::abort_source& as);

    /// \brief Get number of connections
    size_t size() const noexcept;

    size_t max_size() const noexcept;

private:
    void populate_client_pool();
    http_client_ptr make_client() const;
    void release(http_client_ptr leased);

    ///  Wait for credentials to be acquired. Once credentials are acquired,
    ///  based on the policy, optionally wait for client pool to initialize.
    ss::future<> wait_for_credentials();

    const size_t _max_size;
    client_configuration _config;
    client_pool_overdraft_policy _policy;
    std::vector<http_client_ptr> _pool;
    // List of all connections currently used by clients
    intrusive_list<client_lease, &client_lease::_hook> _leased;
    ss::condition_variable _cvar;
    ss::abort_source _as;
    ss::gate _gate;

    /// Holds and applies the credentials for requests to S3. Shared pointer to
    /// enable rotating credentials to all clients.
    ss::lw_shared_ptr<cloud_roles::apply_credentials> _apply_credentials;
    ss::condition_variable _credentials_var;
};

} // namespace cloud_storage_clients
