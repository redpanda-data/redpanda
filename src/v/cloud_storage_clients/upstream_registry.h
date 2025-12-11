/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_storage_clients/upstream.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>

namespace cloud_storage_clients {

class upstream_registry
  : public ss::peering_sharded_service<upstream_registry> {
    static constexpr ss::shard_id coord_id = ss::shard_id{0};

private:
    struct entry_ref {
        ssx::semaphore_units u;
        ss::sharded<upstream>* svc;
    };

public:
    class handle {
    public:
        explicit handle(entry_ref ref)
          : _ref(std::move(ref))
          , _svc(&_ref.svc->local()) {}

        upstream& operator*() { return *_svc; }
        const upstream& operator*() const { return *_svc; }

        upstream* operator->() { return _svc; }
        const upstream* operator->() const { return _svc; }

    private:
        entry_ref _ref;
        upstream* _svc;
    };

public:
    explicit upstream_registry(client_configuration config);

    upstream_registry(const upstream_registry&) = delete;
    upstream_registry& operator=(const upstream_registry&) = delete;
    upstream_registry(upstream_registry&&) noexcept = delete;
    upstream_registry& operator=(upstream_registry&&) noexcept = delete;

    ~upstream_registry();

public:
    ss::future<> stop();

public:
    ss::future<handle> get(upstream_key key);

private:
    struct remote_entry_ref {
        ss::sharded<upstream>* svc;
        ssx::semaphore_units u;
        ss::shard_id shard;

        remote_entry_ref(
          ss::sharded<upstream>* svc,
          ssx::semaphore_units u,
          ss::shard_id shard)
          : svc(svc)
          , u(std::move(u))
          , shard(shard) {}

        remote_entry_ref(remote_entry_ref&&) noexcept = default;
        remote_entry_ref& operator=(remote_entry_ref&&) noexcept = default;

        ~remote_entry_ref() {
            if (u) {
                ssx::background = ss::smp::submit_to(
                  shard, [u = std::move(u)]() mutable { u.return_all(); });
            }
        }
    };

    struct entry {
        // Semaphore to synchronize creation of the upstream instance.
        ssx::semaphore sem;

        using storage_t = std::variant<
          std::monostate,        ///< uninitialized
          ss::sharded<upstream>, ///< owning object (coord_id shard)
          remote_entry_ref       ///< non-owning handle (peer shards)
          >;

        storage_t storage;
    };

private:
    ss::future<> do_stop_coordinator();
    ss::future<> do_stop_peer();

    ss::future<entry_ref> do_upsert_entry(upstream_key key);

private:
    ss::gate _gate;

    client_configuration _config;

    // Note: Using a map to avoid iterator invalidation across yield points.
    using map_t = std::map<upstream_key, entry>;

    map_t _entries;
};

} // namespace cloud_storage_clients
