/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/upstream_registry.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_storage_clients/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/variant_utils.hh>

namespace cloud_storage_clients {

fmt::iterator upstream_key::format_to(fmt::iterator it) const {
    fmt::format_to(
      it,
      "upstream_key{{region={}}}",
      _region().empty() ? "<default>" : _region());
    return it;
}

upstream_registry::upstream_registry(client_configuration config)
  : _config(std::move(config)) {}

upstream_registry::~upstream_registry() {
    vassert(
      _entries.empty(),
      "upstreams still present in upstream_registry on destruction");
}

ss::future<> upstream_registry::stop() {
    // Close the gate to prevent new get() calls.
    auto gate_close_fut = _gate.close();

    if (ss::this_shard_id() == coord_id) {
        co_await do_stop_coordinator();
    } else {
        co_await do_stop_peer();
    }

    co_await std::move(gate_close_fut);
}

ss::future<> upstream_registry::do_stop_coordinator() {
    for (auto it = _entries.begin(); it != _entries.end();) {
        auto& [key, entry] = *it;
        vlog(pool_log.trace, "Removing owned sharded upstream for key {}", key);

        ssx::semaphore_units u;
        try {
            // Wait for entry to be in a stable state:
            // a) all handles released, or
            // b) semaphore broken (creation failed).
            u = co_await ss::get_units(entry.sem, entry.sem.max_counter());
        } catch (...) {
            // TODO: Handle broken semaphore properly.
            vunreachable("unrecoverable error during upstream_registry stop");
        }

        // Ensure no new handles can be created.
        u.release();
        entry.sem.broken();

        // TODO: Generated code. Review TBD! Shall we stop concurrently? Why
        // wait for semaphore?
        //
        // Stop and remove local entry. New insertions are
        // prevented by the closed gate.
        auto& svc = std::get<ss::sharded<upstream>>(entry.storage);
        vlog(pool_log.trace, "Stopping owned sharded upstream for key {}", key);

        co_await svc.stop();

        vlog(pool_log.trace, "Removed owned sharded upstream for key {}", key);

        it = _entries.erase(it);
    }
}

ss::future<> upstream_registry::do_stop_peer() {
    for (auto it = _entries.begin(); it != _entries.end();) {
        auto& [key, entry] = *it;
        vlog(
          pool_log.trace, "Removing foreign sharded upstream for key {}", key);

        ssx::semaphore_units u;
        try {
            // Wait for entry to be in a stable state:
            // a) all handles released, or
            // b) semaphore broken (creation failed).
            u = co_await ss::get_units(entry.sem, entry.sem.max_counter());
        } catch (...) {
            // TODO: Handle broken semaphore properly.
            vunreachable("unrecoverable error during upstream_registry stop");
        }

        // Ensure no new handles can be created.
        u.release();
        entry.sem.broken();

        vlog(
          pool_log.trace, "Removed foreign sharded upstream for key {}", key);

        // Remove local entry. New insertions are prevented by the closed
        // gate.
        it = _entries.erase(it);
    }
}

ss::future<upstream_registry::handle> upstream_registry::get(upstream_key key) {
    auto h = _gate.hold();

    vlog(
      pool_log.trace,
      "Acquiring upstream for key {} shard {}",
      key,
      ss::this_shard_id());

    auto entry_ref = co_await do_upsert_entry(key);

    vlog(
      pool_log.trace,
      "Returning handle for upstream for key {} shard {}",
      key,
      ss::this_shard_id());

    co_return handle{std::move(entry_ref)};
}

ss::future<upstream_registry::entry_ref>
upstream_registry::do_upsert_entry(upstream_key key) {
    auto h = _gate.hold();

    // Check if an entry exists on the local shard.
    if (auto it = _entries.find(key); it != _entries.end()) {
        vlog(pool_log.trace, "Found existing upstream entry for key {}", key);
        auto u = co_await ss::get_units(it->second.sem, 1);
        vlog(pool_log.trace, "Acquired semaphore for key {}", key);

        // The handle is ready. Create and return it.
        co_return entry_ref{
          .u = std::move(u),
          .svc = ss::visit(
            it->second.storage,
            [](std::monostate) -> ss::sharded<upstream>* {
                vassert(false, "upstream entry in monostate");
            },
            [](ss::sharded<upstream>& svc) -> ss::sharded<upstream>* {
                return &svc;
            },
            [](remote_entry_ref& ref) -> ss::sharded<upstream>* {
                return ref.svc;
            })};
    }

    // We need to create the local entry and roundtrip to shard 0 to request
    // creation of the upstream resource.
    auto [it, inserted] = _entries.emplace(
      key, ssx::semaphore{0, fmt::format("upstream_registry::entry::{}", key)});
    dassert(inserted, "entry must not exist");

    if (ss::this_shard_id() == coord_id) {
        vlog(
          pool_log.trace,
          "Creating owned sharded upstream for key {} on coord_id shard",
          key);

        // We are on coord_id shard. Create the upstream instance
        // directly.
        std::exception_ptr e;
        ss::sharded<upstream>* ptr = nullptr;

        try {
            it->second.storage.emplace<ss::sharded<upstream>>();
            ptr = &std::get<ss::sharded<upstream>>(it->second.storage);

            client_configuration upstream_cfg = _config;
            if (!key.region()().empty()) {
                ss::visit(
                  upstream_cfg,
                  [&](s3_configuration& cfg) { cfg.region = key.region(); },
                  [&](abs_configuration&) {});
            }

            if (!key.endpoint()().empty()) {
                ss::visit(
                  upstream_cfg,
                  [&](s3_configuration& cfg) {
                      // TODO: Likely we need to refactor self-configuration.
                      // Too much spaghetti.
                      cfg.tls_sni_hostname = key.endpoint();
                      cfg.uri = access_point_uri{key.endpoint()};
                      cfg.server_addr = net::unresolved_address(
                        key.endpoint(),
                        cfg.server_addr.port(),
                        cfg.server_addr.family());
                  },
                  [&](abs_configuration&) {});
            }

            co_await ptr->start(upstream_cfg);
            co_await ptr->invoke_on_all(
              [](upstream& svc) { return svc.start(); });

            // Hold one unit for the current caller.
            it->second.sem.signal(it->second.sem.max_counter() - 1);
        } catch (...) {
            e = std::current_exception();
        }

        if (e) {
            // Partially created resource.
            if (ptr != nullptr) {
                co_await ptr->stop();
            }

            it->second.storage.emplace<std::monostate>();

            // Release waiters with exception.
            it->second.sem.broken(e);

            // Remove the entry so that future attempts can retry.
            _entries.erase(it);

            // Throw to current caller too.
            std::rethrow_exception(e);
        }

        // Create and return the handle.
        co_return entry_ref{
          .u = ss::semaphore_units(it->second.sem, 1), .svc = ptr};
    } else {
        vlog(
          pool_log.trace,
          "Requesting creation of owned sharded upstream for key {} on "
          "coord_id shard",
          key);

        // We are on a non-coord_id shard. Roundtrip to coord_id to get
        // visibility into its memory and create the foreign pointer.
        remote_entry_ref ref = co_await container().invoke_on(
          coord_id, [key](upstream_registry& registry) {
              return registry.do_upsert_entry(key).then([&](
                                                          entry_ref owner_ref) {
                  return remote_entry_ref{
                    owner_ref.svc, std::move(owner_ref.u), ss::this_shard_id()};
              });
          });

        // Store the foreign pointer and release waiters.
        it->second.storage = std::move(ref);
        // Hold one unit for the current caller.
        it->second.sem.signal(it->second.sem.max_counter() - 1);

        co_return entry_ref{
          .u = ss::semaphore_units(it->second.sem, 1), .svc = ref.svc};
    }
}

} // namespace cloud_storage_clients
