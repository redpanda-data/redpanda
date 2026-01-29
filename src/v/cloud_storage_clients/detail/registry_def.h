/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/vlog.h"
#include "cloud_storage_clients/detail/registry.h"
#include "random/simple_time_jitter.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/variant_utils.hh>

namespace cloud_storage_clients::detail {

template<registry_svc Svc, registry_key Key, typename Derived>
void basic_registry<Svc, Key, Derived>::prepare_stop() {
    vlog(_log->trace, "Preparing to stop registry");
    _abort_source.request_abort();

    for (auto& [_, entry] : _entries) {
        ss::visit(
          entry.storage,
          [](std::monostate&) {},
          [this](sharded_svc& svc) {
              auto h = _gate.hold();

              ssx::background
                = ss::get_units(svc.ctor_sem, 1)
                    .then([&svc](ssx::semaphore_units u) {
                        return svc.svc.invoke_on_all(&Svc::prepare_stop)
                          .finally([u = std::move(u)] {});
                    })
                    .handle_exception([](const std::exception_ptr&) {
                        // Ignore broken semaphore exceptions.
                    })
                    .finally([h = std::move(h)] {});
          },
          [](remote_entry_ref&) {});
    }
}

template<registry_svc Svc, registry_key Key, typename Derived>
ss::future<> basic_registry<Svc, Key, Derived>::stop() {
    vlog(_log->trace, "Stopping registry");
    _abort_source.request_abort();
    _concurrent_eviction_mutex.broken();
    co_await _gate.close();

    if (ss::this_shard_id() == coord_id) {
        vlog(_log->trace, "Stopping coordinator shard");
        co_await stop_coordinator();
    } else {
        vlog(_log->trace, "Stopping peer shard");
        co_await stop_peer();
    }
    vlog(_log->trace, "Registry stopped");
}

template<registry_svc Svc, registry_key Key, typename Derived>
ss::future<typename basic_registry<Svc, Key, Derived>::handle>
basic_registry<Svc, Key, Derived>::get(Key key) {
    for (;;) {
        _abort_source.check();
        auto h = _gate.hold();

        try {
            auto ref = co_await upsert_entry(key);
            co_return handle{std::move(ref)};
        } catch (const entry_evicted_error& e) {
            std::ignore = e;
        }

        co_await ss::coroutine::maybe_yield();
    }
}

template<registry_svc Svc, registry_key Key, typename Derived>
ss::future<typename basic_registry<Svc, Key, Derived>::entry_ref>
basic_registry<Svc, Key, Derived>::upsert_entry(Key key) {
    auto h = _gate.hold();

    vlog(_log->trace, "Requesting upstream entry for key {}", key);

    if (auto it = _entries.find(key); it != _entries.end()) {
        vlog(_log->trace, "Found existing upstream entry for key {}", key);
        auto u = co_await ss::get_units(it->second.started_sem, 1);
        vlog(_log->trace, "Acquired semaphore for key {}", key);

        it->second.last_used = ss::lowres_clock::now();

        co_return entry_ref{
          .u = std::move(u),
          .svc = ss::visit(
            it->second.storage,
            [](std::monostate) -> ss::sharded<Svc>* {
                vassert(false, "upstream entry in monostate");
            },
            [](sharded_svc& svc) -> ss::sharded<Svc>* { return &svc.svc; },
            [](remote_entry_ref& ref) -> ss::sharded<Svc>* {
                return ref.svc;
            })};
    }

    if (ss::this_shard_id() == coord_id && _entries.size() >= _max_entries) {
        throw registry_full_error{};
    }

    // If prepare_stop() has been called, we should not be creating new entries.
    _abort_source.check();

    auto [it, inserted] = _entries.try_emplace(
      key, ssx::semaphore{0, fmt::format("basic_registry::entry::{}", key)});
    dassert(inserted, "entry must not exist");

    if (ss::this_shard_id() == coord_id) {
        vlog(
          _log->trace,
          "Creating owned sharded service for key {} on coord_id shard",
          key);

        it->second.storage.template emplace<sharded_svc>();
        sharded_svc& svc = std::get<sharded_svc>(it->second.storage);

        std::exception_ptr e;
        try {
            auto ctor = sharded_constructor{svc};
            co_await start_svc(ctor, key);
            it->second.started_sem.signal(
              it->second.started_sem.max_counter() - 1);
            it->second.last_used = ss::lowres_clock::now();
        } catch (...) {
            e = std::current_exception();
        }

        if (e) {
            vlog(
              _log->warn,
              "Failed to create owned sharded service for key {}: {}",
              key,
              e);

            try {
                co_await svc.ctor_sem.wait(
                  it->second.started_sem.max_counter());
            } catch (ss::broken_semaphore& e) {
                std::ignore = e;
                // If semaphore is broken it means that construction failed
                // halfway.
            }

            co_await svc.svc.stop();
            it->second.storage.template emplace<std::monostate>();
            it->second.started_sem.broken(e);

            _entries.erase(it);
            std::rethrow_exception(e);
        }

        co_return entry_ref{
          .u = ss::semaphore_units(it->second.started_sem, 1), .svc = &svc.svc};
    } else {
        vlog(
          _log->trace, "Forwarding request for {} to coordinator shard", key);

        std::exception_ptr e;
        remote_entry_ref* ref_ptr = nullptr;

        try {
            auto ref
              = co_await static_cast<Derived*>(this)->container().invoke_on(
                coord_id, [key](auto& registry) {
                    return registry.upsert_entry(key).then(
                      [](entry_ref owner_ref) {
                          return remote_entry_ref{
                            owner_ref.svc,
                            std::move(owner_ref.u),
                          };
                      });
                });

            it->second.storage = std::move(ref);
            ref_ptr = &std::get<remote_entry_ref>(it->second.storage);
        } catch (...) {
            e = std::current_exception();
        }

        if (e) {
            vlog(_log->warn, "Failed to get remote ref for key {}: {}", key, e);

            it->second.started_sem.broken(e);
            _entries.erase(it);
            std::rethrow_exception(e);
        }

        it->second.started_sem.signal(it->second.started_sem.max_counter() - 1);
        it->second.last_used = ss::lowres_clock::now();

        co_return entry_ref{
          .u = ss::semaphore_units(it->second.started_sem, 1),
          .svc = ref_ptr->svc};
    }
}

template<registry_svc Svc, registry_key Key, typename Derived>
ss::future<> basic_registry<Svc, Key, Derived>::stop_coordinator() noexcept {
    for (auto it = _entries.begin(); it != _entries.end();) {
        auto& [key, entry] = *it;
        vlog(_log->trace, "Removing owned sharded upstream for key {}", key);

        co_await entry.started_sem.wait(entry.started_sem.max_counter());
        entry.started_sem.broken();

        vlog(_log->trace, "Stopping owned sharded service for key {}", key);
        auto& svc = std::get<sharded_svc>(entry.storage);
        co_await svc.ctor_sem.wait(ssx::semaphore::max_counter());
        svc.ctor_sem.broken();

        co_await svc.svc.stop();

        vlog(_log->trace, "Removed owned sharded service for key {}", key);
        it = _entries.erase(it);
    }
}

template<registry_svc Svc, registry_key Key, typename Derived>
ss::future<> basic_registry<Svc, Key, Derived>::stop_peer() noexcept {
    for (auto it = _entries.begin(); it != _entries.end();) {
        auto& [key, entry] = *it;
        vlog(_log->trace, "Removing ref for key {}", key);

        co_await entry.started_sem.wait(entry.started_sem.max_counter());
        entry.started_sem.broken();

        vlog(_log->trace, "Removed ref for key {}", key);
        it = _entries.erase(it);
    }
}

template<registry_svc Svc, registry_key Key, typename Derived>
ss::future<size_t> basic_registry<Svc, Key, Derived>::evict_if_older_than(
  ss::lowres_clock::time_point threshold) {
    auto h = _gate.hold();
    auto lock = co_await _concurrent_eviction_mutex.get_units();

    size_t evicted = 0;

    for (auto it = _entries.begin(); it != _entries.end();) {
        auto& [key, e] = *it;

        if (e.last_used >= threshold) {
            ++it;
            continue;
        }

        auto units = ss::try_get_units(
          e.started_sem, e.started_sem.max_counter());
        if (!units) {
            ++it;
            continue;
        }
        vlog(_log->debug, "Evicting entry for key {}", key);

        e.started_sem.broken(std::make_exception_ptr(entry_evicted_error{}));
        units->release();

        if (auto ptr = std::get_if<sharded_svc>(&e.storage); ptr != nullptr) {
            co_await ss::get_units(
              ptr->ctor_sem, ssx::semaphore::max_counter());
            ptr->ctor_sem.broken();
            co_await ptr->svc.stop();
        }

        it = _entries.erase(it);
        ++evicted;
    }

    co_return evicted;
}

template<registry_svc Svc, registry_key Key, typename Derived>
void basic_registry<Svc, Key, Derived>::start_evictor(
  ss::lowres_clock::duration interval,
  ss::lowres_clock::duration max_idle_time) {
    using jitter_t = simple_time_jitter<ss::lowres_clock>;
    ssx::repeat_until_gate_closed_or_aborted(
      _gate,
      _abort_source,
      [this, max_idle_time, jitter = jitter_t{interval}](
        this auto) -> ss::future<> {
          co_await ss::sleep_abortable(jitter.next_duration(), _abort_source);
          auto threshold = ss::lowres_clock::now() - max_idle_time;
          co_await evict_if_older_than(threshold);
      });
}

} // namespace cloud_storage_clients::detail
