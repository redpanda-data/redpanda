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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "ssx/future-util.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <limits>
#include <map>
#include <variant>

namespace cloud_storage_clients::detail {

template<typename T>
concept registry_key = requires(const T& a, const T& b) {
    { a < b } -> std::convertible_to<bool>;
} && fmt::HasFormatToMethod<T>;

template<typename T>
concept registry_svc = requires(ss::sharded<T>& s) { s.stop(); };

/// Thrown when an entry is evicted while a get() is waiting. Caller may retry.
class entry_evicted_error final : public std::exception {
public:
    const char* what() const noexcept override {
        return "registry entry evicted";
    }
};

/// Thrown when the registry has reached its maximum entry limit.
class registry_full_error final : public std::exception {
public:
    const char* what() const noexcept override {
        return "registry entry limit reached";
    }
};

/// Lazy, sharded service registry with coordinator-based ownership.
///
/// Services are created on-demand and owned by shard 0 (coordinator).
/// Peer shards cache lightweight references. Semaphore-based reference
/// counting ensures services outlive all handles.
///
/// CRTP: Derived must inherit from both basic_registry and
/// ss::peering_sharded_service<Derived> to provide container().
///
/// Shutdown: gate closes, peers release refs (async-returns coordinator's
/// semaphore units), coordinator waits for all units then stops services.
template<registry_svc Svc, registry_key Key, typename Derived>
class basic_registry {
    static constexpr ss::shard_id coord_id = ss::shard_id{0};

private:
    /// Holds a semaphore unit that keeps the service alive.
    struct entry_ref {
        ssx::semaphore_units u;
        ss::sharded<Svc>* svc;
    };

public:
    /// RAII handle to a service. Move-only to prevent ref-count bypass.
    class [[nodiscard]] handle {
    public:
        explicit handle(entry_ref ref)
          : _ref(std::move(ref))
          , _svc(&_ref.svc->local()) {}

        handle(const handle&) = delete;
        handle& operator=(const handle&) = delete;
        handle(handle&& other) noexcept
          : _ref(std::move(other._ref))
          , _svc(other._svc) {
            other._svc = nullptr;
        }
        handle& operator=(handle&& other) = delete;
        ~handle() = default;

        Svc& get() { return *_svc; }
        const Svc& get() const { return *_svc; }

        Svc* operator->() { return _svc; }
        const Svc* operator->() const { return _svc; }

        ss::sharded<Svc>& sharded() { return *_ref.svc; }
        const ss::sharded<Svc>& sharded() const { return *_ref.svc; }

    private:
        entry_ref _ref;
        Svc* _svc;
    };

    struct sharded_svc {
        ss::sharded<Svc> svc;

        /// We make assumptions throughout the registry that this semaphore is
        /// held only for very short durations.
        ssx::semaphore ctor_sem{0, "basic_registry::sharded_svc::ctor_sem"};
    };

    class sharded_constructor {
    public:
        explicit sharded_constructor(sharded_svc& svc)
          : svc(&svc) {}

        template<typename... Args>
        ss::future<ss::sharded<Svc>&> start(Args&&... args) {
            try {
                co_await svc->svc.start(std::forward<Args>(args)...);
            } catch (...) {
                svc->ctor_sem.broken();
                throw;
            }
            svc->ctor_sem.signal(ssx::semaphore::max_counter());
            co_return svc->svc;
        }

    private:
        sharded_svc* svc;
    };

    static constexpr size_t no_entry_limit = std::numeric_limits<size_t>::max();

    basic_registry(ss::logger& log, size_t max_entries)
      : _log(&log)
      , _max_entries(max_entries) {}

    basic_registry(const basic_registry&) = delete;
    basic_registry& operator=(const basic_registry&) = delete;
    basic_registry(basic_registry&&) = delete;
    basic_registry& operator=(basic_registry&&) = delete;

    /// \defgroup Lifecycle
    /// @{
    void prepare_stop();
    ss::future<> stop();
    /// @}

    /// Get a service handle for the given key. If a service instance does
    /// not already exist for the key, it will be created.
    ///
    /// \throws registry_full_error if max_entries limit reached
    /// \throws ss::gate_closed_exception if registry is stopping
    /// \throws exceptions from start_svc() on service creation failure
    ss::future<handle> get(Key key);

    /// Evict idle entries last used before the given threshold.
    /// Only evicts entries with no outstanding local handles.
    /// Returns number of entries evicted on this shard.
    ///
    /// Cross-shard eviction: call on peers first (releases coordinator's
    /// semaphore units), then on coordinator. If coordinator is called while
    /// peers still hold refs, those entries are skipped.
    ss::future<size_t>
    evict_if_older_than(ss::lowres_clock::time_point threshold);

    /// Start background eviction loop that periodically evicts entries
    /// idle longer than max_idle_time. Runs until gate is closed.
    void start_evictor(
      ss::lowres_clock::duration interval,
      ss::lowres_clock::duration max_idle_time);

    /// Returns the number of entries on this shard.
    size_t entry_count() const { return _entries.size(); }

protected:
    ~basic_registry() = default;

    virtual ss::future<> start_svc(sharded_constructor&, const Key&) = 0;

private:
    /// Peer shard's reference to coordinator-owned service.
    /// Holds units from coordinator's semaphore (not local). Destructor
    /// async-returns units; safe because coordinator blocks on semaphore
    /// before teardown.
    struct remote_entry_ref {
        struct units {
            ssx::semaphore_units u;

            explicit units(ssx::semaphore_units&& units)
              : u(std::move(units)) {}
            units(const units&) = delete;
            units& operator=(const units&) = delete;
            units(units&&) noexcept = default;
            units& operator=(units&&) noexcept = default;

            ~units() {
                if (u) {
                    // Safe: coordinator's stop() acquires all units before
                    // destroying services (see stop_coordinator())
                    ssx::background = ss::smp::submit_to(
                      coord_id,
                      [u = std::move(u)]() mutable { u.return_all(); });
                }
            }
        };

        ss::sharded<Svc>* svc;
        units u;

        remote_entry_ref(ss::sharded<Svc>* svc, ssx::semaphore_units u)
          : svc(svc)
          , u(std::move(u)) {}

        remote_entry_ref(const remote_entry_ref&) = delete;
        remote_entry_ref& operator=(const remote_entry_ref&) = delete;
        remote_entry_ref(remote_entry_ref&&) noexcept = default;
        remote_entry_ref&
        operator=(remote_entry_ref&& other) noexcept = default;

        ~remote_entry_ref() = default;
    };

    struct entry {
        explicit entry(ssx::semaphore&& started_sem)
          : started_sem(std::move(started_sem))
          , storage(std::monostate{})
          , last_used(ss::lowres_clock::now()) {}

        entry(const entry&) = delete;
        entry& operator=(const entry&) = delete;
        entry(entry&&) noexcept = delete;
        entry& operator=(entry&&) noexcept = delete;
        ~entry() = default;

        /// Dual-purpose semaphore:
        /// - Serialization: starts at 0, concurrent get() blocks until creator
        ///   signals (max-1) after init. On failure, broken with exception.
        /// - Ref counting: each handle holds 1 unit. stop()/evict() acquire
        ///   all units (max_counter) to ensure no outstanding handles.
        ssx::semaphore started_sem;

        /// Entry lifecycle: monostate (init) -> sharded_svc or
        /// remote_entry_ref
        using storage_t = std::variant<
          std::monostate,  ///< init in progress or failed
          sharded_svc,     ///< owning instance (coordinator only)
          remote_entry_ref ///< non-owning ref (peer shards only)
          >;

        storage_t storage;

        /// Last time a handle was acquired on this shard.
        ss::lowres_clock::time_point last_used;
    };

    /// Using std::map for iterator and pointer stability. So that iterators are
    /// not invalidated on insert/erase of other entries.
    /// To safely hold iterators across co_await points you must hold semaphore
    /// units of the entry being accessed, preventing its erasure. That is, you
    /// must call one of semaphore acquire methods (get_units, wait, etc.)
    /// before any co_await that may suspend the coroutine. co_await-ing
    /// semaphore acquire methods is safe.
    using entry_map_t = std::map<Key, entry>;

    ss::future<entry_ref> upsert_entry(Key key);

    /// Wait for all handles (local + remote) to release, then stop services.
    /// \pre gate is closed and there are no gate holders
    ss::future<> stop_coordinator() noexcept;

    /// Wait for local handles to release, then destroy remote refs
    /// (async-returns units to coordinator).
    /// \pre gate is closed and there are no gate holders
    ss::future<> stop_peer() noexcept;

    ss::abort_source _abort_source;
    ss::gate _gate;
    ss::logger* _log;
    size_t _max_entries;
    entry_map_t _entries;
    ssx::mutex _concurrent_eviction_mutex{"basic_registry::eviction"};
};

} // namespace cloud_storage_clients::detail
