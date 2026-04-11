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

#include "absl/container/btree_set.h"
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/future.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <memory>
#include <utility>

namespace cloud_topics::l1 {

template<typename Key>
class entity_lock_map;
template<typename Key>
class entity_rwlock_map;

namespace detail {

/// RAII lock holder that tracks a refcount on the lock map entry and erases
/// the entry when the last holder/waiter is gone. Shared by entity_lock_map
/// and entity_rwlock_map — parameterized on the underlying lock entry and
/// units types.
template<typename Key, typename LockEntry, typename Units>
class tracked_lock_units {
    using map_t = chunked_hash_map<Key, std::unique_ptr<LockEntry>>;

public:
    tracked_lock_units() noexcept = default;
    ~tracked_lock_units() noexcept { release(); }

    tracked_lock_units(const tracked_lock_units&) = delete;
    tracked_lock_units& operator=(const tracked_lock_units&) = delete;

    tracked_lock_units(tracked_lock_units&& o) noexcept
      : units_(std::move(o.units_))
      , entry_(std::exchange(o.entry_, nullptr))
      , map_(std::exchange(o.map_, nullptr))
      , key_(std::move(o.key_)) {}

    tracked_lock_units& operator=(tracked_lock_units&& o) noexcept {
        if (this != &o) {
            release();
            units_ = std::move(o.units_);
            entry_ = std::exchange(o.entry_, nullptr);
            map_ = std::exchange(o.map_, nullptr);
            key_ = std::move(o.key_);
        }
        return *this;
    }

private:
    template<typename>
    friend class cloud_topics::l1::entity_lock_map;
    template<typename>
    friend class cloud_topics::l1::entity_rwlock_map;

    tracked_lock_units(Units units, LockEntry* entry, map_t* map, Key key)
      : units_(std::move(units))
      , entry_(entry)
      , map_(map)
      , key_(std::move(key)) {}

    void release() noexcept {
        if (!entry_) {
            return;
        }
        // Cleanup runs after released is destroyed (reverse declaration
        // order), so the semaphore inside the entry is still alive when
        // units are returned to it.
        auto cleanup = ss::defer([this] {
            if (entry_->active == 0) {
                map_->erase(key_);
            }
            entry_ = nullptr;
            map_ = nullptr;
        });
        auto released = std::move(units_);
        --entry_->active;
    }

    Units units_;
    LockEntry* entry_{nullptr};
    map_t* map_{nullptr};
    Key key_;
};

} // namespace detail

/// \brief Per-entity exclusive lock map using ssx::checkpoint_mutex.
///
/// Dynamically maps keys to mutexes. Entries are created on first access and
/// removed when no holders or waiters remain. Safe for shard-local use only
/// (no internal synchronization on the map itself).
template<typename Key>
class entity_lock_map {
    struct lock_entry {
        ssx::checkpoint_mutex mu;
        uint32_t active{0};
        explicit lock_entry(ss::sstring name)
          : mu(std::move(name)) {}
    };

public:
    using tracked_units = detail::
      tracked_lock_units<Key, lock_entry, ssx::checkpoint_mutex_units>;

    explicit entity_lock_map(ss::sstring name_prefix)
      : name_prefix_(std::move(name_prefix)) {}

    ss::future<tracked_units> acquire(Key key) {
        auto* entry = get_or_create(key);
        try {
            auto units = co_await entry->mu.get_units();
            co_return tracked_units(std::move(units), entry, &locks_, key);
        } catch (...) {
            // get_or_create incremented active; if the co_await threw (e.g.
            // broken mutex during shutdown), we must undo it to avoid leaking
            // the map entry.
            if (--entry->active == 0) {
                locks_.erase(key);
            }
            throw;
        }
    }

    ss::future<chunked_vector<tracked_units>>
    acquire(absl::btree_set<Key> keys) {
        chunked_vector<tracked_units> result;
        result.reserve(keys.size());
        for (const auto& key : keys) {
            result.push_back(co_await acquire(key));
        }
        co_return result;
    }

    void broken() {
        for (auto& [_, entry] : locks_) {
            entry->mu.broken();
        }
    }

    size_t size() const { return locks_.size(); }

private:
    lock_entry* get_or_create(const Key& key) {
        auto it = locks_.find(key);
        if (it == locks_.end()) {
            auto name = fmt::format("{}/{}", name_prefix_, key);
            it = locks_
                   .emplace(key, std::make_unique<lock_entry>(std::move(name)))
                   .first;
        }
        ++it->second->active;
        return it->second.get();
    }

    ss::sstring name_prefix_;
    chunked_hash_map<Key, std::unique_ptr<lock_entry>> locks_;
};

/// \brief Per-entity read/write lock map using ss::rwlock.
///
/// Same lifecycle semantics as entity_lock_map but uses rwlocks
/// for shared/exclusive access patterns.
template<typename Key>
class entity_rwlock_map {
    struct lock_entry {
        ss::rwlock rw;
        uint32_t active{0};
    };

public:
    using tracked_units
      = detail::tracked_lock_units<Key, lock_entry, ss::rwlock::holder>;

    entity_rwlock_map() = default;

    ss::future<chunked_vector<tracked_units>>
    acquire_read(absl::btree_set<Key> keys) {
        chunked_vector<tracked_units> result;
        result.reserve(keys.size());
        for (const auto& key : keys) {
            auto* entry = get_or_create(key);
            try {
                auto holder = co_await entry->rw.hold_read_lock();
                result.push_back(
                  tracked_units(std::move(holder), entry, &locks_, key));
            } catch (...) {
                if (--entry->active == 0) {
                    locks_.erase(key);
                }
                throw;
            }
        }
        co_return result;
    }

    ss::future<chunked_vector<tracked_units>>
    acquire_write(absl::btree_set<Key> keys) {
        chunked_vector<tracked_units> result;
        result.reserve(keys.size());
        for (const auto& key : keys) {
            auto* entry = get_or_create(key);
            try {
                auto holder = co_await entry->rw.hold_write_lock();
                result.push_back(
                  tracked_units(std::move(holder), entry, &locks_, key));
            } catch (...) {
                if (--entry->active == 0) {
                    locks_.erase(key);
                }
                throw;
            }
        }
        co_return result;
    }

    size_t size() const { return locks_.size(); }

private:
    lock_entry* get_or_create(const Key& key) {
        auto it = locks_.find(key);
        if (it == locks_.end()) {
            it = locks_.emplace(key, std::make_unique<lock_entry>()).first;
        }
        ++it->second->active;
        return it->second.get();
    }

    chunked_hash_map<Key, std::unique_ptr<lock_entry>> locks_;
};

} // namespace cloud_topics::l1
