/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/intrusive_list_helpers.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <memory>

namespace ss = seastar;

namespace util {

/// Tracks allocations and deallocations of memory. A mem_tracker can have
/// child trackers effectively forming an n-ary tree like structure.
class mem_tracker {
public:
    explicit mem_tracker(ss::sstring label)
      : _label(std::move(label)) {}
    mem_tracker(mem_tracker&) = delete;
    mem_tracker(mem_tracker&&) = default;
    mem_tracker& operator=(mem_tracker&) = delete;
    mem_tracker& operator=(mem_tracker&&) = delete;
    ~mem_tracker() = default;

    void allocate(int64_t bytes) { _consumption += bytes; }

    void deallocate(int64_t bytes) { _consumption -= bytes; }

    ss::shared_ptr<mem_tracker> create_child(ss::sstring);

    /// Consumption of self + all the children rooted with this tracker..
    int64_t consumption() const;

    /// Pretty prints the tracker tree rooted at this instance.
    /// Example:
    /// |
    /// root: 32.000bytes
    /// | |
    /// | child1: 24.000bytes
    /// | | |
    /// | | child11: 0.000bytes
    /// | | | |
    /// | | | child111: 0.000bytes
    /// | |
    /// | child2: 0.000bytes
    ss::sstring pretty_print_ascii() const;

    ss::sstring pretty_print_json() const;

private:
    ss::sstring _label;
    int64_t _consumption = 0;
    intrusive_list_hook _hook;
    intrusive_list<mem_tracker, &mem_tracker::_hook> _children;
};

/// An allocator paired with a memory tracker to account for consumption.
///
/// When the enclosing container is copied, the same underlying mem_tracker
/// tracks both the copy and the source objects.
///
/// When the enclosing container is moved, the underlying mem_tracker now
/// tracks the the object that is moved into.
///
/// When the enclosing container is swapped, the underlying mem_trackers
/// are swapped.

/// Note on thread-safety: This allocator implementation has the same thread
/// safetly guantees as the underlying allocator when used in a seastar
/// context. All the shard local read/writes can be lock free and safe by
/// default. All cross shard/thread accesses are safe owing to the use
/// of atomics for accounting consumption.

template<class T, class allocator = std::allocator<T>>
class tracking_allocator : public allocator {
public:
    // allocator_traits for AllocatorAwareContainer
    using allocator_type = T;
    using value_type = typename allocator::value_type;
    using difference_type = typename allocator::difference_type;
    using size_type = typename allocator::size_type;
    // Propagates the allocator on copy/move/swap assignment so
    // that the same allocator can track the new container.
    using propagate_on_container_copy_assignment = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    using is_always_equal = std::false_type;

    explicit tracking_allocator(ss::shared_ptr<mem_tracker> tracker)
      : _tracker(tracker) {}

    template<class U>
    explicit tracking_allocator(const tracking_allocator<U>& other)
      : allocator(other)
      , _tracker(other.tracker()) {}

    template<class U>
    struct rebind {
        using other = tracking_allocator<U, std::allocator<U>>;
    };

    tracking_allocator<T, allocator>
    select_on_container_copy_construction() const {
        return tracking_allocator<T, allocator>(_tracker);
    }

    [[nodiscard]] T* allocate(std::size_t n) {
        _tracker->allocate(n * sizeof(T));
        return allocator::allocate(n);
    }

    void deallocate(T* p, std::size_t n) {
        allocator::deallocate(p, n);
        _tracker->deallocate(n * sizeof(T));
    }

    ss::shared_ptr<mem_tracker> tracker() const { return _tracker; }

private:
    /// It is a conscious choice to use a shared_ptr instead of an
    /// lw_shared_ptr to allow threaad-safety when shared across cores.
    ss::shared_ptr<mem_tracker> _tracker;
};

namespace mem_tracked {
/// Since allocator is typically the last parameter for stl containers,
/// these template utilities help avoid template boiler plate by auto
/// setting the intermediate parameters to defaults.
template<class T>
concept is_map = requires {
    typename T::key_type;
    typename T::value_type;
    typename T::mapped_type;
    typename T::key_compare;
    typename T::allocator_type;
};

template<class T>
concept is_unordered_map = requires {
    typename T::key_type;
    typename T::value_type;
    typename T::hasher;
    typename T::mapped_type;
    typename T::key_equal;
    typename T::allocator_type;
};

template<class T>
concept is_set = requires {
    typename T::key_type;
    typename T::key_compare;
    typename T::allocator_type;
};

template<class T>
concept is_unordered_set = requires {
    typename T::key_type;
    typename T::hasher;
    typename T::key_equal;
    typename T::allocator_type;
};

template<
  template<class...>
  class Map,
  class K,
  class V,
  class Hasher = typename Map<K, V>::hasher,
  class KeyEq = typename Map<K, V>::key_equal,
  class Allocator = tracking_allocator<typename Map<K, V>::value_type>>
using unordered_map_t = Map<K, V, Hasher, KeyEq, Allocator>;

template<
  template<class...>
  class Map,
  class K,
  class V,
  class Compare = typename Map<K, V>::key_compare,
  class Allocator = tracking_allocator<typename Map<K, V>::value_type>>
using map_t = Map<K, V, Compare, Allocator>;

template<
  template<class...>
  class Set,
  class K,
  class Hash = typename Set<K>::hasher,
  class KeyEqual = typename Set<K>::key_equal,
  class Allocator = tracking_allocator<K>>
using unordered_set_t = Set<K, Hash, KeyEqual, Allocator>;

template<
  template<class...>
  class Set,
  class K,
  class Compare = typename Set<K>::key_compare,
  class Allocator = tracking_allocator<typename Set<K>::key_type>>
using set_t = Set<K, Compare, Allocator>;

template<
  template<class...>
  class Map,
  class K,
  class V,
  class Hasher = typename Map<K, V>::hasher,
  class KeyEq = typename Map<K, V>::key_equal,
  class Allocator = tracking_allocator<typename Map<K, V>::value_type>>
requires is_unordered_map<unordered_map_t<Map, K, V, Hasher, KeyEq, Allocator>>
auto map(ss::shared_ptr<mem_tracker> tracker)
  -> unordered_map_t<Map, K, V, Hasher, KeyEq, Allocator> {
    using value_type = typename Map<K, V>::value_type;
    return Map<K, V, Hasher, KeyEq, Allocator>(
      tracking_allocator<value_type>{tracker});
}

template<
  template<class...>
  class Map,
  class K,
  class V,
  class Compare = typename Map<K, V>::key_compare,
  class Allocator = tracking_allocator<typename Map<K, V>::value_type>>
requires is_map<map_t<Map, K, V, Compare, Allocator>>
auto map(ss::shared_ptr<mem_tracker> tracker)
  -> map_t<Map, K, V, Compare, Allocator> {
    using value_type = typename Map<K, V>::value_type;
    return Map<K, V, Compare, Allocator>(
      tracking_allocator<value_type>{tracker});
}

template<
  template<class...>
  class Set,
  class K,
  class Hash = typename Set<K>::hasher,
  class KeyEqual = typename Set<K>::key_equal,
  class Allocator = tracking_allocator<K>>
requires is_unordered_set<unordered_set_t<Set, K, Hash, KeyEqual, Allocator>>
auto set(ss::shared_ptr<mem_tracker> tracker)
  -> unordered_set_t<Set, K, Hash, KeyEqual, Allocator> {
    return Set<K, Hash, KeyEqual, Allocator>(tracking_allocator<K>{tracker});
}

template<
  template<class...>
  class Set,
  class K,
  class Compare = typename Set<K>::key_compare,
  class Allocator = tracking_allocator<K>>
requires is_set<set_t<Set, K, Compare, Allocator>>
auto set(ss::shared_ptr<mem_tracker> tracker)
  -> set_t<Set, K, Compare, Allocator> {
    return Set<K, Compare, Allocator>(tracking_allocator<K>{tracker});
}
}; // namespace mem_tracked

}; // namespace util
