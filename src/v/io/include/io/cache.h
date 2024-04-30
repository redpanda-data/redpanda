/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <optional>

/**
 * \defgroup cache Cache
 *
 * An implementation of the s3-fifo cache eviction algorithm.
 *
 * Yang, et al., "FIFO queues are all you need for cache eviction", SOSP '23,
 * https://dl.acm.org/doi/10.1145/3600006.3613147
 *
 * The s3-fifo eviction algorithm uses three queues named small, main, and
 * ghost. The implementation of the s3-fifo algorithm provides a concrete
 * container for the small and main queues, and the user implements the ghost
 * queue and any required indexing.
 *
 * The motivation for the ghost queue existing as a separate data structure
 * provided by the application is that the ghost queue implementation can be a
 * source for optimization depending on the usage context. For example, the page
 * cache uses a btree_map because we want ordering for entries, but another
 * application could use a hash table. Both of those are associative container,
 * so in principle we could make the container type a template. However, the
 * ghost fifo queue could also be implemented as a literal fifo queue, which may
 * be how the open-file-handle cache ends up looking.
 *
 * Usage
 * =====
 *
 * Consider the following entry type and an entry container registry:
 *
 *     struct entry {
 *         std::string address;
 *         std::string description;
 *         const char* image;
 *     };
 *
 *     std::map<std::string, std::unique_ptr<entry>> entries;
 *
 * The first step to transforming this example to provide a bounded size cache
 * using the s3-fifo eviction algorithm is to add a control structure to the
 * entry that stores internal metadata and provides communication between the
 * application and the cache.
 *
 *     struct entry {
 *         std::string address;
 *         std::string description;
 *         const char* image;
 *
 *         io::cache_hook hook;
 *     };
 *
 * Next we'll define an instance of the cache to hold objects of type entry.
 *
 *     struct evict {
 *         bool operator()(entry& e) noexcept {
 *             e.address.clear();
 *             e.description.clear();
 *             free(e.image);
 *             return true;
 *         }
 *     };
 *
 *     using cache_type = io::cache<entry, &entry::hook,
 *       evict, io::default_cache_cost>;
 *
 * The `evict` callable implements the application-specific eviction operation,
 * which in this case frees allocated memory. The type `io::default_cache_cost`
 * provides a default cost function useful to specifiying cache capacity in
 * terms of the number of entries in the cache.
 *
 * Finally, instantiate the cache with a maximum capacity and the desired size
 * of the small queue (10% is a reasonable choice).
 *
 *     io::cache::config config{.capacity = 250, .small_size = 25};
 *     cache_type cache{config};
 *
 * Call `cache::insert` to add an entry into the cache:
 *
 *     auto res = entries.emplace("e0", std::make_unique<entry>(...));
 *     cache.insert(*res.first.second);
 *
 * When the application interacts with an entry it should communicate this to
 * the cache by touching the entry:
 *
 *     auto e = lookup("e0");
 *     compress(e.image);
 *     e.hook.touch();
 *
 * When inserting an item into the cache, existing items may be evicted to make
 * space for the new entry. The application can detect evicted entries either by
 * detecting side effects from the eviction function, or by calling `evicted()`
 * on the entry's hook member.
 *
 * If an entry has been evicted, it should be re-inserted into the cache after
 * handling the cache miss. For example:
 *
 *     auto it = entries.find("e0");
 *
 *     // entry is not in the index
 *     if (it == entries.end()) {
 *         auto e0 = handle_cache_miss("e0");
 *         auto res = entries.emplace("e0", std::move(e0));
 *         cache.insert(*res.first.second);
 *     }
 *
 *     // entry is in index, but was evicted
 *     if (it->second.hook.evicted()) {
 *         handle_cache_miss("e0", it->second);
 *         cache.insert(*it->second);
 *     }
 *
 * Because the cache may evict random entries, the application should consider
 * periodically removing evicted items from the index. However, it is generally
 * desired to not remove items that remain on the ghost queue. The application
 * can query ghost queue membership using:
 *
 *     if (entry.hook.evicted()) {
 *         if (!cache.ghost_queue_contains(entry)) {
 *             // can fully remove
 *         }
 *     }
 *
 * At any time an application may erase an entry using:
 *
 *     cache.remove(entry);
 *
 * Note however that it is required that entries be removed from the cache if
 * they haven't yet been evicted. This is normally important in shutdown
 * scenarios, and is usually as straightforward as removing all the entries in a
 * destructor or other convenient shutdown method.
 *
 * When handling a cache miss for an entry that is on the ghost queue, the entry
 * can be rehydrated in place, but if a replacement entry is constructed the
 * state of the hook should be preserved. The default copy-constructor will
 * handle this automatically.
 *
 * Exceeding limits
 * ================
 *
 * During eviction the algorithm may find that no item is evictable. An item
 * being non-evictable is an application-specific choice. For example, a page
 * cache may mark dirty pages as non-evictable. The eviction algorithm has made
 * a policy decision that if this condition occurs that insertions are allowed
 * to be proceed. There are three arguments for doing this. First, we expect
 * users to make memory or resource reservations before insertion. Second, we
 * believe this would be an exceedingly rare condition, especially for our
 * primary use case as a large page cache. And finally, it is easier to build a
 * robust / exception-safe insertion method and rely on the caller making the
 * necessary resource reservations.
 *
 * Differences from s3-fifo
 * ========================
 *
 * This implementation allows entries to be exempt from eviction. For example in
 * a page cache dirty data should not be evicted until its written to disk. This
 * means that in practice we do not follow a strict fifo order of eviction.
 *
 * @{
 */

namespace experimental::io {

namespace testing_details {
class cache_hook_accessor;
}; // namespace testing_details

/**
 * Specifies that a callable type can be used as a cache eviction function.
 *
 * The cache_evictor concept specifies that the callable type F can be used as
 * a cache eviction callback in \ref cache.
 *
 * The function is passed a reference to a cache entry that is a candidate for
 * eviction from the cache. If the candidate entry should be evicted then the
 * function should return true, in which case the entry will be fully removed
 * from the cache. If the function returns false then the entry is not evicted
 * and the cache and entry's cache hook will not be modified.
 *
 * When an entry is selected for eviction the function may perform additional
 * operations specific to the application. For example, the function may release
 * memory or other resources associated with the entry.
 */
template<typename F, typename E>
concept cache_evictor = requires(F func, E& entry) {
    requires noexcept(func(entry));
    { func(entry) } -> std::same_as<bool>;
};

/**
 * Specifies that a callable type can be used as a cache eviction cost function.
 *
 * The cache_cost concept specifies that a callback type F can be used as a
 * cache eviction callback in \ref cache.
 *
 * The function is passed a reference to a cache entry and should return the
 * cost of storing the entry in the cache. The cost of an entry is expected to
 * be expressed in the same units as the capacity of the cache. For example, the
 * capacity of the cache could be expressed in terms of number of entries, in
 * which case the cost function should return the value `1`.
 *
 * It is expected that the cost function for a given entry is constant. This is
 * because the calculated cost when an entry is inserted into the cache is not
 * stored. Thus in order to achieve accurate accounting, when an entry is
 * removed or evicted from the cache, the same cost needs to be available.
 */
template<typename F, typename E>
concept cache_cost = requires(F func, const E& entry) {
    requires noexcept(func(entry));
    { func(entry) } -> std::same_as<size_t>;
};

/**
 * The metadata and control structure for a cache entry.
 *
 * An entry in the cache should embed a hook which stores metadata and contains
 * data structures for the cache to manage the entry.
 *
 *     struct entry {
 *         std::string data;
 *         cache_hook hook;
 *     };
 *
 * The cache hook also provides an interface between the entry and the cache for
 * communicating entry state changes such as the entry being evicted, or the
 * application indicating that the entry has been used in a meaningful way.
 *
 * When the application using the cache rehydrates an entry on the ghost queue
 * it is important to maintain the metadata contained in the cache hook. If the
 * entry structure is re-used then it should be sufficient to avoid resetting
 * the cache hook member. However, if the entry is destroyed and a replacement
 * is created, then the application should initialize the hook in the new entry
 * from the hook in the previous entry.
 */
class cache_hook {
public:
    cache_hook() noexcept = default;

    /**
     * Copy constructor.
     *
     * The value of evicted() will be false for the newly constructed hook.
     */
    cache_hook(const cache_hook&) = default;

    cache_hook& operator=(const cache_hook&) = delete;
    cache_hook(cache_hook&&) noexcept = delete;
    cache_hook& operator=(cache_hook&&) noexcept = delete;
    ~cache_hook() noexcept = default;

    /**
     * Mark a notable interaction with the entry containing this hook.
     */
    void touch() noexcept { freq_ = std::min(freq_ + 1, 3); }

    /**
     * Check if an entry containing this hook is in the cache.
     *
     * This method only considers the small and main cache queues, and does not
     * reflect membership on the ghost queue. This method will return true for a
     * default constructured hook.
     *
     * An entry need not check if an entry has been evicted using this
     * interface, and may instead arrange for a side-effect of the cache's
     * configured eviction function to mark an entry as evicted. Consider for
     * example the following entry and eviction function:
     *
     *     struct entry { std::unique_ptr<int> ptr; }
     *
     *     struct evict {
     *         bool operator()(entry& e) {
     *             e.ptr = nullptr;
     *             return true;
     *         }
     *     };
     *
     * An application may then use `e.ptr` to indicate that a entry has been
     * evicted instead of invoking cache_hook::evicted.
     */
    [[nodiscard]] bool evicted() const noexcept { return !hook_.is_linked(); }

private:
    using hook_type = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;

    /*
     * the location is checked only in cache::erase where the case of the hook
     * being on neither main nor small is indicated by evicted() being true.
     */
    enum class location : uint8_t { main, small };

    template<
      typename T,
      cache_hook T::*Hook,
      cache_evictor<T> Evictor,
      cache_cost<T> Cost>
    friend class cache;

    friend class testing_details::cache_hook_accessor;

    uint8_t freq_{0};
    std::optional<uint64_t> ghost_insertion_time_;
    location location_{};
    hook_type hook_;
};

/**
 * Default cache eviction function.
 *
 * The default_cache_evictor callable type allows eviction and makes no
 * modification to entry. Users may use cache_hook::evicted to query the
 * eviction status an entry.
 */
struct default_cache_evictor {
    /// Returns true.
    bool operator()(auto&& /*entry*/) noexcept { return true; }
};

/**
 * Default cache cost function.
 *
 * The default_cache_cost callable type returns a constant entry cost value of
 * one which is appropriate for a cache capacity expressed in terms of the
 * maximum number of cache entries.
 */
struct default_cache_cost {
    /// Returns unit cost.
    size_t operator()(const auto& /*entry*/) noexcept { return 1; }
};

/**
 * An implementation of the s3-fifo cache eviction algorithm.
 *
 * See \ref cache for more details and usage instructions.
 */
template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor = default_cache_evictor,
  cache_cost<T> Cost = default_cache_cost>
class cache {
public:
    /**
     * Cache configuration parameters.
     */
    struct config {
        /// Total capacity of the cache.
        size_t cache_size;
        /// Total capacity of the small queue.
        size_t small_size;
    };

    /**
     * Create a cache with the provided \p config.
     */
    explicit cache(
      config config,
      const Evictor& evict = Evictor(),
      const Cost& cost = Cost()) noexcept;

    cache(const cache& other) = delete;
    cache& operator=(const cache& other) = delete;
    cache(cache&& other) noexcept = delete;
    cache& operator=(cache&& other) noexcept = delete;
    ~cache() noexcept = default;

    /**
     * Insert the \p entry into the cache.
     *
     * If the cache is full then entries may be evicted in order to accomodate
     * the new entry.
     */
    void insert(T& entry) noexcept;

    /**
     * Remove the \p entry from the cache.
     *
     * If the entry is not in the cache then the function has no effect.
     */
    void remove(const T& entry) noexcept;

    /**
     * Returns true if the entry is on the ghost queue.
     */
    [[nodiscard]] bool ghost_queue_contains(const T& entry) const noexcept;

    /**
     * Evict one entry from the cache.
     *
     * Returns true if an entry was evicted, or false otherwise.
     */
    bool evict() noexcept;

    /**
     * Cache statistics.
     */
    struct stat {
        /// Current size of the small queue.
        size_t small_queue_size;
        /// Current size of the main queue.
        size_t main_queue_size;
    };

    /**
     * Return the current cache statistics.
     */
    [[nodiscard]] stat stat() const noexcept;

private:
    struct hook_converter {
        using hook_type = cache_hook::hook_type;
        using hook_ptr = hook_type*;
        using const_hook_ptr = const hook_type*;
        using value_type = T;
        using pointer = value_type*;
        using const_pointer = const value_type*;

        static hook_ptr to_hook_ptr(value_type& value) {
            return &((value.*Hook).hook_);
        }

        static const_hook_ptr to_hook_ptr(const value_type& value) {
            return &((value.*Hook).hook_);
        }

        static pointer to_value_ptr(hook_ptr hook) {
            return boost::intrusive::get_parent_from_member(
              boost::intrusive::get_parent_from_member(
                hook, &cache_hook::hook_),
              Hook);
        }

        static const_pointer to_value_ptr(const_hook_ptr hook) {
            return boost::intrusive::get_parent_from_member(
              boost::intrusive::get_parent_from_member(
                hook, &cache_hook::hook_),
              Hook);
        }
    };

    using list_type = boost::intrusive::list<
      T,
      boost::intrusive::function_hook<hook_converter>,
      boost::intrusive::constant_time_size<false>>;

    bool evict_small() noexcept;
    bool evict_main() noexcept;

    size_t max_cache_size_;
    size_t max_small_queue_size_;
    size_t max_main_queue_size_;

    Evictor evict_;
    Cost cost_;

    size_t small_queue_size_{0};
    size_t main_queue_size_{0};
    size_t ghost_queue_age_{0};

    list_type small_fifo_;
    list_type main_fifo_;
};

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
cache<T, Hook, Evictor, Cost>::cache(
  config config, const Evictor& evict, const Cost& cost) noexcept
  : max_cache_size_(config.cache_size)
  , max_small_queue_size_(config.small_size)
  , max_main_queue_size_(max_cache_size_ - max_small_queue_size_)
  , evict_(evict)
  , cost_(cost) {
    assert(max_cache_size_ > max_small_queue_size_);
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
void cache<T, Hook, Evictor, Cost>::insert(T& entry) noexcept {
    while ((small_queue_size_ + main_queue_size_) > max_cache_size_) {
        if (!evict()) {
            break;
        }
    }

    auto& hook = entry.*Hook;
    if (ghost_queue_contains(entry)) {
        // evict from ghost queue
        hook.ghost_insertion_time_.reset();

        main_fifo_.push_back(entry);
        hook.location_ = cache_hook::location::main;
        main_queue_size_ += cost_(entry);
    } else {
        small_fifo_.push_back(entry);
        hook.location_ = cache_hook::location::small;
        small_queue_size_ += cost_(entry);
    }
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
void cache<T, Hook, Evictor, Cost>::remove(const T& entry) noexcept {
    const auto& hook = entry.*Hook;

    if (hook.evicted()) {
        return;
    }

    switch (hook.location_) {
    case cache_hook::location::main:
        main_queue_size_ -= cost_(entry);
        main_fifo_.erase(main_fifo_.iterator_to(entry));
        break;

    case cache_hook::location::small:
        small_queue_size_ -= cost_(entry);
        small_fifo_.erase(small_fifo_.iterator_to(entry));
        break;
    }
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
bool cache<T, Hook, Evictor, Cost>::evict() noexcept {
    if (small_queue_size_ > max_small_queue_size_) {
        return evict_small();
    }
    return evict_main();
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
struct cache<T, Hook, Evictor, Cost>::stat
cache<T, Hook, Evictor, Cost>::stat() const noexcept {
    return {
      .small_queue_size = small_queue_size_,
      .main_queue_size = main_queue_size_,
    };
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
bool cache<T, Hook, Evictor, Cost>::ghost_queue_contains(
  const T& entry) const noexcept {
    /*
     * if the insertion time is unspecified then the entry is not on the ghost
     * queue. for example, it was evicted in insert, or has default-init hook.
     * otherwise, the entry is on the ghost queue if it hasn't yet "timed out":
     *
     *    expires = ghost_insertion_time + main queue capacity
     *        now = ghost_inserted
     *   on_ghost = expires > now
     */
    const auto& hook = entry.*Hook;
    if (hook.ghost_insertion_time_.has_value()) {
        auto expires = *hook.ghost_insertion_time_ + max_main_queue_size_;
        return expires > ghost_queue_age_;
    }
    return false;
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
bool cache<T, Hook, Evictor, Cost>::evict_small() noexcept {
    for (auto it = small_fifo_.begin(); it != small_fifo_.end();) {
        auto& entry = *it;
        auto& hook = entry.*Hook;
        assert(hook.location_ == cache_hook::location::small);
        if (hook.freq_ > 1) {
            hook.freq_ = 0;

            // evict from small queue
            it = small_fifo_.erase(it);
            const auto cost = cost_(entry);
            small_queue_size_ -= cost;

            // promote to main queue
            main_fifo_.push_back(entry);
            hook.location_ = cache_hook::location::main;
            main_queue_size_ += cost;

            if (main_queue_size_ > max_main_queue_size_) {
                if (evict_main()) {
                    return true;
                }
            }

        } else if (evict_(entry)) {
            // evict from small queue
            it = small_fifo_.erase(it);
            const auto cost = cost_(entry);
            small_queue_size_ -= cost;

            // insert into ghost queue
            hook.ghost_insertion_time_ = ghost_queue_age_;
            ghost_queue_age_ += cost;
            return true;

        } else {
            ++it;
        }
    }
    return false;
}

template<
  typename T,
  cache_hook T::*Hook,
  cache_evictor<T> Evictor,
  cache_cost<T> Cost>
bool cache<T, Hook, Evictor, Cost>::evict_main() noexcept {
    for (auto it = main_fifo_.begin(); it != main_fifo_.end();) {
        auto& entry = *it;
        auto& hook = entry.*Hook;
        assert(hook.location_ == cache_hook::location::main);
        if (hook.freq_ > 0) {
            --hook.freq_;

            // promote within main queue
            it = main_fifo_.erase(it);
            main_fifo_.push_back(entry);

        } else if (evict_(entry)) {
            // evict from main queue
            it = main_fifo_.erase(it);
            main_queue_size_ -= cost_(entry);
            return true;

        } else {
            ++it;
        }
    }
    return false;
}

/**
 * @}
 */

} // namespace experimental::io

template<
  typename T,
  experimental::io::cache_hook T::*Hook,
  experimental::io::cache_evictor<T> Evictor,
  experimental::io::cache_cost<T> Cost>
struct fmt::formatter<experimental::io::cache<T, Hook, Evictor, Cost>>
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const experimental::io::cache<T, Hook, Evictor, Cost>& cache,
      FormatContext& ctx) const {
        const auto stat = cache.stat();
        return fmt::format_to(
          ctx.out(),
          "main {} small {}",
          stat.main_queue_size,
          stat.small_queue_size);
    }
};
