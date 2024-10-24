/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/fwd.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/types.h"
#include "container/intrusive_list_helpers.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>

#include <algorithm>
#include <chrono>
#include <exception>
#include <map>
#include <variant>

namespace cloud_storage {

using manifest_cache_key = std::tuple<model::ntp, model::offset>;

/// Materialized spillover manifest
///
/// The object contains the manifest, semaphore units,
/// and it is also a part of an intrusive list.
struct materialized_manifest
  : ss::enable_shared_from_this<materialized_manifest> {
    materialized_manifest(
      const model::ntp& ntp,
      model::initial_revision_id rev,
      ssx::semaphore_units u)
      : manifest(ntp, rev)
      , _units(std::move(u)) {}

    materialized_manifest(spillover_manifest manifest, ssx::semaphore_units u)
      : manifest(std::move(manifest))
      , _units(std::move(u)) {}

    manifest_cache_key get_key() const {
        return std::make_tuple(
          manifest.get_ntp(),
          manifest.get_start_offset().value_or(model::offset{}));
    }

    materialized_manifest() = delete;
    ~materialized_manifest() = default;
    materialized_manifest(const materialized_manifest&) = delete;
    materialized_manifest(materialized_manifest&&) = delete;
    materialized_manifest& operator=(const materialized_manifest&) = delete;
    materialized_manifest& operator=(materialized_manifest&&) = delete;

    spillover_manifest manifest;
    ssx::semaphore_units _units;
    intrusive_list_hook _hook;
    /// Flag which is set to true when the manifest is being evicted
    /// from the cache.
    bool evicted{false};
};

/// Collection of materialized manifests
/// The cache stores manifests in memory (hence "materialized").
/// The memory limit is specified in bytes. The cache uses LRU
/// eviction policy and will try to evict least recently used
/// materialized manifest.
class materialized_manifest_cache {
    using access_list_t
      = intrusive_list<materialized_manifest, &materialized_manifest::_hook>;

public:
    /// C-tor
    ///
    /// \param capacity_bytes max size of all manifests stored in the cache
    explicit materialized_manifest_cache(size_t capacity_bytes);

    /// Reserve space to store next manifest.
    ///
    /// \note This method will wait until another manifest could be evicted.
    ///       The size of the manifest has to be passed as a parameter. The
    ///       cache has fixed capacity. If the request is larger than the
    ///       capacity it will be clamped down to capacity. As result the
    ///       oversized request will have to evict everything else.
    /// \throw ss::timed_out_error if timeout expired. The evicted elements
    ///        are returned to the cache in this case.
    /// \param size_bytes is a size of the manifest in bytes
    /// \param ctxlog logger of the caller
    /// \param timeout is a timeout for the operation
    /// \return future that result in acquired semaphore units
    ss::future<ssx::semaphore_units> prepare(
      size_t size_bytes,
      retry_chain_logger& ctxlog,
      std::optional<ss::lowres_clock::duration> timeout = std::nullopt);

    /// Return number of elements stored in the cache
    size_t size() const noexcept;

    /// Return total size of the cache content.
    /// The result can't be greater than capacity even if the
    /// content exceeds the capacity. This is the result of the oversized
    /// manifest handling in the cache. Oversized manifest assumes size which
    /// is equal to capacity of the cache to be able to get in.
    size_t size_bytes() const noexcept;

    /// Put manifest into the cache
    ///
    /// \param s is a semaphore units acquired using 'prepare' method
    /// \param manifest is a spillover manifest to store
    /// \param ctxlog logger provided by the caller
    /// \note if the manifest with the same start-offset is already
    ///       present the manifest won't be added. Semaphore units
    ///       will be returned.
    void put(
      ssx::semaphore_units s,
      spillover_manifest manifest,
      retry_chain_logger& ctxlog);

    /// Find manifest by its key
    ss::shared_ptr<materialized_manifest>
    get(const manifest_cache_key& key, retry_chain_logger& ctxlog);

    /// Check manifest by its base offset
    bool contains(const manifest_cache_key& key);

    /// Move element forward to avoid its eviction
    ///
    /// \param key is ntp + start offset of the manifest
    /// \returns true on success, false if the manifest is evicted
    bool promote(const manifest_cache_key& key);

    /// Shift element one step forward to avoid eviction
    ///
    /// \param manifest is a manifest pointer
    /// \returns true on success, false if the manifest is evicted
    bool promote(ss::shared_ptr<materialized_manifest>& manifest);

    /// Remove element from cache
    ///
    /// \param key is ntp + start offset of the manifest
    /// \return size of the removed manifest in bytes or 0 if manifest wasn't
    ///         found
    /// \note The method removes the manifest from the cache and releases
    ///       semaphore units so new  manifest can let into the cache. But the
    ///       method doesn't guarantee that the memory consumed by the removed
    ///       manifest is freed. The user might still have a pointer to the
    ///       manifest that will prevent it from being deleted.
    size_t remove(const manifest_cache_key& key, retry_chain_logger& ctxlog);

    /// Starts the cache.
    ss::future<> start();

    /// Stop the cache.
    /// Ensure all async operations are completed.
    ss::future<> stop();

    /// Set new capacity
    ///
    /// \param new_size is a new size of the cache
    /// \param timeout is an optional timeout value for the
    ///                operation
    ss::future<> set_capacity(
      size_t new_size,
      std::optional<ss::lowres_clock::duration> timeout = std::nullopt);

    size_t get_capacity() const { return _capacity_bytes; }

private:
    using map_t
      = std::map<manifest_cache_key, ss::shared_ptr<materialized_manifest>>;

    /// Evict manifest pointed by the iterator
    ///
    /// \param it points to the elements to remove
    /// \param rollback is a temporary storage for the element used for rollback
    /// \return size of the evicted manifest in bytes
    /// \note the manifest could still be used by the cursor
    ///       after eviction.
    size_t evict(
      map_t::iterator it, access_list_t& rollback, retry_chain_logger& ctxlog);

    access_list_t::iterator
    lookup_eviction_rollback_list(const manifest_cache_key& key);

    /// Restore manifest temporarily stored in the _eviction_rollback list.
    void rollback(const manifest_cache_key& key, retry_chain_logger& ctxlog);

    /// Remove manifest from the _eviction_rollback list.
    void discard_rollback_manifest(
      const manifest_cache_key& key, retry_chain_logger& ctxlog);

    /// Current capacity of the cache in bytes
    size_t _capacity_bytes;
    /// Storage for manifests
    map_t _cache;
    ss::gate _gate;
    ssx::semaphore _sem;
    /// LRU order list (least recently used elements are in the back of the
    /// list)
    access_list_t _access_order;
    /// Eviction list that contains all manifests which are being evicted (there
    /// is an ongoing call to prepare that tries to evict these elements)
    access_list_t _eviction_rollback;
    /// Pool of unused semaphore units. Initially, the semaphore count is set to
    /// some predefined large value minus '_capacity_bytes'. Then, when the
    /// capacity changes the units are returned back to '_reserved' or split
    /// from '_reserved'. This solves the problem of static semaphore count
    /// which can't be changed without recreating the semaphore and makes
    /// difficult cache resizing.
    ssx::semaphore_units _reserved;
};

} // namespace cloud_storage
