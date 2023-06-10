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
#include "cloud_storage/materialized_manifest_cache.h"
#include "cloud_storage/probe.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
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

/// Search query type
using async_view_search_query_t
  = std::variant<model::offset, kafka::offset, model::timestamp>;

std::ostream& operator<<(std::ostream&, const async_view_search_query_t&);

class async_manifest_view;

/// The structure represents evicted manifest. The difference between this
/// state and uninitialized state is that 'stale_manifest' will trigger
/// timed_out error on first access.
struct stale_manifest {
    /// Cached last offset value + 1 of the stale manifest
    model::offset next_offset;
};

/// Type that represents section of full metadata. It can either contain
/// a materialized spillover manifest or reference to main STM manifest.
using manifest_section_t = std::variant<
  std::monostate,
  stale_manifest,
  ss::shared_ptr<materialized_manifest>,
  std::reference_wrapper<const partition_manifest>>;

/// Result of the ListObjectsV2 scan
struct spillover_manifest_list {
    /// List of manifest paths
    std::deque<remote_manifest_path> manifests;
    /// List of decoded path components (offsets and timestamps)
    std::deque<spillover_manifest_path_components> components;
    /// List of manifest sizes (in binary format)
    std::deque<size_t> sizes;
};

class async_manifest_view_cursor;

/// Service that maintains a view of the entire
/// partition metadata including STM manifest and
/// all spillover manifests. Spillover manifests are
/// materialized on demand.
class async_manifest_view {
    friend class async_manifest_view_cursor;

public:
    async_manifest_view(
      ss::sharded<remote>& remote,
      ss::sharded<cache>& cache,
      const partition_manifest& stm_manifest,
      cloud_storage_clients::bucket_name bucket,
      partition_probe& probe);

    ss::future<> start();
    ss::future<> stop();

    /// Get active spillover manifests asynchronously
    ///
    /// \note the method may hydrate manifests in the cache or
    ///       wait until the memory for the manifest becomes
    ///       available.
    ss::future<
      result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
    get_active(async_view_search_query_t q) noexcept;

    /// Get inactive spillover manifests which are waiting for
    /// retention
    ss::future<
      result<std::unique_ptr<async_manifest_view_cursor>, error_outcome>>
    get_retention_backlog() noexcept;

    bool is_empty() const noexcept;

    const model::ntp& get_ntp() const { return _stm_manifest.get_ntp(); }

    /// Return STM manifest
    const partition_manifest& stm() const { return _stm_manifest; }

    /// Structure that describes how the start archive offset has
    /// to be advanced forward.
    struct archive_start_offset_advance {
        model::offset offset;
        model::offset_delta delta;
    };

    /// Compute how the start archive offset has to be advanced based on current
    /// log size and retention parameters.
    ss::future<result<archive_start_offset_advance, error_outcome>>
    compute_retention(
      std::optional<size_t> size_limit,
      std::optional<std::chrono::milliseconds> time_limit) noexcept;

private:
    ss::future<result<archive_start_offset_advance, error_outcome>>
    time_based_retention(std::chrono::milliseconds time_limit) noexcept;

    ss::future<result<archive_start_offset_advance, error_outcome>>
    size_based_retention(size_t size_limit) noexcept;

    ss::future<result<archive_start_offset_advance, error_outcome>>
    offset_based_retention() noexcept;

    ss::future<> run_bg_loop();

    /// Return true if the offset belongs to the archive
    bool is_archive(async_view_search_query_t o);

    /// Returns true if the offset belongs to the archival STM manifest
    bool is_stm(async_view_search_query_t o);

    /// Get spillover manifest by offset/timestamp
    ss::future<result<manifest_section_t, error_outcome>>
    get_materialized_manifest(async_view_search_query_t q) noexcept;

    /// Load manifest from the cloud
    ///
    /// On success put serialized copy into the cache. The method should only be
    /// called if the manifest is not available in the cache. The state of the
    /// view is not changed.
    ss::future<result<spillover_manifest, error_outcome>>
    hydrate_manifest(remote_manifest_path path) const noexcept;

    /// Load manifest from the cache
    ///
    /// The method reads manifest from the cache or downloads from the cloud.
    /// Local state is not changed. The returned manifest has to be stored
    /// in the view after the call.
    /// \throws
    ///     - not_found if the manifest doesn't exist
    ///     - repeat if the manifest is being downloaded already
    ///     - TODO
    ss::future<result<spillover_manifest, error_outcome>>
    materialize_manifest(remote_manifest_path path) const noexcept;

    /// Find index of the spillover manifest
    ///
    /// \param query is a search query, either an offset, a kafka offset or a
    ///              timestamp
    /// \return segment_meta struct that represents a spillover manifest or null
    std::optional<segment_meta>
    search_spillover_manifests(async_view_search_query_t query) const;

    /// Convert segment_meta to spillover manifest path
    remote_manifest_path
    get_spillover_manifest_path(const segment_meta& meta) const;

    mutable ss::gate _gate;
    ss::abort_source _as;
    cloud_storage_clients::bucket_name _bucket;
    ss::sharded<remote>& _remote;
    ss::sharded<cache>& _cache;
    partition_probe& _probe;

    const partition_manifest& _stm_manifest;
    mutable retry_chain_node _rtcnode;
    retry_chain_logger _ctxlog;
    config::binding<std::chrono::milliseconds> _timeout;
    config::binding<std::chrono::milliseconds> _backoff;
    config::binding<size_t> _read_buffer_size;
    config::binding<int16_t> _readahead_size;

    config::binding<std::chrono::milliseconds> _manifest_meta_ttl;

    materialized_manifest_cache& _manifest_cache;

    // BG loop state

    /// Materialization request which is sent to background fiber
    struct materialization_request_t {
        segment_meta search_vec;
        ss::promise<result<manifest_section_t, error_outcome>> promise;
        std::unique_ptr<hdr_hist::measurement> _measurement;
    };
    std::deque<materialization_request_t> _requests;
    ss::condition_variable _cvar;
};

enum class async_manifest_view_cursor_status {
    // The cursor is not set to any position and not materialized
    empty,
    // The cursor points to STM manifest
    materialized_stm,
    // The cursor points to spillover manifest
    materialized_spillover,
    // The materialized manifest that cursor pointed to was evicted
    evicted,
};

std::ostream& operator<<(std::ostream&, async_manifest_view_cursor_status);

/// The cursor can be used to traverse manifest
/// asynchronously. The full content of the manifest
/// can't b loaded into memory. Because of that the spillover
/// manifests has to be loaded into memory asynchronously while
/// the full manifest view is being traversed. This is done in
/// batches in the 'refresh' call.
///
/// The cursor has two important properties:
/// - offset range to cover, the cursor can only return manifests within the
///   pre-defined offset range;
/// - time budget limits amount of time the cursor can hold the materialized
///   manifest
class async_manifest_view_cursor {
public:
    /// Create cursor with allowed offset range limits
    ///
    /// \param view is an async_manifest_view instance
    /// \param begin is a start of the allowed offset range
    /// \param end_inclusive is an end of the allowed offset range
    /// \param timeout is a time budget of the cursor
    async_manifest_view_cursor(
      async_manifest_view& view,
      model::offset begin,
      model::offset end_inclusive,
      ss::lowres_clock::duration timeout);

    /// Seek to manifest that contains offset
    ss::future<result<bool, error_outcome>> seek(async_view_search_query_t q);

    async_manifest_view_cursor_status get_status() const;

    /// Move to the next manifest or fail
    ss::future<result<bool, error_outcome>> next();

    /// Shortcut to use with Seastar's future utils.
    ///
    /// The method returns 'stop_iteration' and it will also
    /// throw an exception instead of returning an error code.
    ss::future<ss::stop_iteration> next_iter();

    /// Return current manifest
    ///
    /// \return pointer to the current manifest or nullopt
    std::optional<std::reference_wrapper<const partition_manifest>>
    manifest() const;

    /// Pass current manifest to the functor
    ///
    /// The method guarantees absence of scheduling points between the
    /// calls to manifest methods. The manifest could be evicted after
    /// any scheduling point. The reference will be invalidated in this
    /// case.
    template<class Fn>
    auto manifest(Fn fn) {
        auto ref = manifest();
        vassert(ref.has_value(), "Invalid cursor, {}", _view.get_ntp());
        return fn(ref->get());
    }

private:
    void on_timeout();

    bool manifest_in_range(const manifest_section_t& m);

    /// Manifest view ref
    async_manifest_view& _view;

    manifest_section_t _current;

    ss::lowres_clock::duration _idle_timeout;
    ss::timer<ss::lowres_clock> _timer;
    model::offset _begin;
    model::offset _end;
};

} // namespace cloud_storage