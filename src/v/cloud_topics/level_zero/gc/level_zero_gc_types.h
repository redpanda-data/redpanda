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
#include "cloud_io/io_result.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_zero/gc/level_zero_gc_probe.h"
#include "cloud_topics/types.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/future.hh>

#include <chrono>
#include <expected>
#include <optional>

namespace cloud_topics::l0::gc {

/*
 * Object storage interface used by L0 GC.
 */
class object_storage {
public:
    object_storage() = default;
    object_storage(const object_storage&) = delete;
    object_storage(object_storage&&) = delete;
    object_storage& operator=(const object_storage&) = delete;
    object_storage& operator=(object_storage&&) = delete;
    virtual ~object_storage() = default;

    /*
     * Implementations are expected to limit the listing to only L0 data
     * objects, and provide the listing in _globally_ lexicographic order.
     */
    virtual seastar::future<std::expected<
      cloud_storage_clients::client::list_bucket_result,
      cloud_storage_clients::error_outcome>>
    list_objects(
      seastar::abort_source*,
      std::optional<cloud_storage_clients::object_key> prefix = std::nullopt,
      std::optional<ss::sstring> continuation_token = std::nullopt) = 0;

    virtual seastar::future<std::expected<void, cloud_io::upload_result>>
    delete_objects(
      seastar::abort_source*,
      chunked_vector<cloud_storage_clients::client::list_bucket_item>) = 0;
};

/*
 * Interface for computing the maximum epoch eligible for GC.
 */
class epoch_source {
public:
    struct partitions_snapshot {
        using partition_map = chunked_hash_map<
          model::topic_namespace,
          chunked_vector<model::partition_id>,
          model::topic_namespace_hash,
          model::topic_namespace_eq>;

        partition_map partitions;
        cluster_epoch snap_revision;
    };

    using partitions_max_gc_epoch = chunked_hash_map<
      model::topic_namespace,
      chunked_hash_map<model::partition_id, cluster_epoch>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    epoch_source() = default;
    epoch_source(const epoch_source&) = default;
    epoch_source(epoch_source&&) = delete;
    epoch_source& operator=(const epoch_source&) = default;
    epoch_source& operator=(epoch_source&&) = delete;
    virtual ~epoch_source() = default;

    void set_probe(level_zero_gc_probe* p) { probe_ = p; }

    /*
     * L0 objects with epochs <= the return value may be deleted. An
     * expected return value of std::nullopt is not an error, but rather
     * indicates that no GC eligible epoch could yet be determined.
     */
    virtual seastar::future<
      std::expected<std::optional<cluster_epoch>, std::string>>
    max_gc_eligible_epoch(seastar::abort_source*);

    /*
     * Snapshot of existing cloud topic partition identifiers along with the
     * maximum possible GC eligible epoch for the set of partitions.
     */
    virtual seastar::future<std::expected<partitions_snapshot, std::string>>
    get_partitions(seastar::abort_source*) = 0;

    /*
     * Reported max GC eligible epochs for cloud topic partitions.
     */
    virtual seastar::future<std::expected<partitions_max_gc_epoch, std::string>>
    get_partitions_max_gc_epoch(seastar::abort_source*) = 0;

protected:
    level_zero_gc_probe* probe_{nullptr};
};

/**
 * Interface for determining the total number of shards in the cluster
 * and the current shard's position in logical, ordered list of shard IDs
 * starting at 0 (node 0, shard 0) and ending at total_shards - 1.
 */
struct node_info {
    node_info() = default;
    node_info(const node_info&) = default;
    node_info(node_info&&) = delete;
    node_info& operator=(const node_info&) = default;
    node_info& operator=(node_info&&) = delete;
    virtual ~node_info() = default;

    virtual size_t shard_index() const = 0;
    virtual size_t total_shards() const = 0;
};

/// Interface for gating GC on system-level safety conditions.
///
/// Production implementations poll external signals (e.g. cluster health)
/// in a background fiber and cache the result so that can_proceed() is
/// synchronous and cheap. This is orthogonal to admin pause/start — even
/// if an operator calls start(), GC will not proceed while the safety
/// monitor reports not-ok.
class safety_monitor {
public:
    safety_monitor() = default;
    safety_monitor(const safety_monitor&) = delete;
    safety_monitor(safety_monitor&&) = delete;
    safety_monitor& operator=(const safety_monitor&) = delete;
    safety_monitor& operator=(safety_monitor&&) = delete;
    virtual ~safety_monitor() = default;

    struct result {
        bool ok;
        std::optional<ss::sstring> reason;
    };

    virtual result can_proceed() const = 0;

    virtual void start() {}
    virtual seastar::future<> stop() { return seastar::now(); }
};

/// Outcome of a collection round, used by the worker to decide
/// how long to sleep before the next round.
struct collection_outcome {
    enum class status : int8_t {
        /// Deleted objects — poll at throttle_progress.
        progress,
        /// Objects skipped because their epoch exceeds the
        /// collectible epoch — poll at throttle_no_progress.
        epoch_ineligible,
        /// Objects skipped because they are too young —
        /// sleep until the oldest one ages past the grace period.
        age_ineligible,
        /// No objects listed (empty storage or all deleted) —
        /// sleep for the full grace period.
        empty,
        /// Delete worker at capacity — poll at throttle_progress.
        at_capacity,
    };

    status st;

    explicit collection_outcome(status s)
      : st(s) {}
    size_t eligible() const { return eligible_; }

    /// Record that objects were submitted for deletion. Progress
    /// always takes precedence over any other status.
    void add_eligible(size_t n) {
        eligible_ += n;
        if (eligible_ > 0) {
            st = status::progress;
        }
    }

    /// Record that an object was skipped because its epoch exceeds
    /// the collectible epoch. Does not downgrade from progress.
    void mark_epoch_ineligible() {
        if (st != status::progress) {
            st = status::epoch_ineligible;
        }
    }

    /// Record that an object was skipped because it is younger
    /// than the grace period. Tracks the oldest such object so
    /// we can compute exactly when it becomes eligible.
    /// Does not downgrade from progress or epoch_ineligible.
    void
    mark_age_ineligible(std::chrono::system_clock::time_point last_modified) {
        if (
          !oldest_ineligible_modified_.has_value()
          || last_modified < oldest_ineligible_modified_.value()) {
            oldest_ineligible_modified_ = last_modified;
        }
        if (st != status::progress && st != status::epoch_ineligible) {
            st = status::age_ineligible;
        }
    }

    /// Merge another page's outcome into this one.
    void merge(const collection_outcome& other) {
        add_eligible(other.eligible_);
        if (other.oldest_ineligible_modified_.has_value()) {
            mark_age_ineligible(other.oldest_ineligible_modified_.value());
        }
        if (other.st == status::epoch_ineligible) {
            mark_epoch_ineligible();
        }
    }

    /// Compute the backoff for the age_ineligible case: how long
    /// until the oldest too-young object ages past the grace
    /// period. Returns nullopt if the oldest_ineligible timestamp
    /// is missing OR the object is already old enough (race or clock skew)
    std::optional<std::chrono::milliseconds>
    age_backoff(std::chrono::milliseconds grace_period) const {
        return oldest_ineligible_modified_
          .transform([grace_period](
                       std::chrono::system_clock::time_point oldest_modified) {
              return oldest_modified + grace_period;
          })
          .and_then(
            [](std::chrono::system_clock::time_point wake_at)
              -> std::optional<std::chrono::milliseconds> {
                auto now = std::chrono::system_clock::now();
                if (wake_at <= now) {
                    return std::nullopt;
                }
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                  wake_at - now);
            });
    }

    fmt::iterator format_to(fmt::iterator) const;

private:
    size_t eligible_{0};
    std::optional<std::chrono::system_clock::time_point>
      oldest_ineligible_modified_;
};

enum class collection_error : int8_t {
    // problem occurred interacting with the storage or epoch services
    service_error,
    // the cluster is reporting that no collectible epoch exists
    no_collectible_epoch,
    // object listing contained an invalid object name
    invalid_object_name,
};

/**
 * @brief Shard-local state for an instance of level_zero_gc
 *
 *   - paused: Paused indefinitely, call start() to run
 *   - running: GC will run until paused or stopped
 *   - resetting: reset() is draining in-flight work
 *   - stopping: stop() requested but there may be work still in flight
 *   - stopped: Permanently stopped.
 *   - safety_blocked: GC is started but the safety monitor is preventing
 *     collection (e.g. cluster unhealthy)
 */
enum class state : uint8_t {
    paused,
    running,
    resetting,
    stopping,
    stopped,
    safety_blocked,
};

std::string_view to_string_view(state s);
std::string_view to_string_view(collection_outcome::status s);

} // namespace cloud_topics::l0::gc
