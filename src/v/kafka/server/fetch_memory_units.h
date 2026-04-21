/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/seastarx.h"
#include "base/units.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "model/ktp.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace kafka {

using namespace std::chrono_literals;

class fetch_memory_units;

/***
 * This service handles allocating/releasing semaphore units for the fetch path.
 * It allocates units as `fetch_memory_units` which is safe to pass between
 * shards. And when `fetch_memory_units` is destroyed the units are returned to
 * the shard local instance of this service where they are collected and
 * periodically released back to the shard they were originally allocated from.
 */
class fetch_memory_units_manager {
public:
    /// This class was designed to be embedded into an existing sharded service.
    /// Hence that service needs to provide a way for it to access its local
    /// instance. This function must be thread safe.
    using local_instance_fn = std::function<fetch_memory_units_manager&()>;
    /// The maximum period units will be held by the manager before being
    /// release to their originating shard.
    static constexpr std::chrono::milliseconds max_release_period = 1s;
    /// The maximum number of units per shard that will be held by the manager
    /// before being released.
    static constexpr size_t max_release_size = 10_MiB;

    fetch_memory_units_manager(
      ssx::semaphore& kafka_units,
      ssx::semaphore& fetch_units,
      local_instance_fn&& local_fn,
      config::binding<std::optional<int32_t>>&&);

    ss::future<> stop();

    /** Consume proper amounts of units from memory semaphores and return them
     * as semaphore_units. Fetch semaphore units returned are the indication of
     * available resources: if none, there is no memory for the operation;
     * if less than \p max_bytes, the fetch should be capped to that size.
     *
     * \param max_units The maximum number of units the function will attempt to
     * allocate.
     * \param min_units The minimum number of units the function will attempt to
     * allocate. If it can't allocate at least this many units no units will be
     * allocated.
     * \param require_min_units If true then at least \ref min_units will be
     * allocated regardless of the units available in \ref memory_sem and \ref
     * memory_fetch_sem.
     */
    fetch_memory_units allocate_memory_units(
      const model::ktp& ktp,
      size_t max_bytes,
      size_t max_batch_size,
      const size_t avg_batch_size,
      const bool require_max_batch_size);

    /** Returns a fetch_memory_units object with zero units.
     */
    fetch_memory_units zero_units();

private:
    friend fetch_memory_units;

    struct [[nodiscard]] units {
        units(
          ssx::semaphore_units&& kafka_units,
          ssx::semaphore_units&& fetch_units)
          : kafka_units(std::move(kafka_units))
          , fetch_units(std::move(fetch_units)) {}
        units(units&&) noexcept = default;

        units& operator=(units&&) noexcept;

        units(const units&) = delete;
        units& operator=(const units&) = delete;

        ~units() noexcept;

        bool has_units() const {
            return fetch_units.count() > 0 || kafka_units.count() > 0;
        }
        void adopt(units&& o);
        size_t num_units() const { return fetch_units.count(); }

        ssx::semaphore_units kafka_units;
        ssx::semaphore_units fetch_units;
        ss::shard_id shard = ss::this_shard_id();
    };

    units allocate_units(const size_t);
    void release_units_to_manager(units&& u);
    void release_units_to_semaphore(units&& u);
    void release_all_units_to_semaphore();

    ss::gate _gate;
    ssx::semaphore& _kafka_units;
    ssx::semaphore& _fetch_units;
    size_t _max_fetch_units;
    config::binding<std::optional<int32_t>> _max_message_size;

    ss::timer<> _release_units_timer;
    // Collected units are aggregated together by shard in order to ensure there
    // is at most one cross shard call to every shard when released.
    chunked_hash_map<ss::shard_id, units> _units_to_release;

    local_instance_fn _local_instance_fn;
};

/***
 * Holds semaphore units from memory semaphores. Can be passed across
 * shards, semaphore units will eventually be released in the shard where the
 * instance of this class has been created.
 */
class [[nodiscard]] fetch_memory_units {
public:
    ~fetch_memory_units() noexcept;

    fetch_memory_units() noexcept = delete;

    fetch_memory_units(fetch_memory_units&& o) noexcept = default;
    fetch_memory_units& operator=(fetch_memory_units&& o) noexcept;

    fetch_memory_units(const fetch_memory_units&) = delete;
    fetch_memory_units& operator=(const fetch_memory_units&) = delete;

    size_t num_units() const { return _units.num_units(); }
    bool has_units() const { return _units.has_units(); }

    /** Adjusts units held by \ref fetch_memory_units to the target amount. This
     * operation can only occur on the shard the units were originally allocated
     * on.
     *
     * \param target The number of units that should be held by this \ref
     * fetch_memory_units.
     */
    void adjust_units(const size_t target);

private:
    friend class fetch_memory_units_manager;

    fetch_memory_units(
      fetch_memory_units_manager::units&& units,
      fetch_memory_units_manager::local_instance_fn& local_instance_fn);

    fetch_memory_units_manager& local_manager();

    fetch_memory_units_manager::units _units;
    fetch_memory_units_manager::local_instance_fn _local_instance_fn;
};

} // namespace kafka
