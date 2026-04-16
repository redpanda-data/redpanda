/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "absl/container/fixed_array.h"
#include "base/seastarx.h"
#include "cloud_topics/level_zero/common/level_zero_probe.h"
#include "cloud_topics/level_zero/pipeline/write_pipeline.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "config/property.h"
#include "ssx/semaphore.h"
#include "utils/named_type.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
#include <expected>
#include <mutex>
#include <new>

namespace cloud_topics::l0 {

/// Strongly typed group identifier for scheduler groups.
/// Group ID equals the shard ID of the first shard in the group.
using group_id = named_type<uint32_t, struct scheduler_context_group_id_t>;

/// Padded atomic group_id to avoid false sharing between shards.
/// Each shard's group assignment is on its own cache line.
struct alignas(std::hardware_destructive_interference_size)
  padded_atomic_group_id {
    std::atomic<group_id> value{group_id{0}};

    group_id load() const { return value.load(std::memory_order_relaxed); }

    void store(group_id gid) { value.store(gid, std::memory_order_relaxed); }
};

/// Result of the scheduling decision made by scheduler_context
enum class schedule_action {
    /// Not this shard's turn (round-robin)
    skip,
    /// This shard's turn but upload conditions not met or mutex busy
    wait,
    /// Should perform upload - mutex lock acquired
    upload,
};

// Forward declaration
template<typename Clock>
class schedule_request;

/// Result returned by try_schedule_upload containing the decision
/// and associated state
template<typename Clock>
struct schedule_result {
    schedule_action action;
    /// schedule_request is held when action == upload
    std::optional<schedule_request<Clock>> request;
    /// Group ID for this shard (valid for all actions)
    group_id gid;
};

template<typename Clock>
struct shard_state {
    // Static dummy atomic used only for default construction.
    // Elements should be reassigned before use.
    static inline std::atomic<size_t> _dummy_atomic{0};

    shard_state()
      : backlog_size(_dummy_atomic)
      , next_stage_backlog_size(_dummy_atomic) {}

    shard_state(
      std::reference_wrapper<const std::atomic<size_t>> backlog_size, // NOLINT
      std::reference_wrapper<const std::atomic<size_t>>
        next_backlog_size) // NOLINT
      : backlog_size(backlog_size)
      , next_stage_backlog_size(next_backlog_size) {}

    // Reference to the backlog size counter.
    std::reference_wrapper<const std::atomic<size_t>> backlog_size;

    // Reference to next stage backlog size counter.
    std::reference_wrapper<const std::atomic<size_t>> next_stage_backlog_size;
};

/// This struct stores state shared by all shards in the group.
/// This includes the mutex, last upload time and an atomic counter.
/// The counter is used to round-robin uploads between all shards in
/// the group.
template<typename Clock>
struct shard_group {
    using time_point = Clock::time_point;

    // Round-robin counter - rotates through group members
    std::atomic<uint32_t> ix{0};

    // Mutex is locked when some shard pulls data from other shards.
    // It's not locked when the data is uploaded.
    std::mutex mutex;

    // Last upload time for this group
    std::atomic<time_point> last_upload_time{Clock::now()};

    // Last time this group was modified by a split or merge operation.
    // Used to rate-limit split/merge decisions to avoid group churn.
    std::atomic<time_point> last_modification_time{Clock::now()};
};

/// RAII object representing a scheduled upload request.
/// Holds the group mutex lock and increments the round-robin counter
/// when destroyed.
template<typename Clock>
class schedule_request {
public:
    /// Construct a schedule_request that manages the lock and ix counter.
    /// \param lock The acquired mutex lock for the group
    /// \param ix Reference to the round-robin counter to increment on
    /// destruction
    schedule_request(
      std::unique_lock<std::mutex> lock, std::atomic<uint32_t>& ix) noexcept
      : _lock(std::move(lock))
      , _ix(ix) {}

    schedule_request(schedule_request&&) noexcept = default;
    schedule_request& operator=(schedule_request&&) noexcept = default;

    schedule_request(const schedule_request&) = delete;
    schedule_request& operator=(const schedule_request&) = delete;

    ~schedule_request() {
        if (_lock.owns_lock()) {
            _ix.get().fetch_add(1);
        }
    }

    /// Release the lock early (still increments ix)
    void unlock() {
        if (_lock.owns_lock()) {
            _ix.get().fetch_add(1);
            _lock.unlock();
        }
    }

private:
    std::unique_lock<std::mutex> _lock;
    std::reference_wrapper<std::atomic<uint32_t>> _ix;
};

/**
 Data structure which is created on shard 0 and shared with other shards. It
 tracks the total amount of outstanding work and contains state shared across
 all shards.

 The scheduler_context allocates shards to different upload groups. The group
 contains a set of shards that take turns to upload data from all their
 backlogs.

 Groups can be either split or merged to scale the throughput. The expectation
 is that the scheduler is followed by the batcher or some set of components that
 clean up the pipeline by uploading the data to the cloud storage. Each group
 tracks the backlog size of the next pipeline component. If the backlog grows
 the group can be split which will doubles the throughput. This process can
 continue until there is one group per shard.

 When the group is able to keep up with the workload it can be merged with the
 next group. The decision is made based on the backlog size. Note that if the
 shard is not under excessive load the backlog size of the next stage will be
 equal to zero when the shard checks it. The batcher reacts to pipeline events
 and pulls enough requests to make N objects almost immediately. So in order
 for the backlog to grow there should be more data than the batcher can pull
 in one pass.

 The group allocation uses a spin on a Buddy algorithm.

 Initial state:

 shards: [0, 1, 2, 3, 4, 5, 6, 7]
 groups: [0, 0, 0, 0, 0, 0, 0, 0]

 All shards are in group 0. Shard 0 plays a special role in group 0. It can act
 as a leader that may decide to split the group into two sub-groups. Every shard
 can only make any action when the group's turn counter (shard_group::ix) points
 to it.

 The group can only be split into two equal parts. After the first split the
 scheduler state looks like this:

 shards: [0, 1, 2, 3, 4, 5, 6, 7]
 groups: [0, 0, 0, 0, 4, 4, 4, 4]

 Note that first 4 shards are still assigned to group 0 and the next 4 are
 assigned to group 4. The shard 4 is a group's leader. Now all shards in [4-7]
 range will use groups[4] state to make scheduling decision. When the shard #4
 takes turn to make scheduling decisions it may decide to split the group even
 further. Same is true for shard 0 (which is in a different group). If the
 group 0 splits we will have this state:

 shards: [0, 1, 2, 3, 4, 5, 6, 7]
 groups: [0, 0, 2, 2, 4, 4, 4, 4]

 The splitting is done based on the workload. Every time we split the throughput
 doubles because evert group uploads independently. The merging of groups is
 also done based on the backlog size and the workload. The decision to merge two
 groups can only be done by the group that will become a leader of the group
 after merging. For instance, in the example above only the group 0 can decide
 to merge with group
 2. Not the other way around. After the merging the state of the scheduler will
 look like this:

 shards: [0, 1, 2, 3, 4, 5, 6, 7]
 groups: [0, 0, 0, 0, 4, 4, 4, 4]

 Groups 0 and 4 could be merged next. The groups are split and merged in the
 same order as in the Buddy algorithm. The use of Buddy algorithm allows us to
 make splitting/merging without allocating a controller shard.
*/
template<typename Clock>
struct scheduler_context {
    using time_point = Clock::time_point;

    explicit scheduler_context(size_t num_shards)
      : shards(num_shards)
      , shard_to_group(num_shards)
      , groups(num_shards) {}

    scheduler_context(const scheduler_context&) = delete;
    scheduler_context& operator=(const scheduler_context&) = delete;
    scheduler_context(scheduler_context&&) = delete;
    scheduler_context& operator=(scheduler_context&&) = delete;
    ~scheduler_context() = default;

    /// Populate vector with per-shard byte counts for shards in the given
    /// group. This is a template method so it must be defined in the header.
    template<class Info>
    size_t get_shard_bytes_vec(std::vector<Info>& vec, group_id gid) {
        size_t total_size = 0;
        for (unsigned i = 0; i < shards.size(); i++) {
            if (shard_to_group[i].load() != gid) {
                continue;
            }
            auto bytes = shards[i].backlog_size.get().load();
            vec.at(i).shard = ss::shard_id(i);
            vec.at(i).bytes = bytes;
            total_size += bytes;
        }
        return total_size;
    }

    /// Count the number of unique active groups.
    /// Takes advantage of the fact that groups in shard_to_group are
    /// monotonically increasing (due to buddy algorithm allocation).
    size_t count_active_groups() const;

    /// Count how many shards belong to a given group
    size_t get_group_size(group_id gid) const;

    /// Get the index of a shard within its group (0-indexed)
    size_t get_shard_index_in_group(ss::shard_id shard, group_id gid) const;

    /// Get total bytes across all shards in a given group
    uint64_t get_group_bytes(group_id gid) const;

    /// Get total next-stage bytes across all shards in a given group
    uint64_t get_group_next_stage_bytes(group_id gid) const;

    /// Split a group in half. First half stays, second half moves to new group.
    /// \return true if split was performed, false if group has only one shard
    bool try_split_group(group_id gid);

    /// Find the buddy group that can be merged with this group.
    /// The buddy is the next group in shard order (the one that was split off).
    /// \return The buddy group_id, or nullopt if no buddy exists
    std::optional<group_id> find_buddy_group(group_id gid) const;

    /// Merge this group with its buddy group (reverse of split).
    /// The buddy group's shards are absorbed into this group.
    /// \param gid The group to merge (must be the first group of the pair)
    /// \return true if merge was performed, false otherwise
    bool try_merge_group(group_id gid);

    /// Main scheduling decision method. Encapsulates round-robin logic,
    /// group splitting, upload condition checking, and mutex acquisition.
    ///
    /// \param shard The shard making the scheduling decision
    /// \param max_buffer_size Data threshold for triggering upload
    /// \param scheduling_interval Time threshold for triggering upload
    /// \param now Current time
    /// \param probe Is a metrics probe
    /// \return schedule_result with action and optional lock
    schedule_result<Clock> try_schedule_upload(
      ss::shard_id shard,
      size_t max_buffer_size,
      std::chrono::milliseconds scheduling_interval,
      time_point now,
      write_request_scheduler_probe& probe);

    /// Update last upload time for a group after successful upload
    void record_upload_time(group_id gid, time_point upload_time);

    absl::FixedArray<shard_state<Clock>> shards;
    // Maps shards to groups. The value at offset N
    // can be less or equal to N. Padded to avoid false sharing.
    absl::FixedArray<padded_atomic_group_id> shard_to_group;
    // State shared by all shards in the group (mutex and last upload time).
    // Shards are using this state to upload.
    absl::FixedArray<shard_group<Clock>> groups;
};

/// The scheduler decides the which shard to use to handle write
/// requests. It also batches requests.
///
/// The scheduler pushes data to the next stage of the pipeline based on the
/// upload interval. It can also aggregate data across shards and funnel
/// requests from many different shards to the one target shard.
template<typename Clock = seastar::lowres_clock>
class write_request_scheduler
  : public ss::peering_sharded_service<write_request_scheduler<Clock>> {
    friend struct write_request_balancer_accessor;

    struct shard_info {
        ss::shard_id shard;
        size_t bytes;
    };

    using time_point = Clock::time_point;

public:
    explicit write_request_scheduler(write_pipeline<Clock>::stage s);

    ss::future<> start();

    ss::future<> stop();

private:
    /// Target shard pulls write requests from pipelines of
    /// other shards and forwards them to its own pipeline.
    /// Then it propagates the responses back to the original
    /// shards.
    /// \param infos is a list of shards that have write requests
    ///        to forward (could be outdated).
    /// \note The method is invoked on the target shard. It communicates
    ///       with other shards to instruct them to forward their requests
    ///       to the target shard to upload.
    ss::future<> pull_and_roundtrip(
      std::vector<shard_info> infos, std::optional<schedule_request<Clock>>);

    /// Unified upload path
    ss::future<> bg_handler();

    /// Run one round of upload
    /// \return next wake up time
    ss::future<time_point> run_once();

    using foreign_ptr_t = ss::foreign_ptr<ss::lw_shared_ptr<upload_meta>>;

    using gate_holder_ptr = std::unique_ptr<ss::gate::holder>;

    /// Make a copy of a single write request and enqueue it
    /// to the pipeline on the target shard. Wait until it's
    /// processed and return the response.
    ///
    /// \param req is a write request to forward
    /// \param target_gate_holder is a pointer to the gate holder owned by the
    ///        target shard
    /// \note The method is invoked on the target shard (the shard that uploads
    /// the data).
    ss::future<std::expected<foreign_ptr_t, errc>> proxy_write_request(
      write_request<Clock>* req, ss::gate::holder target_gate_holder) noexcept;

    /// Forward all write requests to the target shard
    /// \param shard is a target shard that should perform the upload
    /// \param list is a list of write requests to forward
    /// \param target_shard_gate_holder is a pointer to the gate holder owned by
    /// the
    ///        target shard
    /// \note The method is invoked on the shard that owns the data. It submits
    /// the continuation
    ///       to the target shard to complete the operation.
    ss::future<> roundtrip(
      ss::shard_id shard,
      write_pipeline<Clock>::write_requests_list list,
      ss::foreign_ptr<gate_holder_ptr> target_shard_gate_holder);

    /// Acknowledge the write request with the response
    /// \param req is a write request to acknowledge
    /// \param resp is a response to propagate
    /// \note The response is created on the target shard, the method
    ///       is invoked on the shard that owns the write request.
    void ack_write_response(
      write_request<Clock>* req, std::expected<foreign_ptr_t, errc> resp);

    /// Forward all write requests to the target shard
    /// \param shard is a target shard that should perform the upload
    /// \param target_shard_gate_holder is a pointer to the gate holder
    /// \note The method is invoked on the shard that owns the data.
    ss::future<> forward_to(
      ss::shard_id shard,
      ss::foreign_ptr<gate_holder_ptr> target_shard_gate_holder);

    /// Compute next wake up time for the background fiber.
    /// The interval could be short or long. If the background fiber expects
    /// that it will not have enough data for L0 upload for a relatively long
    /// time it will request long sleep. If the upload conditions are almost met
    /// it will ask for short sleep interval.
    /// \param long_sleep indicates that the long sleep interval should be used
    /// \return Time to wake up the background fiber next time.
    time_point get_next_wakeup_time(bool long_sleep = false);

    write_pipeline<Clock>::stage _stage;
    ss::abort_source _as;
    ss::gate _gate;

    // This field is used in tests to disable background activit
    bool _test_only_disable_background_loop{false};

    config::binding<size_t> _max_buffer_size;
    config::binding<size_t> _max_cardinality;
    config::binding<std::chrono::milliseconds> _scheduling_interval;

    write_request_scheduler_probe _probe;

    // Field used to allocate scheduler_context. Only initialized
    // on shard zero. Accessed from all shards.
    std::optional<scheduler_context<Clock>> _shard_zero_context;

    // Every shard accesses the context through this reference.
    scheduler_context<Clock>* _context;
    ssx::named_semaphore<Clock> _init_barrier{0, "l0/write_request_scheduler"};
};

} // namespace cloud_topics::l0
