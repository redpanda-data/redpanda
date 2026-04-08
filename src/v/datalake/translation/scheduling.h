/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "absl/container/btree_map.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

namespace datalake::translation::scheduling {

using clock = ss::lowres_clock;

using translator_id = model::ntp;

/**
 * Interface to notify the scheduler about events.
 */
class scheduling_notifications {
public:
    scheduling_notifications() = default;
    scheduling_notifications(const scheduling_notifications&) = delete;
    scheduling_notifications&
    operator=(const scheduling_notifications&) = delete;
    scheduling_notifications(scheduling_notifications&&) = delete;
    scheduling_notifications& operator=(scheduling_notifications&&) = delete;
    virtual ~scheduling_notifications() = default;

    /**
     * Notifies that the translator with the given id is ready to translate
     * data.
     */
    virtual void notify_ready(const translator_id&) noexcept = 0;

    /**
     * Notifies that the translator with the given id has finished the currently
     * in progress translation and has released all the resources.
     */
    virtual void notify_done(const translator_id&) noexcept = 0;

    /**
     * Notifies that the translators cannot make progress due to memory
     * exhaustion.
     */
    virtual void notify_memory_exhausted() = 0;
};

/*
 * Interface for scheduler to interact with the global disk manager.
 */
class disk_manager {
public:
    disk_manager() = default;
    disk_manager(const disk_manager&) = delete;
    disk_manager& operator=(const disk_manager&) = delete;
    disk_manager(disk_manager&& other) noexcept;
    disk_manager& operator=(disk_manager&&) noexcept;
    virtual ~disk_manager() = default;

    /*
     * Request additional disk reservation from the global pool. This method
     * will return zero units if the request cannot be satisfied immediately.
     */
    virtual ss::future<size_t> reserve() = 0;
};

using reservation = ssx::semaphore_units;

/**
 * Tracks the resources needed by translators. Translators work with a
 * reservation tracker to reserve resources needed for translation.
 *
 * note: All reservations must be destroyed before destroying the tracker.
 */
class reservations_tracker {
public:
    reservations_tracker() = default;
    reservations_tracker(const reservations_tracker&) = delete;
    reservations_tracker& operator=(const reservations_tracker&) = delete;
    reservations_tracker(reservations_tracker&&) = delete;
    reservations_tracker& operator=(reservations_tracker&&) = delete;
    virtual ~reservations_tracker() = default;
    /**
     * Returns a block of memory as decided by the tracker. Can be called
     * multiple times until the requirement is satisfied.
     */
    virtual ss::future<reservation> reserve_memory(ss::abort_source&) = 0;
    /* returns true if all the memory is exhausted */
    virtual bool memory_exhausted() const = 0;

    virtual size_t allocated_memory() const = 0;
    virtual size_t reservation_block_size() const = 0;

    /**
     * Returns a reservation of disk space. The reservation may be any size,
     * with the passed-in size being a hint. If more units are reserved than are
     * needed, then release the unused units if they will not be used in the
     * near term (e.g. a translator leaving the running state).
     *
     * Returning the unused units is important because disk space reservations
     * are held across all translator states, including the idle state. Because
     * of this persistence, even small amounts of unused reservation can add up
     * to large reservations across hundreds or thousands of translators.
     */
    virtual ss::future<reservation> reserve_disk(size_t, ss::abort_source&) = 0;
    virtual size_t release_unused_disk_units() = 0;

    static std::unique_ptr<reservations_tracker> make_default(
      size_t total_memory,
      size_t memory_block_size,
      scheduling_notifications&,
      disk_manager&);
};

/**
 * Current status of the translator, used as input to the scheduling policy.
 */
struct translation_status {
    clock::duration target_lag;
    // Time point at which the current lag window ends.
    clock::time_point next_checkpoint_deadline;
    // Current memory byte reservation, if a translation is running
    std::optional<size_t> memory_bytes_reserved;
    // Current bytes flushed to disk, if a translation is running. This is not
    // an counter, and it is expected that if translators have subtracted from
    // or otherwise reset this value then the translator has or is in the
    // process of removing an equivalent amount of data (e.g. upload + delete).
    std::optional<size_t> disk_bytes_flushed;

    std::optional<size_t> translation_backlog;
};
std::ostream& operator<<(std::ostream&, const translation_status&);

/**
 * A single schedulable translator instance. Translation interacts with
 * two main components
 * - a reservation tracker - to reserve resources needed for translation
 * - a scheduler (via scheduling_notifications) - to let the scheduler know
 *   about the translator status.
 *
 * The scheduler then notifies the translator when it can begin translation
 * via \ref translator::start_translation, along with a time slice. This allows
 * the translator to translate for the duration of the time slice.
 *
 * The scheduler may also preempt the inflight translation if it chooses to
 * (via stop_translation) and the translator should stop the inflight
 * translation and release the resources.
 */
class translator {
public:
    translator() = default;
    translator(const translator&) = delete;
    translator& operator=(const translator&) = delete;
    translator(translator&&) = delete;
    translator& operator=(translator&&) = delete;
    virtual ~translator() = default;

    /**
     * A unique id for the translator, used as a handle for further operations.
     */
    virtual const translator_id& id() const = 0;

    /**
     * Invoked by the scheduler once after registration.
     */
    virtual ss::future<>
    init(scheduling_notifications&, reservations_tracker&) = 0;

    /**
     * Invoked by the scheduler once, terminal state, all state should be
     * cleaned and any in progress translations should be stopped and waited on.
     * Maybe concurrently called with \ref translator::start.
     */
    virtual ss::future<> close() noexcept = 0;

    /**
     * Current status of the translation.
     */
    virtual translation_status status() const = 0;

    /**
     * Approximation for current translation lag, in milliseconds.
     * Precise semantics are implementation defined.
     */
    virtual std::chrono::milliseconds current_lag_ms() const = 0;

    /**
     * Notification from the translator to translate until time
     * the slice elapses. May be called any time after
     * \ref scheduler_notifications::notify_ready notification.
     *
     * Once finished, the translator should release all the
     * resources and notify the scheduler via
     * \ref scheduling_notifications::notify_done.
     */
    virtual void start_translation(clock::duration time_slice) = 0;

    /**
     * Invoked by the scheduler to preempt an inprogress translation. All
     * resources should be released and the translator should invoke \ref
     * scheduling_notifications::notify_done when done.
     */
    enum class stop_reason {
        oom,
        out_of_disk,
    };

    virtual void stop_translation(stop_reason) = 0;

    /**
     * Request that the translator finish and upload its data. This is distinct
     * from `stop_translation` in that it can be called on a translator no
     * matter what stat it is in (e.g. running, waiting, idle). The translator
     * is free to clear the request after taking action.
     */
    virtual void set_finish_translation() = 0;

    /**
     * Return true if the translator is still in the progress of satisfying the
     * latest request made via a call to `finish_translation`. This method
     * should return true immediately after a call to `finish_translation`, and
     * then eventually return false.
     */
    virtual bool get_finish_translation() { return false; }
};

std::ostream& operator<<(std::ostream&, const translator&);

struct executor;
class scheduler;

/**
 * Encapsulates all the scheduling state for a single translator.
 *   ┌───────┐
 *   │started│
 *   └┬──────┘
 *   ┌▽────────┐
 *   │  idle   │
 *   └△───────┬┘
 *   ┌┴──────┐│
 *   │running││ (notify_ready())
 *   └△──────┘│
 *   ┌┴───────▽─┐
 *   │ waiting  │
 *   └──────────┘
 *
 * Summary of state transitions
 * * idle -> waiting  on notify_ready()
 * * waiting -> running after scheduling
 * * running -> idle on notify_done()
 */

class translator_executable {
public:
    explicit translator_executable(std::unique_ptr<translator> translator)
      : _translator(std::move(translator)) {}
    translator_executable(const translator_executable&) = delete;
    translator_executable& operator=(const translator_executable&) = delete;
    translator_executable(translator_executable&& other) noexcept;
    translator_executable& operator=(translator_executable&&) noexcept;
    ~translator_executable() = default;

    translation_status status() const;
    const std::unique_ptr<translator>& translator_ptr() const {
        return _translator;
    }
    const clock::time_point start_time() const { return _start_time; }
    clock::duration total_wait_time() const;
    clock::duration total_running_time() const;
    size_t translations_scheduled() const { return _translations_scheduled; }
    bool stop_in_progress() const { return _stop_in_progress; }

    void mark_waiting();
    void mark_running();
    void mark_idle();
    void mark_stopping(translator::stop_reason);

    // hook into the waiting queue
    intrusive_list_hook _waiting_hook;
    // hook into the running queue
    intrusive_list_hook _running_hook;

    friend std::ostream&
    operator<<(std::ostream&, const translator_executable&);

private:
    friend struct executor;
    friend class scheduler;
    std::unique_ptr<translator> _translator;
    clock::time_point _start_time = clock::now();
    // Total time since registration the translator has spent in
    // waiting state.
    clock::duration _total_waiting_time{0};
    // Total time since registration the translator has spent in
    // running state.
    clock::duration _total_running_time{0};
    // Total number times a translator was scheduled to run.
    size_t _translations_scheduled{0};
    // Wait time start if the translator is currently waiting.
    std::optional<clock::time_point> _current_wait_begin_time;
    // Running time start if the translator is currently running.
    std::optional<clock::time_point> _current_running_begin_time;
    bool _stop_in_progress{false};
};

using translators = chunked_hash_map<translator_id, translator_executable>;

/**
 * Encapsulates all the (internal) state the scheduler works with.
 *
 * Note to the users of class members. Be mindful of scheduling points
 * when iterating through these containers. There is no explicit locking
 * in place to avoid racy access across scheduling points.

 * running/waiting lists
 *
 * General guideline is to not iterate thorough these lists across scheduling
 * points. These lists are typically small ands its ok to loop through them in
 * a single continuation.

 * translators map
 *
 * For the most part there is no reason to loop through this map as all
 * operations are scoped to a particular translator instance.
 *
 * translators for immediate finish
 *
 * Set of translatored requested to immedidately finish. This is currently only
 * used by the disk usage monitor. The key is an opaque priority (lower is
 * higher priority) and identifier. See `on_resource_exhaustion` for how it's
 * used for optimization.
 */
struct executor {
    void start_translation(translator_executable&, clock::duration time_slice);
    void stop_translation(translator_executable&, translator::stop_reason);
    translators translators{};
    intrusive_list<translator_executable, &translator_executable::_running_hook>
      running;
    intrusive_list<translator_executable, &translator_executable::_waiting_hook>
      waiting;

    /*
     * finish_priority captures the priority in which translators should be
     * finished with lowest value being highest priority.
     */
    using finish_priority = size_t;
    struct finish_request {
        translator_id id;
        translator::stop_reason reason;

        finish_request(translator_id id, translator::stop_reason reason)
          : id(std::move(id))
          , reason(reason) {}
    };
    absl::btree_map<finish_priority, finish_request>
      translators_for_immediate_finish;

    ss::gate gate;
    ss::abort_source as;
};

/**
 * A pluggable scheduling policy that the scheduler works with.
 */
class scheduling_policy {
public:
    scheduling_policy() = default;
    scheduling_policy(const scheduling_policy&) = delete;
    scheduling_policy& operator=(const scheduling_policy&) = delete;
    scheduling_policy(scheduling_policy&& other) noexcept;
    scheduling_policy& operator=(scheduling_policy&&) noexcept;
    virtual ~scheduling_policy() = default;

    /**
     * Invoked by the scheduler if there are translators waiting to be
     * scheduled.
     */
    virtual ss::future<>
    schedule_one_translation(executor&, const reservations_tracker&) = 0;

    /**
     * Invoked by the scheduler if no further reservations can be made
     * indicating stuck translators.
     */
    virtual ss::future<>
    on_resource_exhaustion(executor&, const reservations_tracker&) = 0;

    static std::unique_ptr<scheduling_policy> make_default(
      config::binding<size_t> max_concurrent_translators,
      clock::duration translation_time_slice);
};

class scheduler : public scheduling_notifications {
public:
    explicit scheduler(
      size_t total_memory,
      size_t memory_block_size,
      std::unique_ptr<scheduling_policy>,
      disk_manager&);
    scheduler(const scheduler&) = delete;
    scheduler& operator=(const scheduler&) = delete;
    scheduler(scheduler&&) = delete;
    scheduler& operator=(scheduler&&) = delete;
    ~scheduler() override = default;

    void notify_ready(const translator_id&) noexcept override;
    void notify_done(const translator_id&) noexcept override;
    void notify_memory_exhausted() override;

    ss::future<> stop();
    /**
     * Registers the input translator and returns true if successful.
     * Returns false if a duplicate translator exists or if the translator
     * does not initialize correctly. In either case, the translator must be
     * removed via \ref scheduler::remove_translator before adding again.
     */
    ss::future<bool> add_translator(std::unique_ptr<translator>);
    /**
     * Removes translator with the given id. Waits on it to finish any
     * inprogress translations. No-op if a translator with given id does not
     * exist.
     */
    ss::future<> remove_translator(const translator_id&);

    // For testing
    size_t running_translators() const;

    std::unique_ptr<reservations_tracker>& reservations() {
        return _mem_tracker;
    }
    const translators& all_translators() const { return _executor.translators; }

    /*
     * Request the scheduler to immediately finish and reclaim disk space for
     * the provided scheduler. The scheduler will treat the ordering of vector
     * as roughly highest to lowest priority. The second element of the pair is
     * the total size of the translator which can be used to avoid requerying
     * for the same information from the translator status API.
     */
    struct finish_request {
        translator_id id;
        translator::stop_reason reason;

        finish_request(translator_id id, translator::stop_reason reason)
          : id(std::move(id))
          , reason(reason) {}
    };
    void request_immediate_finish(chunked_vector<finish_request>);

    /*
     * Consume and return unused disk reservation units. This interface is used
     * by the global disk manager to harvest units from cores in order to
     * redistribute them.
     */
    size_t release_unused_disk_units();

private:
    ss::future<> main();
    bool requires_scheduling_actions() const;
    ss::condition_variable _state_changed_cvar;
    std::unique_ptr<scheduling_policy> _scheduling_policy;
    disk_manager& _disk_monitor;
    std::unique_ptr<reservations_tracker> _mem_tracker;
    executor _executor;
};
} // namespace datalake::translation::scheduling
