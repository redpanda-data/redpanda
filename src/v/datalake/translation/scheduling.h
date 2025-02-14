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

#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
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
    scheduling_notifications& operator=(const scheduling_notifications&)
      = delete;
    scheduling_notifications(scheduling_notifications&&) = delete;
    scheduling_notifications& operator=(scheduling_notifications&&) = delete;
    virtual ~scheduling_notifications() = default;

    /**
     * Notifies that the translator with the given id is ready to translate
     * data.
     */
    virtual void notify_ready(const translator_id&) = 0;

    /**
     * Notifies that the translator with the given id has finished the currently
     * in progress translation and has released all the resources.
     */
    virtual void notify_done(const translator_id&) = 0;

    /**
     * Notifies that the translators cannot make progress due to memory
     * exhaustion.
     */
    virtual void notify_memory_exhausted() = 0;
};

using reservation = ssx::semaphore_units;

/**
 * Tracks the resources needed by translators. Translators work with a
 * reservation tracker to reserve resources needed for translation. Currently
 * tracks memory but can be extended to other resources like disk usage.
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

    static std::unique_ptr<reservations_tracker> make_default(
      size_t total_memory, size_t memory_block_size, scheduling_notifications&);
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
     * Invoked when any of the translation related properties are altered.
     */
    virtual void reconcile_properties() noexcept = 0;

    /**
     * Current status of the translation.
     */
    virtual translation_status status() const = 0;

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
    virtual void stop_translation() = 0;
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
    void mark_stopping();

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
 */
struct executor {
    void start_translation(translator_executable&, clock::duration time_slice);
    void stop_translation(translator_executable&);
    translators translators{};
    intrusive_list<translator_executable, &translator_executable::_running_hook>
      running;
    intrusive_list<translator_executable, &translator_executable::_waiting_hook>
      waiting;
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
      size_t max_concurrent_translators,
      clock::duration translation_time_slice);
};

class scheduler : public scheduling_notifications {
public:
    explicit scheduler(
      size_t total_memory,
      size_t memory_block_size,
      std::unique_ptr<scheduling_policy>);
    scheduler(const scheduler&) = delete;
    scheduler& operator=(const scheduler&) = delete;
    scheduler(scheduler&&) = delete;
    scheduler& operator=(scheduler&&) = delete;
    ~scheduler() override = default;

    void notify_ready(const translator_id&) override;
    void notify_done(const translator_id&) override;
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

private:
    ss::future<> main();
    bool requires_scheduling_actions() const;
    ss::condition_variable _state_changed_cvar;
    std::unique_ptr<scheduling_policy> _scheduling_policy;
    std::unique_ptr<reservations_tracker> _mem_tracker;
    executor _executor;
};
} // namespace datalake::translation::scheduling
