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

#include "datalake/translation/deps.h"
#include "datalake/translation/scheduling.h"
#include "random/simple_time_jitter.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"

namespace datalake::translation {

/**
 * partition_translator - Partition-based implementation of a schedulable
 * translator.
 *
 * The lifecycle of a partition_translator is as follows:
 *   - datalake_manager
 *     - fields a translator state change notification for some ntp
 *     - reads topic configs, raft info, and scheduler state to determine
 *       whether a new translator is needed for this partition
 *     - if so, constructs and configures a translator and transfers ownership
 *       to the scheduler.
 *   - scheduler
 *     - accepts ownership of translator via add_translator
 *     - adds the translator to its executor
 *     - initializes the translator with a mem tracker
 *   - partition_translator
 *     - on init, enters translate_until_stopped loop and waits for translatable
 *       offsets to appear on its partition.
 *     - when some offsets appear, notifies scheduler of its readiness and waits
 *       to be scheduled in
 *   - scheduler
 *     - once a partition translator is ready, let the executor know it is
 *       waiting
 *   - scheduling policy
 *     - periodically commanded to schedule a translation by the scheduler, and
 *       chooses from the list of ready translators
 *     - indirectly calls start_translation on the chosen translator
 *   - partition_translator
 *     - once start_translation is called, wakes up and translates offsets until
 *       none remain or its time slice expires.
 *     - updates various state and reenters the translate_until_stopped loop
 *   - datalake_manager
 *     - a partition_translator so initialized remains until the manager
 *       receives another state change notification and determines that the
 *       translator should be torn down. that is until one of:
 *       - the source partition ceases to exist
 *       - the iceberg mode topic config is unset on the source topic
 *       - the host node no longer leads the partition
 *     - at this point the translator corresponding to the source ntp is
 *       deregistered from the scheduler, closed (aborting any inflight
 *       translations), and destroyed
 *
 * See scheduler.h for more details on scheduling translators in general.
 */
class partition_translator : public scheduling::translator {
private:
    using jitter_t
      = simple_time_jitter<ss::lowres_clock, std::chrono::milliseconds>;

public:
    explicit partition_translator(
      ss::scheduling_group,
      std::unique_ptr<coordinator_api>,
      std::unique_ptr<data_source>,
      std::unique_ptr<translation_context>,
      std::unique_ptr<translation_lag_tracker>,
      jitter_t jitter);

    const scheduling::translator_id& id() const final;

    ss::future<> init(
      scheduling::scheduling_notifications&,
      scheduling::reservations_tracker&) final;

    ss::future<> close() noexcept final;

    scheduling::translation_status status() const final;

    std::chrono::milliseconds current_lag_ms() const final;

    void start_translation(scheduling::clock::duration time_slice) final;

    void stop_translation(stop_reason) final;

    void reconcile_properties() noexcept final;

    void set_finish_translation() final;

    bool get_finish_translation() final;

private:
    /**
     * inflight_translation_state
     *
     * Constructed when a scheduling::executor calls `start_translation` on a
     * partition_translator, encapsulating both the time slice allocated by the
     * scheduler AND an abort source used to interrupt ongoing async translation
     * operations (e.g. waiting for a parquet write) in the event that the
     * scheduler needs to stop translation for any reason (e.g. resource
     * exhaustion).
     */
    struct inflight_translation_state {
        scheduling::clock::duration translate_for;
        ss::abort_source as;
    };

    /**
     * translate_until_stopped - called at translation initialization
     * i.e. immediately after it is registered with the scheduler.
     *
     * Repeatedly until aborted, wait for translatable offsets, notify the
     * scheduler when they appear, wait to be assigned a time slice by the
     * scheduler, then try to translate up to max_translatable_offset or until
     * the time slice expires or the scheduler interrupts (e.g. due to resource
     * exhaustion).
     */
    ss::future<> translate_until_stopped();

    /**
     * translate_when_notified - Called from the translation loop.
     *
     * Having discovered translatable offsets and notified the scheduler, wait
     * until the scheduler calls start_translation, then start reading and
     * translating batches, subject to the supplied time slice and any
     * scheduler-driven interruptions.
     *
     * Preconditions:
     *   - last translated offset < max translatable offset
     *   - scheduler has been notified of readiness
     *   - any previous translations have been cancelled or completed (i.e.
     *     inflight_translation_state is nullopt).
     */
    ss::future<> translate_when_notified(kafka::offset begin_offset);

    ss::future<coordinator::fetch_latest_translated_offset_reply>
    fetch_latest_translated_offset(retry_chain_node&);

    ss::future<coordinator::add_translated_data_files_reply>
    checkpoint_translation_result(
      retry_chain_node&, coordinator::translated_offset_range);

    /**
     * Returns true if the inflight translation should be flushed up on which
     * all the files are rolled and uploaded to the cloud storage and the result
     * will be checkpointed.
     */
    bool should_finish_inflight_translation() const;

    // lto = last translated offset
    struct translation_offsets {
        // Last checkpointed translated offset with coordinator
        // or previous offset to the min translatable offset
        // if no checkpointed translation state in the coordinator
        kafka::offset coordinator_lto;
        // Offset to begin the next translation from.
        // set if there is new data to translate
        std::optional<kafka::offset> next_translation_begin_offset;
    };
    ss::future<std::optional<translation_offsets>>
    fetch_translation_offsets(retry_chain_node&);

    ss::future<translation_errc>
    run_one_translation_iteration(kafka::offset translation_begin_offset);

    /**
     * Returns true if the finish is successful and did not run into any
     * unexpected errors, False otherwise.
     */
    ss::future<bool> finish_inflight_translation(
      kafka::offset coordinator_lto, retry_chain_node&);

    ss::scheduling_group _sg;
    std::unique_ptr<coordinator_api> _coordinator;
    std::unique_ptr<data_source> _data_source;
    std::unique_ptr<translation_context> _translation_ctx;
    std::unique_ptr<translation_lag_tracker> _lag_tracking;
    // TODO: consider baking backoff into the scheduler on translation failure.
    jitter_t _jitter;
    model::term_id _term;
    prefix_logger _logger;
    bool _initialized = false;
    // set in init()
    scheduling::scheduling_notifications* _scheduler{nullptr};
    scheduling::reservations_tracker* _reservations{nullptr};
    ss::gate _gate;
    ss::abort_source _as;

    // duration and abort source for a scheduled translation
    std::optional<inflight_translation_state> _inflight_translation_state;

    // notification medium between scheduler and translator. signalled when the
    // scheduler calls start_translation, waking up translate_when_notified as a
    // result.
    ss::condition_variable _ready_to_translate;

    // true if this translator has been requested to finish. when set, the
    // translator should work towards uploading and removing its staging data.
    // the translator can clear this bit. it may be reset by future requests.
    bool _finish_translation_requested{false};
};
} // namespace datalake::translation
