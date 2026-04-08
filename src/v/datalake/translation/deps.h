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

#include "cluster/fwd.h"
#include "datalake/coordinator/types.h"
#include "datalake/data_writer_interface.h"
#include "datalake/fwd.h"
#include "datalake/location.h"
#include "datalake/table_creator.h"
#include "datalake/translation/scheduling.h"
#include "datalake/translation/translation_probe.h"
#include "features/fwd.h"
#include "model/record_batch_reader.h"
#include "model/timestamp.h"
#include "utils/retry_chain_node.h"

namespace datalake::translation {

class translator_out_of_memory_error final : public std::runtime_error {
public:
    explicit translator_out_of_memory_error()
      : std::runtime_error("translator_out_of_memory") {}
};

class translator_shutdown_error final : public std::runtime_error {
public:
    explicit translator_shutdown_error()
      : std::runtime_error("translator_shutdown") {}
};

class translator_time_quota_exceeded_error final : public std::runtime_error {
public:
    explicit translator_time_quota_exceeded_error()
      : std::runtime_error("translator_time_quota_exceeded") {}
};

class translator_out_of_disk_error final : public std::runtime_error {
public:
    explicit translator_out_of_disk_error()
      : std::runtime_error("translator_out_of_disk") {}
};

class noop_disk_tracker : public writer_disk_tracker {
    ss::future<reservation_error>
    reserve_bytes(size_t, ss::abort_source&) noexcept override;
    ss::future<> free_bytes(size_t, ss::abort_source&) override;
    void release() override;
    void release_unused() override;
};

class noop_mem_tracker : public writer_mem_tracker {
public:
    ss::future<reservation_error>
    reserve_bytes(size_t, ss::abort_source&) noexcept override;
    ss::future<> free_bytes(size_t, ss::abort_source&) override;
    void release() override;
    writer_disk_tracker& disk() override;

private:
    noop_disk_tracker _disk;
};

/**
 * Tracks disk usage across all writers in a single translator.
 */
class translator_disk_tracker : public writer_disk_tracker {
public:
    explicit translator_disk_tracker(
      scheduling::reservations_tracker& scheduling_reservations)
      : _reservations_tracker(scheduling_reservations) {}

    ss::future<reservation_error>
    reserve_bytes(size_t, ss::abort_source&) noexcept override;
    ss::future<> free_bytes(size_t, ss::abort_source&) override;
    void release() override;
    void release_unused() override;

private:
    size_t _current_usage{0};
    scheduling::reservations_tracker& _reservations_tracker;
    ssx::semaphore_units _reservations;
};

/**
 * Tracks memory usage across all writers in a single translator.
 */
class translator_mem_tracker : public writer_mem_tracker {
public:
    explicit translator_mem_tracker(
      scheduling::reservations_tracker& scheduling_reservations)
      : _reservations_tracker(scheduling_reservations)
      , _disk(scheduling_reservations) {}

    ss::future<reservation_error>
    reserve_bytes(size_t, ss::abort_source&) noexcept override;
    ss::future<> free_bytes(size_t, ss::abort_source&) override;
    void release() override;
    writer_disk_tracker& disk() override;

    size_t current_usage() const;
    size_t total_reserved() const;

private:
    size_t _current_usage{0};
    scheduling::reservations_tracker& _reservations_tracker;
    ssx::semaphore_units _reservations;
    translator_disk_tracker _disk;
};

class coordinator_api {
public:
    coordinator_api() = default;
    coordinator_api(const coordinator_api&) = delete;
    coordinator_api& operator=(const coordinator_api&) = delete;
    coordinator_api(coordinator_api&&) = delete;
    coordinator_api& operator=(coordinator_api&&) = delete;

    virtual ~coordinator_api() = default;

    virtual ss::future<coordinator::add_translated_data_files_reply>
      add_translated_data_files(coordinator::add_translated_data_files_request)
      = 0;

    virtual ss::future<coordinator::fetch_latest_translated_offset_reply>
      fetch_latest_translated_offset(
        coordinator::fetch_latest_translated_offset_request) = 0;

    static std::unique_ptr<coordinator_api>
    make_default_coordinator_api(coordinator::frontend&);
};

/**
 * data_source - Interface to a data source for translation.
 *
 * By default implementation, encapsulates a partition and a corresponding
 * translation_stm and comprising various logic and integration points for
 * tracking and bookkeeping translation progress on the partition.
 *
 * Provides async interfaces to poll for translatable offsets and to construct
 * batch readers therefrom.
 */
class data_source {
public:
    data_source() = default;
    data_source(const data_source&) = delete;
    data_source& operator=(const data_source&) = delete;
    data_source(data_source&&) = delete;
    data_source& operator=(data_source&&) = delete;

    virtual ~data_source() = default;

    virtual void close() noexcept = 0;

    virtual const model::ntp& ntp() const = 0;

    virtual model::revision_id topic_revision() const = 0;

    virtual model::term_id term() const = 0;

    /**
     * wait_for_data_to_translate - Poll a data source for records with offset
     * exceeding last_translated_offset.
     *
     * Returns the next offset for translation, either
     * min_offset_for_translation, next(last_translated_offset) if one exists,
     * or nullopt if there is no data to translate.
     * TODO:
     * if we decide not to return on any data appearing we need to add a
     * callback to the method ss::noncopyable_function<void(kafka::offset)>
     * that would notify the lag tracker about new offsets that are ready to be
     * translated.
     */
    virtual ss::future<std::optional<kafka::offset>> wait_for_data_to_translate(
      std::optional<kafka::offset> last_translated_offset,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&) = 0;

    virtual ss::future<std::optional<model::record_batch_reader>>
    make_log_reader(kafka::offset, ss::abort_source&) = 0;

    virtual kafka::offset min_offset_for_translation() const = 0;

    virtual std::optional<kafka::offset> max_offset_for_translation() const = 0;

    virtual ss::future<std::error_code> replicate_highest_translated_offset(
      kafka::offset new_offset,
      std::optional<model::timestamp> translation_timestamp,
      model::term_id,
      model::timeout_clock::duration timeout,
      ss::abort_source&) = 0;

    static std::unique_ptr<data_source>
      make_default_data_source(ss::lw_shared_ptr<cluster::partition>);
};

enum translation_errc {
    no_data,
    file_io_error,
    cloud_io_error,
    flush_error,
    discard_error,
    oom_error,
    time_limit_exceeded,
    shutting_down,
    out_of_disk,
    type_resolution_error,
};

std::ostream& operator<<(std::ostream&, translation_errc);

class translation_context {
public:
    translation_context() = default;
    translation_context(const translation_context&) = delete;
    translation_context& operator=(const translation_context&) = delete;
    translation_context(translation_context&&) = delete;
    translation_context& operator=(translation_context&&) = delete;

    virtual ~translation_context() = default;

    /**
     * Translates using the record reader until aborted.
     */
    virtual ss::future<> translate_now(
      model::record_batch_reader, kafka::offset, ss::abort_source&) = 0;

    /**
     * Flushes all the buffered state guaranteeing release of resources.
     */
    virtual ss::future<> flush() = 0;

    /**
     * Returns the number of bytes that are flushed to the writer from the
     * inflight translation.
     */
    virtual size_t flushed_bytes() const = 0;

    /**
     * Returns the last translated offset by the translator, if one exists.
     */
    virtual std::optional<kafka::offset> last_translated_offset() const = 0;

    /**
     * Cleans up state and uploads data to cloud storage. Should be called in
     * all cases for appropriate cleanup.
     * This is called outside of translation scheduler context so it does not
     * block translation of other partitions.
     */
    virtual ss::future<
      checked<coordinator::translated_offset_range, translation_errc>>
    finish(retry_chain_node&, ss::abort_source&) = 0;

    /**
     * Cleans up state and discards an translated result.
     */
    virtual ss::future<> discard() = 0;
    /**
     *  Returns the number of bytes buffered for the parquet file in memory
     */
    virtual size_t buffered_bytes() const = 0;

    // Report and update the lag of data that has yet to be translated.
    virtual void report_translation_lag(int64_t new_lag) = 0;

    // Report and update the lag of data that has yet to be committed.
    virtual void report_commit_lag(int64_t new_lag) = 0;

    static std::unique_ptr<translation_context>
    make_default_translation_context(
      local_path,
      const model::ntp&,
      model::revision_id,
      cloud_data_io&,
      schema_manager&,
      std::unique_ptr<type_resolver>,
      std::unique_ptr<record_translator>,
      std::unique_ptr<table_creator>,
      location_provider,
      scheduling::reservations_tracker&,
      ss::sharded<cluster::topic_table>*,
      ss::sharded<features::feature_table>*,
      ss::lw_shared_ptr<translation_probe>);
};

class translation_lag_tracker {
public:
    translation_lag_tracker() = default;
    translation_lag_tracker(const translation_lag_tracker&) = delete;
    translation_lag_tracker& operator=(const translation_lag_tracker&) = delete;
    translation_lag_tracker(translation_lag_tracker&&) = delete;
    translation_lag_tracker& operator=(translation_lag_tracker&&) = delete;

    virtual ~translation_lag_tracker() = default;

    virtual bool should_finish_inflight_translation() = 0;
    /**
     * current_lag_ms - Approximation of current translation lag with respect to
     * some data source.
     *
     * Calculated as the difference between current system time and a replicated
     * underestimate for the timestamp of the last offset we translated. The
     * intent here is to consistently _over_ estimate translation lag.
     *
     */
    virtual std::chrono::milliseconds current_lag_ms() const = 0;

    /**
     * Target lag for current partition as stated in either topic configuration
     * or default cluster configuration.
     */
    virtual std::chrono::milliseconds target_lag() const = 0;
    /**
     * Notifies the lag tracker about new data that is ready to be translated.
     *
     * This triggers snapshotting the translation target if it is not already
     * set.
     */
    virtual void notify_new_data_for_translation(kafka::offset) = 0;
    /**
     * Notifies the lag tracker about the data that has been translated.
     *
     * This may reset the translation target if the translated offset is greater
     * than or equal to the target translation offset.
     */
    virtual void notify_data_translated(kafka::offset) = 0;

    /**
     * Notifies the lag tracker about the data that is currently being
     * translated.
     * The inflight translation offset is used to calculate the translation
     * backlog.
     *
     * This state is in memory only and may be lost if the translation state is
     * discarded.
     */
    virtual void
      notify_inflight_translation_iteration(std::optional<kafka::offset>) = 0;

    /**
     * Returns an estimated timestamp for the batch with requested
     * `kafka::offset`.
     *
     * The timestamp is returned only when passed kafka offset is greater than
     * current target offset. This way the stm checkpoint batch contains a
     * timestamp that can be used to  estimate the lag.
     */
    virtual std::optional<model::timestamp>
      get_translated_offset_timestamp_estimate(kafka::offset) = 0;

    /**
     * Returns an estimate size of data that are ready to be translated.
     */
    virtual std::optional<size_t> translation_backlog() const = 0;
    /**
     * Returns a time point which is the deadline for the next translation
     * checkpoint required not to miss the target lag limit.
     */
    virtual scheduling::clock::time_point next_checkpoint_deadline() const = 0;

    static std::unique_ptr<translation_lag_tracker> make_default_lag_tracker(
      ss::lw_shared_ptr<cluster::partition>, cluster::topic_table&);
};

std::unique_ptr<table_creator>
make_default_table_creator(coordinator::frontend&);

} // namespace datalake::translation
