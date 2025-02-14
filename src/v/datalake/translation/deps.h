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
#include "features/fwd.h"
#include "model/record_batch_reader.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/io_priority_class.hh>

namespace datalake::translation {

class noop_mem_tracker : public writer_mem_tracker {
public:
    ss::future<> maybe_reserve_memory(size_t bytes, ss::abort_source&) override;
    void update_current_memory_usage(size_t) override;
    void release() override;
};

class writer_reservations_impl : public writer_mem_tracker {
public:
    explicit writer_reservations_impl(
      scheduling::reservations_tracker& scheduling_reservations)
      : _reservations_tracker(scheduling_reservations) {}

    ss::future<> maybe_reserve_memory(size_t bytes, ss::abort_source&) override;
    void update_current_memory_usage(size_t) override;
    void release() override;

private:
    size_t _available_memory{0};
    size_t _total_reserved_memory{0};
    scheduling::reservations_tracker& _reservations_tracker;
    chunked_vector<ssx::semaphore_units> _reservations;
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
        coordinator::fetch_latest_translated_offset_request)
      = 0;

    static std::unique_ptr<coordinator_api>
    make_default_coordinator_api(coordinator::frontend&);
};

class data_source {
public:
    data_source() = default;
    data_source(const data_source&) = delete;
    data_source& operator=(const data_source&) = delete;
    data_source(data_source&&) = delete;
    data_source& operator=(data_source&&) = delete;

    virtual ~data_source() = default;

    virtual const model::ntp& ntp() const = 0;

    virtual model::revision_id topic_revision() const = 0;

    virtual model::term_id term() const = 0;

    virtual ss::future<kafka::offset> wait_for_data_to_translate(
      std::optional<kafka::offset> last_translated_offset, ss::abort_source&)
      = 0;

    virtual ss::future<std::optional<model::record_batch_reader>>
    make_log_reader(kafka::offset, ss::io_priority_class, ss::abort_source&)
      = 0;

    virtual kafka::offset min_offset_for_translation() const = 0;

    virtual std::optional<kafka::offset> max_offset_for_translation() const = 0;

    virtual ss::future<std::error_code> update_highest_translated_offset(
      kafka::offset new_offset,
      model::term_id,
      model::timeout_clock::duration timeout,
      ss::abort_source&)
      = 0;

    static std::unique_ptr<data_source>
      make_default_data_source(ss::lw_shared_ptr<cluster::partition>);
};

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
    virtual ss::future<>
    translate_now(model::record_batch_reader, ss::abort_source&) = 0;

    /**
     * Flushes all the buffered state guaranteeing release of resources.
     */
    virtual ss::future<> flush() = 0;

    /**
     * Reconciles the translator configurations.
     */
    virtual void reconcile_properties() = 0;

    /**
     * Cleans up state and uploads data to cloud storage. Should be called in
     * all cases for appropriate cleanup.
     */
    virtual ss::future<std::optional<coordinator::translated_offset_range>>
    finish(retry_chain_node&, ss::abort_source&) = 0;

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
      remote_path,
      scheduling::reservations_tracker&,
      ss::sharded<cluster::topic_table>*,
      ss::sharded<features::feature_table>*);
};

std::unique_ptr<table_creator>
make_default_table_creator(coordinator::frontend&);

} // namespace datalake::translation
