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

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "datalake/errors.h"
#include "datalake/fwd.h"
#include "features/fwd.h"
#include "kafka/data/partition_proxy.h"
#include "model/record_batch_reader.h"
#include "random/simple_time_jitter.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

namespace kafka {
class read_committed_reader;
} // namespace kafka
namespace datalake {
class record_translator;
} // namespace datalake
namespace datalake::translation {

/**
 * A partition translator is responsible for translating data from a given
 * partition periodically. A single instance of translator should be active for
 * a given partition and is attached to the leader replica. Temporarily there
 * may be two translators during a leadership change but the translator
 * corresponding the leader that stepped down should eventually stop.
 *
 * The translator periodically wakes up, checks if there is new data that is
 * pending translation and creates a reader for the data range and hands it off
 * to low level iceberg translator that converts the data.
 *
 * The translator works in tandem with the datalake coordinator responsible for
 * this ntp. A successful local iceberg translation of a data range is followed
 * by that state checkpointing with the coordinator. A coordinator tracks the
 * latest translated offset for a given ntp (along with other metadata) which is
 * also then synchronized with the local translation stm that enforces
 * max_collectible_offset across all replicas.
 *
 * The logic for this translator is as follows.
 *
 * while (!aborted && !term_changed):
 *    sleep(interval)
 *    reconcile_with_coordinator()
 *    md = translate_newly_arrived_data_since_last_checkpoint()
 *    checkpoint_with_coordinator(md)
 *    sync_stm_with_coordinator(md)
 */

class partition_translator {
public:
    explicit partition_translator(
      ss::lw_shared_ptr<cluster::partition> partition,
      ss::sharded<coordinator::frontend>* frontend,
      ss::sharded<features::feature_table>* features,
      std::unique_ptr<datalake::cloud_data_io>* cloud_io,
      schema_manager* schema_mgr,
      std::unique_ptr<type_resolver> type_resolver,
      std::unique_ptr<record_translator> record_translator,
      std::chrono::milliseconds translation_interval,
      ss::scheduling_group sg,
      size_t reader_max_bytes,
      std::unique_ptr<ssx::semaphore>* parallel_translations);
    ~partition_translator();

    void start_translation_in_background(ss::scheduling_group);

    ss::future<> stop();

    std::chrono::milliseconds translation_interval() const;
    void reset_translation_interval(std::chrono::milliseconds new_base);

private:
    bool can_continue() const;

    ss::future<std::optional<kafka::offset>> reconcile_with_coordinator();

    ss::future<> do_translate();

    void
    update_translation_lag(kafka::offset max_translated_kafka_offset) const;
    void update_commit_lag(
      std::optional<kafka::offset> max_committed_kafka_offset) const;

    using translation_success = ss::bool_class<struct translation_success>;
    ss::future<translation_success> do_translate_once(retry_chain_node& parent);
    ss::future<model::record_batch_reader> make_reader();
    ss::future<std::optional<coordinator::translated_offset_range>>
    do_translation_for_range(
      retry_chain_node& parent,
      kafka::offset read_begin,
      kafka::offset read_end);

    using checkpoint_result = ss::bool_class<struct checkpoint_result>;
    ss::future<checkpoint_result> checkpoint_translated_data(
      retry_chain_node& parent,
      kafka::offset reader_begin_offset,
      coordinator::translated_offset_range task_result);

    kafka::offset min_offset_for_translation() const;
    // Returns max consumable offset for translation.
    std::optional<kafka::offset> max_offset_for_translation() const;

    model::term_id _term;
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::shared_ptr<translation_stm> _stm;
    ss::sharded<coordinator::frontend>* _frontend;
    ss::sharded<features::feature_table>* _features;
    std::unique_ptr<datalake::cloud_data_io>* _cloud_io;
    schema_manager* _schema_mgr;
    std::unique_ptr<type_resolver> _type_resolver;
    std::unique_ptr<record_translator> _record_translator;
    std::unique_ptr<table_creator> _table_creator;
    std::unique_ptr<kafka::partition_proxy> _partition_proxy;
    using jitter_t
      = simple_time_jitter<ss::lowres_clock, std::chrono::milliseconds>;
    jitter_t _jitter;
    // Maximum number of bytes read in one go of translation.
    // Memory usage tracking is not super sophisticated here, so we assume
    // all data batches from the reader are buffered in the writer until
    // they are flushed to disk. This is also factored into determining
    // how many parallel translations can run at one point as we operate under
    // a memory budget for all translations (semaphore below).
    size_t _max_bytes_per_reader;
    std::unique_ptr<ssx::semaphore>* _parallel_translations;
    std::filesystem::path _writer_scratch_space;
    ss::gate _gate;
    ss::abort_source _as;
    prefix_logger _logger;
};

} // namespace datalake::translation
