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
#include "datalake/data_writer_interface.h"
#include "datalake/fwd.h"
#include "datalake/location.h"
#include "datalake/partitioning_writer.h"
#include "datalake/schema_identifier.h"
#include "datalake/translation/translation_probe.h"
#include "iceberg/field_name_comparison.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timestamp.h"
#include "utils/prefix_logger.h"

#include <seastar/core/future.hh>

#include <memory>

namespace features {
class feature_table;
}

namespace datalake {
class record_translator;
class schema_manager;
class type_resolver;

/**
 * record_multiplexer
 *
 * Consumes logs and sends records to the appropriate translator
 * based on the schema ID. This is meant to be called with a
 * read_committed_reader created from a kafka::partition_proxy.
 * We want the multiplexer to work  across scheduling iterations
 * and release resouces inbetween. The pattern would look something like:
 *
 * mux = create_mux();
 * mux.multiplex(reader1...)
 * mux.flush_writers(); // optional
 * mux.multiplex(reader2..)
 * mux.flush_writers(); // optional
 * ...
 * ...
 *
 * result = co_await std::move(mux).finish();
 */
class record_multiplexer {
public:
    struct write_result {
        // base offset of the first translated batch
        kafka::offset start_offset;
        // last offset of the last translated batch (inclusive)
        kafka::offset last_offset;
        // Total number of kafka bytes processed by the multiplexer
        uint64_t kafka_bytes_processed{0};
    };
    explicit record_multiplexer(
      const model::ntp& ntp,
      model::revision_id topic_revision,
      std::unique_ptr<parquet_file_writer_factory> writer,
      schema_manager& schema_mgr,
      type_resolver& type_resolver,
      record_translator& record_translator,
      table_creator&,
      model::iceberg_invalid_record_action,
      iceberg::field_name_comparison norm,
      location_provider,
      translation_probe&,
      features::feature_table* features);

    /**
     * Multiplex the data from a reader into writers per schema and partition.
     * Can be called multiple times in succession before calling finish().
     *
     * start_offset controls minimum offset from which multiplexer can work. If
     * the previous translation stopped in the middle of a batch, we do not want
     * to multiplex already translated offsets in the batch, start_offset helps
     * solve that problem.
     */
    ss::future<> multiplex(
      model::record_batch_reader reader,
      kafka::offset start_offset,
      model::timeout_clock::time_point deadline,
      ss::abort_source& as);

    /**
     * Abortable multiplexing on a single batch. Visible for testing.
     */
    ss::future<ss::stop_iteration> do_multiplex(
      model::record_batch batch, kafka::offset start_offset, ss::abort_source&);

    /**
     * Forces a flush on all the underlying file writers resulting in freeing
     * up buffered in flight writes. May not be called in parallel with other
     * methods.
     */
    ss::future<writer_error> flush_writers();

    struct finished_files {
        // vector containing a list of files that were written during
        // translation.
        chunked_vector<partitioning_writer::partitioned_file> data_files;
        // files with invalid records
        chunked_vector<partitioning_writer::partitioned_file> dlq_files;
    };
    /**
     * Cleanup and return the result. Should be the last operation to
     * be called. May not be called in parallel while multiplexing is in
     * progress.
     */
    ss::future<result<write_result, writer_error>> finish(finished_files&) &&;

    size_t buffered_bytes() const;

    size_t flushed_bytes() const;

    std::optional<kafka::offset> last_translated_offset() const;

private:
    // Handles the given record components of a record that is invalid for the
    // target table.
    ss::future<result<std::nullopt_t, writer_error>> handle_invalid_record(
      translation_probe::invalid_record_cause,
      kafka::offset,
      std::optional<iobuf>,
      std::optional<iobuf>,
      model::timestamp,
      model::timestamp_type ts_t,
      const chunked_vector<model::record_header>&,
      ss::abort_source&);

    prefix_logger _log;
    const model::ntp& _ntp;
    model::revision_id _topic_revision;
    std::unique_ptr<parquet_file_writer_factory> _writer_factory;
    schema_manager& _schema_mgr;
    type_resolver& _type_resolver;
    record_translator& _record_translator;
    table_creator& _table_creator;
    model::iceberg_invalid_record_action _invalid_record_action;
    iceberg::field_name_comparison _norm;
    location_provider _location_provider;
    translation_probe& _translation_probe;
    [[maybe_unused]] features::feature_table* _features;
    chunked_hash_map<
      record_schema_components,
      std::unique_ptr<partitioning_writer>>
      _writers;
    std::unique_ptr<partitioning_writer> _invalid_record_writer;

    std::optional<writer_error> _error;
    std::optional<write_result> _result;
    // Total number of kafka bytes processed by the multiplexer
    uint64_t _reader_bytes_processed = 0;
};

} // namespace datalake
