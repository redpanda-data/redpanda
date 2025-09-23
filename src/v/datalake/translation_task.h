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
#include "datalake/cloud_data_io.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/data_writer_interface.h"
#include "datalake/fwd.h"
#include "datalake/location.h"
#include "datalake/record_multiplexer.h"
#include "datalake/translation/translation_probe.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "utils/retry_chain_node.h"

namespace features {
class feature_table;
}

namespace datalake {
/**
 * An abstraction representing a task of consuming data, translating them to
 * parquet and uploading to the object store
 */
class translation_task {
public:
    explicit translation_task(
      const model::ntp& ntp,
      model::revision_id topic_revision,
      std::unique_ptr<parquet_file_writer_factory> writer_factory,
      cloud_data_io& uploader,
      features::feature_table* features,
      schema_manager& schema_mgr,
      type_resolver& type_resolver,
      record_translator& record_translator,
      table_creator&,
      model::iceberg_invalid_record_action,
      location_provider,
      translation_probe&);
    enum class errc {
        file_io_error,
        cloud_io_error,
        flush_error,
        no_data,
        oom_error,
        time_limit_exceeded,
        shutting_down,
        out_of_disk,
        type_resolution_error,
    };

    using custom_partitioning_enabled
      = ss::bool_class<struct custom_partitioning_enabled_t>;

    /**
     * Executes translation using the data from the reader until aborted.
     * Can be called multiple times if needed. The results of translation can be
     * uploading using finish().
     */
    ss::future<> translate_once(
      model::record_batch_reader reader,
      kafka::offset start_offset,
      ss::abort_source&);

    /**
     * Current number of bytes flushed by all the open writers.
     */
    size_t flushed_bytes() const;

    /**
     * Flushes all the inflight translated bytes and releases memory
     * reservations. May not be called while translation is in progress.
     */
    ss::future<checked<void, errc>> flush();

    /**
     * Returns the last translated offset, if one exists
     */
    std::optional<kafka::offset> last_translated_offset() const;

    /**
     * Uploads the resulting translated files to the object store. The tasks
     * accepts an abort source indicating when the upload retires should be
     * stopped and a root retry chain node
     */
    ss::future<checked<coordinator::translated_offset_range, errc>> finish(
      custom_partitioning_enabled is_custom_partitioning_enabled,
      retry_chain_node& parent_rcn,
      ss::abort_source&) &&;

    /**
     * Cleans up any translated files locally and discards the translation
     * result. This task does not accept an abort source as it is intended
     * to be short running.
     */
    ss::future<checked<std::nullopt_t, translation_task::errc>> discard() &&;

    size_t buffered_bytes() const;

private:
    friend std::ostream& operator<<(std::ostream&, errc);

    ss::future<errc> delete_remote_files(
      chunked_vector<remote_path>, retry_chain_node& parent_rcn);

    static constexpr std::chrono::milliseconds _read_timeout{30000};
    prefix_logger _log;
    cloud_data_io* _cloud_io;
    schema_manager* _schema_mgr;
    type_resolver* _type_resolver;
    record_translator* _record_translator;
    table_creator* _table_creator;
    model::iceberg_invalid_record_action _invalid_record_action;
    location_provider _location_provider;
    translation_probe* _translation_probe;
    record_multiplexer _multiplexer;
};
} // namespace datalake
