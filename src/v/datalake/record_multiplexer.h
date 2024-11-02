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
#include "datalake/partitioning_writer.h"
#include "datalake/schema_identifier.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <memory>

namespace datalake {
class schema_manager;
class type_resolver;

// Consumes logs and sends records to the appropriate translator
// based on the schema ID. This is meant to be called with a
// read_committed_reader created from a kafka::partition_proxy.
class record_multiplexer {
public:
    struct write_result {
        // base offset of the first translated batch
        kafka::offset start_offset;
        // last offset of the last translated batch (inclusive)
        kafka::offset last_offset;
        // vector containing a list of files that were written during
        // translation.
        chunked_vector<local_file_metadata> data_files;
    };
    explicit record_multiplexer(
      const model::ntp& ntp,
      std::unique_ptr<data_writer_factory> writer,
      schema_manager& schema_mgr,
      type_resolver& type_resolver);

    ss::future<ss::stop_iteration> operator()(model::record_batch batch);
    ss::future<result<write_result, data_writer_error>> end_of_stream();

private:
    const model::ntp& _ntp;
    std::unique_ptr<data_writer_factory> _writer_factory;
    schema_manager& _schema_mgr;
    type_resolver& _type_resolver;
    chunked_hash_map<
      record_schema_components,
      std::unique_ptr<partitioning_writer>>
      _writers;

    std::optional<data_writer_error> _error;
    std::optional<write_result> _result;
};

} // namespace datalake
