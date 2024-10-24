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

#include "datalake/data_writer_interface.h"
#include "datalake/schemaless_translator.h"
#include "model/record.h"

#include <seastar/core/future.hh>

#include <memory>

namespace datalake {

/*
Consumes logs and sends records to the appropriate translator
based on the schema ID. This is meant to be called with a
read_committed_reader created from a kafka::partition_proxy.

This is a skeleton class. Currently it only uses the trivial
schemaless_translator.

pseudocode:
for each record {
    t = get_translator(record.schema_id);
    d = t.translate(record);
    w = get_data_writer(record.schema_id);
    w.record(d);
}
*/
// TODO: add cleanup of files that were already written while translation
// failed.
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
    explicit record_multiplexer(std::unique_ptr<data_writer_factory> writer);

    ss::future<ss::stop_iteration> operator()(model::record_batch batch);
    ss::future<result<write_result, data_writer_error>> end_of_stream();

private:
    schemaless_translator& get_translator();
    ss::future<result<std::reference_wrapper<data_writer>, data_writer_error>>
    get_writer();

    // TODO: in a future PR this will be a map of translators keyed by schema_id
    schemaless_translator _translator;
    std::unique_ptr<data_writer_factory> _writer_factory;

    // TODO: similarly this will be a map keyed by schema_id
    std::unique_ptr<data_writer> _writer;

    std::optional<data_writer_error> _error;
    std::optional<write_result> _result;
};

} // namespace datalake
