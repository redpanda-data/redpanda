/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/record_multiplexer.h"

#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"
#include "datalake/schemaless_translator.h"
#include "datalake/tests/test_data_writer.h"
#include "iceberg/values.h"
#include "model/record.h"
#include "storage/parser_utils.h"

#include <ios>
#include <stdexcept>
#include <system_error>

namespace datalake {
record_multiplexer::record_multiplexer(
  std::unique_ptr<data_writer_factory> writer_factory)
  : _translator{schemaless_translator()}
  , _writer_factory{std::move(writer_factory)} {}

ss::future<ss::stop_iteration>
record_multiplexer::operator()(model::record_batch batch) {
    if (batch.compressed()) {
        batch = co_await storage::internal::decompress_batch(std::move(batch));
    }
    auto first_timestamp = batch.header().first_timestamp.value();
    auto base_offset = static_cast<int64_t>(batch.base_offset());
    auto it = model::record_batch_iterator::create(batch);
    while (it.has_next()) {
        auto record = it.next();
        iobuf key = record.release_key();
        iobuf val = record.release_value();
        // *1000: Redpanda timestamps are milliseconds. Iceberg uses
        // microseconds.
        int64_t timestamp = (first_timestamp + record.timestamp_delta()) * 1000;
        int64_t offset = static_cast<int64_t>(base_offset)
                         + record.offset_delta();
        int64_t estimated_size = key.size_bytes() + val.size_bytes() + 16;

        // Translate the record
        auto& translator = get_translator();
        iceberg::struct_value data = translator.translate_event(
          std::move(key), std::move(val), timestamp, offset);
        // Send it to the writer

        try {
            auto& writer = co_await get_writer();
            _writer_status = co_await writer.add_data_struct(
              std::move(data), estimated_size);
        } catch (const std::runtime_error& err) {
            datalake_log.error("Failed to add data to writer");
        }
        if (_writer_status != data_writer_error::ok) {
            // If a write fails, the writer is left in an indeterminate state,
            // we cannot continue in this case.
            co_return ss::stop_iteration::yes;
        }
    }
    co_return ss::stop_iteration::no;
}

ss::future<result<chunked_vector<data_writer_result>, data_writer_error>>
record_multiplexer::end_of_stream() {
    if (_writer_status != data_writer_error::ok) {
        co_return _writer_status;
    }
    // TODO: once we have multiple _writers this should be a loop
    if (_writer) {
        chunked_vector<data_writer_result> ret;
        auto res = co_await _writer->finish();
        if (res.has_value()) {
            ret.push_back(res.value());
            co_return ret;
        } else {
            co_return res.error();
        }
    } else {
        co_return chunked_vector<data_writer_result>{};
    }
}

schemaless_translator& record_multiplexer::get_translator() {
    return _translator;
}

ss::future<data_writer&> record_multiplexer::get_writer() {
    if (!_writer) {
        auto& translator = get_translator();
        auto schema = translator.get_schema();
        _writer = co_await _writer_factory->create_writer(std::move(schema));
        if (!_writer) {
            // FIXME: modify create_writer to return a result and check that
            // here. This method should also return a result. That is coming in
            // one of the next commits. For now throw & catch.
            throw std::runtime_error("Could not create data writer");
        }
    }
    co_return *_writer;
}
} // namespace datalake
