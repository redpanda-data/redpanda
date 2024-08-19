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
#include "iceberg/values.h"
#include "model/record.h"
#include "storage/parser_utils.h"

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
    batch.for_each_record([&batch, this](model::record&& record) {
        iobuf key = record.release_key();
        iobuf val = record.release_value();
        // *1000: Redpanda timestamps are milliseconds. Iceberg uses
        // microseconds.
        int64_t timestamp = (batch.header().first_timestamp.value()
                             + record.timestamp_delta())
                            * 1000;
        int64_t offset = static_cast<int64_t>(batch.base_offset())
                         + record.offset_delta();
        int64_t estimated_size = key.size_bytes() + val.size_bytes() + 16;

        // Translate the record
        auto& translator = get_translator();
        iceberg::struct_value data = std::visit(
          [&key, &val, timestamp, offset](schemaless_translator& tr) {
              return tr.translate_event(
                std::move(key), std::move(val), timestamp, offset);
          },
          translator);

        // Send it to the writer
        auto& writer = get_writer();
        writer.add_data_struct(std::move(data), estimated_size);
    });
    co_return ss::stop_iteration::no;
}

ss::future<chunked_vector<data_writer_result>>
record_multiplexer::end_of_stream() {
    // TODO: once we have multiple _writers this should be a loop
    if (_writer) {
        chunked_vector<data_writer_result> ret;
        data_writer_result res = _writer->finish();
        ret.push_back(res);
        co_return ret;
    } else {
        co_return chunked_vector<data_writer_result>{};
    }
}

record_multiplexer::translator& record_multiplexer::get_translator() {
    return _translator;
}

data_writer& record_multiplexer::get_writer() {
    if (!_writer) {
        auto schema = std::visit(
          [](schemaless_translator& tr) { return tr.get_schema(); },
          _translator);
        _writer = _writer_factory->create_writer(std::move(schema));
    }
    return *_writer;
}
} // namespace datalake
