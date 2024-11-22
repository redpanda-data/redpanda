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

#include "base/vlog.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_creator.h"
#include "model/record.h"
#include "storage/parser_utils.h"

#include <seastar/core/loop.hh>

namespace datalake {

record_multiplexer::record_multiplexer(
  const model::ntp& ntp,
  model::revision_id topic_revision,
  std::unique_ptr<parquet_file_writer_factory> writer_factory,
  schema_manager& schema_mgr,
  type_resolver& type_resolver,
  record_translator& record_translator,
  table_creator& table_creator)
  : _log(datalake_log, fmt::format("{}", ntp))
  , _ntp(ntp)
  , _topic_revision(topic_revision)
  , _writer_factory{std::move(writer_factory)}
  , _schema_mgr(schema_mgr)
  , _type_resolver(type_resolver)
  , _record_translator(record_translator)
  , _table_creator(table_creator) {}

ss::future<ss::stop_iteration>
record_multiplexer::operator()(model::record_batch batch) {
    if (batch.compressed()) {
        batch = co_await storage::internal::decompress_batch(std::move(batch));
    }
    auto first_timestamp = batch.header().first_timestamp.value();

    auto it = model::record_batch_iterator::create(batch);

    while (it.has_next()) {
        auto record = it.next();
        auto key = record.share_key_opt();
        auto val = record.share_value_opt();
        auto timestamp = model::timestamp{
          first_timestamp + record.timestamp_delta()};
        kafka::offset offset{batch.base_offset()() + record.offset_delta()};
        int64_t estimated_size = (key ? key->size_bytes() : 0)
                                 + (val ? val->size_bytes() : 0);
        chunked_vector<std::pair<std::optional<iobuf>, std::optional<iobuf>>>
          header_kvs;
        for (auto& hdr : record.headers()) {
            header_kvs.emplace_back(hdr.share_key_opt(), hdr.share_value_opt());
        }

        auto val_type_res = co_await _type_resolver.resolve_buf_type(
          std::move(val));
        if (val_type_res.has_error()) {
            switch (val_type_res.error()) {
            case type_resolver::errc::registry_error:
                _error = writer_error::parquet_conversion_error;
                co_return ss::stop_iteration::yes;
            case type_resolver::errc::bad_input:
            case type_resolver::errc::translation_error:
                auto invalid_res = co_await handle_invalid_record(
                  offset,
                  record.share_key(),
                  record.share_value(),
                  timestamp,
                  std::move(header_kvs));
                if (invalid_res.has_error()) {
                    _error = invalid_res.error();
                    co_return ss::stop_iteration::yes;
                }
                continue;
            }
        }

        auto record_data_res = co_await _record_translator.translate_data(
          _ntp.tp.partition,
          offset,
          std::move(key),
          val_type_res.value().type,
          std::move(val_type_res.value().parsable_buf),
          timestamp,
          header_kvs);
        if (record_data_res.has_error()) {
            switch (record_data_res.error()) {
            case record_translator::errc::unexpected_schema:
            case record_translator::errc::translation_error:
                vlog(
                  _log.debug,
                  "Error translating data for record {}: {}",
                  offset,
                  record_data_res.error());
                auto invalid_res = co_await handle_invalid_record(
                  offset,
                  record.share_key(),
                  record.share_value(),
                  timestamp,
                  std::move(header_kvs));
                if (invalid_res.has_error()) {
                    _error = invalid_res.error();
                    co_return ss::stop_iteration::yes;
                }
                continue;
            }
        }
        auto record_type = _record_translator.build_type(
          std::move(val_type_res.value().type));
        auto writer_iter = _writers.find(record_type.comps);
        if (writer_iter == _writers.end()) {
            auto ensure_res = co_await _table_creator.ensure_table(
              _ntp.tp.topic, _topic_revision, record_type.comps);
            if (ensure_res.has_error()) {
                auto e = ensure_res.error();
                switch (e) {
                case table_creator::errc::incompatible_schema: {
                    auto invalid_res = co_await handle_invalid_record(
                      offset,
                      record.share_key(),
                      record.share_value(),
                      timestamp,
                      std::move(header_kvs));
                    if (invalid_res.has_error()) {
                        _error = invalid_res.error();
                        co_return ss::stop_iteration::yes;
                    }
                    continue;
                }
                case table_creator::errc::failed:
                    vlog(
                      _log.warn,
                      "Error ensuring table schema for record {}",
                      offset);
                    [[fallthrough]];
                case table_creator::errc::shutting_down:
                    _error = writer_error::parquet_conversion_error;
                }
                co_return ss::stop_iteration::yes;
            }

            auto get_ids_res = co_await _schema_mgr.get_registered_ids(
              _ntp.tp.topic, record_type.type);
            if (get_ids_res.has_error()) {
                auto e = get_ids_res.error();
                switch (e) {
                case schema_manager::errc::not_supported:
                case schema_manager::errc::failed:
                    vlog(
                      _log.warn,
                      "Error getting field IDs for record {}: {}",
                      offset,
                      get_ids_res.error());
                    [[fallthrough]];
                case schema_manager::errc::shutting_down:
                    _error = writer_error::parquet_conversion_error;
                }
                co_return ss::stop_iteration::yes;
            }

            auto [iter, _] = _writers.emplace(
              record_type.comps,
              std::make_unique<partitioning_writer>(
                *_writer_factory, std::move(record_type.type)));
            writer_iter = iter;
        }

        // TODO: we want to ensure we're using an offset translating reader so
        // that these will be Kafka offsets, not Raft offsets.
        if (!_result.has_value()) {
            _result = write_result{
              .start_offset = offset,
            };
        }

        _result.value().last_offset = offset;

        auto& writer = writer_iter->second;
        auto write_result = co_await writer->add_data(
          std::move(record_data_res.value()), estimated_size);

        if (write_result != writer_error::ok) {
            vlog(
              _log.warn,
              "Error adding data to writer for record {}: {}",
              offset,
              write_result);
            _error = write_result;
            // If a write fails, the writer is left in an indeterminate state,
            // we cannot continue in this case.
            co_return ss::stop_iteration::yes;
        }
    }
    co_return ss::stop_iteration::no;
}

ss::future<result<record_multiplexer::write_result, writer_error>>
record_multiplexer::end_of_stream() {
    if (_error) {
        co_return *_error;
    }
    if (!_result) {
        // no batches were processed.
        co_return writer_error::no_data;
    }
    auto writers = std::move(_writers);
    for (auto& [id, writer] : writers) {
        auto res = co_await std::move(*writer).finish();
        if (res.has_error()) {
            _error = res.error();
            continue;
        }
        auto& files = res.value();
        std::move(
          files.begin(), files.end(), std::back_inserter(_result->data_files));
    }
    if (_error) {
        co_return *_error;
    }
    co_return std::move(*_result);
}

ss::future<result<std::nullopt_t, writer_error>>
record_multiplexer::handle_invalid_record(
  kafka::offset offset,
  std::optional<iobuf>,
  std::optional<iobuf>,
  model::timestamp,
  chunked_vector<std::pair<std::optional<iobuf>, std::optional<iobuf>>>) {
    vlog(_log.debug, "Dropping invalid record at offset {}", offset);
    // TODO: add a metric!
    // TODO: dead-letter table?
    co_return std::nullopt;
}
} // namespace datalake
