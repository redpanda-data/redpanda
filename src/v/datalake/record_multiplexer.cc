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
#include "model/record.h"
#include "storage/parser_utils.h"

#include <seastar/core/loop.hh>

namespace datalake {

record_multiplexer::record_multiplexer(
  const model::ntp& ntp,
  std::unique_ptr<data_writer_factory> writer_factory,
  schema_manager& schema_mgr,
  type_resolver& type_resolver)
  : _log(datalake_log, fmt::format("{}", ntp))
  , _ntp(ntp)
  , _writer_factory{std::move(writer_factory)}
  , _schema_mgr(schema_mgr)
  , _type_resolver(type_resolver) {}

ss::future<ss::stop_iteration>
record_multiplexer::operator()(model::record_batch batch) {
    if (batch.compressed()) {
        batch = co_await storage::internal::decompress_batch(std::move(batch));
    }
    auto first_timestamp = batch.header().first_timestamp.value();

    auto it = model::record_batch_iterator::create(batch);

    while (it.has_next()) {
        auto record = it.next();
        iobuf key = record.share_key();
        iobuf val = record.share_value();
        auto timestamp = model::timestamp{
          first_timestamp + record.timestamp_delta()};
        kafka::offset offset{batch.base_offset()() + record.offset_delta()};
        int64_t estimated_size = key.size_bytes() + val.size_bytes();

        auto val_type_res = co_await _type_resolver.resolve_buf_type(
          std::move(val));
        if (val_type_res.has_error()) {
            switch (val_type_res.error()) {
            case type_resolver::errc::bad_input:
            case type_resolver::errc::translation_error:
                // XXX: need to differentiate missing schema ID from transient
                // registry errors.
            case type_resolver::errc::registry_error:
                auto invalid_res = co_await handle_invalid_record(
                  offset, record.share_key(), record.share_value(), timestamp);
                if (invalid_res.has_error()) {
                    _error = invalid_res.error();
                    co_return ss::stop_iteration::yes;
                }
                continue;
            }
        }

        auto record_data_res = co_await record_translator::translate_data(
          offset,
          std::move(key),
          val_type_res.value().type,
          std::move(val_type_res.value().parsable_buf),
          timestamp);
        if (record_data_res.has_error()) {
            switch (record_data_res.error()) {
            case record_translator::errc::translation_error:
                vlog(
                  _log.debug,
                  "Error translating data for record {}: {}",
                  offset,
                  record_data_res.error());
                auto invalid_res = co_await handle_invalid_record(
                  offset, record.share_key(), record.share_value(), timestamp);
                if (invalid_res.has_error()) {
                    _error = invalid_res.error();
                    co_return ss::stop_iteration::yes;
                }
                continue;
            }
        }
        auto record_type = record_translator::build_type(
          std::move(val_type_res.value().type));
        auto writer_iter = _writers.find(record_type.comps);
        if (writer_iter == _writers.end()) {
            auto get_ids_res = co_await _schema_mgr.get_registered_ids(
              _ntp.tp.topic, record_type.type);
            if (get_ids_res.has_error()) {
                auto e = get_ids_res.error();
                switch (e) {
                case schema_manager::errc::not_supported: {
                    auto invalid_res = co_await handle_invalid_record(
                      offset,
                      record.share_key(),
                      record.share_value(),
                      timestamp);
                    if (invalid_res.has_error()) {
                        _error = invalid_res.error();
                        co_return ss::stop_iteration::yes;
                    }
                    continue;
                }
                case schema_manager::errc::failed:
                case schema_manager::errc::shutting_down:
                    vlog(
                      _log.warn,
                      "Error getting field IDs for record {}: {}",
                      offset,
                      get_ids_res.error());
                    _error = data_writer_error::parquet_conversion_error;
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

        if (write_result != data_writer_error::ok) {
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

ss::future<result<record_multiplexer::write_result, data_writer_error>>
record_multiplexer::end_of_stream() {
    if (_error) {
        co_return *_error;
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

ss::future<result<std::nullopt_t, data_writer_error>>
record_multiplexer::handle_invalid_record(
  kafka::offset offset, iobuf key, iobuf val, model::timestamp ts) {
    vlog(_log.debug, "Handling invalid record {}", offset);
    int64_t estimated_size = key.size_bytes() + val.size_bytes();
    auto record_data_res = co_await record_translator::translate_data(
      offset,
      std::move(key),
      /*val_type*/ std::nullopt,
      std::move(val),
      ts);
    if (record_data_res.has_error()) {
        vlog(
          _log.error,
          "Error translating data to binary record {}: {}",
          offset,
          record_data_res.error());
        co_return data_writer_error::parquet_conversion_error;
    }
    auto record_type = record_translator::build_type(std::nullopt);

    // TODO: maybe this should be a writer specific for a dead-letter queue.
    auto writer_iter = _writers.find(record_type.comps);
    if (writer_iter == _writers.end()) {
        auto get_ids_res = co_await _schema_mgr.get_registered_ids(
          _ntp.tp.topic, record_type.type);
        if (get_ids_res.has_error()) {
            // TODO: log shutting_down errors at debug log and make other
            // errors log an error.
            vlog(
              _log.warn,
              "Error getting field IDs for binary record {}: {}",
              offset,
              get_ids_res.error());
            co_return data_writer_error::parquet_conversion_error;
        }
        auto [iter, _] = _writers.emplace(
          record_type.comps,
          std::make_unique<partitioning_writer>(
            *_writer_factory, std::move(record_type.type)));
        writer_iter = iter;
    }
    if (!_result.has_value()) {
        _result = write_result{
          .start_offset = offset,
        };
    }
    _result.value().last_offset = offset;
    auto& writer = writer_iter->second;
    auto write_result = co_await writer->add_data(
      std::move(record_data_res.value()), estimated_size);
    if (write_result != data_writer_error::ok) {
        vlog(
          _log.error,
          "Error adding data to writer for binary record {}: {}",
          offset,
          write_result);
        co_return write_result;
    }
    co_return std::nullopt;
}
} // namespace datalake
