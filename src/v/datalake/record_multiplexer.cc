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
#include "datalake/location.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_creator.h"
#include "datalake/table_id_provider.h"
#include "datalake/translation/translation_probe.h"
#include "features/feature_table.h"
#include "model/batch_compression.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"

#include <seastar/core/loop.hh>

namespace datalake {

namespace {

// Get the data location for the table. Some catalogs require using the property
// `write.data.path`. Otherwise, it defaults to <table location>/data.
iceberg::uri get_data_location(const schema_manager::table_info& table_info) {
    static constexpr std::string_view write_data_path_prop = "write.data.path";

    if (table_info.properties.has_value()) {
        auto it = table_info.properties->find(write_data_path_prop);
        if (it != table_info.properties->end()) {
            return iceberg::uri(it->second);
        }
    }

    return iceberg::uri(fmt::format("{}/data", table_info.location));
}

template<typename Func>
requires requires(Func f, model::record_batch batch) {
    { f(std::move(batch)) } -> std::same_as<ss::future<ss::stop_iteration>>;
}
class relaying_consumer {
public:
    explicit relaying_consumer(Func f)
      : _func(std::move(f)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        auto batch_raw_size = b.size_bytes();
        return _func(std::move(b))
          .then([this, batch_raw_size](ss::stop_iteration si) {
              _total_read_bytes += batch_raw_size;
              return si;
          });
    }
    uint64_t end_of_stream() { return _total_read_bytes; }

private:
    uint64_t _total_read_bytes{0};
    Func _func;
};
} // namespace

record_multiplexer::record_multiplexer(
  const model::ntp& ntp,
  model::revision_id topic_revision,
  std::unique_ptr<parquet_file_writer_factory> writer_factory,
  schema_manager& schema_mgr,
  type_resolver& type_resolver,
  record_translator& record_translator,
  table_creator& table_creator,
  model::iceberg_invalid_record_action invalid_record_action,
  location_provider location_provider,
  translation_probe& translation_probe,
  features::feature_table* features)
  : _log(datalake_log, fmt::format("{}", ntp))
  , _ntp(ntp)
  , _topic_revision(topic_revision)
  , _writer_factory{std::move(writer_factory)}
  , _schema_mgr(schema_mgr)
  , _type_resolver(type_resolver)
  , _record_translator(record_translator)
  , _table_creator(table_creator)
  , _invalid_record_action(invalid_record_action)
  , _location_provider(std::move(location_provider))
  , _translation_probe(translation_probe)
  , _features(features) {}

ss::future<> record_multiplexer::multiplex(
  model::record_batch_reader reader,
  kafka::offset start_offset,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    _reader_bytes_processed += co_await std::move(reader).consume(
      relaying_consumer{
        [this, start_offset, &as](model::record_batch b) mutable {
            return do_multiplex(std::move(b), start_offset, as);
        }},
      deadline);
}

ss::future<ss::stop_iteration> record_multiplexer::do_multiplex(
  model::record_batch batch, kafka::offset start_offset, ss::abort_source& as) {
    const auto raw_size_bytes = batch.size_bytes();
    _translation_probe.increment_raw_bytes_processed(raw_size_bytes);
    if (batch.compressed()) {
        batch = co_await model::decompress_batch(batch);
    }
    const auto decompressed_size_bytes = batch.size_bytes();
    _translation_probe.increment_decompressed_bytes_processed(
      decompressed_size_bytes);
    vlog(
      _log.trace,
      "processing batch: offset_range=[{},{}], records={}, "
      "raw_bytes={}, decompressed_bytes={}",
      batch.base_offset(),
      batch.last_offset(),
      batch.record_count(),
      raw_size_bytes,
      decompressed_size_bytes);

    auto batch_header = batch.header();
    auto timestamp_type = batch_header.attrs.timestamp_type();
    auto is_broker_time = timestamp_type == model::timestamp_type::append_time;
    auto first_timestamp = batch_header.first_timestamp.value();
    auto max_timestamp = batch_header.max_timestamp;
    auto it = model::record_batch_iterator::create(std::move(batch));
    while (it.has_next()) {
        if (as.abort_requested()) {
            vlog(_log.debug, "Abort requested, stopping translation");
            co_return ss::stop_iteration::yes;
        }
        auto record = it.next();
        auto key = record.share_key_opt();
        auto val = record.share_value_opt();
        auto timestamp = is_broker_time
                           ? max_timestamp
                           : model::timestamp{
                               first_timestamp + record.timestamp_delta()};
        kafka::offset offset{
          batch_header.base_offset() + record.offset_delta()};
        if (offset < start_offset) {
            continue;
        }
        int64_t estimated_size = (key ? key->size_bytes() : 0)
                                 + (val ? val->size_bytes() : 0);

        auto val_type_res = co_await _type_resolver.resolve_buf_type(
          std::move(val));
        if (val_type_res.has_error()) {
            auto err = val_type_res.error();
            vlog(
              _log.warn,
              "Error resolving type for record at offset {}, batch: {}: {}",
              offset,
              batch_header,
              err);
            switch (err) {
            case type_resolver::errc::registry_error:
            case type_resolver::errc::invalid_config:
                _error = writer_error::retryable_type_resolution_error;
                co_return ss::stop_iteration::yes;
            case type_resolver::errc::bad_input:
            case type_resolver::errc::translation_error:
                auto invalid_res = co_await handle_invalid_record(
                  translation_probe::invalid_record_cause::
                    failed_kafka_schema_resolution,
                  offset,
                  record.share_key(),
                  record.share_value(),
                  timestamp,
                  timestamp_type,
                  record.headers(),
                  as);
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
          timestamp_type,
          record.headers());
        if (record_data_res.has_error()) {
            auto err = record_data_res.error();
            vlog(
              _log.warn,
              "Error translating data for record at offset {}, batch: {}: {}",
              offset,
              batch_header,
              err);
            switch (err) {
            case record_translator::errc::unexpected_schema:
            case record_translator::errc::translation_error:
                auto invalid_res = co_await handle_invalid_record(
                  translation_probe::invalid_record_cause::
                    failed_data_translation,
                  offset,
                  record.share_key(),
                  record.share_value(),
                  timestamp,
                  timestamp_type,
                  record.headers(),
                  as);
                if (invalid_res.has_error()) {
                    _error = invalid_res.error();
                    co_return ss::stop_iteration::yes;
                }
                continue;
            }
        }
        auto& val_type = val_type_res.value().type;
        record_schema_components comps{
          .key_identifier = std::nullopt,
          .val_identifier = val_type.has_value()
                              ? std::make_optional((*val_type)->id)
                              : std::nullopt,
        };
        auto writer_iter = _writers.find(comps);
        if (writer_iter == _writers.end()) {
            auto record_type = _record_translator.build_type(
              std::move(val_type));
            auto ensure_res = co_await _table_creator.ensure_table(
              _ntp.tp.topic, _topic_revision, record_type.comps);
            if (ensure_res.has_error()) {
                auto e = ensure_res.error();
                switch (e) {
                case table_creator::errc::incompatible_schema: {
                    auto invalid_res = co_await handle_invalid_record(
                      translation_probe::invalid_record_cause::
                        failed_iceberg_schema_resolution,
                      offset,
                      record.share_key(),
                      record.share_value(),
                      timestamp,
                      timestamp_type,
                      record.headers(),
                      as);
                    if (invalid_res.has_error()) {
                        _error = invalid_res.error();
                        co_return ss::stop_iteration::yes;
                    }
                    continue;
                }
                case table_creator::errc::failed:
                    vlog(
                      _log.warn,
                      "Error ensuring table schema for record {}: {}",
                      offset,
                      e);
                    _error = writer_error::unknown_error;
                    break;
                case table_creator::errc::shutting_down:
                    _error = writer_error::shutting_down;
                }
                co_return ss::stop_iteration::yes;
            }

            auto table_id = table_id_provider::table_id(_ntp.tp.topic);
            std::optional<std::reference_wrapper<iceberg::struct_type>>
              desired_type;
            if (!_features->is_active(
                  features::feature::iceberg_schema_merging)) {
                desired_type = std::make_optional(std::ref(record_type.type));
            }
            auto load_res = co_await _schema_mgr.get_table_info(
              table_id, desired_type);
            if (load_res.has_error()) {
                auto e = load_res.error();
                switch (e) {
                case schema_manager::errc::not_supported:
                case schema_manager::errc::failed:
                    vlog(
                      _log.warn,
                      "Error getting table info for record {}: {}",
                      offset,
                      e);
                    _error = writer_error::unknown_error;
                    break;
                case schema_manager::errc::shutting_down:
                    _error = writer_error::shutting_down;
                }
                co_return ss::stop_iteration::yes;
            }

            if (!load_res.value().fill_registered_ids(record_type.type)) {
                // This shouldn't happen because we ensured the schema with the
                // call to table_creator. Probably someone managed to change the
                // table between two calls.
                vlog(
                  _log.warn,
                  "expected to successfully fill field IDs for record {}",
                  offset);
                _error = writer_error::unknown_error;
                co_return ss::stop_iteration::yes;
            }

            auto data_location = get_data_location(load_res.value());
            auto data_remote_path = _location_provider.from_uri(data_location);
            if (!data_remote_path) {
                vlog(
                  _log.warn,
                  "Error getting location prefix for {} while creating writer "
                  "at offset {}",
                  load_res.value().location,
                  offset);
                _error = writer_error::unknown_error;
                co_return ss::stop_iteration::yes;
            }

            auto [iter, _] = _writers.emplace(
              record_type.comps,
              std::make_unique<partitioning_writer>(
                *_writer_factory,
                load_res.value().schema.schema_id,
                std::move(record_type.type),
                std::move(load_res.value().partition_spec),
                std::move(data_remote_path.value())));
            writer_iter = iter;
        }

        auto& writer = writer_iter->second;
        auto add_data_result = co_await writer->add_data(
          std::move(record_data_res.value()), estimated_size, as);

        if (add_data_result != writer_error::ok) {
            vlogl(
              _log,
              is_recoverable_error(add_data_result) ? ss::log_level::debug
                                                    : ss::log_level::warn,
              "Error adding data to writer for record {}: {}",
              offset,
              add_data_result);
            _error = add_data_result;
            // If a write fails, the writer is left in an indeterminate state,
            // we cannot continue in this case.
            co_return ss::stop_iteration::yes;
        }

        // TODO: we want to ensure we're using an offset translating reader so
        // that these will be Kafka offsets, not Raft offsets.
        if (!_result.has_value()) {
            _result = write_result{
              .start_offset = offset,
            };
        }
        _result.value().last_offset = offset;
    }

    vlog(
      _log.trace,
      "batch processing complete: last_offset={}, writers_active={}",
      _result.has_value() ? _result.value().last_offset : kafka::offset{-1},
      _writers.size());

    co_return ss::stop_iteration::no;
}

ss::future<writer_error> record_multiplexer::flush_writers() {
    if (_error && !is_recoverable_error(_error.value())) {
        co_return *_error;
    }
    auto result = co_await ss::coroutine::as_future(
      ss::max_concurrent_for_each(
        _writers, 10, [](auto& entry) { return entry.second->flush(); }));
    if (result.failed()) {
        auto ex = result.get_exception();
        vlog(_log.warn, "Error flushing writers: {}", ex);
        _error = writer_error::flush_error;
        co_return _error.value();
    }
    co_return writer_error::ok;
}

ss::future<result<record_multiplexer::write_result, writer_error>>
record_multiplexer::finish(
  record_multiplexer::finished_files& finished_files) && {
    vlog(
      _log.trace,
      "starting multiplexer finish: writers={}, kafka_bytes_processed={}",
      _writers.size(),
      _reader_bytes_processed);

    auto writers = std::move(_writers);
    for (auto& [id, writer] : writers) {
        auto res = co_await std::move(*writer).finish();
        if (res.has_error()) {
            vlog(_log.trace, "writer finish error: {}", res.error());
            _error = res.error();
            continue;
        }
        auto& files = res.value();
        vlog(_log.trace, "writer finished: files_created={})", files.size());
        std::move(
          files.begin(),
          files.end(),
          std::back_inserter(finished_files.data_files));
    }
    if (_invalid_record_writer) {
        auto writer = std::move(_invalid_record_writer);
        auto res = co_await std::move(*writer).finish();
        if (res.has_error()) {
            _error = res.error();
        } else {
            auto& files = res.value();
            vlog(
              _log.trace,
              "invalid record writer finished: dlq_files_created={}",
              files.size());
            std::move(
              files.begin(),
              files.end(),
              std::back_inserter(finished_files.dlq_files));
        }
    }
    if (_error && !is_recoverable_error(_error.value())) {
        co_return *_error;
    }
    if (!_result) {
        // no batches were processed.
        co_return writer_error::no_data;
    }
    _result->kafka_bytes_processed = _reader_bytes_processed;

    vlog(
      _log.trace,
      "multiplexer finish complete: offset_range=[{},{}], "
      "total_records={}, kafka_bytes={}",
      _result->start_offset,
      _result->last_offset,
      _result->last_offset() - _result->start_offset() + 1,
      _result->kafka_bytes_processed);

    co_return std::move(*_result);
}

size_t record_multiplexer::buffered_bytes() const {
    size_t result = 0;
    for (const auto& [_, writer] : _writers) {
        result += writer->buffered_bytes();
    }
    return result;
}

size_t record_multiplexer::flushed_bytes() const {
    size_t result = 0;
    for (const auto& [_, writer] : _writers) {
        result += writer->flushed_bytes();
    }
    return result;
}

std::optional<kafka::offset>
record_multiplexer::last_translated_offset() const {
    return _result ? std::make_optional(_result->last_offset) : std::nullopt;
}

ss::future<result<std::nullopt_t, writer_error>>
record_multiplexer::handle_invalid_record(
  translation_probe::invalid_record_cause cause,
  kafka::offset offset,
  std::optional<iobuf> key,
  std::optional<iobuf> val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers,
  ss::abort_source& as) {
    _translation_probe.increment_invalid_record(cause);
    switch (_invalid_record_action) {
    case model::iceberg_invalid_record_action::drop:
        vlog(
          _log.debug,
          "Dropping invalid record at offset {}: {}",
          offset,
          cause);

        // Advance processed offset.
        if (!_result.has_value()) {
            _result = write_result{
              .start_offset = offset,
            };
        }
        _result.value().last_offset = offset;

        co_return std::nullopt;

    case model::iceberg_invalid_record_action::dlq_table:
        vlog(
          _log.debug,
          "Writing to DLQ invalid record at offset {}: {}",
          offset,
          cause);

        if (!_invalid_record_writer) {
            auto ensure_res = co_await _table_creator.ensure_dlq_table(
              _ntp.tp.topic, _topic_revision);

            if (ensure_res.has_error()) {
                auto e = ensure_res.error();

                switch (e) {
                case table_creator::errc::incompatible_schema:
                case table_creator::errc::failed:
                    vlog(
                      _log.warn,
                      "Error ensuring DLQ table schema for invalid record {}: "
                      "{}",
                      offset,
                      e);
                    // Normally this is not possible, so we use blanket error
                    // code.
                    co_return writer_error::unknown_error;
                case table_creator::errc::shutting_down:
                    co_return writer_error::shutting_down;
                }
            }

            auto table_id = table_id_provider::dlq_table_id(_ntp.tp.topic);
            auto load_res = co_await _schema_mgr.get_table_info(table_id);
            if (load_res.has_error()) {
                auto e = load_res.error();
                switch (e) {
                case schema_manager::errc::not_supported:
                case schema_manager::errc::failed:
                    vlog(
                      _log.warn,
                      "Error getting table info for record {}: {}",
                      offset,
                      e);
                    co_return writer_error::unknown_error;
                case schema_manager::errc::shutting_down:
                    co_return writer_error::shutting_down;
                }
            }

            auto record_type = key_value_translator{}.build_type(std::nullopt);
            if (!load_res.value().fill_registered_ids(record_type.type)) {
                // This shouldn't happen because we ensured the schema with the
                // call to table_creator. Probably someone managed to change the
                // table between two calls.
                vlog(
                  _log.warn,
                  "expected to successfully fill field IDs for record {}",
                  offset);
                co_return writer_error::unknown_error;
            }

            auto data_location = get_data_location(load_res.value());
            auto data_remote_path = _location_provider.from_uri(data_location);
            if (!data_remote_path) {
                vlog(
                  _log.warn,
                  "Error getting location prefix for {} while creating writer "
                  "at offset {}",
                  load_res.value().location,
                  offset);
                co_return writer_error::unknown_error;
            }

            _invalid_record_writer = std::make_unique<partitioning_writer>(
              *_writer_factory,
              load_res.value().schema.schema_id,
              std::move(record_type.type),
              std::move(load_res.value().partition_spec),
              std::move(data_remote_path.value()));
        }

        int64_t estimated_size = (key ? key->size_bytes() : 0)
                                 + (val ? val->size_bytes() : 0);

        auto invalid_record_type_resolver = binary_type_resolver{};
        auto resolved_buf_type = co_await invalid_record_type_resolver
                                   .resolve_buf_type(std::move(val));

        auto record_data_res = co_await key_value_translator{}.translate_data(
          _ntp.tp.partition,
          offset,
          std::move(key),
          resolved_buf_type.value().type,
          std::move(resolved_buf_type.value().parsable_buf),
          ts,
          ts_t,
          headers);
        if (record_data_res.has_error()) {
            vlog(
              _log.warn,
              "Error translating DLQ data for record {}: {}",
              offset,
              record_data_res.error());
            co_return writer_error::unknown_error;
        }

        if (!_result.has_value()) {
            _result = write_result{
              .start_offset = offset,
            };
        }

        _result.value().last_offset = offset;

        auto add_data_err = co_await _invalid_record_writer->add_data(
          std::move(record_data_res.value()), estimated_size, as);

        if (add_data_err != writer_error::ok) {
            vlog(
              _log.warn,
              "Error adding data to DLQ writer for record {}: {}",
              offset,
              add_data_err);
            // If a write fails, the writer is left in an indeterminate state,
            // we cannot continue in this case.
            co_return add_data_err;
        }

        co_return std::nullopt;
    }
}
} // namespace datalake
