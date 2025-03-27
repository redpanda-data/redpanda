/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/translation/record_multiplexer.h"

#include "base/vlog.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/data_writer_interface.h"
#include "datalake/location.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_creator.h"
#include "datalake/table_id_provider.h"
#include "datalake/translation/errors.h"
#include "datalake/translation/translation_probe.h"
#include "model/metadata.h"
#include "model/record.h"
#include "storage/parser_utils.h"

#include <seastar/core/loop.hh>

namespace {

datalake::writer_error from_abort_exception(const ss::abort_source& as) {
    auto& ex = as.abort_requested_exception_ptr();
    if (ssx::is_shutdown_exception(ex)) {
        return datalake::writer_error::shutting_down;
    }
    try {
        std::rethrow_exception(ex);
    } catch (const datalake::translation::translator_out_of_memory_error&) {
        return datalake::writer_error::oom_error;
    } catch (const datalake::translation::translator_shutdown_error&) {
        return datalake::writer_error::shutting_down;
    } catch (
      const datalake::translation::translator_time_quota_exceeded_error&) {
        return datalake::writer_error::time_limit_exceeded;
    } catch (const datalake::translation::translator_out_of_disk_error&) {
        return datalake::writer_error::out_of_disk;
    } catch (...) {
        return datalake::writer_error::unknown_error;
    }
}
}; // namespace

namespace datalake {

namespace {
template<typename Func>
requires requires(Func f, model::record_batch batch) {
    { f(std::move(batch)) } -> std::same_as<ss::future<ss::stop_iteration>>;
}
class relaying_consumer {
public:
    explicit relaying_consumer(Func f)
      : _func(std::move(f)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        return _func(std::move(b));
    }
    void end_of_stream() {}

private:
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
  translation_probe& translation_probe)
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
  , _translation_probe(translation_probe) {}

ss::future<writer_error> record_multiplexer::multiplex(
  model::record_batch_reader reader,
  kafka::offset start_offset,
  model::timeout_clock::time_point deadline,
  ss::abort_source& as) {
    co_await std::move(reader).consume(
      relaying_consumer{
        [this, start_offset, &as](model::record_batch b) mutable {
            return do_multiplex(std::move(b), start_offset, as);
        }},
      deadline);

    co_return _error.value_or(writer_error::ok);
}

ss::future<ss::stop_iteration> record_multiplexer::do_multiplex(
  model::record_batch batch, kafka::offset start_offset, ss::abort_source& as) {
    const auto raw_size_bytes = batch.size_bytes();
    _translation_probe.increment_raw_bytes_processed(raw_size_bytes);
    if (batch.compressed()) {
        batch = co_await storage::internal::decompress_batch(std::move(batch));
    }
    const auto decompressed_size_bytes = batch.size_bytes();
    _translation_probe.increment_decompressed_bytes_processed(
      decompressed_size_bytes);
    auto first_timestamp = batch.header().first_timestamp.value();
    auto it = model::record_batch_iterator::create(batch);
    while (it.has_next()) {
        if (as.abort_requested()) {
            _error = from_abort_exception(as);
            vlog(
              _log.debug, "Abort requested, stopping translation: {}", _error);
            co_return ss::stop_iteration::yes;
        }
        auto record = it.next();
        auto key = record.share_key_opt();
        auto val = record.share_value_opt();
        auto timestamp = model::timestamp{
          first_timestamp + record.timestamp_delta()};
        kafka::offset offset{batch.base_offset()() + record.offset_delta()};
        if (offset < start_offset) {
            continue;
        }
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
            case type_resolver::errc::invalid_config:
                _error = writer_error::parquet_conversion_error;
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
                  std::move(header_kvs),
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
                  translation_probe::invalid_record_cause::
                    failed_data_translation,
                  offset,
                  record.share_key(),
                  record.share_value(),
                  timestamp,
                  std::move(header_kvs),
                  as);
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
                      translation_probe::invalid_record_cause::
                        failed_iceberg_schema_resolution,
                      offset,
                      record.share_key(),
                      record.share_value(),
                      timestamp,
                      std::move(header_kvs),
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
                      "Error ensuring table schema for record {}",
                      offset);
                    [[fallthrough]];
                case table_creator::errc::shutting_down:
                    _error = writer_error::parquet_conversion_error;
                }
                co_return ss::stop_iteration::yes;
            }

            auto table_id = table_id_provider::table_id(_ntp.tp.topic);
            auto load_res = co_await _schema_mgr.get_table_info(
              table_id, record_type.type);
            if (load_res.has_error()) {
                auto e = load_res.error();
                switch (e) {
                case schema_manager::errc::not_supported:
                case schema_manager::errc::failed:
                    vlog(
                      _log.warn,
                      "Error getting table info for record {}: {}",
                      offset,
                      load_res.error());
                    [[fallthrough]];
                case schema_manager::errc::shutting_down:
                    _error = writer_error::parquet_conversion_error;
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
                _error = writer_error::parquet_conversion_error;
                co_return ss::stop_iteration::yes;
            }

            auto table_remote_path = _location_provider.from_uri(
              load_res.value().location);
            if (!table_remote_path) {
                vlog(
                  _log.warn,
                  "Error getting location prefix for {} while creating writer "
                  "at offset {}",
                  load_res.value().location,
                  offset);
                _error = writer_error::parquet_conversion_error;
                co_return ss::stop_iteration::yes;
            }

            auto [iter, _] = _writers.emplace(
              record_type.comps,
              std::make_unique<partitioning_writer>(
                *_writer_factory,
                load_res.value().schema.schema_id,
                std::move(record_type.type),
                std::move(load_res.value().partition_spec),
                std::move(table_remote_path.value())));
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
    co_return ss::stop_iteration::no;
}

ss::future<writer_error> record_multiplexer::flush_writers() {
    if (_error && !is_recoverable_error(_error.value())) {
        co_return *_error;
    }
    auto result = co_await ss::coroutine::as_future(ss::max_concurrent_for_each(
      _writers, 10, [](auto& entry) { return entry.second->flush(); }));
    if (result.failed()) {
        vlog(_log.warn, "Error flushing writers: {}", result.get_exception());
        _error = writer_error::flush_error;
        co_return _error.value();
    }
    co_return writer_error::ok;
}

ss::future<result<record_multiplexer::write_result, writer_error>>
record_multiplexer::finish() && {
    auto writers = std::move(_writers);
    for (auto& [id, writer] : writers) {
        auto res = co_await std::move(*writer).finish();
        if (res.has_error()) {
            _error = res.error();
            continue;
        }
        if (_result) {
            auto& files = res.value();
            std::move(
              files.begin(),
              files.end(),
              std::back_inserter(_result->data_files));
        }
    }
    if (_invalid_record_writer) {
        auto writer = std::move(_invalid_record_writer);
        auto res = co_await std::move(*writer).finish();
        if (res.has_error()) {
            _error = res.error();
        } else if (_result) {
            auto& files = res.value();
            std::move(
              files.begin(),
              files.end(),
              std::back_inserter(_result->dlq_files));
        }
    }
    if (_error && !is_recoverable_error(_error.value())) {
        co_return *_error;
    }
    if (!_result) {
        // no batches were processed.
        co_return writer_error::no_data;
    }
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
  chunked_vector<std::pair<std::optional<iobuf>, std::optional<iobuf>>> headers,
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
                    [[fallthrough]];
                case table_creator::errc::failed:
                    [[fallthrough]];
                case table_creator::errc::shutting_down:
                    vlog(
                      _log.warn,
                      "Error ensuring DLQ table schema for invalid record {}",
                      offset);
                    co_return writer_error::parquet_conversion_error;
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
                      load_res.error());
                    [[fallthrough]];
                case schema_manager::errc::shutting_down:
                    co_return writer_error::parquet_conversion_error;
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
                co_return writer_error::parquet_conversion_error;
            }

            auto table_remote_path = _location_provider.from_uri(
              load_res.value().location);
            if (!table_remote_path) {
                vlog(
                  _log.warn,
                  "Error getting location prefix for {} while creating writer "
                  "at offset {}",
                  load_res.value().location,
                  offset);
                co_return writer_error::parquet_conversion_error;
            }

            _invalid_record_writer = std::make_unique<partitioning_writer>(
              *_writer_factory,
              load_res.value().schema.schema_id,
              std::move(record_type.type),
              std::move(load_res.value().partition_spec),
              std::move(table_remote_path.value()));
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
          headers);
        if (record_data_res.has_error()) {
            vlog(
              _log.debug,
              "Error translating DLQ data for record {}: {}",
              offset,
              record_data_res.error());
            co_return writer_error::parquet_conversion_error;
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
