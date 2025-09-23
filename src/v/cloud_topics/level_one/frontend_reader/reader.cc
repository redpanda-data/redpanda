/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/frontend_reader/reader.h"

#include "bytes/iostream.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <iterator>
#include <utility>

namespace cloud_topics {

static ss::future<>
close_reader_safe(std::unique_ptr<l1::object_reader>& reader) {
    try {
        co_await reader->close();
    } catch (const std::exception& e) {
        vlog(
          cd_log.warn,
          "Exception while closing L1 object reader: {}",
          e.what());
    }
}

level_one_log_reader_impl::level_one_log_reader_impl(
  cloud_topic_log_reader_config& cfg,
  model::ntp ntp,
  model::topic_id_partition tidp,
  l1::metastore* metastore,
  l1::io* io_interface)
  : _config(cfg)
  , _ntp(std::move(ntp))
  , _tidp(tidp)
  , _next_offset(cfg.start_offset)
  , _metastore(metastore)
  , _io(io_interface) {}

ss::future<model::record_batch_reader::storage_t>
level_one_log_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    chunked_circular_buffer<model::record_batch> res;

    // First switch: ensure batches are materialized or the reader
    // reaches end-of-stream.
    switch (_state) {
    case state::empty:
        co_await fetch_metadata(deadline);
        [[fallthrough]];
    case state::ready:
        co_await materialize_batches(deadline);
        [[fallthrough]];
    case state::materialized:
    case state::end_of_stream:
        // Handled in the next switch statement.
        break;
    }

    // Second switch: enforces that the reader has materialized batches
    // or reached end-of-stream.
    switch (_state) {
    case state::empty:
    case state::ready:
        vassert(
          false,
          "Invalid reader state after materialization for {} ({}): got {}",
          _ntp,
          _tidp,
          std::to_underlying(_state));
    case state::materialized:
        consume_materialized_batches(&res);
        [[fallthrough]];
    case state::end_of_stream:
        break;
    }
    co_return res;
}

ss::future<> level_one_log_reader_impl::fetch_metadata(
  model::timeout_clock::time_point /*deadline*/) {
    vassert(
      _state == state::empty,
      "Invalid state for metadata fetch: {}",
      std::to_underlying(_state));

    vassert(
      !_current_obj.has_value(),
      "Empty state should not have a current object");

    if (_next_offset > _config.max_offset) {
        vlog(
          cd_log.debug,
          "L1 reader next_offset {} > max_offset {} for {} ({}): ending "
          "stream",
          _next_offset,
          _config.max_offset,
          _ntp,
          _tidp);
        _state = state::end_of_stream;
        co_return;
    }

    if (is_over_limit(0)) {
        vlog(
          cd_log.debug,
          "L1 reader over byte budget for {} ({}): ending stream",
          _ntp,
          _tidp);
        _state = state::end_of_stream;
        co_return;
    }

    auto response_fut = co_await ss::coroutine::as_future(
      _metastore->get_first_ge(_tidp, _next_offset));
    if (response_fut.failed()) {
        auto ex = response_fut.get_exception();
        vlog(
          cd_log.error,
          "Exception fetching metadata from metastore for {} ({}): {}",
          _ntp,
          _tidp,
          ex);
        _state = state::end_of_stream;
        std::rethrow_exception(ex);
    }

    auto response = response_fut.get();
    if (!response.has_value()) {
        switch (response.error()) {
        case l1::metastore::errc::out_of_range:
            vlog(
              cd_log.debug,
              "No L1 objects found for {} ({}) at offset {} or later",
              _ntp,
              _tidp,
              _next_offset);
            _state = state::end_of_stream;
            co_return;
        case l1::metastore::errc::missing_ntp:
            vlog(
              cd_log.debug,
              "Partition {} ({}) not tracked in metastore",
              _ntp,
              _tidp);
            _state = state::end_of_stream;
            co_return;
        default:
            vlog(
              cd_log.error,
              "Failed to query metastore for {} ({}) offset {}: {}",
              _ntp,
              _tidp,
              _next_offset,
              response.error());
            _state = state::end_of_stream;
            throw std::runtime_error(
              fmt::format(
                "Metastore query failed for {} ({}) offset {}: {}",
                _ntp,
                _tidp,
                _next_offset,
                response.error()));
        }
    }

    auto& obj = response.value();
    vlog(
      cd_log.debug,
      "Found L1 object {} for {} ({}) at offset {}",
      obj.oid,
      _ntp,
      _tidp,
      _next_offset);

    auto footer = co_await read_footer(
      obj.oid, obj.footer_pos, obj.object_size);
    _current_obj = current_object{
      .oid = obj.oid,
      .footer = std::move(footer),
      .last_offset = obj.last_offset,
    };
    _state = state::ready;
}

ss::future<l1::footer> level_one_log_reader_impl::read_footer(
  l1::object_id oid, size_t footer_pos, size_t object_size) {
    size_t footer_total_size = object_size - footer_pos;

    l1::object_extent extent{
      .id = oid,
      .position = footer_pos,
      .size = footer_total_size,
    };

    ss::abort_source default_abort_source;
    auto* abort_source = _config.abort_source
                           ? &_config.abort_source.value().get()
                           : &default_abort_source;
    auto read_fut = co_await ss::coroutine::as_future(
      _io->read_object_as_iobuf(extent, abort_source));
    if (read_fut.failed()) {
        auto ex = read_fut.get_exception();
        vlog(
          cd_log.error,
          "Exception opening stream for footer from object {} (pos {} object "
          "size {}): {}",
          oid,
          extent.position,
          object_size,
          ex);
        std::rethrow_exception(ex);
    }

    auto read_result = read_fut.get();
    if (!read_result.has_value()) {
        vlog(
          cd_log.error,
          "Failed to read footer from object {} (pos {} object size {}): {}",
          oid,
          extent.position,
          object_size,
          std::to_underlying(read_result.error()));
        throw std::runtime_error(
          fmt::format(
            "Failed to read footer from object {} (pos {} size {}): {}",
            oid,
            extent.position,
            object_size,
            std::to_underlying(read_result.error())));
    }

    // Parse the footer - we have the complete footer so this should succeed.
    auto footer_result = co_await l1::footer::read(
      std::move(read_result).value());

    if (!std::holds_alternative<l1::footer>(footer_result)) {
        vlog(
          cd_log.error,
          "Failed to parse footer from object {} despite reading complete "
          "footer (pos {} object size {})",
          oid,
          extent.position,
          object_size);
        throw std::runtime_error(
          fmt::format(
            "Failed to parse footer from object {} (pos {} size {})",
            oid,
            extent.position,
            object_size));
    }

    co_return std::get<l1::footer>(std::move(footer_result));
}

ss::future<>
level_one_log_reader_impl::read_batches(l1::object_reader& reader) {
    while (true) {
        auto result = co_await reader.read_next();

        if (std::holds_alternative<model::record_batch>(result)) {
            auto batch = std::move(std::get<model::record_batch>(result));

            // Skip batches before our start offset.
            if (batch.last_offset() < kafka::offset_cast(_next_offset)) {
                continue;
            }

            // Stop if we've gone beyond our max offset.
            if (batch.base_offset() > kafka::offset_cast(_config.max_offset)) {
                break;
            }

            // Check if adding this batch would exceed our byte limit.
            size_t batch_size = batch.size_bytes();
            if (is_over_limit(batch_size)) {
                break;
            }
            _config.bytes_consumed += batch_size;

            // If we make it past all that, emit the batch.
            _batches.push_back(std::move(batch));
        } else {
            // End of data.
            break;
        }
    }
}

ss::future<> level_one_log_reader_impl::materialize_batches(
  model::timeout_clock::time_point /*deadline*/) {
    // Could be EOS because there are no more objects, but
    // there should never be materialized batches remaining.
    vassert(
      _batches.empty(),
      "Materialize batches called with batches already materialized");

    if (_state == state::end_of_stream) {
        co_return;
    }

    vassert(
      _state == state::ready,
      "Invalid state to materialize batches: {}",
      std::to_underlying(_state));

    // I wish I had ADTs to enforce these invariants...
    vassert(
      _current_obj.has_value(),
      "Expected to have current object in ready state");

    auto seek_res = _current_obj->footer.file_position_before_kafka_offset(
      _tidp, _next_offset);
    if (seek_res == l1::footer::npos) {
        // Perhaps this object spans offsets in the metastore but has
        // no data because of compaction.
        vlog(
          cd_log.debug,
          "No data for {} ({}) in object {}: materializing 0 batches",
          _ntp,
          _tidp,
          _current_obj->oid);
        _state = state::materialized;
        co_return;
    }

    l1::object_extent extent{
      .id = _current_obj->oid,
      .position = seek_res.file_position,
      .size = seek_res.length,
    };
    ss::abort_source default_abort_source;
    auto* abort_source = _config.abort_source
                           ? &_config.abort_source.value().get()
                           : &default_abort_source;
    auto stream_fut = co_await ss::coroutine::as_future(
      _io->read_object(extent, abort_source));
    if (stream_fut.failed()) {
        auto ex = stream_fut.get_exception();
        vlog(
          cd_log.error,
          "Exception opening stream for L1 object {} for {} ({}): {}",
          _current_obj->oid,
          _ntp,
          _tidp,
          ex);
        _state = state::end_of_stream;
        std::rethrow_exception(ex);
    }
    auto stream_result = stream_fut.get();
    if (!stream_result.has_value()) {
        vlog(
          cd_log.error,
          "Failed to open stream for L1 object {} for {} ({}): {}",
          _current_obj->oid,
          _ntp,
          _tidp,
          std::to_underlying(stream_result.error()));
        _state = state::end_of_stream;
        throw std::runtime_error(
          fmt::format(
            "Failed to open stream for L1 object {} for {} ({}): {}",
            _current_obj->oid,
            _ntp,
            _tidp,
            std::to_underlying(stream_result.error())));
    }

    auto reader = l1::object_reader::create(std::move(stream_result).value());
    auto read_fut = co_await ss::coroutine::as_future(read_batches(*reader));
    if (read_fut.failed()) {
        auto ex = read_fut.get_exception();
        vlog(
          cd_log.error,
          "Exception reading L1 object {} for {} ({}): {}",
          _current_obj->oid,
          _ntp,
          _tidp,
          ex);
        co_await close_reader_safe(reader);
        _state = state::end_of_stream;
        std::rethrow_exception(ex);
    }

    co_await close_reader_safe(reader);

    // Note that it's possible to materialize zero batches.
    vlog(
      cd_log.debug,
      "Materialized {} batches from L1 object {} for {} ({})",
      _batches.size(),
      _current_obj->oid,
      _ntp,
      _tidp);
    _state = state::materialized;
}

void level_one_log_reader_impl::consume_materialized_batches(
  chunked_circular_buffer<model::record_batch>* dest) {
    vlog(
      cd_log.debug,
      "Consuming {} materialized batches for {} ({})",
      _batches.size(),
      _ntp,
      _tidp);

    dest->swap(_batches);

    // Increment the next offset for the next metastore query.
    // The offset is always incremented so the reader makes
    // progress even if the offset range in the object is
    // smaller than the metastore's metadata about the offset
    // range covered by the object (because of, e.g. compaction).
    auto last_offset = dest->empty()
                         ? _current_obj
                             .transform(
                               [](const auto& obj) { return obj.last_offset; })
                             .value_or(_next_offset)
                         : model::offset_cast(dest->back().last_offset());
    _next_offset = kafka::next_offset(last_offset);

    _current_obj.reset();
    _state = state::empty;
}

void level_one_log_reader_impl::print(std::ostream& o) {
    o << "level_one_cloud_topics_reader";
}

bool level_one_log_reader_impl::is_end_of_stream() const {
    return _state == state::end_of_stream;
}

bool level_one_log_reader_impl::is_over_limit(size_t size) const {
    return (_config.strict_max_bytes || _config.bytes_consumed > 0)
           && (_config.bytes_consumed + size) > _config.max_bytes;
}

} // namespace cloud_topics
