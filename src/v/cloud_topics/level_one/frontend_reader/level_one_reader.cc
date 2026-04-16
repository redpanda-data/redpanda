/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"

#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <utility>

namespace cloud_topics {

ss::future<>
level_one_log_reader_impl::close_reader_safe(l1::object_reader& reader) {
    try {
        co_await reader.close();
    } catch (const std::exception& e) {
        vlog(
          _log.warn, "Exception while closing L1 object reader: {}", e.what());
    }
}

level_one_log_reader_impl::level_one_log_reader_impl(
  const cloud_topic_log_reader_config& cfg,
  model::ntp ntp,
  model::topic_id_partition tidp,
  l1::metastore* metastore,
  l1::io* io_interface,
  level_one_reader_probe* probe,
  l1_reader_cache* cache)
  : _config(cfg)
  , _ntp(std::move(ntp))
  , _tidp(tidp)
  , _next_offset(cfg.start_offset)
  , _metastore(metastore)
  , _io(io_interface)
  , _probe(probe)
  , _cache(cache)
  , _log(cd_log, fmt::format("[{}/{}/{}]", fmt::ptr(this), _ntp, _tidp)) {
    vlog(_log.debug, "New reader created {}", _config);
}

/*
 * Error handling
 * ==============
 *
 * Exceptions should not be used unless you intend for the exception to be
 * propogated back to the user of a model::record_batch_reader. In this case an
 * exception that carries a string message can be useful for debugging.
 */
ss::future<model::record_batch_reader::storage_t>
level_one_log_reader_impl::do_load_slice(
  model::timeout_clock::time_point deadline) {
    try {
        return read_some(deadline);
    } catch (...) {
        vlog(
          _log.error, "Reader caught exception: {}", std::current_exception());
        set_end_of_stream();
        throw;
    }
}

ss::future<std::expected<cached_l1_reader, l1::io::errc>>
level_one_log_reader_impl::open_reader_at(
  l1::object_id oid,
  kafka::offset last_object_offset,
  size_t extent_position,
  size_t extent_size) {
    l1::object_extent extent{
      .id = oid,
      .position = extent_position,
      .size = extent_size,
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
          _log.error, "Exception opening stream for L1 object {}: {}", oid, ex);
        std::rethrow_exception(ex);
    }
    auto stream_result = stream_fut.get();
    if (!stream_result.has_value()) {
        co_return std::unexpected(stream_result.error());
    }
    co_return cached_l1_reader{
      .oid = oid,
      .last_object_offset = last_object_offset,
      .next_offset = _next_offset,
      .reader = l1::object_reader::create(std::move(stream_result).value()),
    };
}

ss::future<model::record_batch_reader::storage_t>
level_one_log_reader_impl::read_some(
  model::timeout_clock::time_point deadline) {
    if (_config.strict_max_bytes && _config.max_bytes == 0) {
        set_end_of_stream();
        co_return model::record_batch_reader::storage_t{};
    }
    while (true) {
        if (_next_offset > _config.max_offset) {
            vlog(
              _log.debug,
              "L1 reader next_offset {} > max_offset {}: ending "
              "stream",
              _next_offset,
              _config.max_offset);
            set_end_of_stream();
            co_return model::record_batch_reader::storage_t{};
        }

        std::optional<cached_l1_reader> local_reader;
        chunked_circular_buffer<model::record_batch> batches;

        // Try to reuse a cached reader from a previous fetch. The cached
        // entry holds a live reader with its I/O stream, preserving
        // readahead buffers.
        if (_cache) {
            local_reader = _cache->take_reader(_tidp, _next_offset);
            if (local_reader) {
                vlog(
                  _log.debug,
                  "Took cached reader for offset {} (object {})",
                  _next_offset,
                  local_reader->oid);
            }
        }

        if (local_reader) {
            auto read_fut = co_await ss::coroutine::as_future(
              read_batches(*local_reader->reader));
            if (read_fut.failed()) {
                auto ex = read_fut.get_exception();
                vlog(
                  _log.error,
                  "Exception reading from cached reader (object {}): {}",
                  local_reader->oid,
                  ex);
                co_await close_reader_safe(*local_reader->reader);
                std::rethrow_exception(ex);
            }
            batches = read_fut.get();
        } else {
            auto object = co_await lookup_object_for_offset(
              _next_offset, deadline);
            if (!object.has_value()) {
                set_end_of_stream();
                co_return model::record_batch_reader::storage_t{};
            }

            auto mat = co_await materialize_batches_from_object_offset(
              object.value(), _next_offset, deadline);
            batches = std::move(mat.batches);
            local_reader = std::move(mat.reader);

            // When materialize found no data (npos), advance past the
            // object so the loop doesn't spin.
            if (batches.empty() && !local_reader) {
                _next_offset = kafka::next_offset(mat.last_object_offset);
                continue;
            }
        }

        if (is_end_of_stream()) {
            if (!batches.empty()) {
                _next_offset = kafka::next_offset(
                  model::offset_cast(batches.back().last_offset()));
                if (local_reader) {
                    local_reader->next_offset = _next_offset;
                }
            }
            co_await return_or_close(std::move(local_reader));
            co_return batches;
        }

        if (batches.empty()) {
            if (local_reader) {
                _next_offset = kafka::next_offset(
                  local_reader->last_object_offset);
                co_await close_reader_safe(*local_reader->reader);
            }
            continue;
        }

        _next_offset = kafka::next_offset(
          model::offset_cast(batches.back().last_offset()));
        if (local_reader) {
            local_reader->next_offset = _next_offset;
        }
        co_await return_or_close(std::move(local_reader));
        co_return batches;
    }
}

std::optional<l1::metastore::object_response>
level_one_log_reader_impl::consume_lookahead_buffer(kafka::offset offset) {
    // Discard stale entries whose data is entirely before the requested
    // offset.
    while (!_lookahead_buffer.empty()
           && _lookahead_buffer.front().last_offset < offset) {
        _lookahead_buffer.pop_front();
    }
    if (_lookahead_buffer.empty()) {
        return std::nullopt;
    }
    auto entry = std::move(_lookahead_buffer.front());
    _lookahead_buffer.pop_front();
    return entry;
}

ss::future<> level_one_log_reader_impl::fill_lookahead_buffer(
  kafka::offset offset, size_t num_objects) {
    ss::abort_source default_abort_source;
    auto* abort_source = _config.abort_source
                           ? &_config.abort_source.value().get()
                           : &default_abort_source;
    retry_chain_node rtc = l1::make_default_metastore_rtc(*abort_source);
    auto response = co_await l1::retry_metastore_op(
      [this, offset, num_objects] -> ss::future<std::expected<
                                    l1::metastore::extent_metadata_response,
                                    l1::metastore::errc>> {
          return _metastore->get_extent_metadata_forwards(
            _tidp,
            offset,
            kafka::offset::max(),
            num_objects,
            l1::metastore::include_object_metadata::yes);
      },
      rtc);
    if (!response.has_value()) {
        switch (response.error()) {
        case l1::metastore::errc::out_of_range:
            vlog(
              _log.debug, "No L1 objects found at offset {} or later", offset);
            co_return;
        case l1::metastore::errc::missing_ntp:
            vlog(_log.debug, "Partition not tracked in metastore");
            co_return;
        default:
            throw std::runtime_error(_log.format(
              "Metastore query failed offset {}: {}",
              offset,
              response.error()));
        }
    }

    for (auto& em : response.value().extents) {
        vassert(
          em.object_info.has_value(),
          "extent metadata missing object_info for offsets ({}~{})",
          em.base_offset,
          em.last_offset);
        _lookahead_buffer.push_back(
          l1::metastore::object_response{
            .oid = em.object_info->oid,
            .footer_pos = em.object_info->footer_pos,
            .object_size = em.object_info->object_size,
            .first_offset = em.base_offset,
            .last_offset = em.last_offset,
          });
    }
}

ss::future<std::optional<level_one_log_reader_impl::object_info>>
level_one_log_reader_impl::lookup_object_for_offset(
  kafka::offset offset, model::timeout_clock::time_point /*deadline*/) {
    if (_lookahead_buffer.empty()) {
        auto num_objects = std::max<size_t>(1, _config.lookahead_objects);
        co_await fill_lookahead_buffer(offset, num_objects);
    }
    auto obj_resp = consume_lookahead_buffer(offset);
    if (!obj_resp.has_value()) {
        co_return std::nullopt;
    }

    auto& obj = obj_resp.value();
    vlog(_log.debug, "Found L1 object {} at offset {}", obj.oid, offset);

    auto footer = co_await read_footer(
      obj.oid, obj.footer_pos, obj.object_size);

    co_return object_info{
      .oid = obj.oid,
      .footer = std::move(footer),
      .last_offset = obj.last_offset,
    };
}

ss::future<l1::footer> level_one_log_reader_impl::read_footer(
  l1::object_id oid, size_t footer_pos, size_t object_size) {
    size_t footer_total_size = object_size - footer_pos;
    if (_probe != nullptr) {
        _probe->register_footer_read(footer_total_size);
    }

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
          _log.error,
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
          _log.warn,
          "Failed to read footer from object {} (pos {} object size {}): {}",
          oid,
          extent.position,
          object_size,
          std::to_underlying(read_result.error()));
        throw std::runtime_error(_log.format(
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
          _log.error,
          "Failed to parse footer from object {} despite reading complete "
          "footer (pos {} object size {})",
          oid,
          extent.position,
          object_size);
        throw std::runtime_error(_log.format(
          "Failed to parse footer from object {} (pos {} size {})",
          oid,
          extent.position,
          object_size));
    }

    co_return std::get<l1::footer>(std::move(footer_result));
}

ss::future<chunked_circular_buffer<model::record_batch>>
level_one_log_reader_impl::read_batches(l1::object_reader& reader) {
    chunked_circular_buffer<model::record_batch> batches;
    size_t bytes_read = 0;
    size_t bytes_skipped = 0;

    while (true) {
        auto peeked = co_await reader.peek();
        auto* hdr = std::get_if<model::record_batch_header>(&peeked);
        if (!hdr) {
            break;
        }

        // Stop before consuming the batch body when the batch is past
        // max_offset or would exceed the byte budget. The stream stays
        // positioned after the header, so a cached reader can resume
        // from this point on the next fetch.
        if (hdr->base_offset > kafka::offset_cast(_config.max_offset)) {
            break;
        }
        if (is_over_limit_with_bytes(hdr->size_bytes)) {
            set_end_of_stream();
            break;
        }

        // Accept — consume the batch body.
        auto result = co_await reader.read_next();
        auto batch = std::move(std::get<model::record_batch>(result));

        if (batch.last_offset() < kafka::offset_cast(_next_offset)) {
            bytes_skipped += batch.size_bytes();
            continue;
        }

        auto batch_size = batch.size_bytes();
        _bytes_consumed += batch_size;
        bytes_read += batch_size;
        batches.push_back(std::move(batch));
    }

    if (_probe != nullptr) {
        _probe->register_bytes_read(bytes_read);
        _probe->register_bytes_skipped(bytes_skipped);
    }
    co_return batches;
}

ss::future<level_one_log_reader_impl::materialize_result>
level_one_log_reader_impl::materialize_batches_from_object_offset(
  const object_info& object,
  kafka::offset offset,
  model::timeout_clock::time_point /*deadline*/) {
    // When a timestamp hint is available, use the footer's timestamp index
    // to narrow the seek position. Both offset and timestamp constraints
    // must hold, so we start at whichever position is further into the
    // file.
    auto seek_res = [&] {
        auto offset_seek = object.footer.file_position_before_kafka_offset(
          _tidp, offset);
        if (!_config.first_timestamp) {
            return offset_seek;
        }
        auto time_seek = object.footer.file_position_before_max_timestamp(
          _tidp, *_config.first_timestamp);
        if (time_seek == l1::footer::npos) {
            return offset_seek;
        }
        if (offset_seek == l1::footer::npos) {
            return time_seek;
        }
        return time_seek.file_position > offset_seek.file_position
                 ? time_seek
                 : offset_seek;
    }();
    if (seek_res == l1::footer::npos) {
        // Perhaps this object spans offsets in the metastore but has
        // no data because of compaction.
        vlog(
          _log.debug,
          "No data in object {}: materializing 0 batches",
          object.oid);
        co_return materialize_result{
          .last_object_offset = object.last_offset,
        };
    }

    auto reader_result = co_await open_reader_at(
      object.oid, object.last_offset, seek_res.file_position, seek_res.length);
    if (!reader_result.has_value()) {
        vlog(
          _log.warn,
          "Failed to open stream for L1 object {} reading offset {}: {}",
          object.oid,
          offset,
          std::to_underlying(reader_result.error()));
        throw std::runtime_error(_log.format(
          "Failed to open stream for L1 object {}: {}",
          object.oid,
          std::to_underlying(reader_result.error())));
    }

    auto reader = std::move(reader_result).value();

    auto read_fut = co_await ss::coroutine::as_future(
      read_batches(*reader.reader));
    if (read_fut.failed()) {
        auto ex = read_fut.get_exception();
        vlog(_log.error, "Exception reading L1 object {}: {}", object.oid, ex);
        co_await close_reader_safe(*reader.reader);
        std::rethrow_exception(ex);
    }

    auto batches = read_fut.get();

    vlog(
      _log.debug,
      "Materialized {} batches from L1 object {}",
      batches.size(),
      object.oid);

    co_return materialize_result{
      .batches = std::move(batches),
      .reader = std::move(reader),
      .last_object_offset = object.last_offset,
    };
}

ss::future<>
level_one_log_reader_impl::return_or_close(std::optional<cached_l1_reader> r) {
    if (!r) {
        co_return;
    }
    if (_cache) {
        co_await _cache->return_reader(_tidp, std::move(*r));
    } else {
        co_await close_reader_safe(*r->reader);
    }
}

void level_one_log_reader_impl::print(std::ostream& o) {
    o << "level_one_cloud_topics_reader";
}

void level_one_log_reader_impl::set_end_of_stream() { _end_of_stream = true; }

bool level_one_log_reader_impl::is_end_of_stream() const {
    return _end_of_stream;
}

bool level_one_log_reader_impl::is_over_limit_with_bytes(size_t size) const {
    return (_config.strict_max_bytes || _bytes_consumed > 0)
           && (_bytes_consumed + size) > _config.max_bytes;
}

} // namespace cloud_topics
