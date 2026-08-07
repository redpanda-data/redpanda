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

#include "cloud_topics/level_one/common/open_object.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

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
  size_t max_slice_bytes)
  : _config(cfg)
  , _ntp(std::move(ntp))
  , _tidp(tidp)
  , _next_offset(cfg.start_offset)
  , _metastore(metastore)
  , _io(io_interface)
  , _probe(probe)
  , _log(cd_log, fmt::format("[{}/{}/{}]", fmt::ptr(this), _ntp, _tidp))
  , _max_slice_bytes(max_slice_bytes) {
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
        co_return co_await read_some(deadline);
    } catch (...) {
        auto ex = std::current_exception();
        vlogl(
          _log,
          ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                         : ss::log_level::warn,
          "Reader caught exception: {}",
          ex);
        set_end_of_stream();
        throw;
    }
}

ss::future<model::record_batch_reader::storage_t>
level_one_log_reader_impl::read_some(
  model::timeout_clock::time_point deadline) {
    if (_config.strict_max_bytes && _config.max_bytes == 0) {
        set_end_of_stream();
        co_await close_current_stream();
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

        chunked_circular_buffer<model::record_batch> batches;

        if (_current_stream) {
            // Reuse the inline stream from a previous read_some iteration
            // or from a prior materialize call.
            vlog(
              _log.debug,
              "Reusing open stream for offset {} (object {})",
              _next_offset,
              _current_stream->oid);

            auto read_fut = co_await ss::coroutine::as_future(
              read_batches(*_current_stream->reader));
            if (read_fut.failed()) {
                auto ex = read_fut.get_exception();
                auto log_level = ssx::is_shutdown_exception(ex)
                                   ? ss::log_level::debug
                                   : ss::log_level::warn;
                vlogl(
                  _log,
                  log_level,
                  "Exception reading from open stream (object {}): {}",
                  _current_stream->oid,
                  ex);
                co_await close_current_stream();
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

            // When materialize found no data (npos), advance past the
            // object so the loop doesn't spin.
            if (batches.empty() && !_current_stream) {
                _next_offset = kafka::next_offset(mat.last_object_offset);
                continue;
            }
        }

        if (is_end_of_stream()) {
            if (!batches.empty()) {
                _next_offset = kafka::next_offset(
                  model::offset_cast(batches.back().last_offset()));
            }
            co_return batches;
        }

        if (batches.empty()) {
            if (_current_stream) {
                _next_offset = kafka::next_offset(
                  _current_stream->last_object_offset);
                co_await close_current_stream();
            }
            continue;
        }

        _next_offset = kafka::next_offset(
          model::offset_cast(batches.back().last_offset()));

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
            .imported = em.object_info->imported,
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

    // For native objects: position = footer_pos, size = footer_total_size.
    // For imported extents: footer_pos = 0, object_size = segment size, so
    // size = segment_size, which ts_segment_index uses to bound seek results.
    size_t footer_size = obj.object_size - obj.footer_pos;
    l1::object_extent extent{
      .id = obj.oid,
      .position = obj.footer_pos,
      .size = footer_size,
      .imported = obj.imported,
    };

    ss::abort_source default_abort_source;
    auto* as = _config.abort_source ? &_config.abort_source.value().get()
                                    : &default_abort_source;

    auto handle_fut = co_await ss::coroutine::as_future(
      l1::open_object(*_io, extent, as, _config.group, _config.skip_cache));
    if (handle_fut.failed()) {
        auto ex = handle_fut.get_exception();
        auto log_level = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                        : ss::log_level::warn;
        vlogl(_log, log_level, "Exception opening object {}: {}", obj.oid, ex);
        std::rethrow_exception(ex);
    }
    auto handle_result = handle_fut.get();
    if (!handle_result.has_value()) {
        throw std::runtime_error(_log.format(
          "Failed to open object {} (pos {} size {}): {}",
          obj.oid,
          obj.footer_pos,
          obj.object_size,
          std::to_underlying(handle_result.error())));
    }

    co_return object_info{
      .oid = obj.oid,
      .handle = std::move(*handle_result),
      .last_offset = obj.last_offset,
    };
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

        if (bytes_read > 0 && bytes_read + hdr->size_bytes > _max_slice_bytes) {
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
    // Use the handle's index to seek. When a timestamp hint is available,
    // take whichever position is further into the file.
    auto seek_res = [&]() -> std::optional<l1::seek_result> {
        auto offset_seek = object.handle->index().seek_offset_le(_tidp, offset);
        if (!_config.first_timestamp) {
            return offset_seek;
        }
        auto time_seek = object.handle->index().seek_timestamp_le(
          _tidp, *_config.first_timestamp);
        if (!time_seek) {
            return offset_seek;
        }
        if (!offset_seek) {
            return time_seek;
        }
        return time_seek->file_position > offset_seek->file_position
                 ? time_seek
                 : offset_seek;
    }();
    if (!seek_res) {
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

    // Use the read config's abort source, which the Kafka fetch path normally
    // supplies; only readers built without one fall back to the impl-owned
    // source. Not a local: the reader open_reader returns is persisted in
    // _current_stream and consumed across later do_load_slice calls, so the
    // abort source must outlive this call.
    auto* as = _config.abort_source ? &_config.abort_source.value().get()
                                    : &_fallback_read_abort_source;

    auto reader_fut = co_await ss::coroutine::as_future(
      object.handle->open_reader(*seek_res, as));
    if (reader_fut.failed()) {
        auto ex = reader_fut.get_exception();
        vlog(
          _log.error,
          "Exception opening reader for L1 object {}: {}",
          object.oid,
          ex);
        std::rethrow_exception(ex);
    }
    auto reader_result = reader_fut.get();
    if (!reader_result.has_value()) {
        vlog(
          _log.warn,
          "Failed to open stream for L1 object {} reading offset {}: {}",
          object.oid,
          offset,
          std::to_underlying(reader_result.error()));
        co_await ss::coroutine::return_exception(
          std::runtime_error(_log.format(
            "Failed to open stream for L1 object {}: {}",
            object.oid,
            std::to_underlying(reader_result.error()))));
    }

    _current_stream = open_stream{
      .oid = object.oid,
      .last_object_offset = object.last_offset,
      .reader = std::move(*reader_result),
    };

    auto read_fut = co_await ss::coroutine::as_future(
      read_batches(*_current_stream->reader));
    if (read_fut.failed()) {
        auto ex = read_fut.get_exception();
        auto log_level = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                        : ss::log_level::warn;
        vlogl(
          _log,
          log_level,
          "Exception reading L1 object {}: {}",
          object.oid,
          ex);
        co_await close_current_stream();
        co_await ss::coroutine::return_exception_ptr(std::move(ex));
    }

    auto batches = read_fut.get();

    vlog(
      _log.debug,
      "Materialized {} batches from L1 object {}",
      batches.size(),
      object.oid);

    co_return materialize_result{
      .batches = std::move(batches),
      .last_object_offset = object.last_offset,
    };
}

ss::future<> level_one_log_reader_impl::close_current_stream() {
    if (!_current_stream) {
        co_return;
    }
    co_await close_reader_safe(*_current_stream->reader);
    _current_stream.reset();
}

fmt::iterator level_one_log_reader_impl::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "level_one_cloud_topics_reader");
}

void level_one_log_reader_impl::set_end_of_stream() { _end_of_stream = true; }

bool level_one_log_reader_impl::is_end_of_stream() const {
    return _end_of_stream;
}

ss::future<> level_one_log_reader_impl::finally() noexcept {
    return close_current_stream();
}

std::optional<level_one_log_reader_impl::private_flags>
level_one_log_reader_impl::get_flags() const {
    return private_flags{
      .is_reusable = is_reusable(),
      .was_cached = _was_cached,
    };
}

void level_one_log_reader_impl::reset_config(
  const cloud_topic_log_reader_config& cfg) {
    vassert(
      cfg.start_offset == _next_offset,
      "reset_config: start_offset {} != next_offset {}",
      cfg.start_offset,
      _next_offset);
    _config = cfg;
    _end_of_stream = false;
    _bytes_consumed = 0;
    _was_cached = true;
}

bool level_one_log_reader_impl::is_reusable() const {
    return _current_stream.has_value() || !_lookahead_buffer.empty();
}

bool level_one_log_reader_impl::is_over_limit_with_bytes(size_t size) const {
    // Always accept the first batch to guarantee progress.
    if (_bytes_consumed == 0) {
        return false;
    }
    return (_bytes_consumed + size) > _config.max_bytes;
}

} // namespace cloud_topics
