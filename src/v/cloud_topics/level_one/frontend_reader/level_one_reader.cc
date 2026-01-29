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
  l1::io* io_interface)
  : _config(cfg)
  , _ntp(std::move(ntp))
  , _tidp(tidp)
  , _next_offset(cfg.start_offset)
  , _metastore(metastore)
  , _io(io_interface)
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

ss::future<model::record_batch_reader::storage_t>
level_one_log_reader_impl::read_some(
  model::timeout_clock::time_point deadline) {
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

        auto object = co_await lookup_object_for_offset(_next_offset, deadline);
        if (!object.has_value()) {
            set_end_of_stream();
            co_return model::record_batch_reader::storage_t{};
        }

        auto batches = co_await materialize_batches_from_object_offset(
          object.value(), _next_offset, deadline);

        /*
         * When EOS is reached this reader is done. So we don't need to worry
         * about what is in batches. If it's empty, the reader will yield no
         * batches. Otherwise the batches will be consumed but we don't need to
         * worry about incrementing the next offset.
         */
        if (is_end_of_stream()) {
            co_return batches;
        }

        /*
         * If we didn't read any batches, then start again past the end of the
         * object. Otherwise start again after the range that was read.
         */
        if (batches.empty()) {
            _next_offset = kafka::next_offset(object.value().last_offset);
        } else {
            _next_offset = kafka::next_offset(
              model::offset_cast(batches.back().last_offset()));
            co_return batches;
        }
    }
}

ss::future<std::optional<level_one_log_reader_impl::object_info>>
level_one_log_reader_impl::lookup_object_for_offset(
  kafka::offset offset, model::timeout_clock::time_point /*deadline*/) {
    ss::abort_source default_abort_source;
    auto* abort_source = _config.abort_source
                           ? &_config.abort_source.value().get()
                           : &default_abort_source;
    retry_chain_node rtc = l1::make_default_metastore_rtc(*abort_source);
    auto response = co_await l1::retry_metastore_op(
      [this, offset]
      -> ss::future<
        std::expected<l1::metastore::object_response, l1::metastore::errc>> {
          return _metastore->get_first_ge(_tidp, offset);
      },
      rtc);
    if (!response.has_value()) {
        switch (response.error()) {
        case l1::metastore::errc::out_of_range:
            vlog(
              _log.debug, "No L1 objects found at offset {} or later", offset);
            co_return std::nullopt;

        case l1::metastore::errc::missing_ntp:
            vlog(_log.debug, "Partition not tracked in metastore");
            co_return std::nullopt;

        default:
            throw std::runtime_error(_log.format(
              "Metastore query failed offset {}: {}",
              offset,
              response.error()));
        }
    }

    auto& obj = response.value();
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

            auto batch_size = batch.size_bytes();
            if (is_over_limit_with_bytes(batch_size)) {
                set_end_of_stream();
                break;
            }
            _bytes_consumed += batch_size;

            batches.push_back(std::move(batch));

        } else {
            // End of data.
            break;
        }
    }

    co_return batches;
}

ss::future<chunked_circular_buffer<model::record_batch>>
level_one_log_reader_impl::materialize_batches_from_object_offset(
  const object_info& object,
  kafka::offset offset,
  model::timeout_clock::time_point /*deadline*/) {
    auto seek_res = object.footer.file_position_before_kafka_offset(
      _tidp, offset);
    if (seek_res == l1::footer::npos) {
        // Perhaps this object spans offsets in the metastore but has
        // no data because of compaction.
        vlog(
          _log.debug,
          "No data in object {}: materializing 0 batches",
          object.oid);
        co_return chunked_circular_buffer<model::record_batch>{};
    }

    l1::object_extent extent{
      .id = object.oid,
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
          _log.error,
          "Exception opening stream for L1 object {}: {}",
          object.oid,
          ex);
        std::rethrow_exception(ex);
    }
    auto stream_result = stream_fut.get();
    if (!stream_result.has_value()) {
        vlog(
          _log.warn,
          "Failed to open stream for L1 object {} reading offset {}: {}",
          object.oid,
          offset,
          std::to_underlying(stream_result.error()));
        throw std::runtime_error(_log.format(
          "Failed to open stream for L1 object {}: {}",
          object.oid,
          std::to_underlying(stream_result.error())));
    }

    auto reader = l1::object_reader::create(std::move(stream_result).value());
    auto read_fut = co_await ss::coroutine::as_future(read_batches(*reader));
    if (read_fut.failed()) {
        auto ex = read_fut.get_exception();
        vlog(_log.error, "Exception reading L1 object {}: {}", object.oid, ex);
        co_await close_reader_safe(*reader);
        std::rethrow_exception(ex);
    }

    co_await close_reader_safe(*reader);

    auto batches = read_fut.get();

    // Note that it's possible to materialize zero batches.
    vlog(
      _log.debug,
      "Materialized {} batches from L1 object {}",
      batches.size(),
      object.oid);

    co_return batches;
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
