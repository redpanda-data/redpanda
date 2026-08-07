/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/l1_object_sink.h"

#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/maintenance/logger.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "compaction/reducer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

namespace cloud_topics::l1 {

l1_object_sink::l1_object_sink(
  model::topic_id_partition tp,
  io* io,
  metastore* metastore,
  ss::abort_source& as,
  config::binding<size_t> max_object_size,
  config::binding<size_t> commit_interval_bytes,
  size_t upload_part_size,
  prefix_logger& ctxlog,
  object_builder::options opts)
  : _tp(tp)
  , _io(io)
  , _metastore(metastore)
  , _as(as)
  , _max_object_size(std::move(max_object_size))
  , _commit_interval_bytes(std::move(commit_interval_bytes))
  , _ctxlog(ctxlog)
  , _upload_part_size(upload_part_size)
  , _opts(opts) {}

ss::future<> l1_object_sink::init_metadata_builder() {
    auto metadata_builder_res
      = co_await l1::retry_metastore_op_with_default_rtc(
        [this]() { return _metastore->object_builder(); }, _as);
    if (!metadata_builder_res.has_value()) {
        auto msg = fmt::format(
          "Could not create object metadata builder: {}. Aborting operation",
          metadata_builder_res.error());
        vlog(_ctxlog.warn, "{}", msg);
        throw std::runtime_error(msg);
    }
    _metadata_builder = std::move(metadata_builder_res).value();
}

ss::future<>
l1_object_sink::initialize_builder(kafka::offset object_base_offset) {
    if (!_metadata_builder) {
        co_await init_metadata_builder();
    }
    auto oid_res = co_await _metadata_builder->create_object_for(_tp);
    if (!oid_res.has_value()) {
        auto msg = fmt::format("Failed to create object: {}", oid_res.error());
        vlog(_ctxlog.warn, "{}", msg);
        throw std::runtime_error(msg);
    }
    auto oid = std::move(oid_res).value();

    auto upload_res = co_await _io->create_multipart_upload(
      oid, _upload_part_size, &_as);

    if (!upload_res.has_value()) {
        std::ignore = _metadata_builder->remove_pending_object(oid);
        auto msg = fmt::format(
          "Failed to create multipart upload for object {}: {}",
          oid,
          upload_res.error());
        vlog(_ctxlog.warn, "{}", msg);
        throw std::runtime_error(msg);
    }

    auto upload = std::move(upload_res).value();
    auto output_stream = upload->as_stream();

    auto builder = object_builder::create(std::move(output_stream), _opts);
    co_await builder->start_partition(_tp);

    _inflight_object = std::make_unique<inflight_object_t>(
      std::move(upload), std::move(builder), oid, object_base_offset);
}

ss::future<> l1_object_sink::discard_object(
  cloud_storage_clients::multipart_upload_ref upload,
  std::unique_ptr<object_builder> builder,
  object_id oid) {
    (co_await ss::coroutine::as_future(upload->abort())).ignore_ready_future();
    (co_await ss::coroutine::as_future(builder->close())).ignore_ready_future();
    std::ignore = _metadata_builder->remove_pending_object(oid);
}

ss::future<> l1_object_sink::flush(kafka::offset object_last_offset) {
    if (!_inflight_object) {
        co_return;
    }

    auto inflight_object = std::exchange(_inflight_object, nullptr);
    auto upload = std::move(inflight_object->upload);
    auto builder = std::exchange(inflight_object->builder, nullptr);
    auto oid = inflight_object->oid;
    auto object_base_offset = inflight_object->object_base_offset;

    // Write the footer and get object metadata.
    auto object_info_fut = co_await ss::coroutine::as_future(builder->finish());
    if (object_info_fut.failed()) {
        auto e = object_info_fut.get_exception();
        vlogl(
          _ctxlog,
          ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                        : ss::log_level::warn,
          "Exception creating object_info: {}.",
          e);
        co_await discard_object(std::move(upload), std::move(builder), oid);
        // A discarded object would leave a hole in the offset span of the
        // next commit, which the metastore would reject; abort the run.
        co_await ss::coroutine::return_exception_ptr(std::move(e));
        // Unreachable: return_exception_ptr() completes the coroutine. The
        // explicit co_return terminates the branch for static analysis.
        co_return;
    }

    // close() completes the multipart upload via the stream's data sink.
    auto close_fut = co_await ss::coroutine::as_future(builder->close());
    if (close_fut.failed()) {
        auto e = close_fut.get_exception();
        vlogl(
          _ctxlog,
          ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                        : ss::log_level::warn,
          "Exception closing object builder: {}.",
          e);
        if (!upload->is_finalized()) {
            co_await upload->abort();
        }
        std::ignore = _metadata_builder->remove_pending_object(oid);
        co_await ss::coroutine::return_exception_ptr(std::move(e));
    }

    // Upload succeeded — register the object with the metadata builder.
    auto object_info = object_info_fut.get();

    vlog(
      _ctxlog.trace,
      "Completed multipart upload for object {} ({}~{})",
      oid,
      object_base_offset,
      object_last_offset);

    auto [first, last] = object_info.index.partitions.equal_range(_tp);
    vassert(
      std::distance(first, last) == 1,
      "Expected one partition range in builder.");
    dassert(
      object_base_offset <= object_last_offset,
      "Compaction sink produced inverted extent for tidp {}: base_offset {} "
      "> last_offset {}",
      _tp,
      object_base_offset,
      object_last_offset);
    auto ntp_md = metastore::object_metadata::ntp_metadata{
      .tidp = _tp,
      .base_offset = object_base_offset,
      .last_offset = object_last_offset,
      .max_timestamp = first->second.max_timestamp,
      .pos = first->second.file_position,
      .size = first->second.length};

    auto add_res = _metadata_builder->add(oid, std::move(ntp_md));
    if (!add_res.has_value()) {
        auto msg = fmt::format(
          "Failed to add object {} to metadata builder: {}",
          oid,
          add_res.error());
        vlog(_ctxlog.warn, "{}", msg);
        std::ignore = _metadata_builder->remove_pending_object(oid);
        co_await ss::coroutine::return_exception(std::runtime_error(msg));
    }

    auto finish_res = _metadata_builder->finish(
      oid, object_info.footer_offset, object_info.size_bytes);
    if (!finish_res.has_value()) {
        auto msg = fmt::format(
          "Failed to finish object {} in metadata builder: {}",
          oid,
          finish_res.error());
        vlog(_ctxlog.warn, "{}", msg);
        std::ignore = _metadata_builder->remove_pending_object(oid);
        co_await ss::coroutine::return_exception(std::runtime_error(msg));
    }

    ++_pending_objects;
    _pending_bytes += object_info.size_bytes;
}

ss::future<ss::stop_iteration>
l1_object_sink::operator()(model::record_batch b) {
    auto next_offset = model::offset_cast(b.base_offset());
    auto prev_offset = kafka::prev_offset(next_offset);

    if (
      _inflight_object
      && _inflight_object->builder->file_size() >= _max_object_size()) {
        co_await flush(prev_offset);
    }

    if (!_inflight_object) {
        co_await initialize_builder(next_offset);
    }

    co_await _inflight_object->builder->add_batch(std::move(b));

    co_return ss::stop_iteration::no;
}

ss::future<> l1_object_sink::prepare_iteration(kafka::offset next_extent_base) {
    bool is_first_extent = _processed_extents.empty();
    if (!is_first_extent) {
        auto prev_extent_last_offset
          = _processed_extents.make_reverse_stream().next().last_offset;
        if (next_extent_base == kafka::next_offset(prev_extent_last_offset)) {
            if (_inflight_object) {
                co_return;
            }
            // Intentional fallthrough.
        } else {
            // Passed extents are non-contiguous. Force a roll of the
            // currently built L1 object with the previous extent's last
            // offset, and start a new L1 object with the new extent's base
            // offset.
            co_await flush(prev_extent_last_offset);
            // Intentional fallthrough.
        }
    }
    co_await initialize_builder(next_extent_base);
}

size_t l1_object_sink::effective_commit_interval_bytes() const {
    auto configured = _commit_interval_bytes();
    auto max_object_size = _max_object_size();
    if (configured >= max_object_size) {
        return configured;
    }
    static thread_local ss::logger::rate_limit rate(std::chrono::minutes{5});
    vloglr(
      _ctxlog,
      ss::log_level::warn,
      rate,
      "Commit interval of {} bytes is below the target object size of {} "
      "bytes; clamping commit interval to {} bytes instead.",
      configured,
      max_object_size,
      max_object_size);
    return max_object_size;
}

ss::future<> l1_object_sink::finish_iteration(
  kafka::offset prev_extent_base, kafka::offset prev_extent_last) {
    _processed_extents.insert(prev_extent_base, prev_extent_last);
    ++_processed_extent_count;
    // Accumulate output until at least one commit interval's worth is
    // pending, then cut the inflight object at this extent boundary and
    // commit everything finished so far.
    auto uncommitted_bytes
      = _pending_bytes
        + (_inflight_object ? _inflight_object->builder->file_size() : 0);
    if (uncommitted_bytes < effective_commit_interval_bytes()) {
        co_return;
    }
    co_await flush(prev_extent_last);
    co_await commit_finished_objects();
}

ss::future<> l1_object_sink::commit_finished_objects() {
    if (!_metadata_builder || _metadata_builder->is_empty()) {
        // Everything finished has already been committed.
        co_return;
    }
    co_await commit_objects(std::exchange(_metadata_builder, nullptr));
    _pending_objects = 0;
    _pending_bytes = 0;
}

ss::future<> l1_object_sink::finalize_inflight(bool success) {
    if (!_metadata_builder) {
        co_return;
    }

    if (_inflight_object) {
        if (success && !_processed_extents.empty()) {
            auto last_offset
              = _processed_extents.make_reverse_stream().next().last_offset;
            co_await flush(last_offset);
        } else {
            // On the exceptional path, the inflight object may either:
            // 1. Have a base offset > the last processed extent's last offset.
            //    E.g., iterating over extents [[0,9], [10,19]], a new object is
            //    rolled with `object_base_offset=15`, an exception is thrown
            //    and caught, and we call `sink->finalize()` with
            //    `last_offset=9`. Flushing would give us an object with offsets
            //    [15,9], violating the offset space.
            // 2. Have partially-processed extent data that extends beyond what
            //    `_processed_extents` tracks. E.g., iterating over extents
            //    [[0,9], [10,19]] with `object_base_offset=0`, we iterate up to
            //    offset 15 in the second extent, an exception is thrown and and
            //    caught, and we call `sink->finalize()` with `last_offset=9`.
            //    Flushing would give us an object with offsets [0,9], even
            //    though the inflight object now contains data for the offset
            //    space [0,15].
            // In either case, discard the inflight object in the exceptional
            // path to avoid uploading an object whose contents don't match the
            // recorded metadata.
            auto inflight_object = std::exchange(_inflight_object, nullptr);
            co_await discard_object(
              std::move(inflight_object->upload),
              std::exchange(inflight_object->builder, nullptr),
              inflight_object->oid);
        }
    }

    // The flush above cut the last object at the last processed extent
    // boundary; commit whatever finished output has not been committed yet.
    if (success) {
        co_await commit_finished_objects();
    }
}

} // namespace cloud_topics::l1
