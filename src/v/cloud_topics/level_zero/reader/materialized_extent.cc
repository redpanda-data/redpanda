/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/materialized_extent.h"

#include "bytes/iostream.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/io_result.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/extent_meta.h"
#include "cloud_topics/level_zero/common/micro_probe.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "storage/record_batch_utils.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <exception>

namespace cloud_topics::l0 {

/// Map error codes from one type to another
template<class Src, class Dst>
struct errc_converter;

template<>
struct errc_converter<cloud_io::download_result, errc> {
    errc operator()(cloud_io::download_result r) {
        switch (r) {
        case cloud_io::download_result::notfound:
            return errc::download_not_found;
        case cloud_io::download_result::failed:
            return errc::download_failure;
        case cloud_io::download_result::timedout:
            return errc::timeout;
        case cloud_io::download_result::success:
            return errc::success;
        };
    }
};

/// Convert ready future to expected<> type
template<errc unexpected = errc::unexpected_failure, class T>
result<T> result_from_ready_future(ss::future<T>&& ready) {
    if (ready.failed()) {
        auto err = ready.get_exception();
        if (ssx::is_shutdown_exception(err)) {
            return errc::shutting_down;
        }
        return unexpected;
    }
    return ready.get();
}

/// Convert ready future to result<> type, log unexpected
/// exception using provided functor
template<errc unexpected = errc::unexpected_failure, class T, class FormatFunc>
result<T> result_from_ready_future(ss::future<T>&& ready, FormatFunc fmt) {
    if (ready.failed()) {
        auto err = ready.get_exception();
        if (ssx::is_shutdown_exception(err)) {
            return errc::shutting_down;
        }
        fmt(err);
        return unexpected;
    }
    return ready.get();
}

/// Convert result<> type to expected<>
///
/// The type of the error code should be known
template<class T, class E>
result<T> result_convert(result<T>&& res) {
    if (!res.has_value()) {
        errc_converter<E, errc> conv;
        return conv(res.error());
    }
    return res.value();
}

model::record_batch make_raft_data_batch(materialized_extent ext) {
    auto offset = ext.meta.first_byte_offset;
    auto size = ext.meta.byte_range_size;
    vassert(
      size() > model::packed_record_batch_header_size,
      "L0 object is smaller ({}) than the batch header",
      size());
    auto header_bytes = ext.object.share(
      offset(), model::packed_record_batch_header_size);
    auto records_bytes = ext.object.share(
      offset() + model::packed_record_batch_header_size,
      size() - model::packed_record_batch_header_size);
    auto header = storage::batch_header_from_disk_iobuf(
      std::move(header_bytes));
    // NOTE: the serialized raft_data batch doesn't have the offset set
    // so we need to populate it from the placeholder batch. We also need
    // to make sure that crc is correct.
    header.base_offset = kafka::offset_cast(ext.meta.base_offset);
    header.crc = model::crc_record_batch(header, records_bytes);
    crc::crc32c crc;
    model::crc_record_batch_header(crc, header);
    header.header_crc = crc.value();
    model::record_batch batch(
      header,
      std::move(records_bytes),
      model::record_batch::tag_ctor_ng{}); // TODO: fix compression
    return batch;
}

ss::future<result<iobuf>> materialize_from_cache(
  std::filesystem::path cache_file_name,
  uint64_t offset,
  uint64_t length,
  cloud_io::basic_cache_service_api<>* cache,
  micro_probe* probe);

ss::future<result<iobuf>> materialize_from_cloud_storage(
  std::filesystem::path cache_file_name,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe);

ss::future<result<bool>> materialize(
  materialized_extent* ext,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe) {
    bool hydrated = false;
    // This iobuf contains the record batch replaced by the placeholder. It
    // might potentially contain data that belongs to other placeholder
    // batches and in order to get the extent of the record batch
    // placeholder we need to use byte offset and size.
    iobuf L0_object_content;

    // 2. download object from S3
    auto cache_file_name = std::filesystem::path(
      object_path_factory::level_zero_path(ext->meta.id));

    std::optional<cloud_io::cache_element_status> status = std::nullopt;
    basic_retry_chain_node<> is_cached_rtc(retry_strategy::backoff, rtc);
    retry_permit rp = is_cached_rtc.retry();
    while (rp.is_allowed && !status.has_value()) {
        auto is_cached_result
          = result_from_ready_future<errc::cache_read_error>(
            co_await ss::coroutine::as_future(
              cache->is_cached(cache_file_name)));

        if (!is_cached_result.has_value()) {
            co_return is_cached_result.error();
        }

        switch (is_cached_result.value()) {
        case cloud_io::cache_element_status::available:
        case cloud_io::cache_element_status::not_available:
            status = is_cached_result.value();
            break;
        case cloud_io::cache_element_status::in_progress:
            // Another fiber is trying to put value into the cache.
            // Wait until the operation is completed but stay within the
            // time budget.
            if (rp.abort_source != nullptr) {
                co_await ss::sleep_abortable(rp.delay, *rp.abort_source);
            } else {
                co_await ss::sleep(rp.delay);
            }
            rp = is_cached_rtc.retry();
            continue;
        }
    }

    if (!rp.is_allowed) {
        co_return errc::timeout;
    }

    if (status.value() == cloud_io::cache_element_status::available) {
        auto res = co_await materialize_from_cache(
          cache_file_name,
          ext->meta.first_byte_offset(),
          ext->meta.byte_range_size(),
          cache,
          probe);
        if (!res.has_value()) {
            co_return res.error();
        }
        ext->object = std::move(res.value());
        // Object now contains just the extent range, so reset offset to 0
        ext->meta.first_byte_offset = cloud_topics::first_byte_offset_t{0};
        hydrated = true; // Indicates range read from cache
    } else {
        auto res = co_await materialize_from_cloud_storage(
          cache_file_name, bucket, api, cache, rtc, probe);
        if (!res.has_value()) {
            co_return res.error();
        }
        ext->object = std::move(res.value());
    }
    co_return hydrated;
}

ss::future<result<iobuf>> materialize_from_cache(
  std::filesystem::path cache_file_name,
  uint64_t offset,
  uint64_t length,
  cloud_io::basic_cache_service_api<>* cache,
  micro_probe* probe) {
    iobuf result_buf;
    probe->num_cache_reads++;
    auto buffer_size = config::shard_local_cfg().storage_read_buffer_size();
    // Disable readahead: we're reading a specific extent range where
    // neighboring bytes belong to different partitions and won't be useful.
    constexpr unsigned int read_ahead = 0;
    auto fut = co_await ss::coroutine::as_future(cache->get_stream_range(
      cache_file_name, offset, length, buffer_size, read_ahead));
    auto sz_stream_result = result_from_ready_future<errc::cache_read_error>(
      std::move(fut));
    if (!sz_stream_result.has_value()) {
        co_return sz_stream_result.error();
    }
    auto sz_stream = std::move(sz_stream_result.value());
    if (!sz_stream.has_value()) {
        co_return errc::cache_read_error;
    }

    auto target = make_iobuf_ref_output_stream(result_buf);
    probe->cache_read_bytes += sz_stream->size;
    co_await ss::copy(sz_stream->body, target);
    co_await sz_stream->body.close();
    co_return result_buf;
}

ss::future<result<iobuf>> materialize_from_cloud_storage(
  std::filesystem::path cache_file_name,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  cloud_io::basic_cache_service_api<>* cache,
  basic_retry_chain_node<>* rtc,
  micro_probe* probe) {
    // Populate the cache
    iobuf payload;
    cloud_io::download_request req{
      .transfer_details = {
        .bucket = bucket,
        .key = cloud_storage_clients::object_key(cache_file_name),
        .parent_rtc = *rtc,
        .success_cb =
          [probe, &payload] {
              probe->num_cloud_reads++;
              probe->cloud_read_bytes += payload.size_bytes();
          },
        .backoff_cb = [probe] { probe->num_cloud_reads++; },
      },
      .display_str = "L0",
      .payload = payload};

    auto dl_result = result_from_ready_future(
      co_await ss::coroutine::as_future(api->download_object(std::move(req))),
      [](std::exception_ptr e) {
          vlog(cd_log.error, "Unexpected error during L0 download: {}", e);
      });

    if (!dl_result.has_value()) {
        co_return dl_result.error();
    }

    if (dl_result.value() != cloud_io::download_result::success) {
        errc_converter<cloud_io::download_result, errc> conv;
        co_return conv(dl_result.value());
    }

    // TODO: use circuit-breaker here, if the operation fails
    // repeatedly it can be temporarily short-circuited to avoid
    // burning cycles.
    auto sr_guard = result_from_ready_future(
      co_await ss::coroutine::as_future(
        cache->reserve_space(payload.size_bytes(), 1)),
      [](std::exception_ptr e) {
          vlog(cd_log.error, "Failed to reserve space: {}", e);
      });

    // The failure to reserve space should only trigger an error
    // if the cause of the error is a cluster shutdown. If the
    // failure is caused by anything else we can still return
    // data to the client. The effect of this is that the client
    // will not retry the request and will not make things worse
    // by increasing the load. And we do have data from the cloud
    // storage at this point anyway.

    if (sr_guard.has_value()) {
        // TODO: use proper priority class
        probe->num_cache_writes++;
        auto buf_str = make_iobuf_input_stream(payload.share());
        auto put_future = co_await ss::coroutine::as_future(
          cache->put(cache_file_name, buf_str, sr_guard.value()));

        if (put_future.failed()) {
            auto e = put_future.get_exception();
            if (ssx::is_shutdown_exception(e)) {
                co_return errc::shutting_down;
            }
            vlog(
              cd_log.warn,
              "Failed to put L0 object into the cache: {}. The error will not "
              "be "
              "propagated to the client but Redpanda may use more resources.",
              e);
        } else {
            probe->cache_write_bytes += payload.size_bytes();
        }
    } else if (sr_guard.error() == errc::shutting_down) {
        co_return errc::shutting_down;
    }

    co_return std::move(payload);
}

} // namespace cloud_topics::l0
