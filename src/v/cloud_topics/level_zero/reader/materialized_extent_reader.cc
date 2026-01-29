/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/reader/materialized_extent_reader.h"

#include "absl/container/node_hash_map.h"
#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_topics/errc.h"
#include "cloud_topics/level_zero/common/object.h"
#include "cloud_topics/level_zero/reader/materialized_extent.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/object_utils.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;

namespace cloud_topics::l0 {

namespace {

/// Download full L0 object from cloud storage
ss::future<result<iobuf>> download_l0_object(
  const object_id& id,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  retry_chain_node* rtc,
  micro_probe* probe) {
    auto cache_file_name = std::filesystem::path(
      object_path_factory::level_zero_path(id));

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

    auto fut = co_await ss::coroutine::as_future(
      api->download_object(std::move(req)));
    if (fut.failed()) {
        auto err = fut.get_exception();
        if (ssx::is_shutdown_exception(err)) {
            co_return errc::shutting_down;
        }
        vlog(cd_log.error, "Unexpected error during L0 download: {}", err);
        co_return errc::unexpected_failure;
    }

    auto dl_result = fut.get();
    if (dl_result != cloud_io::download_result::success) {
        switch (dl_result) {
        case cloud_io::download_result::notfound:
            co_return errc::download_not_found;
        case cloud_io::download_result::failed:
            co_return errc::download_failure;
        case cloud_io::download_result::timedout:
            co_return errc::timeout;
        case cloud_io::download_result::success:
            break;
        }
    }

    co_return std::move(payload);
}

/// Cache all partition data from an L0 object by reading its footer.
/// This is called after downloading an L0 object to populate the cache
/// for all partitions contained in the object.
ss::future<> cache_all_partitions_from_l0_object(
  const object_id& id,
  iobuf& full_object,
  partition_hydrated_cache_api* cache,
  micro_probe* probe) {
    if (cache == nullptr || full_object.size_bytes() < sizeof(uint32_t)) {
        co_return;
    }

    // Read the footer to find all partition regions
    auto footer_result = footer::read(
      full_object.share(0, full_object.size_bytes()));

    if (std::holds_alternative<size_t>(footer_result)) {
        // Unexpected - we have the full object, footer read should succeed
        vlog(
          cd_log.warn,
          "Failed to read footer from L0 object {}, needs {} more bytes",
          id,
          std::get<size_t>(footer_result));
        co_return;
    }

    auto& footer = std::get<l0::footer>(footer_result);

    // Cache data for each partition in the L0 object
    for (const auto& [tidp, info] : footer.partitions) {
        if (info.file_position + info.length > full_object.size_bytes()) {
            vlog(
              cd_log.warn,
              "Invalid partition info in L0 object {}: partition {} has "
              "position {} + length {} > object size {}",
              id,
              tidp,
              info.file_position,
              info.length,
              full_object.size_bytes());
            continue;
        }

        // Extract the partition data from the full object
        iobuf partition_data = full_object.share(
          info.file_position, info.length);

        // Put into the per-partition cache
        // The put may silently reject if epoch ordering is violated
        probe->num_cache_writes++;
        probe->cache_write_bytes += partition_data.size_bytes();
        cache->put(
          tidp,
          id,
          first_byte_offset_t{info.file_position},
          std::move(partition_data));
    }
}

ss::future<result<chunked_vector<materialized_extent>>> materialize_sorted_run(
  const model::topic_id_partition& tidp,
  chunked_vector<extent_meta> query,
  cloud_storage_clients::bucket_name bucket,
  cloud_io::remote_api<>* api,
  partition_hydrated_cache_api* cache,
  retry_chain_node* rtc,
  micro_probe* probe) {
    // Map from object_id to full object payload (for objects downloaded this
    // request). This provides within-request optimization - if multiple extents
    // from the same L0 object are requested, we download once.
    absl::node_hash_map<object_id, iobuf> downloaded;

    chunked_vector<materialized_extent> extents;
    for (const auto& extent : query) {
        extents.push_back(materialized_extent{.meta = extent});
        auto& back = extents.back();

        // 1. Check memory cache for this extent (per-partition)
        if (
          cache != nullptr
          && cache->is_cached(
            tidp,
            back.meta.id,
            back.meta.first_byte_offset,
            back.meta.byte_range_size)) {
            auto cached = cache->get(
              tidp,
              back.meta.id,
              back.meta.first_byte_offset,
              back.meta.byte_range_size);
            if (cached.has_value()) {
                probe->num_cache_reads++;
                probe->cache_read_bytes += cached->size_bytes();
                back.object = std::move(*cached);
                // Data from cache is already the extent slice
                back.meta.first_byte_offset = first_byte_offset_t{0};
                continue;
            }
        }

        // 2. Check if we already downloaded this object in this request
        auto it = downloaded.find(back.meta.id);
        if (it != downloaded.end()) {
            auto& full_object = it->second;
            auto extent_offset = back.meta.first_byte_offset();
            auto extent_size = back.meta.byte_range_size();

            if (extent_offset + extent_size > full_object.size_bytes()) {
                vlog(
                  cd_log.error,
                  "Extent range [{}, {}) exceeds object size {}",
                  extent_offset,
                  extent_offset + extent_size,
                  full_object.size_bytes());
                co_return errc::unexpected_failure;
            }

            // Extract extent slice from the already-downloaded object
            iobuf extent_data = full_object.share(extent_offset, extent_size);

            back.object = std::move(extent_data);
            back.meta.first_byte_offset = first_byte_offset_t{0};
            continue;
        }

        // 3. Download full object from cloud storage
        auto dl_result = co_await download_l0_object(
          back.meta.id, bucket, api, rtc, probe);
        if (!dl_result.has_value()) {
            co_return dl_result.error();
        }

        iobuf full_object = std::move(dl_result.value());

        // 4. Cache ALL partitions from this L0 object
        // This reads the footer and caches data for every partition
        co_await cache_all_partitions_from_l0_object(
          back.meta.id, full_object, cache, probe);

        // 5. Validate and extract extent for current request
        auto extent_offset = back.meta.first_byte_offset();
        auto extent_size = back.meta.byte_range_size();

        if (extent_offset + extent_size > full_object.size_bytes()) {
            vlog(
              cd_log.error,
              "Extent range [{}, {}) exceeds object size {}",
              extent_offset,
              extent_offset + extent_size,
              full_object.size_bytes());
            co_return errc::download_failure;
        }

        // Extract extent slice
        iobuf extent_data = full_object.share(extent_offset, extent_size);

        // 6. Store full object for potential reuse within this request
        downloaded.insert(
          std::make_pair(
            back.meta.id, full_object.share(0, full_object.size_bytes())));

        back.object = std::move(extent_data);
        back.meta.first_byte_offset = first_byte_offset_t{0};
    }

    co_return std::move(extents);
}

} // namespace

ss::future<materialize_result> materialize_placeholders(
  const model::topic_id_partition& tidp,
  cloud_storage_clients::bucket_name bucket,
  chunked_vector<extent_meta> query,
  cloud_io::remote_api<ss::lowres_clock>& api,
  partition_hydrated_cache_api* cache,
  retry_chain_node& rtc,
  retry_chain_logger& logger) {
    micro_probe probe;
    auto extents = co_await materialize_sorted_run(
      tidp, std::move(query), bucket, &api, cache, &rtc, &probe);
    if (!extents.has_value()) {
        vlog(
          logger.warn,
          "Failed to materialize sorted run: {}",
          extents.error().message());
        co_return materialize_result{
          .batches = extents.error(),
          .probe = probe,
        };
    }

    chunked_vector<model::record_batch> results;
    for (auto& e : extents.value()) {
        results.push_back(make_raft_data_batch(std::move(e)));
    }
    co_return materialize_result{
      .batches = std::move(results),
      .probe = probe,
    };
}

} // namespace cloud_topics::l0
