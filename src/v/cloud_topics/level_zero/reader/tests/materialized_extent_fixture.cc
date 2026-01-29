/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "materialized_extent_fixture.h"

#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/object_utils.h"
#include "cloud_topics/types.h"
#include "gmock/gmock.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "ssx/sformat.h"
#include "storage/record_batch_builder.h"
#include "storage/record_batch_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <exception>
#include <filesystem>
#include <stdexcept>

static ss::logger test_log("materialized_extent_fixture");

ss::future<> materialized_extent_fixture::add_random_batches(int record_count) {
    vassert(expected.empty(), "Already initialized");
    auto res = co_await model::test::make_random_batches(
      model::offset(0), record_count, false);
    for (auto&& b : res) {
        b.header().crc = model::crc_record_batch(b.header(), b.data());
        crc::crc32c crc;
        model::crc_record_batch_header(crc, b.header());
        b.header().header_crc = crc.value();
        expected.push_back(std::move(b));
    }
}

void materialized_extent_fixture::produce_placeholders(
  bool use_cache,
  int group_by,
  std::queue<injected_failure> injected_failures,
  int begin,
  int end) {
    // Serialize batch as an iobuf
    auto serialize_batch = [](model::record_batch batch) -> iobuf {
        // serialize to disk format
        auto hdr_iobuf = storage::batch_header_to_disk_iobuf(batch.header());
        auto rec_iobuf = std::move(batch).release_data();
        iobuf res;
        res.append(std::move(hdr_iobuf));
        res.append(std::move(rec_iobuf));
        return res;
    };
    // Generate a placeholder batch based on current offset/size and the
    // source record batch
    auto generate_placeholder =
      [](
        cloud_topics::object_id id,
        size_t offset,
        size_t size,
        const model::record_batch& source) -> model::record_batch {
        cloud_topics::ctp_placeholder p{
          .id = id,
          .offset = cloud_topics::first_byte_offset_t(offset),
          .size_bytes = cloud_topics::byte_range_size_t(size),
        };

        storage::record_batch_builder builder(
          model::record_batch_type::ctp_placeholder, source.base_offset());

        builder.add_raw_kv(std::nullopt, serde::to_iobuf(p));
        // Match number of records in the batch with the 'source'
        for (auto i = 1; i < source.record_count(); i++) {
            builder.add_raw_kv(std::nullopt, std::nullopt);
        }
        return std::move(builder).build();
    };
    // Per-batch metadata for setting up cache range expectations
    struct batch_cache_info {
        std::filesystem::path path;
        uint64_t offset;
        uint64_t size;
        iobuf data;
    };
    // List of placeholder batches alongside the list of L0 objects
    // that has to be added to the cloud storage mock and (optionally) cache
    // mock
    struct placeholders_and_uploads {
        chunked_vector<model::record_batch> placeholders;
        std::map<std::filesystem::path, iobuf> uploads;
        // Per-batch info for cache range reads
        std::vector<batch_cache_info> batch_infos;
    };
    // Produce data for the partition and the cloud/cache. Group data
    // together using 'group_by' parameter.
    auto generate_placeholders_and_uploads =
      [&](
        std::queue<model::record_batch> sources,
        std::queue<iobuf> serialized_batches) -> placeholders_and_uploads {
        chunked_vector<model::record_batch> placeholders;
        std::map<std::filesystem::path, iobuf> uploads;
        std::vector<batch_cache_info> batch_infos;
        while (!sources.empty()) {
            iobuf current;
            auto id = cloud_topics::object_id::create(
              cloud_topics::cluster_epoch(1));
            auto fname = cloud_topics::object_path_factory::level_zero_path(id);
            for (int i = 0; i < group_by; i++) {
                auto buf = std::move(serialized_batches.front());
                serialized_batches.pop();
                auto batch = std::move(sources.front());
                sources.pop();
                auto offset = current.size_bytes();
                auto size = buf.size_bytes();
                auto placeholder = generate_placeholder(
                  id, offset, size, batch);
                placeholders.push_back(std::move(placeholder));
                // Track per-batch info for cache expectations
                batch_infos.push_back(
                  batch_cache_info{
                    .path = fname,
                    .offset = offset,
                    .size = size,
                    .data = buf.copy(),
                  });
                current.append(std::move(buf));
            }
            uploads[fname] = std::move(current);
        }
        return {
          .placeholders = std::move(placeholders),
          .uploads = std::move(uploads),
          .batch_infos = std::move(batch_infos),
        };
    };
    std::queue<model::record_batch> sources;
    std::queue<iobuf> serialized_batches;
    int ix = 0;
    for (const auto& b : expected) {
        if (ix < begin || ix > end) {
            ix++;
            vlog(
              test_log.info,
              "Skip batch: {}:{}",
              b.header().base_offset,
              b.header().last_offset());
            continue;
        } else {
            ix++;
            vlog(
              test_log.info,
              "Expected batch: {}:{}",
              b.header().base_offset,
              b.header().last_offset());
        }
        // serialize the batch
        // add batch to the cache
        auto buf = serialize_batch(b.copy());
        serialized_batches.push(buf.copy());
        sources.push(b.copy());
    }
    auto [placeholders, uploads, batch_infos]
      = generate_placeholders_and_uploads(
        std::move(sources), std::move(serialized_batches));
    vlog(
      test_log.info,
      "Generated {} placeholders and {} L0 objects",
      placeholders.size(),
      uploads.size());

    if (use_cache) {
        // For memory cache reads, pre-populate the cache with extent data
        // Handle injected failures by setting appropriate failure modes
        for (auto&& info : batch_infos) {
            injected_failure failure = {};
            if (!injected_failures.empty()) {
                failure = injected_failures.back();
                injected_failures.pop();
            }

            // Extract object_id and byte_offset from the path
            // The path is in format: level_zero/{epoch}/{name}
            // We need to find the corresponding placeholder to get the id
            cloud_topics::object_id id;
            cloud_topics::first_byte_offset_t byte_offset{info.offset};

            // Find the placeholder that corresponds to this batch info
            for (const auto& p : placeholders) {
                iobuf payload = p.copy().release_data();
                iobuf_parser parser(std::move(payload));
                auto record = model::parse_one_record_from_buffer(parser);
                iobuf value = std::move(record).release_value();
                auto placeholder
                  = serde::from_iobuf<cloud_topics::ctp_placeholder>(
                    std::move(value));
                if (
                  placeholder.offset() == info.offset
                  && placeholder.size_bytes() == info.size) {
                    id = placeholder.id;
                    break;
                }
            }

            // Handle is_cached failures (these map to has() behavior)
            switch (failure.is_cached) {
            case injected_is_cached_failure::noop:
                // The code is supposed to timeout before invoking methods
                continue;
            case injected_is_cached_failure::throw_error:
            case injected_is_cached_failure::throw_shutdown:
                // For now, skip - the new API doesn't throw on has()
                // These failures will need to be handled differently
                continue;
            default:
                break;
            }

            // Handle cache_get failures
            // For all cases, we need to populate the cache first so has()
            // returns true
            switch (failure.cache_get) {
            case injected_cache_get_failure::none:
                // Pre-populate cache with the extent data
                cache.add_data(
                  fixture_tidp, id, byte_offset, std::move(info.data));
                break;
            case injected_cache_get_failure::return_error:
                // Pre-populate cache so has() returns true, then set failure
                cache.add_data(
                  fixture_tidp, id, byte_offset, std::move(info.data));
                cache.set_get_failure(
                  hydrated_cache_mock::get_failure::return_nullopt);
                break;
            case injected_cache_get_failure::throw_error:
                // Pre-populate cache so has() returns true, then set failure
                cache.add_data(
                  fixture_tidp, id, byte_offset, std::move(info.data));
                cache.set_get_failure(
                  hydrated_cache_mock::get_failure::throw_error);
                break;
            case injected_cache_get_failure::throw_shutdown:
                // Pre-populate cache so has() returns true, then set failure
                cache.add_data(
                  fixture_tidp, id, byte_offset, std::move(info.data));
                cache.set_get_failure(
                  hydrated_cache_mock::get_failure::throw_shutdown);
                break;
            }
        }
    }

    // For cloud storage reads (not cached), set up expectations per L0 object
    if (!use_cache) {
        for (auto&& kv : uploads) {
            injected_failure failure = {};
            if (!injected_failures.empty()) {
                failure = injected_failures.back();
                injected_failures.pop();
            }
            // Simplified event flow:
            // cache.is_cached() -> false (no data in memory cache)
            // remote.download_object() -> payload
            // cache.put() -> store in memory cache
            switch (failure.cloud_get) {
            case injected_cloud_get_failure::none:
                remote.expect_download_object(
                  cloud_storage_clients::object_key(kv.first),
                  cloud_io::download_result::success,
                  kv.second.copy());
                break;
            case injected_cloud_get_failure::return_failure:
                remote.expect_download_object(
                  cloud_storage_clients::object_key(kv.first),
                  cloud_io::download_result::failed,
                  kv.second.copy());
                continue;
            case injected_cloud_get_failure::return_notfound:
                remote.expect_download_object(
                  cloud_storage_clients::object_key(kv.first),
                  cloud_io::download_result::notfound,
                  kv.second.copy());
                continue;
            case injected_cloud_get_failure::return_timeout:
                remote.expect_download_object(
                  cloud_storage_clients::object_key(kv.first),
                  cloud_io::download_result::timedout,
                  kv.second.copy());
                continue;
            case injected_cloud_get_failure::throw_shutdown:
                remote.expect_download_object_throw(
                  cloud_storage_clients::object_key(kv.first),
                  ss::abort_requested_exception());
                continue;
            case injected_cloud_get_failure::throw_error:
                remote.expect_download_object_throw(
                  cloud_storage_clients::object_key(kv.first),
                  std::runtime_error("boo"));
                continue;
            }
            // Memory cache put failures (if needed)
            switch (failure.cache_put) {
            case injected_cache_put_failure::none:
                // No failure - put will succeed
                break;
            case injected_cache_put_failure::throw_error:
                cache.set_put_failure(
                  hydrated_cache_mock::put_failure::throw_error);
                break;
            case injected_cache_put_failure::throw_shutdown:
                cache.set_put_failure(
                  hydrated_cache_mock::put_failure::throw_shutdown);
                break;
            }
        }
    }
    partition = std::move(placeholders);
    for (const auto& b : partition) {
        vlog(
          test_log.info,
          "Placeholder batch: {}:{}",
          b.header().base_offset,
          b.header().last_offset());
    }
}

model::offset materialized_extent_fixture::get_expected_committed_offset() {
    if (expected.empty()) {
        return model::offset{};
    }
    return expected.back().last_offset();
}

chunked_vector<model::record_batch>
materialized_extent_fixture::make_underlying() {
    vlog(
      test_log.info,
      "make_log_reader called, partition's size: {}, expected size: {}",
      partition.size(),
      expected.size());

    return std::move(partition);
}

bool operator==(
  const chunked_vector<model::record_batch>& lhs,
  const chunked_vector<model::record_batch>& rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    auto it_l = lhs.begin();
    auto it_r = rhs.begin();
    for (; it_l != lhs.end() && it_r != rhs.end(); ++it_l, ++it_r) {
        if (*it_l != *it_r) {
            return false;
        }
    }
    return true;
}
