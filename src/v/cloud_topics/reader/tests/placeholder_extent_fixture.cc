#include "placeholder_extent_fixture.h"

#include "cloud_storage_clients/types.h"
#include "cloud_topics/dl_placeholder.h"
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

namespace cloud_topics = experimental::cloud_topics;
static ss::logger test_log("placeholder_extent_fixture");

ss::future<> placeholder_extent_fixture::add_random_batches(int record_count) {
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

void placeholder_extent_fixture::produce_placeholders(
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
        uuid_t id,
        size_t offset,
        size_t size,
        const model::record_batch& source) -> model::record_batch {
        cloud_topics::dl_placeholder p{
          .id = cloud_topics::object_id(id),
          .offset = cloud_topics::first_byte_offset_t(offset),
          .size_bytes = cloud_topics::byte_range_size_t(size),
        };

        storage::record_batch_builder builder(
          model::record_batch_type::dl_placeholder, source.base_offset());

        builder.add_raw_kv(
          serde::to_iobuf(cloud_topics::dl_placeholder_record_key::payload),
          serde::to_iobuf(p));
        // Match number of records in the batch with the 'source'
        for (auto i = 1; i < source.record_count(); i++) {
            iobuf empty;
            builder.add_raw_kv(
              serde::to_iobuf(cloud_topics::dl_placeholder_record_key::empty),
              std::move(empty));
        }
        return std::move(builder).build();
    };
    // List of placeholder batches alongside the list of L0 objects
    // that has to be added to the cloud storage mock and (optionally) cache
    // mock
    struct placeholders_and_uploads {
        fragmented_vector<model::record_batch> placeholders;
        std::map<std::filesystem::path, iobuf> uploads;
    };
    // Produce data for the partition and the cloud/cache. Group data
    // together using 'group_by' parameter.
    auto generate_placeholders_and_uploads =
      [&](
        std::queue<model::record_batch> sources,
        std::queue<iobuf> serialized_batches) -> placeholders_and_uploads {
        fragmented_vector<model::record_batch> placeholders;
        std::map<std::filesystem::path, iobuf> uploads;
        while (!sources.empty()) {
            iobuf current;
            auto id = uuid_t::create();
            for (int i = 0; i < group_by; i++) {
                auto buf = std::move(serialized_batches.front());
                serialized_batches.pop();
                auto batch = std::move(sources.front());
                sources.pop();
                auto placeholder = generate_placeholder(
                  id, current.size_bytes(), buf.size_bytes(), batch);
                placeholders.push_back(std::move(placeholder));
                current.append(std::move(buf));
            }
            auto fname = std::filesystem::path(ssx::sformat("{}", id));
            uploads[fname] = std::move(current);
        }
        return {
          .placeholders = std::move(placeholders),
          .uploads = std::move(uploads),
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
    auto [placeholders, uploads] = generate_placeholders_and_uploads(
      std::move(sources), std::move(serialized_batches));
    vlog(
      test_log.info,
      "Generated {} placeholders and {} L0 objects",
      placeholders.size(),
      uploads.size());

    for (auto&& kv : uploads) {
        auto sz = kv.second.size_bytes();
        injected_failure failure = {};
        if (!injected_failures.empty()) {
            failure = injected_failures.back();
            injected_failures.pop();
        }
        if (use_cache) {
            // Simplified event flow:
            // cache.is_cached() -> available
            // cache.get() -> payload

            switch (failure.is_cached) {
            case injected_is_cached_failure::none:
                cache.expect_is_cached(
                  kv.first, cloud_io::cache_element_status::available);
                break;
            case injected_is_cached_failure::stall_then_ok:
                cache.expect_is_cached(
                  kv.first,
                  std::vector<cloud_io::cache_element_status>{
                    cloud_io::cache_element_status::in_progress,
                    cloud_io::cache_element_status::available});
                break;
            case injected_is_cached_failure::noop:
                // The code is supposed to timeout before even
                // invoking any methods.
                continue;
            case injected_is_cached_failure::stall_then_fail:
                throw std::runtime_error("Not implemented");
            case injected_is_cached_failure::throw_error:
                cache.expect_is_cached_throws(
                  kv.first,
                  std::make_exception_ptr(std::runtime_error("dummy")));
                continue;
            case injected_is_cached_failure::throw_shutdown:
                cache.expect_is_cached_throws(
                  kv.first,
                  std::make_exception_ptr(ss::gate_closed_exception()));
                continue;
            };

            cloud_io::cache_item_stream s{
              .body = make_iobuf_input_stream(kv.second.copy()),
              .size = sz,
            };
            switch (failure.cache_get) {
            case injected_cache_get_failure::none:
                cache.expect_get(kv.first, std::move(s));
                break;
            case injected_cache_get_failure::return_error:
                cache.expect_get(kv.first, std::nullopt);
                break;
            case injected_cache_get_failure::throw_error:
                cache.expect_get_throws(
                  kv.first,
                  std::make_exception_ptr(std::runtime_error("dummy")));
                break;
            case injected_cache_get_failure::throw_shutdown:
                cache.expect_get_throws(
                  kv.first,
                  std::make_exception_ptr(ss::gate_closed_exception()));
                break;
            };
        } else {
            // Simplified event flow:
            // cache.is_cached() -> not_available
            // remote.download_object() -> payload
            // cache.reserve_space() -> guard
            // cache.put(payload, guard)
            cache.expect_is_cached(
              kv.first, cloud_io::cache_element_status::not_available);
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
            switch (failure.cache_rsv) {
            case injected_cache_rsv_failure::none:
                cache.expect_reserve_space(
                  sz,
                  1,
                  cloud_io::basic_space_reservation_guard<ss::lowres_clock>(
                    cache, 0, 0));
                break;
            case injected_cache_rsv_failure::throw_error:
                cache.expect_reserve_space_throw(
                  std::make_exception_ptr(std::runtime_error("boo")));
                continue;
            case injected_cache_rsv_failure::throw_shutdown:
                cache.expect_reserve_space_throw(
                  std::make_exception_ptr(ss::abort_requested_exception()));
                continue;
            }
            switch (failure.cache_put) {
            case injected_cache_put_failure::none:
                cache.expect_put(kv.first);
                break;
            case injected_cache_put_failure::throw_error:
                cache.expect_put(
                  kv.first, std::make_exception_ptr(std::runtime_error("boo")));
                break;
            case injected_cache_put_failure::throw_shutdown:
                cache.expect_put(
                  kv.first,
                  std::make_exception_ptr(ss::abort_requested_exception()));
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

model::offset placeholder_extent_fixture::get_expected_committed_offset() {
    if (expected.empty()) {
        return model::offset{};
    }
    return expected.back().last_offset();
}

model::record_batch_reader placeholder_extent_fixture::make_log_reader() {
    vlog(
      test_log.info,
      "make_log_reader called, partition's size: {}, expected size: {}",
      partition.size(),
      expected.size());
    return model::make_fragmented_memory_record_batch_reader(
      std::move(partition));
}
