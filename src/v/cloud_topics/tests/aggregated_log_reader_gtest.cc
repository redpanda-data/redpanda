// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_api_mocks.h"
#include "cloud_topics/aggregated_log_reader.h"
#include "cloud_topics/dl_placeholder.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "storage/record_batch_builder.h"
#include "storage/record_batch_utils.h"
#include "storage/types.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <system_error>

inline ss::logger test_log("aggregated_log_reader_gtest");

using namespace cloud_storage;
using namespace std::chrono_literals;

class aggregated_log_reader_fixture : public seastar_test {
public:
    ss::future<> SetUpAsync() override { co_await cache.dir.create(); }
    ss::future<> TearDownAsync() override { co_await cache.dir.remove(); }
    // Generate random batches.
    // This is a source of truth for the test. The goal is to consume
    // these batches from placeholder/cache/cloud indirection.
    ss::future<> add_random_batches(int record_count) {
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

    // Generate the 'partition' collection from the source of truth. If the
    // 'cache' is set to 'true' the data is added to the cloud storage cache.
    // The 'group_by' parameter control how many batches are stored per L0
    // object.
    void produce_placeholders(bool use_cache, int group_by) {
        // Serialize batch as an iobuf
        auto serialize_batch = [](model::record_batch batch) -> iobuf {
            // serialize to disk format
            auto hdr_iobuf = storage::batch_header_to_disk_iobuf(
              batch.header());
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
                  serde::to_iobuf(
                    cloud_topics::dl_placeholder_record_key::empty),
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
        for (const auto& b : expected) {
            vlog(
              test_log.info,
              "Expected batch: {}:{}",
              b.header().base_offset,
              b.header().last_offset());
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
            if (use_cache) {
                cache.insert(kv.first, std::move(kv.second));
            }
            remote.get_successful_requests().insert_or_assign(
              kv.first, std::move(kv.second));
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

    model::offset get_expected_committed_offset() {
        if (expected.empty()) {
            return model::offset{};
        }
        return expected.back().last_offset();
    }

    model::record_batch_reader make_log_reader() {
        vlog(
          test_log.info,
          "make_log_reader called, partition's size: {}, expected size: {}",
          partition.size(),
          expected.size());
        return model::make_fragmented_memory_record_batch_reader(
          std::move(partition));
    }

    fragmented_vector<model::record_batch> partition;
    fragmented_vector<model::record_batch> expected;
    remote_mock<ss::lowres_clock> remote;
    cache_mock cache;
};

struct fragmented_vector_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
        target->push_back(std::move(rb));
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    fragmented_vector<model::record_batch>* target;
};

TEST_F_CORO(aggregated_log_reader_fixture, full_scan_test) {
    const int num_batches = 10;
    co_await add_random_batches(num_batches);
    produce_placeholders(true, 1);
    auto underlying = make_log_reader();
    storage::log_reader_config config(
      model::offset(0),
      get_expected_committed_offset(),
      ss::default_priority_class());
    auto reader = cloud_topics::make_aggregated_log_reader(
      config,
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      remote,
      cache);
    fragmented_vector<model::record_batch> actual;
    fragmented_vector_consumer consumer{
      .target = &actual,
    };
    co_await reader.consume(consumer, model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(actual.size(), expected.size());
    ASSERT_TRUE_CORO(actual == expected);
}

// Same as 'full_scan_test' but the range can be arbitrary
ss::future<> test_aggregated_log_partial_scan(
  aggregated_log_reader_fixture* fx, int num_batches, int begin, int end) {
    co_await fx->add_random_batches(num_batches);
    fx->produce_placeholders(true, 1);

    // Copy batches that we expect to read
    model::offset base;
    model::offset last;
    fragmented_vector<model::record_batch> expected_view;
    for (int i = begin; i <= end; i++) {
        const auto& hdr = fx->expected.at(i);
        if (base == model::offset{}) {
            base = hdr.base_offset();
        }
        last = hdr.last_offset();
        expected_view.push_back(std::move(fx->expected.at(i)));
    }
    vlog(
      test_log.info,
      "Fetching offsets {} to {}, expect to read {} batches",
      base,
      last,
      expected_view.size());
    for (const auto& b : expected_view) {
        vlog(
          test_log.info,
          "Expect fetching {}:{}",
          b.base_offset(),
          b.last_offset());
    }

    auto underlying = fx->make_log_reader();
    storage::log_reader_config config(base, last, ss::default_priority_class());

    auto reader = cloud_topics::make_aggregated_log_reader(
      config,
      cloud_storage_clients::bucket_name("test-bucket-name"),
      std::move(underlying),
      fx->remote,
      fx->cache);

    fragmented_vector<model::record_batch> actual;
    fragmented_vector_consumer consumer{
      .target = &actual,
    };

    co_await reader.consume(consumer, model::timeout_clock::now() + 10s);
    ASSERT_EQ_CORO(actual.size(), expected_view.size());
    ASSERT_TRUE_CORO(actual == expected_view);
}

TEST_F_CORO(aggregated_log_reader_fixture, scan_range) {
    co_await test_aggregated_log_partial_scan(this, 100, 1, 5);
}
