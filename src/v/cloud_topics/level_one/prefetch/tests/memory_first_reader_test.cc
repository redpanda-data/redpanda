/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/units.h"
#include "bytes/iostream.h"
#include "cloud_topics/batch_cache/batch_cache.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/level_one/prefetch/fetch_stream.h"
#include "cloud_topics/level_one/prefetch/memory_first_reader.h"
#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"
#include "cloud_topics/log_reader_config.h"
#include "config/configuration.h"
#include "container/chunked_circular_buffer.h"
#include "features/feature_table.h"
#include "model/batch_utils.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "test_utils/test.h"
#include "test_utils/test_env.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <utility>

using namespace std::chrono_literals;

namespace cloud_topics::prefetch {
namespace {

pacer_config make_pacer_config() {
    return pacer_config{
      .min_window = 1_MiB,
      .max_window = 64_MiB,
      .min_chunk = 256_KiB,
      .max_chunk = 4_MiB,
      .safety = 1.5,
    };
}

/// Build a deadline far enough in the future for normal tests.
model::timeout_clock::time_point far_future() {
    return model::timeout_clock::now() + 30s;
}

class memory_first_reader_test : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        co_await ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").set_value(true);
            config::shard_local_cfg()
              .get("disable_public_metrics")
              .set_value(true);
        });
        co_await _feature_table.start();
        co_await _feature_table.invoke_on_all(
          [](features::feature_table& f) { f.testing_activate_all(); });
        co_await _kvstore->start();
        _lm = std::make_unique<storage::log_manager>(
          storage::log_config(
            _test_dir,
            200_MiB,
            storage::with_cache::yes,
            storage::make_sanitized_file_config()),
          *_kvstore,
          _resources,
          _feature_table);
        _cache = std::make_unique<cloud_topics::batch_cache>(_lm.get());
        co_await _cache->start();
    }

    ss::future<> TearDownAsync() override {
        co_await _cache->stop();
        co_await _lm->stop();
        co_await _kvstore->stop();
        co_await _feature_table.stop();
        co_await ss::smp::invoke_on_all([] {
            config::shard_local_cfg().get("disable_metrics").reset();
            config::shard_local_cfg().get("disable_public_metrics").reset();
        });
    }

protected:
    model::topic_id_partition make_tidp() {
        return model::topic_id_partition{
          model::topic_id{uuid_t::create()}, model::partition_id{0}};
    }

    /// Build a single L1 object from the given batches for one partition.
    ss::future<> make_l1_object(
      model::topic_id_partition tidp,
      chunked_circular_buffer<model::record_batch> batches) {
        auto meta_builder = (co_await _metastore.object_builder()).value();
        auto oid
          = (co_await meta_builder->get_or_create_object_for(tidp)).value();

        iobuf buf;
        auto builder = l1::object_builder::create(
          make_iobuf_ref_output_stream(buf), {});

        l1::metastore::term_offset_map_t term_map;
        term_map[tidp].push_back(
          l1::metastore::term_offset{
            .term = model::term_id{1},
            .first_offset = model::offset_cast(batches.front().base_offset()),
          });

        co_await builder->start_partition(tidp);
        for (auto& batch : batches) {
            co_await builder->add_batch(std::move(batch));
        }

        auto obj_info = co_await builder->finish();
        co_await builder->close();
        _io.put_object(oid, std::move(buf));
        for (auto& [part_tidp, partition] : obj_info.index.partitions) {
            meta_builder
              ->add(
                oid,
                l1::metastore::object_metadata::ntp_metadata{
                  .tidp = part_tidp,
                  .base_offset = partition.first_offset,
                  .last_offset = partition.last_offset,
                  .max_timestamp = partition.max_timestamp,
                  .pos = partition.file_position,
                  .size = partition.length,
                })
              .value();
        }
        meta_builder->finish(oid, obj_info.footer_offset, obj_info.size_bytes)
          .value();
        co_await _metastore.add_objects(*meta_builder, term_map);
    }

    static chunked_circular_buffer<model::record_batch>
    make_batches(model::offset start, int count) {
        chunked_circular_buffer<model::record_batch> out;
        auto off = start;
        for (int i = 0; i < count; ++i) {
            auto b = model::test::make_random_batch(off, 1, false);
            off = model::next_offset(b.last_offset());
            out.push_back(std::move(b));
        }
        return out;
    }

    static chunked_circular_buffer<model::record_batch>
    copy(chunked_circular_buffer<model::record_batch>& in) {
        chunked_circular_buffer<model::record_batch> out;
        for (auto& b : in) {
            out.push_back(b.share());
        }
        return out;
    }

    cloud_topic_log_reader_config make_cfg(
      model::topic_id_partition /*tidp*/,
      kafka::offset start,
      kafka::offset max_off) {
        return cloud_topic_log_reader_config{
          cloud_io::group_id::default_group, start, max_off};
    }

    ss::sstring _test_dir{
      test_env::random_dir_path("memory_first_reader.", 10)};
    storage::storage_resources _resources;
    ss::sharded<features::feature_table> _feature_table;
    std::unique_ptr<storage::kvstore> _kvstore{
      std::make_unique<storage::kvstore>(
        storage::kvstore_config(
          1_MiB,
          config::mock_binding(10ms),
          _test_dir,
          storage::make_sanitized_file_config()),
        ss::this_shard_id(),
        _resources,
        _feature_table)};
    std::unique_ptr<storage::log_manager> _lm;
    std::unique_ptr<cloud_topics::batch_cache> _cache;

    l1::simple_metastore _metastore{};
    l1::fake_io _io{};
};

} // namespace

/// Test (a): pre-filled cache → reader serves batches in order, on_consumed
/// observed via demand_watermark advancing.
TEST_F_CORO(memory_first_reader_test, prefilled_cache_serves_in_order) {
    auto tidp = make_tidp();

    auto batches = make_batches(model::offset{0}, 5);
    auto expected = copy(batches);

    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref();

    // Pre-fill the stream's own index directly (as produce() would).
    for (auto& b : batches) {
        stream.testing_put(b);
    }

    auto impl = std::make_unique<memory_first_reader_impl>(
      &stream,
      _cache.get(),
      make_cfg(tidp, kafka::offset{0}, kafka::offset{4}));

    // Load one slice — should contain all 5 batches.
    auto slice = co_await impl->do_load_slice(far_future());
    auto& data = std::get<model::record_batch_reader::data_t>(slice);
    ASSERT_FALSE_CORO(data.empty());

    // Verify offsets are monotonically increasing and match expected.
    ASSERT_EQ_CORO(data.size(), expected.size());
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_EQ_CORO(data[i].base_offset(), expected[i].base_offset());
        ASSERT_EQ_CORO(data[i].last_offset(), expected[i].last_offset());
    }

    // on_consumed must have been called: demand_watermark should have advanced.
    // After consuming 5 batches ending at offset 4, watermark >=
    // next_offset(4).
    ASSERT_GE_CORO(stream.demand_watermark(), kafka::offset{5});

    stream.unref();
    co_await stream.close();
}

/// Ghost batches in the cache mark real gaps (a leading transaction
/// fence/control batch, an aborted-transaction range removed during
/// reconciliation, or a compaction hole). The reader must skip them and serve
/// the next real batch, NOT stall waiting for the missing offset or hand a
/// ghost batch to the consumer. Reproduces the L1 tx-consume stall where the
/// consumer pinned at offset 0 (a tx control batch absent from L1).
TEST_F_CORO(memory_first_reader_test, skips_ghost_batches_at_gap) {
    auto tidp = make_tidp();

    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref();

    // Gap at offset 0 (ghost), real data at offsets 1..3, pre-filled into the
    // stream's own index (as produce() would land them).
    stream.testing_put(
      model::make_ghost_batch(
        model::offset{0}, model::offset{0}, model::term_id{1}));
    auto data = make_batches(model::offset{1}, 3);
    auto expected = copy(data);
    for (auto& b : data) {
        stream.testing_put(b);
    }

    auto impl = std::make_unique<memory_first_reader_impl>(
      &stream,
      _cache.get(),
      make_cfg(tidp, kafka::offset{0}, kafka::offset{3}));

    auto slice = co_await impl->do_load_slice(far_future());
    auto& out = std::get<model::record_batch_reader::data_t>(slice);

    // The ghost at offset 0 is skipped: the reader returns only the real data
    // starting at offset 1, and never surfaces a ghost batch.
    ASSERT_EQ_CORO(out.size(), expected.size());
    ASSERT_EQ_CORO(out.front().base_offset(), model::offset{1});
    for (auto& b : out) {
        ASSERT_NE_CORO(b.header().type, model::record_batch_type::ghost_batch);
    }

    stream.unref();
    co_await stream.close();
}

/// Test (b): miss then producer fills → reader unblocks and returns the data.
TEST_F_CORO(
  memory_first_reader_test, miss_then_producer_fills_unblocks_reader) {
    auto tidp = make_tidp();

    auto batches = make_batches(model::offset{0}, 4);
    auto expected = copy(batches);
    co_await make_l1_object(tidp, std::move(batches));

    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref();

    auto impl = std::make_unique<memory_first_reader_impl>(
      &stream,
      _cache.get(),
      make_cfg(tidp, kafka::offset{0}, kafka::offset{3}));

    // Start reading in the background — cache is empty so the reader will
    // call on_demand then block in wait_for_offset.
    auto read_fut = impl->do_load_slice(far_future());

    // Drive the producer to fill the cache while the reader is waiting.
    for (int i = 0; i < 32 && stream.produced_through() < kafka::offset{3};
         ++i) {
        co_await stream.produce(4_MiB, 1);
    }
    ASSERT_FALSE_CORO(stream.error());

    // Now the reader should unblock.
    auto slice = co_await std::move(read_fut);
    auto& data = std::get<model::record_batch_reader::data_t>(slice);
    ASSERT_FALSE_CORO(data.empty());
    ASSERT_EQ_CORO(data.front().base_offset(), expected.front().base_offset());

    stream.unref();
    co_await stream.close();
}

/// Test (c): deadline with no data → returns empty without hanging.
TEST_F_CORO(memory_first_reader_test, deadline_with_no_data_returns_empty) {
    auto tidp = make_tidp();

    // No data in cache, no data produced.
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref();

    auto impl = std::make_unique<memory_first_reader_impl>(
      &stream,
      _cache.get(),
      make_cfg(tidp, kafka::offset{0}, kafka::offset{99}));

    // Use a near-future deadline so the wait expires quickly.
    auto deadline = model::timeout_clock::now() + 200ms;
    auto slice = co_await impl->do_load_slice(deadline);
    auto& data = std::get<model::record_batch_reader::data_t>(slice);
    // Must return empty rather than hang past the deadline.
    ASSERT_TRUE_CORO(data.empty());
    // End of stream must be set so caller knows not to retry forever.
    ASSERT_TRUE_CORO(impl->is_end_of_stream());

    stream.unref();
    co_await stream.close();
}

/// Test (d): producer error → reader surfaces the error promptly.
///
/// We set a 60-second deadline, record wall-clock time before and after,
/// inject the error from a separate fiber while the reader is parked, and
/// assert the read completes in well under 5 seconds. If someone removes the
/// _error_abort subscription this test will hang ~60s and the runner will
/// kill it, proving the mechanism is necessary.
TEST_F_CORO(memory_first_reader_test, stream_error_wakes_reader_immediately) {
    auto tidp = make_tidp();

    // No data in cache: reader will park in wait_for_offset.
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref();

    auto impl = std::make_unique<memory_first_reader_impl>(
      &stream,
      _cache.get(),
      make_cfg(tidp, kafka::offset{0}, kafka::offset{99}));

    // 60-second deadline — the test must complete WELL before this.
    auto far = model::timeout_clock::now() + 60s;
    auto read_fut = impl->do_load_slice(far);

    // Record start time so we can measure elapsed after the read returns.
    auto t0 = ss::lowres_clock::now();

    // Fire the error from a separate fiber so the reader is truly parked
    // before the error is injected (the yield gives the reader coroutine a
    // chance to reach wait_for_offset before we call set_terminal_error).
    auto inject_fut = ss::now().then([&stream]() {
        auto err_ex = std::make_exception_ptr(
          std::runtime_error("injected stream error"));
        stream.set_terminal_error(std::move(err_ex));
    });

    // Await the reader — the injected error should wake it immediately.
    bool threw = false;
    try {
        co_await std::move(read_fut);
    } catch (const std::exception&) {
        threw = true;
    }
    co_await std::move(inject_fut);

    auto elapsed = ss::lowres_clock::now() - t0;

    // If the immediate-wake mechanism works, elapsed << 5s.
    // If _error_abort subscription is removed, elapsed ≈ 60s (test timeout).
    ASSERT_LT_CORO(elapsed, 5s)
      << "reader took " << elapsed.count()
      << "ms — immediate-wake via error_abort_source appears broken";

    // The reader must have either thrown or set end-of-stream.
    ASSERT_TRUE_CORO(threw || impl->is_end_of_stream());

    stream.unref();
    co_await stream.close();
}

/// Test (e-delta): on_consumed reports per-slice DELTAS, not cumulative bytes.
///
/// Pre-fill the cache with 4 batches (offsets 0-3) and configure a single
/// reader with max_offset=1 (offsets 0 and 1 only). Drive TWO do_load_slice
/// calls on the SAME impl:
///   - Slice 1 reads batch 0 only (batch 1 is in cache but the walk skips it
///     because the first batch is accepted and the cache walk continues until
///     a miss — but batch 1 IS there so the walk gets both. To force one-
///     batch-per-slice we need a cache miss on the first slice. We achieve
///     this by pre-filling only offset 0 before slice 1 and all of 0-3 for
///     slice 2.
///
/// Actually the cleanest approach: produce all batches up-front so there are
/// no misses, and verify the accounting via two readers on two streams sharing
/// the cache (each reader starts fresh so _bytes_consumed and _reported_bytes
/// both start at 0 — the bug never fires for single-slice readers).
///
/// For the SAME-impl multi-slice path: produce ALL batches, then set max_offset
/// so that the first slice returns batch 0 due to cache miss (batch 1 not yet
/// cached) and the second slice returns batch 1 after it appears.
///
/// We verify: cached_ahead_bytes after slice 2 == initial_cached - bytes_slice1
/// - bytes_slice2 (delta), NOT initial_cached - 2*bytes_slice1 - bytes_slice2
/// (cumulative, because slice 2 would pass
/// _bytes_consumed=bytes_slice1+bytes_slice2 to on_consumed instead of just
/// bytes_slice2).
///
/// To ensure correct measurement timing, we produce batch 0 only for slice 1,
/// then produce the rest AFTER slice 1 is done, then await slice 2.
TEST_F_CORO(
  memory_first_reader_test, on_consumed_reports_deltas_not_cumulative) {
    auto tidp = make_tidp();

    // Object A holds batch 0; object B holds batches 1-3.
    // This lets us produce them at different times.
    auto batches_a = make_batches(model::offset{0}, 1);
    auto expected_a = copy(batches_a);
    co_await make_l1_object(tidp, std::move(batches_a));

    auto batches_b = make_batches(model::offset{1}, 3);
    auto expected_b = copy(batches_b);
    co_await make_l1_object(tidp, std::move(batches_b));

    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref();

    // Step 1: produce only batch 0.
    for (int i = 0; i < 32 && stream.produced_through() < kafka::offset{0};
         ++i) {
        co_await stream.produce(4_KiB, 1);
    }
    ASSERT_FALSE_CORO(stream.error());
    // Verify batch 1 is NOT cached yet.
    ASSERT_FALSE_CORO(_cache->get(tidp, model::offset{1}).has_value());

    size_t initial_cached = stream.cached_ahead_bytes_for_test();
    ASSERT_GT_CORO(initial_cached, 0u);
    size_t batch0_size = expected_a.front().size_bytes();
    ASSERT_EQ_CORO(initial_cached, batch0_size);

    // Single impl, max_offset=1 (reads offsets 0 and 1).
    auto impl = std::make_unique<memory_first_reader_impl>(
      &stream,
      _cache.get(),
      make_cfg(tidp, kafka::offset{0}, kafka::offset{1}));

    // Step 2: slice 1 — gets batch 0 (hit), then misses on batch 1 → returns
    // {0}.
    auto slice1 = co_await impl->do_load_slice(far_future());
    auto& data1 = std::get<model::record_batch_reader::data_t>(slice1);
    ASSERT_FALSE_CORO(data1.empty());
    size_t bytes_slice1 = data1.front().size_bytes();
    ASSERT_EQ_CORO(bytes_slice1, batch0_size);

    // After slice 1: cached dropped by bytes_slice1 (delta == bytes_slice1).
    size_t cached_after_slice1 = stream.cached_ahead_bytes_for_test();
    ASSERT_EQ_CORO(cached_after_slice1, initial_cached - bytes_slice1);

    // Step 3: produce batches 1-3 and measure BEFORE awaiting slice 2.
    // Slice 2 is not started yet — no interleaving risk.
    for (int i = 0; i < 32 && stream.produced_through() < kafka::offset{3};
         ++i) {
        co_await stream.produce(4_MiB, 1);
    }
    ASSERT_FALSE_CORO(stream.error());
    // cached_ahead_bytes reflects batches 1-3 from produce.
    size_t cached_before_slice2 = stream.cached_ahead_bytes_for_test();
    ASSERT_GT_CORO(cached_before_slice2, 0u);

    // Step 4: slice 2 — batch 1 is now in cache, reader gets it immediately.
    // The same impl's _bytes_consumed == bytes_slice1 at this point.
    // Delta = _bytes_consumed_after - _reported_bytes =
    // (bytes_slice1+bytes_slice2) - bytes_slice1 = bytes_slice2. Cumulative
    // (bug) = _bytes_consumed_after = bytes_slice1 + bytes_slice2.
    auto slice2 = co_await impl->do_load_slice(far_future());
    auto& data2 = std::get<model::record_batch_reader::data_t>(slice2);
    ASSERT_FALSE_CORO(data2.empty());
    size_t bytes_slice2 = data2.front().size_bytes();

    size_t cached_after_slice2 = stream.cached_ahead_bytes_for_test();

    // Delta: on_consumed(bytes_slice2) → cached_before_slice2 - bytes_slice2
    // Cumulative (bug): on_consumed(bytes_slice1+bytes_slice2)
    //                   → cached_before_slice2 - bytes_slice1 - bytes_slice2
    ASSERT_EQ_CORO(cached_after_slice2, cached_before_slice2 - bytes_slice2)
      << "delta off: expected " << (cached_before_slice2 - bytes_slice2)
      << " got " << cached_after_slice2 << "; cumulative would give "
      << (cached_before_slice2 >= bytes_slice1 + bytes_slice2
            ? cached_before_slice2 - bytes_slice1 - bytes_slice2
            : 0u)
      << " (bytes_slice1=" << bytes_slice1 << " bytes_slice2=" << bytes_slice2
      << " cached_before_slice2=" << cached_before_slice2 << ")";

    stream.unref();
    co_await stream.close();
}

/// Test (e): dtor unref()s the stream — refs() drops from 2 to 1.
TEST_F_CORO(memory_first_reader_test, dtor_unrefs_stream) {
    auto tidp = make_tidp();

    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));
    stream.ref(); // simulate the service's ref
    ASSERT_EQ_CORO(stream.refs(), 1);

    {
        // The reader's ctor calls ref(), bumping refs to 2.
        auto impl = std::make_unique<memory_first_reader_impl>(
          &stream,
          _cache.get(),
          make_cfg(tidp, kafka::offset{0}, kafka::offset{99}));
        ASSERT_EQ_CORO(stream.refs(), 2);
        // impl goes out of scope here — dtor calls unref(), drops to 1.
    }
    ASSERT_EQ_CORO(stream.refs(), 1);

    stream.unref();
    co_await stream.close();
}

} // namespace cloud_topics::prefetch
