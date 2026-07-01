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
#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"
#include "config/configuration.h"
#include "container/chunked_circular_buffer.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "test_utils/test.h"
#include "test_utils/test_env.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <utility>

using namespace std::chrono_literals;

namespace cloud_topics {
// Friend accessor (declared in batch_cache.h) that lets the prefetch tests
// drive the storage LRU reclaimer directly, the same way the batch_cache unit
// test does. Used to force an eviction pass to assert that un-consumed
// prefetched offsets (inserted clean, without pins) ARE evictable.
struct batch_cache_accessor {
    static void
    reclaim_all(batch_cache& c, const model::topic_id_partition& tidp) {
        // Reclaim everything for this partition.
        c._entries[tidp].index->testing_reclaim_from_cache(
          std::numeric_limits<size_t>::max());
    }
};
} // namespace cloud_topics

namespace cloud_topics::prefetch {
namespace {

// A metastore wrapper that delegates everything to a simple_metastore but
// counts get_extent_metadata_forwards calls so tests can assert the producer
// stays within an object without re-querying.
class counting_metastore : public l1::metastore {
public:
    explicit counting_metastore(l1::simple_metastore* inner)
      : _inner(inner) {}

    size_t forwards_calls() const { return _forwards_calls; }

    ss::future<std::expected<extent_metadata_response, errc>>
    get_extent_metadata_forwards(
      const model::topic_id_partition& tidp,
      kafka::offset min_off,
      kafka::offset max_off,
      size_t max_extents,
      include_object_metadata include) override {
        ++_forwards_calls;
        return _inner->get_extent_metadata_forwards(
          tidp, min_off, max_off, max_extents, include);
    }

    // Everything below simply forwards to the inner metastore.
    ss::future<std::expected<std::unique_ptr<object_metadata_builder>, errc>>
    object_builder() override {
        return _inner->object_builder();
    }
    ss::future<std::expected<offsets_response, errc>>
    get_offsets(const model::topic_id_partition& t) override {
        return _inner->get_offsets(t);
    }
    ss::future<std::expected<size_response, errc>>
    get_size(const model::topic_id_partition& t) override {
        return _inner->get_size(t);
    }
    ss::future<std::expected<add_response, errc>> add_objects(
      const object_metadata_builder& b, const term_offset_map_t& m) override {
        return _inner->add_objects(b, m);
    }
    ss::future<std::expected<void, errc>> replace_objects(
      const object_metadata_builder& b, const replace_epoch_map_t& m) override {
        return _inner->replace_objects(b, m);
    }
    ss::future<std::expected<void, errc>> set_start_offset(
      const model::topic_id_partition& t, kafka::offset o) override {
        return _inner->set_start_offset(t, o);
    }
    ss::future<std::expected<topic_removal_response, errc>>
    remove_topics(const chunked_vector<model::topic_id>& t) override {
        return _inner->remove_topics(t);
    }
    ss::future<std::expected<object_response, errc>>
    get_first_ge(const model::topic_id_partition& t, kafka::offset o) override {
        return _inner->get_first_ge(t, o);
    }
    ss::future<std::expected<object_response, errc>> get_first_ge(
      const model::topic_id_partition& t,
      kafka::offset o,
      model::timestamp ts) override {
        return _inner->get_first_ge(t, o, ts);
    }
    ss::future<std::expected<kafka::offset, errc>> get_first_offset_for_bytes(
      const model::topic_id_partition& t, uint64_t size) override {
        return _inner->get_first_offset_for_bytes(t, size);
    }
    ss::future<std::expected<kafka::offset, errc>> get_end_offset_for_term(
      const model::topic_id_partition& t, model::term_id term) override {
        return _inner->get_end_offset_for_term(t, term);
    }
    ss::future<std::expected<model::term_id, errc>> get_term_for_offset(
      const model::topic_id_partition& t, kafka::offset o) override {
        return _inner->get_term_for_offset(t, o);
    }
    ss::future<std::expected<void, errc>> compact_objects(
      const object_metadata_builder& b, const compaction_map_t& m) override {
        return _inner->compact_objects(b, m);
    }
    ss::future<std::expected<compaction_info_response, errc>>
    get_compaction_info(const compaction_info_spec& s) override {
        return _inner->get_compaction_info(s);
    }
    ss::future<std::expected<compaction_info_map, errc>> get_compaction_infos(
      const chunked_vector<compaction_info_spec>& s) override {
        return _inner->get_compaction_infos(s);
    }
    ss::future<std::expected<leveling_info_map, errc>>
    get_leveling_infos(const chunked_vector<leveling_info_spec>& s) override {
        return _inner->get_leveling_infos(s);
    }
    ss::future<std::expected<extent_metadata_response, errc>>
    get_extent_metadata_backwards(
      const model::topic_id_partition& t,
      kafka::offset min_off,
      kafka::offset max_off,
      size_t max_extents) override {
        return _inner->get_extent_metadata_backwards(
          t, min_off, max_off, max_extents);
    }
    ss::future<std::expected<std::nullopt_t, errc>> flush() override {
        return _inner->flush();
    }
    ss::future<std::expected<std::nullopt_t, errc>>
    restore(const cloud_storage::remote_label& l) override {
        return _inner->restore(l);
    }

private:
    l1::simple_metastore* _inner;
    size_t _forwards_calls{0};
};

// l1::io wrapper that fails a specified (position, size) read.
class fault_io : public l1::io {
public:
    explicit fault_io(l1::fake_io& delegate)
      : _delegate(delegate) {}

    void fail_range(size_t position, size_t size) {
        _fail.emplace(position, size);
    }

    ss::future<std::expected<std::unique_ptr<l1::staging_file>, errc>>
    create_tmp_file() override {
        return _delegate.create_tmp_file();
    }
    ss::future<std::expected<void, errc>> put_object(
      l1::object_id oid, l1::staging_file* f, ss::abort_source* as) override {
        return _delegate.put_object(oid, f, as);
    }
    ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      l1::object_extent extent,
      ss::abort_source* as,
      cloud_io::group_id g) override {
        if (_fail.count({extent.position, extent.size})) {
            co_return std::unexpected(errc::cloud_missing_object);
        }
        co_return co_await _delegate.read_object(extent, as, g);
    }
    ss::future<std::expected<void, errc>> delete_objects(
      chunked_vector<l1::object_id> oids, ss::abort_source* as) override {
        return _delegate.delete_objects(std::move(oids), as);
    }
    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      l1::object_id oid, size_t part_size, ss::abort_source* as) override {
        return _delegate.create_multipart_upload(oid, part_size, as);
    }

private:
    l1::fake_io& _delegate;
    std::set<std::pair<size_t, size_t>> _fail;
};

// l1::io wrapper that counts how many read_object calls overlap in time, so a
// test can assert a single stream issued multiple concurrent chunk GETs. Footer
// reads (whose extent matches the registered footer extents) are excluded so
// only data-chunk concurrency is measured. Each data read is held for a short
// delay to widen the overlap window deterministically.
class counting_io : public l1::io {
public:
    explicit counting_io(l1::fake_io& delegate)
      : _delegate(delegate) {}

    // Mark a (position,size) extent as a footer read to exclude from counting.
    void mark_footer(size_t position, size_t size) {
        _footers.emplace(position, size);
    }

    size_t peak_concurrent() const { return _peak; }

    ss::future<std::expected<std::unique_ptr<l1::staging_file>, errc>>
    create_tmp_file() override {
        return _delegate.create_tmp_file();
    }
    ss::future<std::expected<void, errc>> put_object(
      l1::object_id oid, l1::staging_file* f, ss::abort_source* as) override {
        return _delegate.put_object(oid, f, as);
    }
    ss::future<std::expected<ss::input_stream<char>, errc>> read_object(
      l1::object_extent extent,
      ss::abort_source* as,
      cloud_io::group_id g) override {
        bool is_footer = _footers.count({extent.position, extent.size}) > 0;
        if (is_footer) {
            co_return co_await _delegate.read_object(extent, as, g);
        }
        ++_concurrent;
        _peak = std::max(_peak, _concurrent);
        // Hold the GET briefly so concurrent dispatches overlap.
        co_await ss::sleep(std::chrono::milliseconds(20));
        auto res = co_await _delegate.read_object(extent, as, g);
        --_concurrent;
        co_return res;
    }
    ss::future<std::expected<void, errc>> delete_objects(
      chunked_vector<l1::object_id> oids, ss::abort_source* as) override {
        return _delegate.delete_objects(std::move(oids), as);
    }
    ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, errc>>
    create_multipart_upload(
      l1::object_id oid, size_t part_size, ss::abort_source* as) override {
        return _delegate.create_multipart_upload(oid, part_size, as);
    }

private:
    l1::fake_io& _delegate;
    std::set<std::pair<size_t, size_t>> _footers;
    size_t _concurrent{0};
    size_t _peak{0};
};

pacer_config make_pacer_config() {
    return pacer_config{
      .min_window = 1_MiB,
      .max_window = 64_MiB,
      .min_chunk = 256_KiB,
      .max_chunk = 4_MiB,
      .safety = 1.5,
    };
}

class fetch_stream_test : public seastar_test {
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
    using tidp_batches_t = std::pair<
      model::topic_id_partition,
      chunked_circular_buffer<model::record_batch>>;

    std::pair<model::ntp, model::topic_id_partition>
    make_ntidp(std::string_view topic_name) {
        auto ntp = model::ntp{
          model::ns{"test_ns"},
          model::topic{topic_name},
          model::partition_id{0}};
        auto tidp = model::topic_id_partition{
          model::topic_id{uuid_t::create()}, model::partition_id{0}};
        return std::make_pair(ntp, tidp);
    }

    // Build a single L1 object from the given batches for one partition and
    // register it in the metastore + fake_io. Each call produces a separate
    // object, so calling it twice for the same tidp yields two objects.
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
    make_batches(model::offset start, int count, int records_per_batch = 1) {
        chunked_circular_buffer<model::record_batch> out;
        auto off = start;
        for (int i = 0; i < count; ++i) {
            auto b = model::test::make_random_batch(
              off, records_per_batch, false);
            off = model::next_offset(b.last_offset());
            out.push_back(std::move(b));
        }
        return out;
    }

    // Batches large enough (64 KiB record each) that every batch lands in its
    // own storage batch_cache range (range_size is 32 KiB). This places each
    // offset in a distinct reclaim range so eviction can be asserted
    // per-offset after a forced reclaim pass.
    static chunked_circular_buffer<model::record_batch>
    make_large_batches(model::offset start, int count) {
        chunked_circular_buffer<model::record_batch> out;
        auto off = start;
        for (int i = 0; i < count; ++i) {
            auto b = model::test::make_random_batch(
              model::test::record_batch_spec{
                .offset = off,
                .allow_compression = false,
                .count = 1,
                .record_sizes = std::vector<size_t>{64_KiB},
              });
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

    // Force the storage LRU reclaimer to drop everything it can for `tidp`.
    // Pinned ranges survive.
    void reclaim_all(const model::topic_id_partition& tidp) {
        cloud_topics::batch_cache_accessor::reclaim_all(*_cache, tidp);
    }

    ss::sstring _test_dir{test_env::random_dir_path("fetch_stream.", 10)};
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

// Cold produce: a single pass of produce fills the cache from start and
// produced_through advances batch by batch.
TEST_F_CORO(fetch_stream_test, cold_produce_fills_cache) {
    auto [ntp, tidp] = make_ntidp("t");
    auto batches = make_batches(model::offset{0}, 6);
    auto expected = copy(batches);
    co_await make_l1_object(tidp, std::move(batches));

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    ASSERT_EQ_CORO(stream.position(), kafka::offset{0});
    ASSERT_EQ_CORO(
      stream.produced_through(), kafka::prev_offset(kafka::offset{0}));

    // Drive the stream to completion. Use a small chunk so multiple
    // produce calls are needed within the run.
    for (int i = 0; i < 32 && stream.position() <= kafka::offset{5}; ++i) {
        co_await stream.produce(4_KiB, 1);
    }

    ASSERT_FALSE_CORO(stream.error());
    // All 6 batches produced: position past the last offset, produced_through
    // equal to the last offset.
    auto last = model::offset_cast(expected.back().last_offset());
    ASSERT_EQ_CORO(stream.produced_through(), last);
    ASSERT_EQ_CORO(stream.position(), kafka::next_offset(last));

    // The batches are now retrievable from the cache.
    for (auto& b : expected) {
        auto got = stream.cached_get(b.base_offset());
        ASSERT_TRUE_CORO(got.has_value());
        ASSERT_EQ_CORO(got->base_offset(), b.base_offset());
        ASSERT_EQ_CORO(got->last_offset(), b.last_offset());
    }

    co_await stream.close();
}

// An L1 object whose data has an internal offset gap (offsets 0,1,3,4 present;
// offset 2 absent because reconciliation removed an aborted transaction, or it
// is a compaction hole). produce() must ghost-fill the gap so the cache has
// contiguous coverage; the reader then skips the ghost instead of stalling on
// the missing offset. Mirrors the L1 tx-consume stall (a tx control batch at
// offset 0 left a leading gap).
TEST_F_CORO(fetch_stream_test, produce_ghost_fills_gap) {
    auto [ntp, tidp] = make_ntidp("gap");

    chunked_circular_buffer<model::record_batch> batches;
    for (int off : {0, 1, 3, 4}) {
        batches.push_back(
          model::test::make_random_batch(model::offset{off}, 1, false));
    }
    co_await make_l1_object(tidp, std::move(batches));

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    for (int i = 0; i < 32 && stream.position() <= kafka::offset{4}; ++i) {
        co_await stream.produce(4_MiB, 1);
    }
    ASSERT_FALSE_CORO(stream.error());

    // Real data present at the non-gap offsets.
    for (int off : {0, 1, 3, 4}) {
        ASSERT_TRUE_CORO(stream.cached_get(model::offset{off}).has_value());
    }
    // The hole at offset 2 is filled with a ghost batch so a reader can tell it
    // apart from an evicted offset.
    auto gap = stream.cached_get(model::offset{2});
    ASSERT_TRUE_CORO(gap.has_value());
    ASSERT_EQ_CORO(gap->header().type, model::record_batch_type::ghost_batch);

    co_await stream.close();
}

// Footer is fetched before any data chunk: the first produce issues the
// footer GET, and the first batch appears at/after that.
TEST_F_CORO(fetch_stream_test, footer_fetched_before_data) {
    auto [ntp, tidp] = make_ntidp("t");
    auto batches = make_batches(model::offset{0}, 4);
    auto expected = copy(batches);
    co_await make_l1_object(tidp, std::move(batches));

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    // A single produce with a chunk large enough to cover the whole run
    // fetches the footer, then dispatches and decodes all batches.
    co_await stream.produce(8_MiB, 1);

    ASSERT_FALSE_CORO(stream.error());
    ASSERT_EQ_CORO(meta.forwards_calls(), 1u);
    ASSERT_TRUE_CORO(
      stream.cached_get(expected.front().base_offset()).has_value());

    co_await stream.close();
}

// Consecutive produce calls within one object do not re-query the
// metastore: exactly one forwards call covers the whole object.
TEST_F_CORO(fetch_stream_test, stays_within_one_object) {
    auto [ntp, tidp] = make_ntidp("t");
    // 30 batches in one object so several small chunks are needed.
    auto batches = make_batches(model::offset{0}, 30);
    auto expected = copy(batches);
    co_await make_l1_object(tidp, std::move(batches));

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    auto last = model::offset_cast(expected.back().last_offset());
    int calls = 0;
    while (stream.position() <= last && calls < 200) {
        co_await stream.produce(1_KiB, 1);
        ++calls;
    }

    ASSERT_FALSE_CORO(stream.error());
    // Expect several produce calls to cover the object.
    ASSERT_GT_CORO(calls, 1);
    // The whole object was covered with a single metastore query.
    ASSERT_EQ_CORO(meta.forwards_calls(), 1u);
    ASSERT_EQ_CORO(stream.produced_through(), last);

    co_await stream.close();
}

// Reaching the end of one object advances to the next object via the
// lookahead, with one metastore query per object boundary crossing.
TEST_F_CORO(fetch_stream_test, advances_to_next_object) {
    auto [ntp, tidp] = make_ntidp("t");
    // Two separate objects: offsets [0,5] and [6,11].
    co_await make_l1_object(tidp, make_batches(model::offset{0}, 6));
    co_await make_l1_object(tidp, make_batches(model::offset{6}, 6));

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    int calls = 0;
    while (stream.position() <= kafka::offset{11} && calls < 200) {
        co_await stream.produce(8_MiB, 1);
        ++calls;
    }

    ASSERT_FALSE_CORO(stream.error());
    ASSERT_EQ_CORO(stream.produced_through(), kafka::offset{11});
    ASSERT_EQ_CORO(stream.position(), kafka::offset{12});
    // Data from both objects landed in the cache.
    ASSERT_TRUE_CORO(stream.cached_get(model::offset{0}).has_value());
    ASSERT_TRUE_CORO(stream.cached_get(model::offset{6}).has_value());
    ASSERT_TRUE_CORO(stream.cached_get(model::offset{11}).has_value());

    co_await stream.close();
}

// A terminal IO error on a data chunk is recorded and surfaced via error();
// the producer makes no further progress and does not hang.
TEST_F_CORO(fetch_stream_test, terminal_error_recorded) {
    auto [ntp, tidp] = make_ntidp("t");
    auto batches = make_batches(model::offset{0}, 6);
    co_await make_l1_object(tidp, std::move(batches));

    // Find the object's run position so we can fail the first data chunk.
    auto resp = (co_await _metastore.get_extent_metadata_forwards(
                   tidp,
                   kafka::offset{0},
                   kafka::offset::max(),
                   1,
                   l1::metastore::include_object_metadata::yes))
                  .value();
    auto& em = resp.extents.front();
    auto oid = em.object_info->oid;
    // Fetch the footer to learn the run's file position.
    chunk_downloader cd(
      &_io, oid, cloud_io::group_id::default_group, std::nullopt);
    auto footer = co_await cd.fetch_footer(
      em.object_info->footer_pos, em.object_info->object_size);
    auto seek = footer.file_position_before_kafka_offset(
      tidp, kafka::offset{0});
    co_await cd.close();

    fault_io flt(_io);
    // Fail the first data chunk dispatched from the run start. The stream
    // dispatches min(chunk_bytes, run_remaining) bytes, so match that.
    constexpr size_t chunk_bytes = 4_KiB;
    auto first_chunk = std::min<size_t>(chunk_bytes, seek.length);
    flt.fail_range(seek.file_position, first_chunk);

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &flt,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    // Drive a bounded number of produce calls; the error must surface and
    // no infinite loop occurs.
    for (int i = 0; i < 8; ++i) {
        co_await stream.produce(chunk_bytes, 1);
    }

    ASSERT_TRUE_CORO(stream.error());
    // produced_through never advanced because the first chunk failed.
    ASSERT_EQ_CORO(
      stream.produced_through(), kafka::prev_offset(kafka::offset{0}));

    co_await stream.close();
}

// Within-stream concurrency: a SINGLE stream issues multiple concurrent chunk
// GETs within one run. With a window budget covering several chunks and a
// max_concurrent > 1, the counting_io observes more than one overlapping data
// GET, while the produced batches stay ordered/contiguous in the cache.
TEST_F_CORO(fetch_stream_test, within_stream_concurrent_chunks) {
    auto [ntp, tidp] = make_ntidp("t");
    // Large batches so the run is well over 1 MiB — big enough to split into
    // multiple pacer-sized (256 KiB) chunks dispatched concurrently.
    auto batches = make_batches(model::offset{0}, 40, 256);
    auto expected = copy(batches);
    co_await make_l1_object(tidp, std::move(batches));

    // Discover the object's footer extent so the counting io can exclude it,
    // and confirm the run is large enough for several chunks.
    auto resp = (co_await _metastore.get_extent_metadata_forwards(
                   tidp,
                   kafka::offset{0},
                   kafka::offset::max(),
                   1,
                   l1::metastore::include_object_metadata::yes))
                  .value();
    auto& em = resp.extents.front();

    counting_io cio(_io);
    // The footer GET reads [footer_pos, object_size); exclude it from counting.
    cio.mark_footer(
      em.object_info->footer_pos,
      em.object_info->object_size - em.object_info->footer_pos);

    counting_metastore meta(&_metastore);
    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &meta,
      &cio,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    auto last = model::offset_cast(expected.back().last_offset());
    // A window budget that spans several pacer-sized (256 KiB) chunks so the
    // run splits, and allow up to 8 concurrent in-flight GETs for this single
    // stream.
    constexpr size_t window_budget = 4_MiB;
    constexpr size_t max_concurrent = 8;
    int calls = 0;
    while (stream.position() <= last && calls < 200) {
        co_await stream.produce(window_budget, max_concurrent);
        ++calls;
    }

    ASSERT_FALSE_CORO(stream.error());
    // A single stream issued more than one overlapping data GET.
    ASSERT_GT_CORO(cio.peak_concurrent(), 1u)
      << "a single stream must issue concurrent chunk GETs";
    // Only one metastore query for the single object.
    ASSERT_EQ_CORO(meta.forwards_calls(), 1u);
    // produced_through reached the last offset, contiguously.
    ASSERT_EQ_CORO(stream.produced_through(), last);
    ASSERT_EQ_CORO(stream.position(), kafka::next_offset(last));

    // Every batch landed in the cache, in order.
    for (auto& b : expected) {
        auto got = stream.cached_get(b.base_offset());
        ASSERT_TRUE_CORO(got.has_value())
          << "batch at " << b.base_offset() << " missing from cache";
        ASSERT_EQ_CORO(got->base_offset(), b.base_offset());
        ASSERT_EQ_CORO(got->last_offset(), b.last_offset());
    }

    co_await stream.close();
}

// Non-zero start: when the footer index seek lands before `start`, batches
// whose last_offset < start must be skipped and never appear in the cache.
// position() and produced_through() must never fall below `start`.
TEST_F_CORO(fetch_stream_test, nonzero_start_skips_pre_start_batches) {
    auto [ntp, tidp] = make_ntidp("t");
    // 6 single-record batches at offsets 0..5 in one L1 object.
    auto batches = make_batches(model::offset{0}, 6);
    auto expected = copy(batches);
    co_await make_l1_object(tidp, std::move(batches));

    // Start at offset 2 (the base of the 3rd batch).  The footer seek lands
    // at the start of the run (offset 0), so without the fix the stream would
    // cache offsets 0 and 1 and pull produced_through below start.
    constexpr kafka::offset start{2};

    fetch_stream stream(
      tidp,
      start,
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    ASSERT_EQ_CORO(stream.position(), start);
    ASSERT_EQ_CORO(stream.produced_through(), kafka::prev_offset(start));

    auto last_expected = model::offset_cast(expected.back().last_offset());
    for (int i = 0; i < 32 && stream.position() <= last_expected; ++i) {
        // position() must never drop below start between calls.
        ASSERT_GE_CORO(stream.position(), start);
        ASSERT_GE_CORO(stream.produced_through(), kafka::prev_offset(start));
        co_await stream.produce(4_KiB, 1);
    }

    ASSERT_FALSE_CORO(stream.error());

    // produced_through and position reflect only at/after-start batches.
    ASSERT_GE_CORO(stream.produced_through(), start);
    ASSERT_GE_CORO(stream.position(), kafka::next_offset(start));

    // Pre-start batches must NOT be in the cache.
    for (auto& b : expected) {
        auto base = b.base_offset();
        if (model::offset_cast(b.last_offset()) < start) {
            ASSERT_FALSE_CORO(stream.cached_get(base).has_value())
              << "pre-start batch at offset " << base << " must not be cached";
        }
    }

    // At/after-start batches MUST be in the cache.
    for (auto& b : expected) {
        if (model::offset_cast(b.base_offset()) >= start) {
            auto got = stream.cached_get(b.base_offset());
            ASSERT_TRUE_CORO(got.has_value())
              << "post-start batch at offset " << b.base_offset()
              << " must be cached";
        }
    }

    co_await stream.close();
}

// Prefetch pins produced-but-unconsumed batches so the shared cache's LRU
// reclaimer cannot drop them: an eviction would force the reader to cold-miss
// and a fresh stream to re-seek (re-downloading the coarse-index pre-roll),
// collapsing throughput. After the reader consumes through them (on_consumed ->
// unpin) they become evictable again.
TEST_F_CORO(fetch_stream_test, prefetch_is_pinned_until_consumed) {
    auto [ntp, tidp] = make_ntidp("t");
    // Large batches so each lands in its own reclaim range.
    co_await make_l1_object(tidp, make_large_batches(model::offset{0}, 4));

    fetch_stream stream(
      tidp,
      kafka::offset{0},
      &_metastore,
      &_io,
      _cache.get(),
      prefetch_pacer(make_pacer_config()));

    // Drive the producer to land all 4 batches in the cache.
    for (int i = 0; i < 32 && stream.produced_through() < kafka::offset{3};
         ++i) {
        co_await stream.produce(/*window_budget=*/8_MiB, /*max_concurrent=*/1);
    }
    ASSERT_GE_CORO(stream.produced_through(), kafka::offset{3});
    ASSERT_TRUE_CORO(stream.cached_get(model::offset{0}).has_value());
    ASSERT_GT_CORO(stream.pinned_offsets_for_test(), 0u);

    // Un-consumed prefetch is pinned: a reclaim pass must NOT drop it.
    stream.testing_reclaim_cache(std::numeric_limits<size_t>::max());
    ASSERT_TRUE_CORO(stream.cached_get(model::offset{0}).has_value())
      << "pinned prefetch must survive reclaim";
    ASSERT_TRUE_CORO(stream.cached_get(model::offset{3}).has_value());

    // Once the reader consumes through the last offset the pins are released,
    // and the batches become collectible by the reclaimer.
    stream.on_consumed(kafka::offset{3}, /*bytes=*/0, ss::lowres_clock::now());
    ASSERT_EQ_CORO(stream.pinned_offsets_for_test(), 0u);
    stream.testing_reclaim_cache(std::numeric_limits<size_t>::max());
    ASSERT_FALSE_CORO(stream.cached_get(model::offset{0}).has_value());
    ASSERT_FALSE_CORO(stream.cached_get(model::offset{3}).has_value());

    co_await stream.close();
}

} // namespace cloud_topics::prefetch
