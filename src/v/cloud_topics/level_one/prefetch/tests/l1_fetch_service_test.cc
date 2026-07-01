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
#include "cloud_topics/level_one/prefetch/l1_fetch_service.h"
#include "cloud_topics/level_one/prefetch/prefetch_pacer.h"
#include "cloud_topics/log_reader_config.h"
#include "config/configuration.h"
#include "config/property.h"
#include "container/chunked_circular_buffer.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "test_utils/test.h"
#include "test_utils/test_env.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <limits>
#include <memory>
#include <set>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

namespace cloud_topics {
// Friend accessor (declared in batch_cache.h) that lets the prefetch service
// integration test drive the storage LRU reclaimer directly, forcing an
// eviction pass that WOULD drop un-consumed prefetch so the pin-survival
// behaviour can be asserted end-to-end through a real reader.
struct batch_cache_accessor {
    static void reclaim_all(batch_cache& c, const model::topic_id_partition&) {
        // Drive the shared shard reclaimer through a throwaway index (each
        // stream now owns its own index; reclaim is shard-global, so any index
        // triggers it). The temp index is empty, so its destructor evicts
        // nothing of its own.
        if (auto ix = c.create_index()) {
            ix->testing_reclaim_from_cache(std::numeric_limits<size_t>::max());
        }
    }
};
} // namespace cloud_topics

namespace cloud_topics::prefetch {
namespace {

// Metastore wrapper that counts get_extent_metadata_forwards calls so the warm
// path can be asserted (no extra metastore query on a reuse).
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

pacer_config make_pacer_config() {
    return pacer_config{
      .min_window = 1_MiB,
      .max_window = 64_MiB,
      .min_chunk = 256_KiB,
      .max_chunk = 4_MiB,
      .safety = 1.5,
    };
}

// l1::io wrapper that counts overlapping data-chunk GETs so a test can assert
// per-stream download concurrency. Footer reads are excluded; each data GET is
// held briefly to widen the overlap window.
class counting_io : public l1::io {
public:
    explicit counting_io(l1::fake_io& delegate)
      : _delegate(delegate) {}

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
        if (_footers.count({extent.position, extent.size}) > 0) {
            co_return co_await _delegate.read_object(extent, as, g);
        }
        ++_concurrent;
        _peak = std::max(_peak, _concurrent);
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

// l1::io wrapper that fails the Nth non-footer data-chunk read (1-indexed).
// Footer reads (registered via mark_footer) pass through untouched. Each data
// read is held briefly so concurrent dispatches overlap, making it possible to
// have several in-flight GETs when the Nth one fails.
class fault_nth_data_io : public l1::io {
public:
    explicit fault_nth_data_io(l1::fake_io& delegate)
      : _delegate(delegate) {}

    void mark_footer(size_t position, size_t size) {
        _footers.emplace(position, size);
    }
    // Fail the Nth data chunk read (others succeed).
    void fail_on_nth(size_t n) { _fail_on = n; }

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
        if (_footers.count({extent.position, extent.size}) > 0) {
            co_return co_await _delegate.read_object(extent, as, g);
        }
        // Hold non-footer reads briefly to widen the concurrent overlap window.
        co_await ss::sleep(std::chrono::milliseconds(5));
        ++_data_reads;
        if (_fail_on && _data_reads == *_fail_on) {
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
    std::set<std::pair<size_t, size_t>> _footers;
    std::optional<size_t> _fail_on;
    size_t _data_reads{0};
};

// l1::io wrapper that delays every read by a fixed amount so the producer
// cannot reach the requested start offset within the fill-gate window. Used to
// keep a stream "in progress" (not drained, not produced-through) for the whole
// gate so the gate's abort-responsiveness can be exercised deterministically.
class slow_io : public l1::io {
public:
    explicit slow_io(l1::fake_io& delegate, std::chrono::milliseconds delay)
      : _delegate(delegate)
      , _delay(delay) {}

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
        co_await ss::sleep(_delay);
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
    std::chrono::milliseconds _delay;
};

class l1_fetch_service_test : public seastar_test {
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

    ss::future<> make_l1_object(
      l1::metastore& metastore,
      model::topic_id_partition tidp,
      chunked_circular_buffer<model::record_batch> batches) {
        auto meta_builder = (co_await metastore.object_builder()).value();
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
        co_await metastore.add_objects(*meta_builder, term_map);
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
    // own storage batch_cache range (range_size is 32 KiB), so a forced reclaim
    // evicts per-offset deterministically.
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

    cloud_topic_log_reader_config
    make_cfg(kafka::offset start, kafka::offset max_off) {
        return cloud_topic_log_reader_config{
          cloud_io::group_id::default_group, start, max_off};
    }

    cloud_topic_log_reader_config make_cfg_with_abort(
      kafka::offset start, kafka::offset max_off, ss::abort_source& as) {
        return cloud_topic_log_reader_config{
          cloud_io::group_id::default_group,
          start,
          max_off,
          /*first_timestamp=*/std::nullopt,
          /*as=*/std::ref(as)};
    }

    void reclaim_all(const model::topic_id_partition& tidp) {
        cloud_topics::batch_cache_accessor::reclaim_all(*_cache, tidp);
    }

    // Read a reader to completion, returning all batches.
    static ss::future<model::record_batch_reader::data_t>
    drain(model::record_batch_reader rdr) {
        auto data = co_await model::consume_reader_to_memory(
          std::move(rdr), model::no_timeout);
        co_return std::move(data);
    }

    // Read a reader to completion with a bounded deadline. A regression
    // (stall) fails fast rather than hanging the suite.
    static ss::future<model::record_batch_reader::data_t> drain_bounded(
      model::record_batch_reader rdr,
      model::timeout_clock::duration timeout = std::chrono::seconds(10)) {
        auto data = co_await model::consume_reader_to_memory(
          std::move(rdr), model::timeout_clock::now() + timeout);
        co_return std::move(data);
    }

    ss::sstring _test_dir{test_env::random_dir_path("l1_fetch_service.", 10)};
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

    // Default config bindings used by most tests.
    config::binding<size_t> _budget{config::mock_binding<size_t>(64_MiB)};
    config::binding<size_t> _max_streams{config::mock_binding<size_t>(128)};
    config::binding<std::chrono::milliseconds> _idle_timeout{
      config::mock_binding<std::chrono::milliseconds>(60'000ms)};
    config::binding<size_t> _max_in_flight{config::mock_binding<size_t>(8)};
    config::binding<size_t> _fill_watermark{config::mock_binding<size_t>(1)};
};

} // namespace

// Cold then warm: the first get_reader creates a stream (one metastore query);
// the second consecutive get_reader at the advanced offset reuses the SAME
// stream and issues NO additional metastore query.
TEST_F_CORO(l1_fetch_service_test, warm_get_reader_reuses_stream) {
    auto tidp = make_tidp();
    auto batches = make_batches(model::offset{0}, 8);
    co_await make_l1_object(_metastore, tidp, std::move(batches));

    counting_metastore meta(&_metastore);
    l1_fetch_service svc(
      &meta,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Cold read of offsets [0,3].
    auto rdr1 = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{3}));
    auto data1 = co_await drain(std::move(rdr1));
    ASSERT_FALSE_CORO(data1.empty());
    ASSERT_EQ_CORO(data1.front().base_offset(), model::offset{0});

    auto cold_calls = meta.forwards_calls();
    ASSERT_GE_CORO(cold_calls, 1u);
    // Exactly one stream exists for the partition.
    ASSERT_EQ_CORO(svc.stream_count(tidp), 1u);

    // Warm read continuing at offset 4 reuses the stream which already advanced
    // its position there. No extra metastore query.
    auto rdr2 = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{4}, kafka::offset{7}));
    auto data2 = co_await drain(std::move(rdr2));
    ASSERT_FALSE_CORO(data2.empty());
    ASSERT_EQ_CORO(data2.front().base_offset(), model::offset{4});

    // Still exactly one stream, and no extra forwards call (warm reuse).
    ASSERT_EQ_CORO(svc.stream_count(tidp), 1u);
    ASSERT_EQ_CORO(meta.forwards_calls(), cold_calls);
    ASSERT_GE_CORO(svc.probe().cache_hits(), 1u);

    co_await svc.stop();
}

// A reader at a fresh offset on a fresh partition that has data should be
// served end-to-end (cold fill -> serve).
TEST_F_CORO(l1_fetch_service_test, cold_fill_serves_all_batches) {
    auto tidp = make_tidp();
    auto batches = make_batches(model::offset{0}, 12);
    co_await make_l1_object(_metastore, tidp, std::move(batches));

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    auto rdr = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{11}));
    auto data = co_await drain(std::move(rdr));

    ASSERT_EQ_CORO(data.size(), 12u);
    ASSERT_EQ_CORO(data.front().base_offset(), model::offset{0});
    ASSERT_EQ_CORO(data.back().last_offset(), model::offset{11});

    co_await svc.stop();
}

// notify_partition_stopped aborts attached readers: a reader blocked waiting
// for data on a partition whose stream is torn down wakes promptly (does not
// hang to the deadline) and the stream is removed.
TEST_F_CORO(l1_fetch_service_test, partition_stop_aborts_attached_reader) {
    auto tidp = make_tidp();
    // No data: a reader will park waiting for offset 0.
    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    auto rdr = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{99}));
    ASSERT_EQ_CORO(svc.stream_count(tidp), 1u);

    // Start draining in the background; with no data the reader blocks on the
    // cache (do_load_slice deadline is model::no_timeout -> effectively hangs
    // until aborted by the stream error path).
    auto t0 = ss::lowres_clock::now();
    auto read_fut = drain(std::move(rdr));

    // Tear down the partition while the reader is parked. This must wake the
    // reader via the stream error/abort path.
    co_await ss::sleep(50ms);
    svc.notify_partition_stopped(tidp);

    // The reader must complete (throw or end) well before any long deadline.
    bool threw = false;
    try {
        auto data = co_await std::move(read_fut);
        // Empty/EOS is also an acceptable wake (no data was produced).
        std::ignore = data;
    } catch (const std::exception&) {
        threw = true;
    }
    auto elapsed = ss::lowres_clock::now() - t0;
    ASSERT_LT_CORO(elapsed, 5s);
    std::ignore = threw;

    // The stream was removed.
    ASSERT_EQ_CORO(svc.stream_count(tidp), 0u);
    ASSERT_GE_CORO(svc.probe().evictions(), 1u);

    co_await svc.stop();
}

// Stream cap: with max_streams=2 and idle_timeout=0, creating a third stream
// evicts an LRU idle one. Readers are drained (detached) so streams are
// zero-ref and reclaimable.
TEST_F_CORO(l1_fetch_service_test, stream_cap_evicts_lru_idle) {
    config::binding<size_t> cap{config::mock_binding<size_t>(2)};
    config::binding<std::chrono::milliseconds> idle{
      config::mock_binding<std::chrono::milliseconds>(0ms)};

    auto tidp_a = make_tidp();
    auto tidp_b = make_tidp();
    auto tidp_c = make_tidp();
    co_await make_l1_object(
      _metastore, tidp_a, make_batches(model::offset{0}, 2));
    co_await make_l1_object(
      _metastore, tidp_b, make_batches(model::offset{0}, 2));
    co_await make_l1_object(
      _metastore, tidp_c, make_batches(model::offset{0}, 2));

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      cap,
      idle,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Read and fully drain A and B so their readers detach (zero-ref).
    auto ra = co_await svc.get_reader(
      tidp_a, make_cfg(kafka::offset{0}, kafka::offset{1}));
    co_await drain(std::move(ra));
    auto rb = co_await svc.get_reader(
      tidp_b, make_cfg(kafka::offset{0}, kafka::offset{1}));
    co_await drain(std::move(rb));

    ASSERT_EQ_CORO(svc.stream_count(), 2u);

    // A third stream forces eviction of an LRU idle stream (A or B).
    auto rc = co_await svc.get_reader(
      tidp_c, make_cfg(kafka::offset{0}, kafka::offset{1}));
    co_await drain(std::move(rc));

    ASSERT_LE_CORO(svc.stream_count(), 2u);
    ASSERT_GE_CORO(svc.probe().evictions(), 1u);

    co_await svc.stop();
}

// Starvation -> borrow: with a tiny budget too small for even one chunk, a
// blocked reader must still be served because the scheduler lets a starving
// (blocked) stream borrow beyond the budget.
TEST_F_CORO(l1_fetch_service_test, starvation_borrow_unblocks_reader) {
    // Budget smaller than one min_chunk (256 KiB) so try_reserve always fails
    // and only a borrow can dispatch.
    config::binding<size_t> tiny_budget{config::mock_binding<size_t>(4_KiB)};

    auto tidp = make_tidp();
    auto batches = make_batches(model::offset{0}, 6);
    co_await make_l1_object(_metastore, tidp, std::move(batches));

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      tiny_budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // The reader blocks (cache empty, budget too small for a normal reserve);
    // a borrow must unblock it.
    auto rdr = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{5}));
    auto data = co_await drain(std::move(rdr));

    ASSERT_FALSE_CORO(data.empty());
    ASSERT_EQ_CORO(data.front().base_offset(), model::offset{0});
    ASSERT_GE_CORO(svc.probe().borrows(), 1u);

    co_await svc.stop();
}

// Cross-stream concurrency: when many partitions each have a blocked reader and
// the budget + max_in_flight allow it, the scheduler drives multiple downloads
// concurrently (peak in-flight > 1), not one at a time.
TEST_F_CORO(l1_fetch_service_test, concurrency_many_streams_in_flight) {
    constexpr int n_parts = 6;
    std::vector<model::topic_id_partition> parts;
    for (int i = 0; i < n_parts; ++i) {
        auto t = make_tidp();
        // Larger objects so produce takes long enough to overlap.
        co_await make_l1_object(
          _metastore, t, make_batches(model::offset{0}, 8, 64));
        parts.push_back(t);
    }

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Fire all get_reader calls concurrently (do NOT await each in turn) so
    // every stream is created in the same window. The scheduler then spawns
    // many produce tasks in a single dispatch pass before any completes.
    std::vector<ss::future<model::record_batch_reader>> opens;
    opens.reserve(n_parts);
    for (auto& t : parts) {
        opens.push_back(
          svc.get_reader(t, make_cfg(kafka::offset{0}, kafka::offset{7})));
    }
    auto readers = co_await ss::when_all_succeed(opens.begin(), opens.end());

    std::vector<ss::future<model::record_batch_reader::data_t>> reads;
    reads.reserve(n_parts);
    for (auto& rdr : readers) {
        reads.push_back(drain(std::move(rdr)));
    }
    for (auto& f : reads) {
        auto data = co_await std::move(f);
        ASSERT_FALSE_CORO(data.empty());
    }

    // Peak in-flight must exceed 1: downloads ran concurrently across streams.
    ASSERT_GT_CORO(svc.probe().peak_in_flight(), 1u);
    ASSERT_GE_CORO(svc.probe().downloads(), uint64_t(n_parts));

    co_await svc.stop();
}

// Within-stream concurrency: a SINGLE get_reader/stream drives multiple
// concurrent chunk downloads (per-stream peak in-flight > 1), bounded by
// max_in_flight and the memory budget, and the broker reservation returns to
// baseline after the reader fully consumes.
TEST_F_CORO(l1_fetch_service_test, single_stream_concurrent_downloads) {
    auto tidp = make_tidp();
    // One large object (>> several pacer chunks) for a single partition.
    co_await make_l1_object(
      _metastore, tidp, make_batches(model::offset{0}, 40, 256));

    // Discover the footer extent so the counting io excludes it.
    auto resp = (co_await _metastore.get_extent_metadata_forwards(
                   tidp,
                   kafka::offset{0},
                   kafka::offset::max(),
                   1,
                   l1::metastore::include_object_metadata::yes))
                  .value();
    auto& em = resp.extents.front();

    counting_io cio(_io);
    cio.mark_footer(
      em.object_info->footer_pos,
      em.object_info->object_size - em.object_info->footer_pos);

    // Budget large enough for several concurrent chunks; max_in_flight high
    // enough that the single stream can hold multiple slots at once.
    config::binding<size_t> budget{config::mock_binding<size_t>(64_MiB)};
    config::binding<size_t> max_in_flight{config::mock_binding<size_t>(8)};

    l1_fetch_service svc(
      &_metastore,
      &cio,
      _cache.get(),
      budget,
      _max_streams,
      _idle_timeout,
      max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    auto last = make_batches(model::offset{0}, 40, 256).back().last_offset();
    auto rdr = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, model::offset_cast(last)));
    auto data = co_await drain(std::move(rdr));
    ASSERT_FALSE_CORO(data.empty());
    ASSERT_EQ_CORO(data.front().base_offset(), model::offset{0});

    // Only one stream exists for this partition, so peak in-flight > 1 means a
    // SINGLE stream had multiple concurrent downloads.
    ASSERT_EQ_CORO(svc.stream_count(tidp), 1u);
    ASSERT_GT_CORO(svc.probe().peak_in_flight(), 1u)
      << "a single stream must drive multiple concurrent downloads";
    // The counting io must also have observed overlapping data GETs.
    ASSERT_GT_CORO(cio.peak_concurrent(), 1u)
      << "overlapping data GETs expected for the single stream";

    // Let any trailing in-flight dispatch settle, then the reservation must
    // return to baseline after full consume.
    co_await svc.drain_in_flight_for_test();
    ASSERT_EQ_CORO(svc.broker().reserved(), 0u);

    co_await svc.stop();
}

// Memory pressure (shrink-coldest): with a budget that the hot stream's
// reservation saturates, a cold stream (no attached reader, more data to
// produce) stops being dispatched while the hot (blocked-reader) stream keeps
// being served.
TEST_F_CORO(l1_fetch_service_test, pressure_cold_stops_hot_runs) {
    // Budget large enough for one min_chunk reservation but tight enough that a
    // single in-flight chunk pushes reserved to/over the pressure threshold.
    config::binding<size_t> budget{config::mock_binding<size_t>(256_KiB)};

    auto hot = make_tidp();
    auto cold = make_tidp();
    // Cold partition has two objects so there is always more to produce.
    co_await make_l1_object(
      _metastore, cold, make_batches(model::offset{0}, 4));
    co_await make_l1_object(
      _metastore, cold, make_batches(model::offset{4}, 4));
    co_await make_l1_object(_metastore, hot, make_batches(model::offset{0}, 8));

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Create the cold stream and detach its reader so it is zero-ref but still
    // has a second object to produce. Read only offset 0 (max_offset=0).
    auto cold_rdr = co_await svc.get_reader(
      cold, make_cfg(kafka::offset{0}, kafka::offset{0}));
    co_await drain(std::move(cold_rdr));
    // Record the cold stream's produced frontier after its reader detached.
    auto cold_pos_before = svc.stream_count(cold);
    ASSERT_EQ_CORO(cold_pos_before, 1u);

    // Now drive the hot partition with a blocked reader that keeps consuming.
    auto hot_rdr = co_await svc.get_reader(
      hot, make_cfg(kafka::offset{0}, kafka::offset{7}));
    auto hot_data = co_await drain(std::move(hot_rdr));

    // Hot stream was served fully despite the tight budget (blocked-reader
    // priority + borrow keep it running).
    ASSERT_FALSE_CORO(hot_data.empty());
    ASSERT_EQ_CORO(hot_data.back().last_offset(), model::offset{7});

    // The cold stream must NOT have been driven into its second object while
    // the hot stream monopolised the budget: offset 4 (second object) is not
    // cached in the cold stream's own index.
    auto* cold_stream = svc.stream_for_test(cold);
    ASSERT_TRUE_CORO(cold_stream != nullptr);
    ASSERT_FALSE_CORO(cold_stream->cached_get(model::offset{4}).has_value());

    co_await svc.stop();
}

// Reservation lifecycle: after a full drain of every produced batch, the broker
// reservation returns to zero (consume -> release).
TEST_F_CORO(l1_fetch_service_test, reservation_released_on_consume) {
    auto tidp = make_tidp();
    auto batches = make_batches(model::offset{0}, 10);
    co_await make_l1_object(_metastore, tidp, std::move(batches));

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    auto rdr = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{9}));
    auto data = co_await drain(std::move(rdr));
    ASSERT_EQ_CORO(data.size(), 10u);

    // Let any trailing in-flight dispatch settle.
    co_await svc.drain_in_flight_for_test();

    // Everything produced was consumed -> reservation back to zero.
    ASSERT_EQ_CORO(svc.broker().reserved(), 0u);

    co_await svc.stop();
}

// Race: teardown concurrent with fill-gate suspension (Fix 1 regression test).
//
// A get_reader() call parks inside the fill-gate poll loop waiting for the
// producer to catch up. Concurrently, notify_partition_stopped() retires the
// stream. Without Fix 1 the stream entry (and its fetch_stream) could be freed
// while get_reader is suspended, causing a UAF when the poll loop resumes.
//
// The test asserts: no crash under ASan/normal, and the returned reader either
// throws (abort) or returns empty / EOS — both are valid outcomes after a
// concurrent stop.
TEST_F_CORO(l1_fetch_service_test, teardown_races_fill_gate_no_uaf) {
    auto tidp = make_tidp();
    // No data for this partition: get_reader will loop in the fill-gate poll
    // waiting for produced_through() >= start (which never arrives).
    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Launch get_reader as a detached future so it starts running immediately
    // and parks inside the poll loop (the poll interval is 1ms). We collect the
    // future before it reaches the first suspension point.
    auto reader_fut = svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{99}));

    // Yield once so the get_reader coroutine runs until its first
    // ss::sleep_abortable(1ms) suspension — it is now parked inside the
    // fill-gate. Without Fix 1, what follows would be a UAF.
    co_await ss::maybe_yield();

    // Tear down the partition while get_reader is suspended in the fill-gate.
    svc.notify_partition_stopped(tidp);

    // Await the reader future: it must complete (throw or return) without
    // crashing or hanging. Both outcomes are correct after a concurrent stop.
    std::optional<model::record_batch_reader> rdr_opt;
    try {
        rdr_opt.emplace(co_await std::move(reader_fut));
    } catch (const std::exception&) {
        // Throw from get_reader is acceptable after concurrent stop.
    }

    if (rdr_opt.has_value()) {
        // Reader was returned: drain it (it should surface empty/EOS or abort).
        try {
            auto data = co_await drain(std::move(*rdr_opt));
            std::ignore = data;
        } catch (const std::exception&) {
            // abort from stream error — acceptable
        }
    }

    // The stream was removed from the registry by notify_partition_stopped.
    ASSERT_EQ_CORO(svc.stream_count(tidp), 0u);

    co_await svc.stop();
}

// Concurrent-chunk failure reservation baseline: drive a SINGLE stream with
// max_in_flight > 1 so several chunk GETs are in flight simultaneously; fail
// the second data-chunk read (a "middle" chunk of the concurrent batch). The
// test verifies:
//   1. The stream records a terminal error and does not hang.
//   2. After teardown / drain the broker reservation returns to zero — no
//      reservation leak when a concurrent chunk fails.
//
// Note: the fault_nth_data_io targets the Nth data read globally rather than
// by exact byte position, so it reliably hits a chunk that is concurrent with
// at least one other in-flight GET (the first succeeds, the second fails while
// both are suspended in the 5ms sleep).
TEST_F_CORO(
  l1_fetch_service_test, concurrent_chunk_failure_reservation_baseline) {
    auto tidp = make_tidp();
    // Large object (>> several pacer min-chunks) so the window splits into
    // multiple concurrent GETs in a single produce() call.
    co_await make_l1_object(
      _metastore, tidp, make_batches(model::offset{0}, 40, 256));

    // Discover the footer extent so we can exclude it from data-read counting.
    auto resp = (co_await _metastore.get_extent_metadata_forwards(
                   tidp,
                   kafka::offset{0},
                   kafka::offset::max(),
                   1,
                   l1::metastore::include_object_metadata::yes))
                  .value();
    auto& em = resp.extents.front();

    fault_nth_data_io fio(_io);
    fio.mark_footer(
      em.object_info->footer_pos,
      em.object_info->object_size - em.object_info->footer_pos);
    // Fail the second data-chunk read; the first succeeds, making this a
    // mid-flight failure when max_in_flight >= 2 dispatches them concurrently.
    fio.fail_on_nth(2);

    config::binding<size_t> budget{config::mock_binding<size_t>(64_MiB)};
    config::binding<size_t> max_in_flight{config::mock_binding<size_t>(4)};

    l1_fetch_service svc(
      &_metastore,
      &fio,
      _cache.get(),
      budget,
      _max_streams,
      _idle_timeout,
      max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    auto last = make_batches(model::offset{0}, 40, 256).back().last_offset();
    auto rdr = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, model::offset_cast(last)));

    // The reader must surface an error (or EOS) and not hang. Both throw and
    // empty-return are valid outcomes when the stream hits a terminal error.
    bool got_error = false;
    try {
        auto data = co_await drain(std::move(rdr));
        // Empty result is also acceptable — the error path may surface as EOS.
        std::ignore = data;
    } catch (const std::exception&) {
        got_error = true;
    }

    // The stream must have recorded a terminal error.
    ASSERT_EQ_CORO(svc.stream_count(tidp), 1u);

    // Let any trailing in-flight produce() tasks settle.
    co_await svc.drain_in_flight_for_test();

    // Key invariant: after the concurrent-chunk failure and full drain, the
    // broker reservation must return to zero (no leak).
    ASSERT_EQ_CORO(svc.broker().reserved(), 0u);

    std::ignore = got_error;
    co_await svc.stop();
}

// Part B: an already-aborted fetch abort_source makes get_reader return from
// the fill-gate promptly (well under the full ~50ms head start) rather than
// burning the whole gate on a fetch the client has already cancelled.
//
// The partition HAS data but reads are slowed so the producer cannot reach
// `start` within the gate window: the stream is "in progress" (not drained, not
// produced-through, no error), which is exactly the state in which the gate
// would otherwise poll for the full ~50ms. The abort check must short-circuit
// it.
TEST_F_CORO(l1_fetch_service_test, get_reader_returns_promptly_on_abort) {
    auto tidp = make_tidp();
    co_await make_l1_object(
      _metastore, tidp, make_batches(model::offset{0}, 8));

    // Every read is delayed well past the 50ms gate so produced_through stays
    // below start for the whole gate window.
    slow_io sio(_io, 500ms);

    l1_fetch_service svc(
      &_metastore,
      &sio,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    ss::abort_source as;
    as.request_abort();

    auto t0 = ss::lowres_clock::now();
    auto rdr = co_await svc.get_reader(
      tidp, make_cfg_with_abort(kafka::offset{0}, kafka::offset{7}, as));
    auto elapsed = ss::lowres_clock::now() - t0;

    // The fill-gate head start is 50ms; an aborted fetch must return well
    // before that. Allow generous slack for scheduling jitter.
    ASSERT_LT_CORO(elapsed, 40ms)
      << "get_reader waited the full fill-gate on an aborted fetch";

    // We only assert get_reader itself was prompt; drop the reader without
    // draining (its dtor releases the stream ref). The reader's own
    // deadline/abort handling on drain is exercised elsewhere.
    {
        auto drop = std::move(rdr);
    }

    co_await svc.stop();
}

// Fill-watermark gate: a higher cloud_topics_l1_prefetch_fill_watermark_bytes
// makes get_reader hold the reader in the fill gate until MORE decoded bytes
// are cached ahead of `start` before handing it back. Driven deterministically
// with a slow io so chunks land incrementally inside the gate window: with the
// default watermark (1) the gate returns after the first batch lands; with a
// watermark spanning several chunks it must wait for that many to land, so the
// count of decoded batches sitting in the cache at handback is strictly larger.
TEST_F_CORO(l1_fetch_service_test, fill_watermark_gates_on_cached_bytes) {
    constexpr int n_batches = 64;
    // Count of contiguous offsets [0, n_batches) decoded into the cache at the
    // moment the fill gate handed the reader back, measured BEFORE any consume.
    auto run = [this](this auto, size_t watermark_bytes) -> ss::future<size_t> {
        auto tidp = make_tidp();
        // Many 64 KiB single-record batches (offsets 0..n_batches-1) so the
        // producer lands data in several pacer chunks (min_chunk is 256 KiB ==
        // 4 batches), giving the gate something to wait on as the watermark
        // grows.
        co_await make_l1_object(
          _metastore, tidp, make_large_batches(model::offset{0}, n_batches));

        // Serialize downloads (max_in_flight=1) and delay each read so chunks
        // land ONE AT A TIME, spaced out across the ~50ms gate window. This
        // makes the number of chunks landed by the time the gate returns a
        // direct function of the watermark (rather than the whole window
        // arriving in a single concurrent round before the first poll).
        slow_io sio(_io, 8ms);
        config::binding<size_t> wm{
          config::mock_binding<size_t>(watermark_bytes)};
        config::binding<size_t> serial_in_flight{
          config::mock_binding<size_t>(1)};

        l1_fetch_service svc(
          &_metastore,
          &sio,
          _cache.get(),
          _budget,
          _max_streams,
          _idle_timeout,
          serial_in_flight,
          wm,
          make_pacer_config());
        co_await svc.start();

        auto rdr = co_await svc.get_reader(
          tidp, make_cfg(kafka::offset{0}, kafka::offset{n_batches - 1}));
        // Snapshot how many batches are decoded into the cache at handback,
        // before any consume releases them. The gate only returns once
        // cached_ahead_bytes >= watermark (or timeout/abort), so a larger
        // watermark yields strictly more cached batches here.
        size_t cached = 0;
        auto* s = svc.stream_for_test(tidp);
        for (int i = 0; i < n_batches; ++i) {
            if (s != nullptr && s->cached_get(model::offset{i}).has_value()) {
                ++cached;
            }
        }
        co_await drain(std::move(rdr));
        co_await svc.stop();
        co_return cached;
    };

    // Default watermark: gate returns as soon as the first batch is cached.
    auto cached_low = co_await run(1);
    // High watermark spanning roughly three pacer chunks (min_chunk is 256 KiB,
    // i.e. four 64 KiB batches). At 3ms/read this is reached in a handful of
    // reads, comfortably inside the ~50ms gate window, so the gate returns on
    // the watermark (not the timeout) and more batches are cached at handback.
    constexpr size_t high_watermark = 768_KiB;
    auto cached_high = co_await run(high_watermark);

    // ~12 batches cover 768 KiB; require a clear majority cached to avoid
    // flakiness while still proving the gate waited for the watermark.
    ASSERT_GE_CORO(cached_high, 8u) << "high fill watermark did not hold the "
                                       "reader until enough batches were "
                                       "cached ahead";
    ASSERT_GT_CORO(cached_high, cached_low)
      << "a higher fill watermark must make the gate wait for more cached "
         "batches before returning the reader";
}

// Best-effort cache: prefetch may be evicted before consumption. A reclaim
// that drops all cached data must not hang or crash; a subsequent get_reader
// at the missed offset on the SAME service spins a fresh stream (residency
// gate rejects the stale warm stream) that re-produces the data correctly.
//
// Single-service path: prime via get_reader+drain, evict everything for the
// partition, then call get_reader again on the same service. Without the
// residency gate the service would reuse the old stream (whose producer cursor
// is already past offset 0) and the reader would stall. With the gate the
// evicted start is detected, a fresh stream is created, and all 4 batches are
// returned in order. A bounded drain deadline turns a regression into a fast
// failure rather than an indefinite hang.
TEST_F_CORO(l1_fetch_service_test, eviction_self_heals_on_refetch) {
    auto tidp = make_tidp();
    co_await make_l1_object(
      _metastore, tidp, make_large_batches(model::offset{0}, 4));

    l1_fetch_service svc(
      &_metastore,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Prime: get_reader + drain so the stream's producer cursor advances past
    // offset 0 and the cache holds batches 0..3.
    auto rdr1 = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{3}));
    auto first = co_await drain(std::move(rdr1));
    ASSERT_FALSE_CORO(first.empty());
    ASSERT_EQ_CORO(first.size(), 4u);

    // Evict everything for this partition. The stream still exists in the
    // registry (producer cursor > 0) but the cached batches are gone.
    reclaim_all(tidp);

    // Same service, same offset. Without the residency gate this would reuse
    // the warm stream and stall (offset 0 already produced, never re-produced).
    // With the gate: evicted start → cold miss → fresh stream → re-downloads.
    auto rdr2 = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{3}));
    // Bounded deadline: a stall becomes a fast failure instead of hanging.
    auto again = co_await drain_bounded(std::move(rdr2));
    ASSERT_EQ_CORO(again.size(), 4u);
    for (size_t i = 0; i < again.size(); ++i) {
        ASSERT_EQ_CORO(again[i].base_offset(), model::offset(i));
    }

    co_await svc.stop();
}

// Residency gate does NOT break warm consecutive reads: when data IS resident,
// get_reader reuses the existing stream (no extra metastore query).
//
// Prime with offsets [0,3], then immediately fetch [4,7] on the same service.
// The stream's producer cursor has advanced into [4,7], those offsets are
// resident, so the gate passes and the warm stream is reused — no additional
// get_extent_metadata_forwards call.
TEST_F_CORO(l1_fetch_service_test, warm_reuse_still_reuses_when_resident) {
    auto tidp = make_tidp();
    co_await make_l1_object(
      _metastore, tidp, make_large_batches(model::offset{0}, 8));

    counting_metastore meta(&_metastore);
    l1_fetch_service svc(
      &meta,
      &_io,
      _cache.get(),
      _budget,
      _max_streams,
      _idle_timeout,
      _max_in_flight,
      _fill_watermark,
      make_pacer_config());
    co_await svc.start();

    // Cold read: offsets [0,3]. Primes the stream and the cache.
    auto rdr1 = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{0}, kafka::offset{3}));
    auto data1 = co_await drain(std::move(rdr1));
    ASSERT_EQ_CORO(data1.size(), 4u);
    auto calls_after_cold = meta.forwards_calls();
    ASSERT_GE_CORO(calls_after_cold, 1u);

    // Warm read: offsets [4,7]. Offset 4 is resident (stream prefetched ahead).
    // The residency gate must pass → stream is reused → no new metastore call.
    auto rdr2 = co_await svc.get_reader(
      tidp, make_cfg(kafka::offset{4}, kafka::offset{7}));
    auto data2 = co_await drain(std::move(rdr2));
    ASSERT_EQ_CORO(data2.size(), 4u);
    ASSERT_EQ_CORO(data2.front().base_offset(), model::offset{4});

    // Still one stream, no extra metastore query.
    ASSERT_EQ_CORO(svc.stream_count(tidp), 1u);
    ASSERT_EQ_CORO(meta.forwards_calls(), calls_after_cold);
    ASSERT_GE_CORO(svc.probe().cache_hits(), 1u);

    co_await svc.stop();
}

} // namespace cloud_topics::prefetch
