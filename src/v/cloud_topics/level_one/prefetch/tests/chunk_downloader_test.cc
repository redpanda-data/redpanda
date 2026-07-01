/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/prefetch/chunk_downloader.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <set>

namespace cloud_topics::prefetch {
namespace {

// Build a L1 object from a set of batches and return the object buffer plus
// the footer metadata needed to locate the partition run within it.
struct built_object {
    iobuf data;
    l1::object_builder::object_info info;
};

static const model::topic_id_partition test_tidp{
  model::topic_id::create(), model::partition_id{0}};

// A second partition whose run is written after test_tidp's run, so its data
// starts at a non-zero file position within the object.
static const model::topic_id_partition test_tidp2{
  model::topic_id::create(), model::partition_id{0}};

built_object build_object(std::vector<model::record_batch> batches) {
    iobuf buf;
    auto builder = l1::object_builder::create(
      make_iobuf_ref_output_stream(buf), {});
    builder->start_partition(test_tidp).get();
    for (auto& b : batches) {
        builder->add_batch(std::move(b)).get();
    }
    auto info = builder->finish().get();
    builder->close().get();
    return {.data = std::move(buf), .info = std::move(info)};
}

// Build an object with two partitions written sequentially. The second
// partition (test_tidp2) is preceded by the first partition's run, so it begins
// at a non-zero file position — exactly the case start_position must handle.
built_object build_two_partition_object(
  std::vector<model::record_batch> first,
  std::vector<model::record_batch> second) {
    iobuf buf;
    auto builder = l1::object_builder::create(
      make_iobuf_ref_output_stream(buf), {});
    builder->start_partition(test_tidp).get();
    for (auto& b : first) {
        builder->add_batch(std::move(b)).get();
    }
    builder->start_partition(test_tidp2).get();
    for (auto& b : second) {
        builder->add_batch(std::move(b)).get();
    }
    auto info = builder->finish().get();
    builder->close().get();
    return {.data = std::move(buf), .info = std::move(info)};
}

/// A thin l1::io wrapper that can be told to fail specific (position,size)
/// read requests with cloud_missing_object, while delegating everything else
/// to the underlying fake_io.
class fault_io : public l1::io {
public:
    explicit fault_io(l1::fake_io& delegate)
      : _delegate(delegate) {}

    void fail_range(size_t position, size_t size) {
        _fail_ranges.emplace(position, size);
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
      cloud_io::group_id g,
      bool skip_cache) override {
        if (_fail_ranges.count({extent.position, extent.size})) {
            co_return std::unexpected(errc::cloud_missing_object);
        }
        co_return co_await _delegate.read_object(extent, as, g, skip_cache);
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
    std::set<std::pair<size_t, size_t>> _fail_ranges;
};

} // namespace

class chunk_downloader_test : public seastar_test {};

// (a) Dispatch 3 contiguous ranges out of order, collect batches from
// take_ready() in order, and verify the reassembler is empty at the run end.
TEST_F(chunk_downloader_test, out_of_order_ranges_yield_ordered_batches) {
    auto batches_src
      = model::test::make_random_batches(model::offset(0), 6).get();
    std::vector<model::record_batch> batches_vec;
    for (auto& b : batches_src) {
        batches_vec.push_back(b.share());
    }

    auto [obj_data, obj_info] = build_object(std::move(batches_vec));
    auto oid = l1::create_object_id();

    l1::fake_io fio;
    fio.put_object(oid, std::move(obj_data));

    // Locate the run within the object.
    auto& part = obj_info.index.partitions.begin()->second;
    size_t run_pos = part.file_position;
    size_t run_len = part.length;

    // Split the run into three roughly equal chunks.
    size_t c1_size = run_len / 3;
    size_t c2_size = run_len / 3;
    size_t c3_size = run_len - c1_size - c2_size;

    size_t pos1 = run_pos;
    size_t pos2 = run_pos + c1_size;
    size_t pos3 = run_pos + c1_size + c2_size;

    // Anchor the decode cursor at the run start, exactly as the prefetch
    // producer always does. The no-start_position lazy-cursor path only decodes
    // correctly when the run-start chunk happens to drain first, which is not
    // guaranteed under concurrent out-of-order completion (and differs between
    // debug and release builds). With the cursor anchored, the reorder buffer
    // must reconstruct byte order regardless of GET completion order.
    chunk_downloader cd(
      &fio, oid, cloud_io::group_id::default_group, std::nullopt, run_pos);

    // Dispatch all three chunks concurrently, in non-sequential order.
    // All three GETs are in-flight at the same time; their completions may
    // arrive in any order.  The downloader must reconstruct byte order via its
    // reorder buffer.
    auto f3 = cd.dispatch(pos3, c3_size);
    auto f1 = cd.dispatch(pos1, c1_size);
    auto f2 = cd.dispatch(pos2, c2_size);
    std::move(f3).get();
    std::move(f1).get();
    std::move(f2).get();

    ASSERT_EQ(cd.in_flight(), 0u);

    auto ready = cd.take_ready();
    // We should have all 6 batches decoded in order.
    ASSERT_EQ(ready.size(), batches_src.size());
    for (size_t i = 0; i < ready.size(); ++i) {
        EXPECT_EQ(ready[i].header(), batches_src[i].header())
          << "batch " << i << " header mismatch";
    }

    // Reassembler carries no slack at a clean run boundary.
    EXPECT_EQ(cd.reassembler_slack_bytes(), 0u)
      << "chunk_reassembler must be empty after decoding all contiguous chunks";

    cd.close().get();
}

// (a2) A run that does NOT start at byte 0: with an explicit start_position the
// downloader decodes correctly even when chunks arrive out of order and the
// run-start chunk is not dispatched first. This exercises the start_position
// constructor parameter that replaces the implicit min-dispatched-position
// cursor inference.
TEST_F(chunk_downloader_test, start_position_decodes_midobject_run) {
    auto first_src
      = model::test::make_random_batches(model::offset(0), 5).get();
    auto second_src
      = model::test::make_random_batches(model::offset(100), 6).get();
    std::vector<model::record_batch> first_vec;
    for (auto& b : first_src) {
        first_vec.push_back(b.share());
    }
    std::vector<model::record_batch> second_vec;
    for (auto& b : second_src) {
        second_vec.push_back(b.share());
    }

    auto [obj_data, obj_info] = build_two_partition_object(
      std::move(first_vec), std::move(second_vec));
    auto oid = l1::create_object_id();

    l1::fake_io fio;
    fio.put_object(oid, std::move(obj_data));

    // Locate the SECOND partition's run; it begins after the first run so its
    // file position is non-zero.
    auto it = obj_info.index.partitions.find(test_tidp2);
    ASSERT_NE(it, obj_info.index.partitions.end());
    auto& part = it->second;
    size_t run_pos = part.file_position;
    ASSERT_GT(run_pos, 0u)
      << "second partition run must start at a non-zero file position";
    size_t run_len = part.length;

    size_t c1_size = run_len / 3;
    size_t c2_size = run_len / 3;
    size_t c3_size = run_len - c1_size - c2_size;

    size_t pos1 = run_pos;
    size_t pos2 = run_pos + c1_size;
    size_t pos3 = run_pos + c1_size + c2_size;

    // Construct with an EXPLICIT start_position equal to the run start. The
    // run-start chunk (pos1) is intentionally NOT dispatched first.
    chunk_downloader cd(
      &fio, oid, cloud_io::group_id::default_group, std::nullopt, run_pos);

    auto f2 = cd.dispatch(pos2, c2_size);
    auto f3 = cd.dispatch(pos3, c3_size);
    auto f1 = cd.dispatch(pos1, c1_size);
    std::move(f2).get();
    std::move(f3).get();
    std::move(f1).get();

    ASSERT_EQ(cd.in_flight(), 0u);

    auto ready = cd.take_ready();
    ASSERT_EQ(ready.size(), second_src.size());
    for (size_t i = 0; i < ready.size(); ++i) {
        EXPECT_EQ(ready[i].header(), second_src[i].header())
          << "batch " << i << " header mismatch";
    }

    EXPECT_EQ(cd.reassembler_slack_bytes(), 0u)
      << "chunk_reassembler must be empty after decoding the whole run";

    cd.close().get();
}

// (b) A middle range fails → decoding past it surfaces an error, nothing hangs.
TEST_F(chunk_downloader_test, failed_middle_range_poisons_decode) {
    auto batches_src
      = model::test::make_random_batches(model::offset(0), 6).get();
    std::vector<model::record_batch> batches_vec;
    for (auto& b : batches_src) {
        batches_vec.push_back(b.share());
    }

    auto [obj_data, obj_info] = build_object(std::move(batches_vec));
    auto oid = l1::create_object_id();

    l1::fake_io fio;
    fio.put_object(oid, std::move(obj_data));

    auto& part = obj_info.index.partitions.begin()->second;
    size_t run_pos = part.file_position;
    size_t run_len = part.length;

    size_t c1_size = run_len / 3;
    size_t c2_size = run_len / 3;
    size_t c3_size = run_len - c1_size - c2_size;

    size_t pos1 = run_pos;
    size_t pos2 = run_pos + c1_size;
    size_t pos3 = run_pos + c1_size + c2_size;

    fault_io flt(fio);
    // Inject failure for the middle chunk.
    flt.fail_range(pos2, c2_size);

    // Anchor the decode cursor at the run start (as the producer does) so the
    // poison behaviour is deterministic regardless of GET completion order.
    chunk_downloader cd(
      &flt, oid, cloud_io::group_id::default_group, std::nullopt, run_pos);

    // Dispatch all three concurrently; the middle one will fail.
    auto f1 = cd.dispatch(pos1, c1_size);
    auto f2 = cd.dispatch(pos2, c2_size); // will fail
    auto f3 = cd.dispatch(pos3, c3_size);
    std::move(f1).get();
    std::move(f2).get();
    std::move(f3).get();

    ASSERT_EQ(cd.in_flight(), 0u);

    // After all three dispatches have settled:
    //  - chunk 1 (pos1) succeeded: cursor advanced to pos2, batches in _ready.
    //  - chunk 2 (pos2) failed:    poison_pos = pos2.
    //  - chunk 3 (pos3) succeeded: stuck behind the gap at pos2.
    //
    // cursor == pos2 == poison_pos → take_ready() must throw to surface the
    // error rather than returning an empty result and hanging forever.
    EXPECT_THROW({ auto _ = cd.take_ready(); }, std::exception);

    cd.close().get();
}

// (c) fetch_footer returns a parsed l1::footer with the correct partition data.
TEST_F(chunk_downloader_test, fetch_footer_returns_parsed_footer) {
    auto batches_src
      = model::test::make_random_batches(model::offset(0), 4).get();
    std::vector<model::record_batch> batches_vec;
    for (auto& b : batches_src) {
        batches_vec.push_back(b.share());
    }

    auto [obj_data, obj_info] = build_object(std::move(batches_vec));
    auto oid = l1::create_object_id();

    size_t obj_size = obj_info.size_bytes;
    size_t footer_pos = obj_info.footer_offset;

    l1::fake_io fio;
    fio.put_object(oid, std::move(obj_data));

    chunk_downloader cd(
      &fio, oid, cloud_io::group_id::default_group, std::nullopt);

    auto footer = cd.fetch_footer(footer_pos, obj_size).get();

    // The footer should have an entry for our test_tidp.
    ASSERT_FALSE(footer.partitions.empty());
    auto it = footer.partitions.find(test_tidp);
    ASSERT_NE(it, footer.partitions.end());

    // Offset range should match our 4 batches.
    EXPECT_EQ(it->second.first_offset(), batches_src.front().base_offset()());
    EXPECT_EQ(it->second.last_offset(), batches_src.back().last_offset()());

    cd.close().get();
}

} // namespace cloud_topics::prefetch
