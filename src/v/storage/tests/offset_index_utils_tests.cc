// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "random/generators.h"
#include "storage/segment_index.h"
#include "test_utils/tmpbuf_file.h"
#include "utils/file_io.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>

#include <gtest/gtest.h>

using namespace storage;

namespace storage {
class offset_index_utils_fixture : public testing::Test {
public:
    offset_index_utils_fixture(model::offset base = model::offset(0)) {
        _base_offset = base;
        // index
        _idx = std::unique_ptr<segment_index>(new segment_index(
          segment_full_path::mock("In memory iobuf"),
          ss::file(ss::make_shared(tmpbuf_file(_data))),
          _base_offset,
          storage::segment_index::default_data_buffer_step,
          _feature_table));
    }

    ~offset_index_utils_fixture() { _feature_table.stop().get(); }

    ss::future<> start() {
        return _feature_table.start().then([this]() {
            return _feature_table.invoke_on_all(
              [](features::feature_table& f) { f.testing_activate_all(); });
        });
    }

    const model::record_batch_header
    modify_get(model::offset o, int32_t batch_size) {
        _base_hdr.type = model::record_batch_type::raft_data;
        _base_hdr.base_offset = o;
        _base_hdr.size_bytes = batch_size;
        return _base_hdr;
    }
    void index_entry_expect(uint32_t offset, size_t filepos) {
        auto o = model::offset(offset);
        auto p = _idx->find_nearest(o);
        ASSERT_TRUE(bool(p));
        ASSERT_EQ(p->offset, o);
        ASSERT_EQ(p->filepos, filepos);
    }

    model::offset _base_offset;
    model::record_batch_header _base_hdr;
    storage::segment_index_ptr _idx;
    tmpbuf_file::store_t _data;
    ss::sharded<features::feature_table> _feature_table;
};
} // namespace storage

TEST_F(offset_index_utils_fixture, index_round_trip) {
    start().get();

    EXPECT_TRUE(true);
    SUCCEED() << "index: " << _idx;
    for (uint32_t i = 0; i < 1024; ++i) {
        model::offset o = _base_offset + model::offset(i);
        _idx->maybe_track(
          modify_get(o, storage::segment_index::default_data_buffer_step),
          std::nullopt,
          i);
    }
    SUCCEED() << "About to flush index";
    _idx->flush().get();
    auto data = _data.share_iobuf();
    SUCCEED() << _idx << " - serializing from bytes into mem: buffer" << data;
    auto raw_idx = serde::from_iobuf<storage::index_state>(
      data.share(0, data.size_bytes()));
    SUCCEED() << "verifying tracking info: " << raw_idx;
    ASSERT_EQ(raw_idx.max_offset(), 1023);
    ASSERT_EQ(raw_idx.index.size(), 1024);
}

TEST_F(offset_index_utils_fixture, bucket_bug1) {
    start().get();

    SUCCEED() << "index: " << _idx;
    SUCCEED() << "Testing bucket find";
    _idx->maybe_track(
      modify_get(model::offset{824}, 155103), std::nullopt, 0); // indexed
    _idx->maybe_track(
      modify_get(model::offset{849}, 168865), std::nullopt, 155103); // indexed
    _idx->maybe_track(
      modify_get(model::offset{879}, 134080), std::nullopt, 323968); // indexed
    _idx->maybe_track(
      modify_get(model::offset{901}, 142073), std::nullopt, 458048); // indexed
    _idx->maybe_track(
      modify_get(model::offset{926}, 126886), std::nullopt, 600121); // indexed
    _idx->maybe_track(
      modify_get(model::offset{948}, 1667),
      std::nullopt,
      727007); // not indexed

    index_entry_expect(824, 0);
    index_entry_expect(849, 155103);
    index_entry_expect(879, 323968);
    index_entry_expect(901, 458048);
    index_entry_expect(926, 600121);
    {
        auto p = _idx->find_nearest(model::offset(947));
        ASSERT_TRUE(bool(p));
        ASSERT_EQ(p->offset, model::offset(926));
        ASSERT_EQ(p->filepos, 600121);
    }
}
TEST_F(offset_index_utils_fixture, bucket_truncate) {
    start().get();

    SUCCEED() << "index: " << _idx;
    SUCCEED() << "Testing bucket truncate";
    _idx->maybe_track(
      modify_get(model::offset{824}, 155103), std::nullopt, 0); // indexed
    _idx->maybe_track(
      modify_get(model::offset{849}, 168865), std::nullopt, 155103); // indexed
    _idx->maybe_track(
      modify_get(model::offset{879}, 134080), std::nullopt, 323968); // indexed
    _idx->maybe_track(
      modify_get(model::offset{901}, 142073), std::nullopt, 458048); // indexed
    _idx->maybe_track(
      modify_get(model::offset{926}, 126886), std::nullopt, 600121); // indexed
    _idx->maybe_track(
      modify_get(model::offset{948}, 1667),
      std::nullopt,
      727007); // not indexed
    ASSERT_EQ(_idx->num_compactible_records_appended(), 0);

    // test range truncation next
    _idx->truncate(model::offset(926), model::timestamp{100}).get();
    index_entry_expect(879, 323968);
    index_entry_expect(901, 458048);
    ASSERT_EQ(_idx->num_compactible_records_appended(), 0);
    {
        auto p = _idx->find_nearest(model::offset(926));
        ASSERT_TRUE(bool(p));
        ASSERT_EQ(p->offset, model::offset(901));
        ASSERT_EQ(p->filepos, 458048);
    }
    {
        auto p = _idx->find_nearest(model::offset(947));
        ASSERT_TRUE(bool(p));
        ASSERT_EQ(p->offset, model::offset(901));
        ASSERT_EQ(p->filepos, 458048);
    }

    ASSERT_TRUE(_idx->max_timestamp() == model::timestamp{100});

    _idx->truncate(model::offset(824), model::timestamp{100}).get();
    ASSERT_EQ(_idx->num_compactible_records_appended(), 0);
    {
        auto p = _idx->find_nearest(model::offset(824));
        ASSERT_FALSE(bool(p));
    }

    _idx->truncate(model::offset(823), model::timestamp{100}).get();
    ASSERT_EQ(_idx->num_compactible_records_appended(), 0);
    {
        auto p = _idx->find_nearest(model::offset(824));
        ASSERT_FALSE(bool(p));
    }
}
