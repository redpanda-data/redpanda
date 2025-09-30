// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "model/tests/randoms.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/fs_utils.h"
#include "storage/segment_utils.h"
#include "storage/spill_key_index.h"
#include "test_utils/random_bytes.h"
#include "test_utils/randoms.h"
#include "test_utils/tmpbuf_file.h"
#include "utils/vint.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

std::unique_ptr<storage::compacted_index_writer> make_dummy_compacted_index(
  tmpbuf_file::store_t& index_data,
  size_t max_mem,
  storage::storage_resources& resources) {
    auto f = ss::file(ss::make_shared(tmpbuf_file(index_data)));
    return std::make_unique<storage::internal::spill_key_index>(
      "dummy name", f, max_mem, resources);
}

class compacted_topic_fixture : public ::testing::Test {
public:
    storage::storage_resources resources;
    ss::abort_source as;
};

bytes extract_record_key(bytes prefixed_key) {
    size_t sz = prefixed_key.size() - 2;
    bytes read_key(bytes::initialized_later{}, sz);

    std::copy_n(prefixed_key.begin() + 2, sz, read_key.begin());
    return read_key;
}

TEST_F(compacted_topic_fixture, format_verification) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);
    const auto key = tests::random_bytes(1024);
    auto bt = tests::random_batch_type();
    auto is_control_type = tests::random_bool();
    idx->index(bt, is_control_type, bytes(key), model::offset(42), 66).get();
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    iobuf data = std::move(index_data).release_iobuf();
    ASSERT_EQ(data.size_bytes(), 1065);
    iobuf_parser p(data.share(0, data.size_bytes()));
    (void)p.consume_type<uint16_t>(); // SIZE
    (void)p.consume_type<uint8_t>();  // TYPE
    auto [offset, _1] = p.read_varlong();
    ASSERT_EQ(model::offset(offset), model::offset(42));
    auto [delta, _2] = p.read_varlong();
    ASSERT_EQ(delta, 66);
    const auto key_result = p.read_bytes(1026);

    auto read_key = extract_record_key(key_result);
    ASSERT_EQ(key, read_key);
    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(p);
    SCOPED_TRACE(fmt::format("{}", footer));
    ASSERT_EQ(footer.keys, 1);
    ASSERT_EQ(
      footer.size,
      sizeof(uint16_t) + 1 /*type*/ + 1 /*offset*/ + 2 /*delta*/
        + 1 /*batch_type*/ + 1 /* control bit */ + 1024 /*key*/);
    ASSERT_EQ(
      footer.version, storage::compacted_index::footer::current_version);
    ASSERT_NE(footer.crc, 0);
}
TEST_F(compacted_topic_fixture, format_verification_max_key) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB, resources);
    const auto key = tests::random_bytes(1_MiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx->index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    /**
     * Length of an entry is equal to
     *
     * max_key_size + sizeof(uint8_t) + sizeof(uint16_t) + vint(42) +
     * vint(66)
     */
    iobuf data = std::move(index_data).release_iobuf();

    ASSERT_EQ(
      data.size_bytes(),
      storage::compacted_index::footer::footer_size
        + std::numeric_limits<uint16_t>::max() - 2 * vint::max_length
        + vint::vint_size(42) + vint::vint_size(66) + 1 + 2);
    iobuf_parser p(data.share(0, data.size_bytes()));

    const size_t entry = p.consume_type<uint16_t>(); // SIZE

    ASSERT_EQ(
      entry,
      std::numeric_limits<uint16_t>::max() - sizeof(uint16_t)
        - 2 * vint::max_length + vint::vint_size(42) + vint::vint_size(66) + 1
        + 2);
}
TEST_F(compacted_topic_fixture, format_verification_roundtrip) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB, resources);
    const auto key = tests::random_bytes(20);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx->index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      32_KiB,
      &as);
    auto footer = rdr.load_footer().get();
    ASSERT_EQ(footer.keys, 1);
    ASSERT_EQ(
      footer.version, storage::compacted_index::footer::current_version);
    ASSERT_NE(footer.crc, 0);
    auto vec = compaction_index_reader_to_memory(std::move(rdr)).get();
    ASSERT_EQ(vec.size(), 1);
    ASSERT_EQ(vec[0].offset, model::offset(42));
    ASSERT_EQ(vec[0].delta, 66);
    ASSERT_EQ(extract_record_key(vec[0].key), key);
}
TEST_F(
  compacted_topic_fixture, format_verification_roundtrip_exceeds_capacity) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB, resources);
    const auto key = tests::random_bytes(1_MiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx->index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      32_KiB,
      &as);
    auto footer = rdr.load_footer().get();
    ASSERT_EQ(footer.keys, 1);
    ASSERT_EQ(
      footer.version, storage::compacted_index::footer::current_version);
    ASSERT_NE(footer.crc, 0);
    auto vec = compaction_index_reader_to_memory(std::move(rdr)).get();
    ASSERT_EQ(vec.size(), 1);
    ASSERT_EQ(vec[0].offset, model::offset(42));
    ASSERT_EQ(vec[0].delta, 66);
    auto max_sz = storage::internal::spill_key_index::max_key_size;
    ASSERT_EQ(vec[0].key.size(), max_sz);
    ASSERT_EQ(
      extract_record_key(vec[0].key), bytes_view(key.data(), max_sz - 2));
}

TEST_F(compacted_topic_fixture, key_reducer_no_truncate_filter) {
    tmpbuf_file::store_t index_data;
    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);

    const auto key1 = tests::random_bytes(1_KiB);
    const auto key2 = tests::random_bytes(1_KiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx->index(bt, is_control, bytes(put_key), model::offset(i), 0).get();
    }
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      32_KiB,
      &as);
    auto key_bitmap = rdr
                        .consume(
                          storage::internal::compaction_key_reducer(),
                          model::no_timeout)
                        .get();

    // get all keys
    auto vec = compaction_index_reader_to_memory(rdr).get();
    ASSERT_EQ(vec.size(), 100);

    SCOPED_TRACE(fmt::format("key bitmap: {}", key_bitmap.toString()));
    ASSERT_EQ(key_bitmap.cardinality(), 2);
    ASSERT_TRUE(key_bitmap.contains(98));
    ASSERT_TRUE(key_bitmap.contains(99));
}

TEST_F(compacted_topic_fixture, key_reducer_max_mem) {
    tmpbuf_file::store_t index_data;
    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);

    const auto key1 = tests::random_bytes(1_KiB);
    const auto key2 = tests::random_bytes(1_KiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx->index(bt, is_control, bytes(put_key), model::offset(i), 0).get();
    }
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      32_KiB,
      &as);

    rdr.verify_integrity().get();
    rdr.reset();
    auto small_mem_bitmap = rdr
                              .consume(
                                storage::internal::compaction_key_reducer(
                                  1_KiB + 16),
                                model::no_timeout)
                              .get();

    /*
      There are 2 keys exactly.
      Each key is exactly 1KB
      We need 2KB + 2* (capacity * sizeof(std::pair) + 1)
      memory map
     */
    rdr.reset();
    auto entry_size
      = sizeof(
          std::
            pair<bytes, storage::internal::compaction_key_reducer::value_type>)
        + 1;
    auto exact_mem_bitmap = rdr
                              .consume(
                                storage::internal::compaction_key_reducer(
                                  2_KiB + 2 * entry_size * 2),
                                model::no_timeout)
                              .get();

    // get all keys
    auto vec = compaction_index_reader_to_memory(rdr).get();
    ASSERT_EQ(vec.size(), 100);

    SCOPED_TRACE(
      fmt::format("small key bitmap: {}", small_mem_bitmap.toString()));
    SCOPED_TRACE(
      fmt::format("exact key bitmap: {}", exact_mem_bitmap.toString()));
    ASSERT_EQ(exact_mem_bitmap.cardinality(), 2);
    ASSERT_EQ(small_mem_bitmap.cardinality(), 100);
    ASSERT_TRUE(exact_mem_bitmap.contains(98));
    ASSERT_TRUE(exact_mem_bitmap.contains(99));
}
TEST_F(compacted_topic_fixture, index_filtered_copy_tests) {
    tmpbuf_file::store_t index_data;

    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);

    const auto key1 = tests::random_bytes(128_KiB);
    const auto key2 = tests::random_bytes(1_KiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx->index(bt, is_control, bytes(put_key), model::offset(i), 0).get();
    }
    idx->close().get();
    SCOPED_TRACE(fmt::format("{}", *idx));

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      32_KiB,
      &as);

    rdr.verify_integrity().get();
    auto bitmap
      = storage::internal::natural_index_of_entries_to_keep(rdr).get();
    {
        auto vec = compaction_index_reader_to_memory(rdr).get();
        ASSERT_EQ(vec.size(), 100);
    }
    SCOPED_TRACE(fmt::format("key bitmap: {}", bitmap.toString()));
    ASSERT_EQ(bitmap.cardinality(), 2);
    ASSERT_TRUE(bitmap.contains(98));
    ASSERT_TRUE(bitmap.contains(99));

    // the main test
    tmpbuf_file::store_t final_data;
    auto final_idx = make_dummy_compacted_index(final_data, 1_KiB, resources);

    rdr.reset();
    rdr
      .consume(
        storage::internal::index_filtered_copy_reducer(
          std::move(bitmap), *final_idx),
        model::no_timeout)
      .get();
    final_idx->close().get();
    {
        auto final_rdr = storage::make_file_backed_compacted_reader(
          storage::segment_full_path::mock("dummy name - final "),
          ss::file(ss::make_shared(tmpbuf_file(final_data))),
          32_KiB,
          &as);
        final_rdr.verify_integrity().get();
        {
            auto vec = compaction_index_reader_to_memory(final_rdr).get();
            ASSERT_EQ(vec.size(), 2);
            ASSERT_EQ(vec[0].offset, model::offset(98));
            ASSERT_EQ(vec[1].offset, model::offset(99));
        }
        {
            auto offset_list = storage::internal::generate_compacted_list(
                                 model::offset(0), final_rdr)
                                 .get();

            ASSERT_TRUE(offset_list.contains(model::offset(98)));
            ASSERT_TRUE(offset_list.contains(model::offset(99)));
        }
    }
}

namespace storage {

struct index_footer_v1 {
    uint32_t size{0};
    uint32_t keys{0};
    compacted_index::footer_flags flags{0};
    uint32_t crc{0}; // crc32
    int8_t version{1};

    static constexpr size_t footer_size = sizeof(size) + sizeof(keys)
                                          + sizeof(flags) + sizeof(crc)
                                          + sizeof(version);
};

} // namespace storage

TEST_F(compacted_topic_fixture, footer_v1_compatibility) {
    tmpbuf_file::store_t store;
    auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
    const auto key = tests::random_bytes(1024);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx->index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx->close().get();

    iobuf data = std::move(store).release_iobuf();

    // check that the footer suffix can be read as v1 footer

    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(
      data.share(
        data.size_bytes() - storage::compacted_index::footer::footer_size,
        storage::compacted_index::footer::footer_size));
    SCOPED_TRACE(fmt::format("{}", footer));

    auto footer_v1 = reflection::adl<storage::index_footer_v1>{}.from(
      data.share(
        data.size_bytes() - storage::index_footer_v1::footer_size,
        storage::index_footer_v1::footer_size));

    EXPECT_EQ(footer.size, footer.size_deprecated);
    EXPECT_EQ(footer.size, footer_v1.size);
    EXPECT_EQ(footer.keys, footer.keys_deprecated);
    EXPECT_EQ(footer.keys, footer_v1.keys);
    EXPECT_EQ(footer.flags, footer_v1.flags);
    EXPECT_EQ(footer.crc, footer_v1.crc);
    EXPECT_EQ(footer.version, footer_v1.version);
}

static iobuf substitute_index_for_older_ver(iobuf data, int8_t version) {
    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(
      data.share(
        data.size_bytes() - storage::compacted_index::footer::footer_size,
        storage::compacted_index::footer::footer_size));

    // v1 and v0 footers have the same format
    assert(version == 0 || version == 1);
    auto footer_v1 = storage::index_footer_v1{
      .size = static_cast<uint32_t>(footer.size),
      .keys = static_cast<uint32_t>(footer.keys),
      .flags = footer.flags,
      .crc = footer.crc,
      .version = version};

    auto ret = data
                 .share(
                   0,
                   data.size_bytes()
                     - storage::compacted_index::footer::footer_size)
                 .copy();
    reflection::adl<storage::index_footer_v1>{}.to(ret, footer_v1);
    return ret;
};

static storage::compacted_index::footer
verify_index_integrity(const iobuf& data) {
    tmpbuf_file::store_t store;
    ss::file file{ss::make_shared(tmpbuf_file(store))};
    auto fstream = ss::make_file_output_stream(file).get();
    for (const auto& fragment : data) {
        fstream.write(fragment.get(), fragment.size()).get();
    }
    fstream.flush().get();
    ss::abort_source as;
    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"), file, 32_KiB, &as);
    rdr.verify_integrity().get();
    return rdr.load_footer().get();
}

// test that indices with v1 footers are correctly loaded
TEST_F(compacted_topic_fixture, v1_footers_compatibility) {
    {
        // empty index
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        idx->close().get();
        auto idx_data = std::move(store).release_iobuf();
        verify_index_integrity(idx_data);
        auto idx_data_v1 = substitute_index_for_older_ver(
          std::move(idx_data), 1);
        auto footer = verify_index_integrity(idx_data_v1);
        EXPECT_EQ(footer.size, 0);
        EXPECT_EQ(footer.keys, 0);
        EXPECT_EQ(footer.flags, storage::compacted_index::footer_flags::none);
        EXPECT_EQ(footer.crc, 0);
    }

    {
        // index with some keys
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        const auto key = tests::random_bytes(1024);
        auto bt = tests::random_batch_type();
        auto is_control = tests::random_bool();
        idx->index(bt, is_control, bytes(key), model::offset(42), 66).get();
        idx->close().get();
        auto idx_data = std::move(store).release_iobuf();
        auto footer_before = verify_index_integrity(idx_data);
        auto idx_data_v1 = substitute_index_for_older_ver(
          std::move(idx_data), 1);
        auto footer_after = verify_index_integrity(idx_data_v1);
        EXPECT_EQ(footer_before.size, footer_after.size);
        EXPECT_EQ(footer_after.keys, 1);
        EXPECT_EQ(
          footer_after.flags, storage::compacted_index::footer_flags::none);
        EXPECT_EQ(footer_before.crc, footer_after.crc);
    }
}

// test that indices with v0 footers are marked as "needs rebuild"
TEST_F(compacted_topic_fixture, v0_footers_compatibility) {
    {
        // empty index
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        idx->close().get();
        auto idx_data = std::move(store).release_iobuf();
        auto idx_data_v0 = substitute_index_for_older_ver(
          std::move(idx_data), 0);
        EXPECT_THROW(
          verify_index_integrity(idx_data_v0),
          storage::compacted_index::needs_rebuild_error);
    }

    {
        // index with some keys
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        const auto key = tests::random_bytes(1024);
        auto bt = tests::random_batch_type();
        auto is_control = tests::random_bool();
        idx->index(bt, is_control, bytes(key), model::offset(42), 66).get();
        idx->close().get();
        auto idx_data = std::move(store).release_iobuf();
        auto idx_data_v0 = substitute_index_for_older_ver(
          std::move(idx_data), 0);
        EXPECT_THROW(
          verify_index_integrity(idx_data_v0),
          storage::compacted_index::needs_rebuild_error);
    }
}
