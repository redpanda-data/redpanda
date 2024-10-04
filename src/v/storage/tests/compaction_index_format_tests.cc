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
#include "bytes/random.h"
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
#include "test_utils/fixture.h"
#include "test_utils/randoms.h"
#include "utils/tmpbuf_file.h"
#include "utils/vint.h"

#include <boost/test/unit_test_suite.hpp>

storage::compacted_index_writer make_dummy_compacted_index(
  tmpbuf_file::store_t& index_data,
  size_t max_mem,
  storage::storage_resources& resources) {
    auto f = ss::file(ss::make_shared(tmpbuf_file(index_data)));
    return storage::compacted_index_writer(
      std::make_unique<storage::internal::spill_key_index>(
        "dummy name", f, max_mem, resources));
}

struct compacted_topic_fixture {
    storage::storage_resources resources;
    ss::abort_source as;
};

bytes extract_record_key(bytes prefixed_key) {
    size_t sz = prefixed_key.size() - 2;
    bytes read_key(bytes::initialized_later{}, sz);

    std::copy_n(prefixed_key.begin() + 2, sz, read_key.begin());
    return read_key;
}

FIXTURE_TEST(format_verification, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);
    const auto key = random_generators::get_bytes(1024);
    auto bt = tests::random_batch_type();
    auto is_control_type = tests::random_bool();
    idx.index(bt, is_control_type, bytes(key), model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    iobuf data = std::move(index_data).release_iobuf();
    BOOST_REQUIRE_EQUAL(data.size_bytes(), 1065);
    iobuf_parser p(data.share(0, data.size_bytes()));
    (void)p.consume_type<uint16_t>(); // SIZE
    (void)p.consume_type<uint8_t>();  // TYPE
    auto [offset, _1] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(model::offset(offset), model::offset(42));
    auto [delta, _2] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(delta, 66);
    const auto key_result = p.read_bytes(1026);

    auto read_key = extract_record_key(key_result);
    BOOST_REQUIRE_EQUAL(key, read_key);
    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(p);
    info("{}", footer);
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(
      footer.size,
      sizeof(uint16_t) + 1 /*type*/ + 1 /*offset*/ + 2 /*delta*/
        + 1 /*batch_type*/ + 1 /* control bit */ + 1024 /*key*/);
    BOOST_REQUIRE_EQUAL(
      footer.version, storage::compacted_index::footer::current_version);
    BOOST_REQUIRE(footer.crc != 0);
}
FIXTURE_TEST(format_verification_max_key, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB, resources);
    const auto key = random_generators::get_bytes(1_MiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx.index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    /**
     * Length of an entry is equal to
     *
     * max_key_size + sizeof(uint8_t) + sizeof(uint16_t) + vint(42) +
     * vint(66)
     */
    iobuf data = std::move(index_data).release_iobuf();

    BOOST_REQUIRE_EQUAL(
      data.size_bytes(),
      storage::compacted_index::footer::footer_size
        + std::numeric_limits<uint16_t>::max() - 2 * vint::max_length
        + vint::vint_size(42) + vint::vint_size(66) + 1 + 2);
    iobuf_parser p(data.share(0, data.size_bytes()));

    const size_t entry = p.consume_type<uint16_t>(); // SIZE

    BOOST_REQUIRE_EQUAL(
      entry,
      std::numeric_limits<uint16_t>::max() - sizeof(uint16_t)
        - 2 * vint::max_length + vint::vint_size(42) + vint::vint_size(66) + 1
        + 2);
}
FIXTURE_TEST(format_verification_roundtrip, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB, resources);
    const auto key = random_generators::get_bytes(20);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx.index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB,
      &as);
    auto footer = rdr.load_footer().get();
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(
      footer.version, storage::compacted_index::footer::current_version);
    BOOST_REQUIRE(footer.crc != 0);
    auto vec = compaction_index_reader_to_memory(std::move(rdr)).get();
    BOOST_REQUIRE_EQUAL(vec.size(), 1);
    BOOST_REQUIRE_EQUAL(vec[0].offset, model::offset(42));
    BOOST_REQUIRE_EQUAL(vec[0].delta, 66);
    BOOST_REQUIRE_EQUAL(extract_record_key(vec[0].key), key);
}
FIXTURE_TEST(
  format_verification_roundtrip_exceeds_capacity, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB, resources);
    const auto key = random_generators::get_bytes(1_MiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx.index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB,
      &as);
    auto footer = rdr.load_footer().get();
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(
      footer.version, storage::compacted_index::footer::current_version);
    BOOST_REQUIRE(footer.crc != 0);
    auto vec = compaction_index_reader_to_memory(std::move(rdr)).get();
    BOOST_REQUIRE_EQUAL(vec.size(), 1);
    BOOST_REQUIRE_EQUAL(vec[0].offset, model::offset(42));
    BOOST_REQUIRE_EQUAL(vec[0].delta, 66);
    auto max_sz = storage::internal::spill_key_index::max_key_size;
    BOOST_REQUIRE_EQUAL(vec[0].key.size(), max_sz);
    BOOST_REQUIRE_EQUAL(
      extract_record_key(vec[0].key), bytes_view(key.data(), max_sz - 2));
}

FIXTURE_TEST(key_reducer_no_truncate_filter, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);

    const auto key1 = random_generators::get_bytes(1_KiB);
    const auto key2 = random_generators::get_bytes(1_KiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx.index(bt, is_control, bytes(put_key), model::offset(i), 0).get();
    }
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB,
      &as);
    auto key_bitmap = rdr
                        .consume(
                          storage::internal::compaction_key_reducer(),
                          model::no_timeout)
                        .get();

    // get all keys
    auto vec = compaction_index_reader_to_memory(rdr).get();
    BOOST_REQUIRE_EQUAL(vec.size(), 100);

    info("key bitmap: {}", key_bitmap.toString());
    BOOST_REQUIRE_EQUAL(key_bitmap.cardinality(), 2);
    BOOST_REQUIRE(key_bitmap.contains(98));
    BOOST_REQUIRE(key_bitmap.contains(99));
}

FIXTURE_TEST(key_reducer_max_mem, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);

    const auto key1 = random_generators::get_bytes(1_KiB);
    const auto key2 = random_generators::get_bytes(1_KiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx.index(bt, is_control, bytes(put_key), model::offset(i), 0).get();
    }
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
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
    BOOST_REQUIRE_EQUAL(vec.size(), 100);

    info("small key bitmap: {}", small_mem_bitmap.toString());
    info("exact key bitmap: {}", exact_mem_bitmap.toString());
    BOOST_REQUIRE_EQUAL(exact_mem_bitmap.cardinality(), 2);
    BOOST_REQUIRE_EQUAL(small_mem_bitmap.cardinality(), 100);
    BOOST_REQUIRE(exact_mem_bitmap.contains(98));
    BOOST_REQUIRE(exact_mem_bitmap.contains(99));
}
FIXTURE_TEST(index_filtered_copy_tests, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;

    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB, resources);

    const auto key1 = random_generators::get_bytes(128_KiB);
    const auto key2 = random_generators::get_bytes(1_KiB);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx.index(bt, is_control, bytes(put_key), model::offset(i), 0).get();
    }
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      storage::segment_full_path::mock("dummy name"),
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB,
      &as);

    rdr.verify_integrity().get();
    auto bitmap
      = storage::internal::natural_index_of_entries_to_keep(rdr).get();
    {
        auto vec = compaction_index_reader_to_memory(rdr).get();
        BOOST_REQUIRE_EQUAL(vec.size(), 100);
    }
    info("key bitmap: {}", bitmap.toString());
    BOOST_REQUIRE_EQUAL(bitmap.cardinality(), 2);
    BOOST_REQUIRE(bitmap.contains(98));
    BOOST_REQUIRE(bitmap.contains(99));

    // the main test
    tmpbuf_file::store_t final_data;
    auto final_idx = make_dummy_compacted_index(final_data, 1_KiB, resources);

    rdr.reset();
    rdr
      .consume(
        storage::internal::index_filtered_copy_reducer(
          std::move(bitmap), final_idx),
        model::no_timeout)
      .get();
    final_idx.close().get();
    {
        auto final_rdr = storage::make_file_backed_compacted_reader(
          storage::segment_full_path::mock("dummy name - final "),
          ss::file(ss::make_shared(tmpbuf_file(final_data))),
          ss::default_priority_class(),
          32_KiB,
          &as);
        final_rdr.verify_integrity().get();
        {
            auto vec = compaction_index_reader_to_memory(final_rdr).get();
            BOOST_REQUIRE_EQUAL(vec.size(), 2);
            BOOST_REQUIRE_EQUAL(vec[0].offset, model::offset(98));
            BOOST_REQUIRE_EQUAL(vec[1].offset, model::offset(99));
        }
        {
            auto offset_list = storage::internal::generate_compacted_list(
                                 model::offset(0), final_rdr)
                                 .get();

            BOOST_REQUIRE(offset_list.contains(model::offset(98)));
            BOOST_REQUIRE(offset_list.contains(model::offset(99)));
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

FIXTURE_TEST(footer_v1_compatibility, compacted_topic_fixture) {
    tmpbuf_file::store_t store;
    auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
    const auto key = random_generators::get_bytes(1024);
    auto bt = tests::random_batch_type();
    auto is_control = tests::random_bool();
    idx.index(bt, is_control, bytes(key), model::offset(42), 66).get();
    idx.close().get();

    iobuf data = std::move(store).release_iobuf();

    // check that the footer suffix can be read as v1 footer

    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(
      data.share(
        data.size_bytes() - storage::compacted_index::footer::footer_size,
        storage::compacted_index::footer::footer_size));
    info("{}", footer);

    auto footer_v1 = reflection::adl<storage::index_footer_v1>{}.from(
      data.share(
        data.size_bytes() - storage::index_footer_v1::footer_size,
        storage::index_footer_v1::footer_size));

    BOOST_CHECK_EQUAL(footer.size, footer.size_deprecated);
    BOOST_CHECK_EQUAL(footer.size, footer_v1.size);
    BOOST_CHECK_EQUAL(footer.keys, footer.keys_deprecated);
    BOOST_CHECK_EQUAL(footer.keys, footer_v1.keys);
    BOOST_CHECK(footer.flags == footer_v1.flags);
    BOOST_CHECK_EQUAL(footer.crc, footer_v1.crc);
    BOOST_CHECK_EQUAL(footer.version, footer_v1.version);
}

static iobuf substitute_index_for_older_ver(iobuf data, int8_t version) {
    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(
      data.share(
        data.size_bytes() - storage::compacted_index::footer::footer_size,
        storage::compacted_index::footer::footer_size));

    // v1 and v0 footers have the same format
    BOOST_REQUIRE(version == 0 || version == 1);
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
      storage::segment_full_path::mock("dummy name"),
      file,
      ss::default_priority_class(),
      32_KiB,
      &as);
    rdr.verify_integrity().get();
    return rdr.load_footer().get();
}

// test that indices with v1 footers are correctly loaded
FIXTURE_TEST(v1_footers_compatibility, compacted_topic_fixture) {
    {
        // empty index
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        idx.close().get();
        auto idx_data = std::move(store).release_iobuf();
        verify_index_integrity(idx_data);
        auto idx_data_v1 = substitute_index_for_older_ver(
          std::move(idx_data), 1);
        auto footer = verify_index_integrity(idx_data_v1);
        BOOST_CHECK_EQUAL(footer.size, 0);
        BOOST_CHECK_EQUAL(footer.keys, 0);
        BOOST_CHECK(
          footer.flags == storage::compacted_index::footer_flags::none);
        BOOST_CHECK_EQUAL(footer.crc, 0);
    }

    {
        // index with some keys
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        const auto key = random_generators::get_bytes(1024);
        auto bt = tests::random_batch_type();
        auto is_control = tests::random_bool();
        idx.index(bt, is_control, bytes(key), model::offset(42), 66).get();
        idx.close().get();
        auto idx_data = std::move(store).release_iobuf();
        auto footer_before = verify_index_integrity(idx_data);
        auto idx_data_v1 = substitute_index_for_older_ver(
          std::move(idx_data), 1);
        auto footer_after = verify_index_integrity(idx_data_v1);
        BOOST_CHECK_EQUAL(footer_before.size, footer_after.size);
        BOOST_CHECK_EQUAL(footer_after.keys, 1);
        BOOST_CHECK(
          footer_after.flags == storage::compacted_index::footer_flags::none);
        BOOST_CHECK_EQUAL(footer_before.crc, footer_after.crc);
    }
}

// test that indices with v0 footers are marked as "needs rebuild"
FIXTURE_TEST(v0_footers_compatibility, compacted_topic_fixture) {
    {
        // empty index
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        idx.close().get();
        auto idx_data = std::move(store).release_iobuf();
        auto idx_data_v0 = substitute_index_for_older_ver(
          std::move(idx_data), 0);
        BOOST_CHECK_THROW(
          verify_index_integrity(idx_data_v0),
          storage::compacted_index::needs_rebuild_error);
    }

    {
        // index with some keys
        tmpbuf_file::store_t store;
        auto idx = make_dummy_compacted_index(store, 1_KiB, resources);
        const auto key = random_generators::get_bytes(1024);
        auto bt = tests::random_batch_type();
        auto is_control = tests::random_bool();
        idx.index(bt, is_control, bytes(key), model::offset(42), 66).get();
        idx.close().get();
        auto idx_data = std::move(store).release_iobuf();
        auto idx_data_v0 = substitute_index_for_older_ver(
          std::move(idx_data), 0);
        BOOST_CHECK_THROW(
          verify_index_integrity(idx_data_v0),
          storage::compacted_index::needs_rebuild_error);
    }
}
