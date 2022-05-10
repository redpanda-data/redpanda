// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_reader.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_reducers.h"
#include "storage/segment_utils.h"
#include "storage/spill_key_index.h"
#include "test_utils/fixture.h"
#include "units.h"
#include "utils/tmpbuf_file.h"
#include "utils/vint.h"

#include <boost/test/unit_test_suite.hpp>

storage::compacted_index_writer make_dummy_compacted_index(
  tmpbuf_file::store_t& index_data, size_t max_memory) {
    auto f = ss::file(ss::make_shared(tmpbuf_file(index_data)));
    return storage::compacted_index_writer(
      std::make_unique<storage::internal::spill_key_index>(
        "dummy name", f, max_memory));
}

struct compacted_topic_fixture {};
FIXTURE_TEST(format_verification, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_KiB);
    const auto key = random_generators::get_bytes(1024);
    idx.index(key, model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    iobuf data = std::move(index_data).release_iobuf();
    BOOST_REQUIRE_EQUAL(data.size_bytes(), 1047);
    iobuf_parser p(data.share(0, data.size_bytes()));
    (void)p.consume_type<uint16_t>(); // SIZE
    (void)p.consume_type<uint8_t>();  // TYPE
    auto [offset, _1] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(model::offset(offset), model::offset(42));
    auto [delta, _2] = p.read_varlong();
    BOOST_REQUIRE_EQUAL(delta, 66);
    const auto key_result = p.read_bytes(1024);
    BOOST_REQUIRE_EQUAL(key, key_result);
    auto footer = reflection::adl<storage::compacted_index::footer>{}.from(p);
    info("{}", footer);
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(
      footer.size,
      sizeof(uint16_t)
        + 1 /*type*/ + 1 /*offset*/ + 2 /*delta*/ + 1024 /*key*/);
    BOOST_REQUIRE_EQUAL(footer.version, 0);
    BOOST_REQUIRE(footer.crc != 0);
}
FIXTURE_TEST(format_verification_max_key, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB);
    const auto key = random_generators::get_bytes(1_MiB);
    idx.index(key, model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    /**
     * Length of an entry is equal to
     *
     * max_key_size + sizeof(uint8_t) + sizeof(uint16_t) + vint(42) + vint(66)
     */
    iobuf data = std::move(index_data).release_iobuf();

    BOOST_REQUIRE_EQUAL(
      data.size_bytes(),
      storage::compacted_index::footer_size
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
    auto idx = make_dummy_compacted_index(index_data, 1_MiB);
    const auto key = random_generators::get_bytes(20);
    idx.index(key, model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      "dummy name",
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB);
    auto footer = rdr.load_footer().get0();
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(footer.version, 0);
    BOOST_REQUIRE(footer.crc != 0);
    auto vec = compaction_index_reader_to_memory(std::move(rdr)).get0();
    BOOST_REQUIRE_EQUAL(vec.size(), 1);
    BOOST_REQUIRE_EQUAL(vec[0].offset, model::offset(42));
    BOOST_REQUIRE_EQUAL(vec[0].delta, 66);
    BOOST_REQUIRE_EQUAL(vec[0].key, key);
}
FIXTURE_TEST(
  format_verification_roundtrip_exceeds_capacity, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    auto idx = make_dummy_compacted_index(index_data, 1_MiB);
    const auto key = random_generators::get_bytes(1_MiB);
    idx.index(key, model::offset(42), 66).get();
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      "dummy name",
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB);
    auto footer = rdr.load_footer().get0();
    BOOST_REQUIRE_EQUAL(footer.keys, 1);
    BOOST_REQUIRE_EQUAL(footer.version, 0);
    BOOST_REQUIRE(footer.crc != 0);
    auto vec = compaction_index_reader_to_memory(std::move(rdr)).get0();
    BOOST_REQUIRE_EQUAL(vec.size(), 1);
    BOOST_REQUIRE_EQUAL(vec[0].offset, model::offset(42));
    BOOST_REQUIRE_EQUAL(vec[0].delta, 66);
    auto max_sz = storage::internal::spill_key_index::max_key_size;
    BOOST_REQUIRE_EQUAL(vec[0].key.size(), max_sz);
    BOOST_REQUIRE_EQUAL(vec[0].key, bytes_view(key.data(), max_sz));
}

FIXTURE_TEST(key_reducer_no_truncate_filter, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB);

    const auto key1 = random_generators::get_bytes(1_KiB);
    const auto key2 = random_generators::get_bytes(1_KiB);
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx.index(put_key, model::offset(i), 0).get();
    }
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      "dummy name",
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB);
    auto key_bitmap = rdr
                        .consume(
                          storage::internal::compaction_key_reducer(),
                          model::no_timeout)
                        .get0();

    // get all keys
    auto vec = compaction_index_reader_to_memory(rdr).get0();
    BOOST_REQUIRE_EQUAL(vec.size(), 100);

    info("key bitmap: {}", key_bitmap.toString());
    BOOST_REQUIRE_EQUAL(key_bitmap.cardinality(), 2);
    BOOST_REQUIRE(key_bitmap.contains(98));
    BOOST_REQUIRE(key_bitmap.contains(99));
}

FIXTURE_TEST(key_reducer_max_mem, compacted_topic_fixture) {
    tmpbuf_file::store_t index_data;
    // 1 KiB to FORCE eviction with every key basically
    auto idx = make_dummy_compacted_index(index_data, 1_KiB);

    const auto key1 = random_generators::get_bytes(1_KiB);
    const auto key2 = random_generators::get_bytes(1_KiB);
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx.index(put_key, model::offset(i), 0).get();
    }
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      "dummy name",
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB);

    rdr.verify_integrity().get();
    rdr.reset();
    auto small_mem_bitmap = rdr
                              .consume(
                                storage::internal::compaction_key_reducer(
                                  1_KiB + 16),
                                model::no_timeout)
                              .get0();

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
                              .get0();

    // get all keys
    auto vec = compaction_index_reader_to_memory(rdr).get0();
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
    auto idx = make_dummy_compacted_index(index_data, 1_KiB);

    const auto key1 = random_generators::get_bytes(128_KiB);
    const auto key2 = random_generators::get_bytes(1_KiB);
    for (auto i = 0; i < 100; ++i) {
        bytes_view put_key;
        if (i % 2) {
            put_key = key1;
        } else {
            put_key = key2;
        }
        idx.index(put_key, model::offset(i), 0).get();
    }
    idx.close().get();
    info("{}", idx);

    auto rdr = storage::make_file_backed_compacted_reader(
      "dummy name",
      ss::file(ss::make_shared(tmpbuf_file(index_data))),
      ss::default_priority_class(),
      32_KiB);

    rdr.verify_integrity().get();
    auto bitmap
      = storage::internal::natural_index_of_entries_to_keep(rdr).get0();
    {
        auto vec = compaction_index_reader_to_memory(rdr).get0();
        BOOST_REQUIRE_EQUAL(vec.size(), 100);
    }
    info("key bitmap: {}", bitmap.toString());
    BOOST_REQUIRE_EQUAL(bitmap.cardinality(), 2);
    BOOST_REQUIRE(bitmap.contains(98));
    BOOST_REQUIRE(bitmap.contains(99));

    // the main test
    tmpbuf_file::store_t final_data;
    auto final_idx = make_dummy_compacted_index(final_data, 1_KiB);

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
          "dummy name - final ",
          ss::file(ss::make_shared(tmpbuf_file(final_data))),
          ss::default_priority_class(),
          32_KiB);
        final_rdr.verify_integrity().get();
        {
            auto vec = compaction_index_reader_to_memory(final_rdr).get0();
            BOOST_REQUIRE_EQUAL(vec.size(), 2);
            BOOST_REQUIRE_EQUAL(vec[0].offset, model::offset(98));
            BOOST_REQUIRE_EQUAL(vec[1].offset, model::offset(99));
        }
        {
            auto offset_list = storage::internal::generate_compacted_list(
                                 model::offset(0), final_rdr)
                                 .get0();

            BOOST_REQUIRE(offset_list.contains(model::offset(98)));
            BOOST_REQUIRE(offset_list.contains(model::offset(99)));
        }
    }
}
