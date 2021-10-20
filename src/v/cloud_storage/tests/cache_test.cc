/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cache_test_fixture.h"
#include "cloud_storage/cache_service.h"
#include "test_utils/fixture.h"
#include "units.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>

using namespace cloud_storage;

FIXTURE_TEST(put_creates_file, cache_test_fixture) {
    auto data_string = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string, KEY);

    bool file_exists = ss::file_exists((CACHE_DIR / KEY).native()).get();
    BOOST_CHECK(file_exists);
}

FIXTURE_TEST(get_after_put, cache_test_fixture) {
    auto data_string = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string, KEY);

    std::optional<cloud_storage::cache_item> returned_item
      = cache_service.get(KEY).get();
    BOOST_REQUIRE(returned_item);
    BOOST_CHECK_EQUAL(returned_item->size, data_string.length());

    auto read_buf
      = returned_item->body.read_exactly(data_string.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string);
    BOOST_CHECK(!returned_item->body.read().get().get());
    returned_item->body.close().get();
}

FIXTURE_TEST(put_rewrites_file, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    auto data_string2 = create_data_string('b', 1_MiB + 1_KiB);
    put_into_cache(data_string2, KEY);

    std::optional<cloud_storage::cache_item> returned_item
      = cache_service.get(KEY).get();
    BOOST_REQUIRE(returned_item);
    BOOST_CHECK_EQUAL(returned_item->size, data_string2.length());

    auto read_buf
      = returned_item->body.read_exactly(data_string2.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string2);
    BOOST_CHECK(!returned_item->body.read().get().get());
    returned_item->body.close().get();
}

FIXTURE_TEST(get_missing_file, cache_test_fixture) {
    std::optional<cloud_storage::cache_item> returned_item
      = cache_service.get(WRONG_KEY).get();

    BOOST_CHECK(!returned_item);
}

FIXTURE_TEST(missing_file_not_cached, cache_test_fixture) {
    auto is_cached = cache_service.is_cached(WRONG_KEY).get();

    BOOST_CHECK_EQUAL(is_cached, cache_element_status::not_available);
}

FIXTURE_TEST(is_cached_after_put_success, cache_test_fixture) {
    iobuf buf;
    auto input = make_iobuf_input_stream(std::move(buf));
    cache_service.put(KEY, input).get();

    auto is_cached = cache_service.is_cached(KEY).get();

    BOOST_CHECK_EQUAL(is_cached, cache_element_status::available);
}

FIXTURE_TEST(after_invalidate_is_not_cached, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    cache_service.invalidate(KEY).get();

    auto is_cached = cache_service.is_cached(KEY).get();
    BOOST_CHECK_EQUAL(is_cached, cache_element_status::not_available);
}

FIXTURE_TEST(invalidate_missing_file_ok, cache_test_fixture) {
    BOOST_CHECK_NO_THROW(cache_service.invalidate(WRONG_KEY).get());
}

FIXTURE_TEST(empty_cache_nothing_deleted, cache_test_fixture) {
    ss::sleep(ss::lowres_clock::duration(2s)).get();

    BOOST_CHECK_EQUAL(0, cache_service.get_total_cleaned());
}

FIXTURE_TEST(files_up_to_max_cache_size_not_deleted, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    ss::sleep(ss::lowres_clock::duration(2s)).get();

    BOOST_CHECK_EQUAL(0, cache_service.get_total_cleaned());
}

FIXTURE_TEST(file_bigger_than_max_cache_size_deleted, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 2_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    ss::sleep(ss::lowres_clock::duration(2s)).get();

    BOOST_CHECK_EQUAL(2_MiB + 1_KiB, cache_service.get_total_cleaned());
}

FIXTURE_TEST(
  files_bigger_than_max_cache_size_oldest_deleted, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);
    auto data_string2 = create_data_string('b', 1_MiB + 1_KiB);
    ss::sleep(1s).get();
    put_into_cache(data_string1, KEY2);

    ss::sleep(ss::lowres_clock::duration(2s)).get();

    BOOST_CHECK_EQUAL(1_MiB + 1_KiB, cache_service.get_total_cleaned());
    BOOST_REQUIRE(!ss::file_exists((CACHE_DIR / KEY).native()).get());
    BOOST_REQUIRE(ss::file_exists((CACHE_DIR / KEY2).native()).get());
}

FIXTURE_TEST(cannot_put_tmp_file, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_KiB);
    BOOST_CHECK_THROW(
      put_into_cache(data_string1, TEMP_KEY), std::invalid_argument);
}
