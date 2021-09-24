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

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

FIXTURE_TEST(put_creates_file, cache_test_fixture) {
    ss::sstring data_string;
    while (data_string.length() < 4096) {
        data_string += SAMPLE_STRING1;
    }
    iobuf buf;
    buf.append(data_string.data(), data_string.length());

    auto input = make_iobuf_input_stream(std::move(buf));
    cache_service.put(KEY, input).get();

    bool file_exists = ss::file_exists((CACHE_DIR / KEY).native()).get();
    BOOST_CHECK(file_exists);
}

FIXTURE_TEST(get_after_put, cache_test_fixture) {
    ss::sstring data_string;
    while (data_string.length() < 4096) {
        data_string += SAMPLE_STRING1;
    }
    iobuf buf;
    buf.append(data_string.data(), data_string.length());

    auto input = make_iobuf_input_stream(std::move(buf));
    cache_service.put(KEY, input).get();

    std::optional<cloud_storage::cache_item> returned_item
      = cache_service.get(KEY).get();
    BOOST_REQUIRE(returned_item);
    BOOST_CHECK_EQUAL(returned_item->size, data_string.length());

    auto read_buf
      = returned_item->body.read_exactly(data_string.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string);
    BOOST_CHECK(!returned_item->body.read().get().get());
}

FIXTURE_TEST(put_rewrites_file, cache_test_fixture) {
    ss::sstring data_string1, data_string2;
    while (data_string1.length() < 4096) {
        data_string1 += SAMPLE_STRING1;
    }
    while (data_string2.length() < 4096) {
        data_string2 += SAMPLE_STRING2;
    }
    iobuf buf1, buf2;
    buf1.append(data_string1.data(), data_string1.length());
    buf2.append(data_string2.data(), data_string2.length());

    auto input1 = make_iobuf_input_stream(std::move(buf1));
    cache_service.put(KEY, input1).get();

    auto input2 = make_iobuf_input_stream(std::move(buf2));
    cache_service.put(KEY, input2).get();

    std::optional<cloud_storage::cache_item> returned_item
      = cache_service.get(KEY).get();
    BOOST_REQUIRE(returned_item);
    BOOST_CHECK_EQUAL(returned_item->size, data_string2.length());

    auto read_buf
      = returned_item->body.read_exactly(data_string2.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string2);
    BOOST_CHECK(!returned_item->body.read().get().get());
}

FIXTURE_TEST(get_missing_file, cache_test_fixture) {
    std::optional<cloud_storage::cache_item> returned_item
      = cache_service.get(WRONG_KEY).get();

    BOOST_CHECK(!returned_item);
}

FIXTURE_TEST(missing_file_not_cached, cache_test_fixture) {
    bool is_cached = cache_service.is_cached(WRONG_KEY).get();

    BOOST_CHECK(!is_cached);
}

FIXTURE_TEST(is_cached_after_put_success, cache_test_fixture) {
    iobuf buf;
    auto input = make_iobuf_input_stream(std::move(buf));
    cache_service.put(KEY, input).get();

    bool is_cached = cache_service.is_cached(KEY).get();

    BOOST_CHECK(is_cached);
}

FIXTURE_TEST(after_invalidate_is_not_cached, cache_test_fixture) {
    iobuf buf;
    auto input = make_iobuf_input_stream(std::move(buf));
    cache_service.put(KEY, input).get();
    cache_service.invalidate(KEY).get();

    bool is_cached = cache_service.is_cached(KEY).get();

    BOOST_CHECK(!is_cached);
}

FIXTURE_TEST(invalidate_missing_file_ok, cache_test_fixture) {
    BOOST_CHECK_NO_THROW(cache_service.invalidate(WRONG_KEY).get());
}
