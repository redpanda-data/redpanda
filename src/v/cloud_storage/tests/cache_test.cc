/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/units.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cache_test_fixture.h"
#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/cache_service.h"
#include "random/generators.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"
#include "utils/file_io.h"
#include "utils/human.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/file.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <cstdint>
#include <fstream>
#include <optional>
#include <stdexcept>

using namespace cloud_storage;

static ss::logger test_log("cache_test_logger");

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
      = sharded_cache.local().get(KEY).get();
    BOOST_REQUIRE(returned_item);
    BOOST_CHECK_EQUAL(returned_item->size, data_string.length());

    auto stream = ss::make_file_input_stream(returned_item->body);

    auto read_buf = stream.read_exactly(data_string.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string);
    BOOST_CHECK(!stream.read().get().get());
    stream.close().get();
}

FIXTURE_TEST(stremaing_get_after_put, cache_test_fixture) {
    auto data_string = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string, KEY);

    auto stream
      = sharded_cache.local().get(KEY, seastar::default_priority_class()).get();
    BOOST_REQUIRE(stream);
    BOOST_CHECK_EQUAL(stream->size, data_string.length());

    auto read_buf = stream->body.read_exactly(data_string.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string);
    BOOST_CHECK(!stream->body.read().get().get());
    stream->body.close().get();
}

FIXTURE_TEST(put_rewrites_file, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    auto data_string2 = create_data_string('b', 1_MiB + 1_KiB);
    put_into_cache(data_string2, KEY);

    std::optional<cloud_storage::cache_item> returned_item
      = sharded_cache.local().get(KEY).get();
    BOOST_REQUIRE(returned_item);
    BOOST_CHECK_EQUAL(returned_item->size, data_string2.length());

    auto body = ss::make_file_input_stream(returned_item->body);

    auto read_buf = body.read_exactly(data_string2.length()).get();
    BOOST_CHECK_EQUAL(
      std::string_view(read_buf.get(), read_buf.size()), data_string2);
    BOOST_CHECK(!body.read().get().get());
    body.close().get();
}

FIXTURE_TEST(get_missing_file, cache_test_fixture) {
    std::optional<cloud_storage::cache_item> returned_item
      = sharded_cache.local().get(WRONG_KEY).get();

    BOOST_CHECK(!returned_item);
}

FIXTURE_TEST(missing_file_not_cached, cache_test_fixture) {
    auto is_cached = sharded_cache.local().is_cached(WRONG_KEY).get();

    BOOST_CHECK_EQUAL(is_cached, cache_element_status::not_available);
}

FIXTURE_TEST(is_cached_after_put_success, cache_test_fixture) {
    iobuf buf;
    auto reservation
      = sharded_cache.local().reserve_space(buf.size_bytes(), 1).get();
    auto input = make_iobuf_input_stream(std::move(buf));
    sharded_cache.local().put(KEY, input, reservation).get();

    auto is_cached = sharded_cache.local().is_cached(KEY).get();

    BOOST_CHECK_EQUAL(is_cached, cache_element_status::available);
}

FIXTURE_TEST(after_invalidate_is_not_cached, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    sharded_cache.local().invalidate(KEY).get();

    auto is_cached = sharded_cache.local().is_cached(KEY).get();
    BOOST_CHECK_EQUAL(is_cached, cache_element_status::not_available);
}

FIXTURE_TEST(invalidate_missing_file_ok, cache_test_fixture) {
    BOOST_CHECK_NO_THROW(sharded_cache.local().invalidate(WRONG_KEY).get());
}

FIXTURE_TEST(empty_cache_nothing_deleted, cache_test_fixture) {
    ss::sleep(ss::lowres_clock::duration(2s)).get();

    BOOST_CHECK_EQUAL(0, sharded_cache.local().get_total_cleaned());
}

FIXTURE_TEST(files_up_to_max_cache_size_not_deleted, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    ss::sleep(ss::lowres_clock::duration(2s)).get();

    BOOST_CHECK_EQUAL(0, sharded_cache.local().get_total_cleaned());
}

FIXTURE_TEST(file_bigger_than_max_cache_size_deleted, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 2_MiB + 1_KiB);
    put_into_cache(data_string1, KEY);

    trim_cache();

    BOOST_CHECK_EQUAL(2_MiB + 1_KiB, sharded_cache.local().get_total_cleaned());
}

FIXTURE_TEST(
  files_bigger_than_max_cache_size_oldest_deleted, cache_test_fixture) {
    // put() calls are _not_ strictly capacity checked: we are circumventing
    // the capacity checks that would usually happen in reserve_space()
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    put_into_cache(data_string1, KEY, true);
    auto data_string2 = create_data_string('b', 1_MiB + 1_KiB);
    ss::sleep(1s).get(); // Sleep long enough to ensure low res atimes differ
    put_into_cache(data_string1, KEY2, true);

    // Give backgrounded futures a chance to execute
    ss::sleep(ss::lowres_clock::duration(2s)).get();

    // Our direct put() calls succeed and violate the cache capacity
    BOOST_REQUIRE(ss::file_exists((CACHE_DIR / KEY).native()).get());
    BOOST_REQUIRE(ss::file_exists((CACHE_DIR / KEY2).native()).get());

    // A trim will delete the oldest.  We call this explicitly: ordinarily
    // it would get called either periodically, or in the reserve_space path.
    trim_cache();

    BOOST_REQUIRE(!ss::file_exists((CACHE_DIR / KEY).native()).get());
    BOOST_REQUIRE(ss::file_exists((CACHE_DIR / KEY2).native()).get());

    BOOST_CHECK_EQUAL(1_MiB + 1_KiB, sharded_cache.local().get_total_cleaned());
}

FIXTURE_TEST(cannot_put_tmp_file, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_KiB);
    BOOST_CHECK_THROW(
      put_into_cache(data_string1, TEMP_KEY), std::invalid_argument);
}

FIXTURE_TEST(invalidate_cleans_directory, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    const std::filesystem::path unique_prefix_key{
      "unique_prefix/test_topic/test_cache_file.txt"};
    put_into_cache(data_string1, unique_prefix_key);

    sharded_cache.local().invalidate(unique_prefix_key).get();

    BOOST_CHECK(
      !ss::file_exists((CACHE_DIR / unique_prefix_key).native()).get());
    BOOST_CHECK(
      !ss::file_exists((CACHE_DIR / "unique_prefix/test_topic").native())
         .get());
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / "unique_prefix").native()).get());
    BOOST_CHECK(ss::file_exists(CACHE_DIR.native()).get());
}

FIXTURE_TEST(eviction_cleans_directory, cache_test_fixture) {
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    const std::filesystem::path key1{"a/b/c/first_topic/file1.txt"};
    // this file will be evicted
    put_into_cache(data_string1, key1);

    auto data_string2 = create_data_string('b', 1_MiB + 1_KiB);
    ss::sleep(1s).get();
    const std::filesystem::path key2{"a/b/c/second_topic/file2.txt"};
    // this file will not be evicted
    put_into_cache(data_string2, key2);

    trim_cache();

    BOOST_CHECK_EQUAL(1_MiB + 1_KiB, sharded_cache.local().get_total_cleaned());
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / key1).native()).get());
    BOOST_CHECK(
      !ss::file_exists((CACHE_DIR / "a/b/c/first_topic").native()).get());
    BOOST_CHECK(ss::file_exists((CACHE_DIR / "a/b/c").native()).get());
    BOOST_CHECK(ss::file_exists(CACHE_DIR.native()).get());
}

FIXTURE_TEST(invalidate_outside_cache_dir_throws, cache_test_fixture) {
    // make sure the cache directory is empty to get reliable results
    ss::recursive_touch_directory(CACHE_DIR.native()).get();
    auto target_dir = ss::open_directory(CACHE_DIR.native()).get();
    target_dir
      .list_directory([this](const ss::directory_entry& entry) -> ss::future<> {
          auto entry_path = std::filesystem::path(CACHE_DIR)
                            / std::filesystem::path(entry.name);
          return ss::recursive_remove_directory(entry_path.native());
      })
      .done()
      .get();
    target_dir.close().get();

    auto key = std::filesystem::path("../outside_cache/file.txt");
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::exclusive;

    auto dir_path = (CACHE_DIR / key).remove_filename();
    ss::recursive_touch_directory(dir_path.string()).get();
    auto tmp_cache_file
      = ss::open_file_dma((CACHE_DIR / key).native(), flags).get();

    BOOST_CHECK_THROW(
      sharded_cache.local().invalidate(key).get(), std::invalid_argument);
    BOOST_CHECK(ss::file_exists((CACHE_DIR / key).native()).get());
}

FIXTURE_TEST(invalidate_prefix_outside_cache_dir_throws, cache_test_fixture) {
    // make sure the cache directory is empty to get reliable results
    ss::recursive_touch_directory(CACHE_DIR.native()).get();
    auto target_dir = ss::open_directory(CACHE_DIR.native()).get();
    target_dir
      .list_directory([this](const ss::directory_entry& entry) -> ss::future<> {
          auto entry_path = std::filesystem::path(CACHE_DIR)
                            / std::filesystem::path(entry.name);
          return ss::recursive_remove_directory(entry_path.native());
      })
      .done()
      .get();
    target_dir.close().get();

    // CACHE_DIR is "test_cache_dir"
    auto key = std::filesystem::path("../test_cache_dir_bar/file.txt");
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::exclusive;
    auto dir_path = (CACHE_DIR / key).remove_filename();
    ss::recursive_touch_directory(dir_path.string()).get();
    auto tmp_cache_file
      = ss::open_file_dma((CACHE_DIR / key).native(), flags).get();

    BOOST_CHECK_THROW(
      sharded_cache.local().invalidate(key).get(), std::invalid_argument);
    BOOST_CHECK(ss::file_exists((CACHE_DIR / key).native()).get());
}

FIXTURE_TEST(put_outside_cache_dir_throws, cache_test_fixture) {
    // make sure the cache directory is empty to get reliable results
    ss::recursive_touch_directory(CACHE_DIR.native()).get();
    auto target_dir = ss::open_directory(CACHE_DIR.native()).get();
    target_dir
      .list_directory([this](const ss::directory_entry& entry) -> ss::future<> {
          auto entry_path = std::filesystem::path(CACHE_DIR)
                            / std::filesystem::path(entry.name);
          return ss::recursive_remove_directory(entry_path.native());
      })
      .done()
      .get();
    target_dir.close().get();

    // CACHE_DIR is "test_cache_dir"
    auto key = std::filesystem::path("../test_cache_dir_put/file.txt");
    auto data_string1 = create_data_string('a', 1_MiB + 1_KiB);
    iobuf buf;
    buf.append(data_string1.data(), data_string1.length());
    auto reservation
      = sharded_cache.local().reserve_space(buf.size_bytes(), 1).get();
    auto input = make_iobuf_input_stream(std::move(buf));

    BOOST_CHECK_EXCEPTION(
      sharded_cache.local().put(key, input, reservation).get(),
      std::invalid_argument,
      [](const std::invalid_argument& e) {
          return std::string(e.what()).find(
                   "test_cache_dir_put/file.txt, which is outside of cache_dir")
                 != std::string::npos;
      });
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / key).native()).get());
}

static std::chrono::system_clock::time_point make_ts(uint64_t val) {
    auto seconds = std::chrono::seconds(val);
    return std::chrono::system_clock::time_point(seconds);
}

SEASTAR_THREAD_TEST_CASE(test_access_time_tracker) {
    access_time_tracker cm;

    std::vector<std::chrono::system_clock::time_point> timestamps = {
      make_ts(1653000000),
      make_ts(1653000001),
      make_ts(1653000002),
      make_ts(1653000003),
      make_ts(1653000004),
      make_ts(1653000005),
      make_ts(1653000006),
      make_ts(1653000007),
      make_ts(1653000008),
      make_ts(1653000009),
    };

    std::vector<ss::sstring> names = {
      "key0",
      "key1",
      "key2",
      "key3",
      "key4",
      "key5",
      "key6",
      "key7",
      "key8",
      "key9",
    };

    for (int i = 0; i < 10; i++) {
        cm.add(names[i], timestamps[i], i);
    }

    for (unsigned i = 0; i < 10; i++) {
        auto ts = cm.get(names[i]);
        BOOST_REQUIRE(ts.has_value());
        BOOST_REQUIRE(ts->time_point() == timestamps[i]);
        BOOST_REQUIRE(ts->size == i);
    }
}

static access_time_tracker serde_roundtrip(
  access_time_tracker& t, tracker_version version = tracker_version::v2) {
    // Round trip
    iobuf serialized;
    auto out_stream = make_iobuf_ref_output_stream(serialized);
    t.write(out_stream, version).get();
    out_stream.flush().get();

    access_time_tracker out;
    try {
        auto serialized_copy = serialized.copy();
        auto in_stream = make_iobuf_input_stream(std::move(serialized_copy));
        out.read(in_stream).get();

    } catch (...) {
        // Dump serialized buffer on failure.
        std::cerr << serialized.hexdump(4096) << std::endl;
        throw;
    }

    return out;
}

SEASTAR_THREAD_TEST_CASE(test_access_time_tracker_serializer) {
    access_time_tracker in;

    std::vector<std::chrono::system_clock::time_point> timestamps = {
      make_ts(0xbeefed00),
      make_ts(0xbeefed01),
      make_ts(0xbeefed02),
      make_ts(0xbeefed03),
      make_ts(0xbeefed04),
      make_ts(0xbeefed05),
      make_ts(0xbeefed06),
      make_ts(0xbeefed07),
      make_ts(0xbeefed08),
      make_ts(0xbeefed09),
    };

    std::vector<ss::sstring> names = {
      "key0",
      "key1",
      "key2",
      "key3",
      "key4",
      "key5",
      "key6",
      "key7",
      "key8",
      "key9",
    };

    for (int i = 0; i < 10; i++) {
        in.add(names[i], timestamps[i], i);
    }

    auto out = serde_roundtrip(in);

    for (unsigned i = 0; i < timestamps.size(); i++) {
        auto ts = out.get(names[i]);
        BOOST_REQUIRE(ts.has_value());
        BOOST_REQUIRE(ts->time_point() == timestamps[i]);
        BOOST_REQUIRE(ts->size == i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_access_time_tracker_serializer_large) {
    access_time_tracker in;

    // Serialization uses chunking of 2048 items: use more items than this
    // to verify the chunking code works properly;
    uint32_t item_count = 7777;
    for (uint32_t i = 0; i < item_count; i++) {
        in.add(fmt::format("key{:08x}", i), make_ts(i), i);
    }

    auto out = serde_roundtrip(in);
    BOOST_REQUIRE_EQUAL(out.size(), item_count);
    for (size_t i = 0; i < item_count; ++i) {
        const auto entry = in.get(fmt::format("key{:08x}", i));
        BOOST_REQUIRE(entry.has_value());
        BOOST_REQUIRE_EQUAL(entry->size, i);
        BOOST_REQUIRE_EQUAL(entry->atime_sec, i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_access_time_tracker_read_skipped_on_old_version) {
    access_time_tracker in;
    in.add("key", make_ts(0), 0);
    auto out = serde_roundtrip(in, tracker_version::v1);
    BOOST_REQUIRE_EQUAL(out.size(), 0);
}

/**
 * Validate that .part files and empty directories are deleted if found during
 * the startup walk of the cache.
 */
FIXTURE_TEST(test_clean_up_on_start, cache_test_fixture) {
    // A temporary file, this should be deleted on startup
    put_into_cache(create_data_string('a', 1_KiB), KEY);
    std::filesystem::path tmp_key = KEY;
    tmp_key.replace_extension(".part");
    ss::rename_file((CACHE_DIR / KEY).native(), (CACHE_DIR / tmp_key).native())
      .get();

    // A normal looking segment, we'll check this isn't deleted
    put_into_cache(create_data_string('b', 1_KiB), KEY);

    // An empty directory, this should be deleted
    auto empty_dir_path = std::filesystem::path{CACHE_DIR / "empty_dir"};
    ss::make_directory(empty_dir_path.native()).get();

    // A non-empty-directory, this should be preserved
    auto populated_dir_path = CACHE_DIR / "populated_dir";
    ss::make_directory(populated_dir_path.native()).get();
    write_fully(populated_dir_path / "populated_file", {}).get();

    clean_up_at_start().get();

    BOOST_CHECK(ss::file_exists((CACHE_DIR / KEY).native()).get());
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / tmp_key).native()).get());
    BOOST_CHECK(!ss::file_exists(empty_dir_path.native()).get());
    BOOST_CHECK(ss::file_exists(populated_dir_path.native()).get());
    BOOST_CHECK_EQUAL(get_object_count(), 2);
}

/**
 * Validate that the empty directory deletion code in clean_up_at_start
 * does not consider the cache directory itself an empty directory
 * to be deleted.
 */
FIXTURE_TEST(test_clean_up_on_start_empty, cache_test_fixture) {
    clean_up_at_start().get();

    BOOST_CHECK(ss::file_exists(CACHE_DIR.native()).get());
}

/**
 * Given a cache dir populated with files which are filtered out by fast trim,
 * validate that a failing fast trim should be followed up by an exhaustive trim
 * and clean up the required object count.
 */
FIXTURE_TEST(test_exhaustive_trim_runs_after_fast_trim, cache_test_fixture) {
    std::vector<std::filesystem::path> indices;
    const auto count_indices = 5;
    indices.reserve(count_indices);

    for (auto i = 0; i < count_indices; ++i) {
        indices.emplace_back(CACHE_DIR / fmt::format("{}.index", i, i, i));
        std::ofstream f{indices.back()};
        f.flush();
    }

    BOOST_REQUIRE(
      std::all_of(indices.cbegin(), indices.cend(), [](const auto& path) {
          return std::filesystem::exists(path);
      }));

    // Make cache service scan the disk for objects
    clean_up_at_start().get();

    // Only allow the access time tracker to remain on disk.
    trim_cache(std::nullopt, 1);

    BOOST_REQUIRE(
      std::all_of(indices.cbegin(), indices.cend(), [](const auto& path) {
          return !std::filesystem::exists(path);
      }));
}

FIXTURE_TEST(test_log_segment_cleanup, cache_test_fixture) {
    std::vector<std::filesystem::path> objects{
      CACHE_DIR / "test.log.1",
      CACHE_DIR / "test.log.1.index",
      CACHE_DIR / "test.log.1.tx",
      CACHE_DIR / "test.log",
      CACHE_DIR / "test.log.index",
      CACHE_DIR / "test.log.tx"};
    for (const auto& obj : objects) {
        std::ofstream f{obj};
        f.flush();
    }

    // An un-removable file makes sure the fast trim will have to remove the two
    // segment files.
    {
        std::ofstream f{CACHE_DIR / "accesstime"};
        f.flush();
    }

    BOOST_REQUIRE(
      std::all_of(objects.cbegin(), objects.cend(), [](const auto& path) {
          return std::filesystem::exists(path);
      }));

    clean_up_at_start().get();

    // With this limit all of the index+tx files should not have to be removed,
    // but fast trim will remove all of these files after removing the two
    // segments.
    trim_cache(std::nullopt, 4);

    BOOST_REQUIRE(
      std::all_of(objects.cbegin(), objects.cend(), [](const auto& path) {
          return !std::filesystem::exists(path);
      }));
}

FIXTURE_TEST(test_cache_carryover_trim, cache_test_fixture) {
    scoped_config cfg;
    cfg.get("cloud_storage_cache_trim_carryover_bytes")
      .set_value(uint32_t{256_KiB});

    std::string write_buf(1_MiB, ' ');
    random_generators::fill_buffer_randomchars(
      write_buf.data(), write_buf.size());
    size_t bytes_used = 0;
    size_t num_objects = 0;
    std::vector<std::filesystem::path> object_keys;
    for (int i = 0; i < 10; i++) {
        object_keys.emplace_back(fmt::format("test_{}.log.1", i));
        std::ofstream segment{CACHE_DIR / object_keys.back()};
        segment.write(
          write_buf.data(), static_cast<std::streamsize>(write_buf.size()));
        segment.flush();
        bytes_used += write_buf.size();
        num_objects++;
        object_keys.emplace_back(fmt::format("test_{}.log.1.index", i));
        std::ofstream index{CACHE_DIR / object_keys.back()};
        index.write(
          write_buf.data(), static_cast<std::streamsize>(write_buf.size()));
        index.flush();
        bytes_used += write_buf.size();
        num_objects++;
        object_keys.emplace_back(fmt::format("test_{}.log.1.tx", i));
        std::ofstream tx{CACHE_DIR / object_keys.back()};
        tx.write(
          write_buf.data(), static_cast<std::streamsize>(write_buf.size()));
        tx.flush();
        bytes_used += write_buf.size();
        num_objects++;
    }
    // Account all files in the cache (30 MiB).
    clean_up_at_start().get();
    for (const auto& key : object_keys) {
        // Touch every object so they have access times assigned to them
        auto item = sharded_cache.local().get(key).get();
        item->body.close().get();
    }

    // Force trim to create a carryover list.
    // Nothing is deleted by the trim.
    vlog(test_log.info, "Initial trim");
    auto cache_size_target_bytes = bytes_used;
    auto cache_size_target_objects = num_objects;
    trim_cache(cache_size_target_bytes, cache_size_target_objects);

    // Start trim using only carryover data
    auto before_bytes = sharded_cache.local().get_usage_bytes();
    auto before_objects = sharded_cache.local().get_usage_objects();
    vlog(
      test_log.info,
      "Trim {}, {} bytes used, {} objects",
      human::bytes(bytes_used),
      human::bytes(before_bytes),
      before_objects);
    BOOST_REQUIRE(before_bytes > 0);
    BOOST_REQUIRE(before_objects > 0);

    // Note that 'trim_carryover' accepts number of bytes/objects
    // that has to be deleted. This is the opposite of how 'trim'
    // method behaves which accepts target size for the cache.
    // This behavior is similar to 'trim_fast'.
    auto bytes_to_delete = before_bytes;
    auto objects_to_delete = before_objects;
    trim_carryover(bytes_to_delete, objects_to_delete);

    // At this point we should be able to delete all objects
    auto after_bytes = sharded_cache.local().get_usage_bytes();
    auto after_objects = sharded_cache.local().get_usage_objects();
    vlog(
      test_log.info,
      "After trim {} bytes used, {} objects",
      human::bytes(after_bytes),
      after_objects);
    BOOST_REQUIRE_EQUAL(after_bytes, 0);
    BOOST_REQUIRE_EQUAL(after_objects, 0);
}

FIXTURE_TEST(bucketing_works_with_old_objects, cache_test_fixture) {
    auto data_string = create_data_string('a', 1_KiB);
    const std::filesystem::path key1{"80000001/a/b/c/d/file1.txt"};
    const std::filesystem::path key2{"80000002/a/b/c/d/file2.txt"};
    const std::filesystem::path key2_rehashed{"2/a/b/c/d/file2.txt"};
    put_into_cache(data_string, key1);

    scoped_config cfg;
    cfg.get("cloud_storage_cache_num_buckets").set_value((uint32_t)16);
    // Now key2 should be mapped to "2/a/b/c/d/file2.txt"

    ss::sleep(1s).get();
    put_into_cache(data_string, key2);

    BOOST_CHECK(ss::file_exists((CACHE_DIR / key1).native()).get());
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / key2).native()).get());
    BOOST_CHECK(ss::file_exists((CACHE_DIR / key2_rehashed).native()).get());

    // check that GET works
    auto r1 = sharded_cache.local().get(key1).get();
    auto r2 = sharded_cache.local().get(key2).get();
    BOOST_CHECK(r1.has_value());
    BOOST_CHECK(r2.has_value());

    // check is_cached
    BOOST_CHECK(
      sharded_cache.local().is_cached(key1).get()
      == cache_element_status::available);
    BOOST_CHECK(
      sharded_cache.local().is_cached(key2).get()
      == cache_element_status::available);

    // check cache invalidation
    sharded_cache.local().invalidate(key1).get();
    sharded_cache.local().invalidate(key2).get();
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / key1).native()).get());
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / key2).native()).get());
    BOOST_CHECK(!ss::file_exists((CACHE_DIR / key2_rehashed).native()).get());
}

FIXTURE_TEST(test_background_maybe_trim, cache_test_fixture) {
    std::string write_buf(1_KiB, ' ');
    random_generators::fill_buffer_randomchars(
      write_buf.data(), write_buf.size());
    size_t num_objects = 100;
    std::vector<std::filesystem::path> object_keys;
    for (unsigned i = 0; i < num_objects; i++) {
        object_keys.emplace_back(fmt::format("test_{}.log.1", i));
        std::ofstream segment{CACHE_DIR / object_keys.back()};
        segment.write(
          write_buf.data(), static_cast<std::streamsize>(write_buf.size()));
        segment.flush();
    }

    // Account all files in the cache (100 MiB).
    clean_up_at_start().get();
    BOOST_REQUIRE_EQUAL(get_object_count(), 100);

    for (const auto& key : object_keys) {
        // Touch every object so they have access times assigned to them
        auto item = sharded_cache.local().get(key).get();
        item->body.close().get();
    }
    BOOST_REQUIRE_EQUAL(get_object_count(), 100);

    set_trim_thresholds(100.0, 50.0, 100);

    // Do a put which should trigger a background trim and reduce the
    // object count to 40, followed by a put that will increase it to 41.
    auto data_string = create_data_string('a', 1_KiB);
    put_into_cache(data_string, KEY);
    wait_for_trim();

    // 41 because we set a trigger of 50. That means that trim is triggered at
    // 50%, but the low water mark adjusts this by an additional 80%, 50% * 80%
    // = 40%. +1 for the object we just added.
    BOOST_REQUIRE_EQUAL(get_object_count(), 41);
}

FIXTURE_TEST(test_tracker_sync_only_remove, cache_test_fixture) {
    put_into_cache(create_data_string('a', 1_KiB), KEY);
    auto& cache = sharded_cache.local();
    cache.get(KEY).get();

    const auto full_key_path = CACHE_DIR / KEY;

    const auto& t = tracker();
    BOOST_REQUIRE_EQUAL(t.size(), 1);

    {
        const auto entry = t.get(full_key_path.native());
        BOOST_REQUIRE(entry.has_value());
        BOOST_REQUIRE_EQUAL(entry->size, 1_KiB);
        BOOST_REQUIRE_EQUAL(cache.get_usage_bytes(), 1_KiB);
        BOOST_REQUIRE_EQUAL(cache.get_usage_objects(), 1);
    }

    ss::remove_file(full_key_path.native()).get();

    {
        const auto entry = t.get(full_key_path.native());
        BOOST_REQUIRE(entry.has_value());
        BOOST_REQUIRE_EQUAL(entry->size, 1_KiB);
        BOOST_REQUIRE_EQUAL(cache.get_usage_bytes(), 1_KiB);
        BOOST_REQUIRE_EQUAL(cache.get_usage_objects(), 1);
    }

    sync_tracker();

    BOOST_REQUIRE_EQUAL(t.size(), 0);
    BOOST_REQUIRE(!t.get(full_key_path.native()).has_value());
    BOOST_REQUIRE_EQUAL(cache.get_usage_bytes(), 0);
    BOOST_REQUIRE_EQUAL(cache.get_usage_objects(), 0);
}

FIXTURE_TEST(test_tracker_sync_add_remove, cache_test_fixture) {
    put_into_cache(create_data_string('a', 1_KiB), KEY);
    auto& cache = sharded_cache.local();
    const auto full_key_path = CACHE_DIR / KEY;
    const auto& t = tracker();
    BOOST_REQUIRE_EQUAL(t.size(), 0);
    sync_tracker(access_time_tracker::add_entries_t::yes);
    BOOST_REQUIRE_EQUAL(t.size(), 1);
    BOOST_REQUIRE(t.get(full_key_path.native()).has_value());
    BOOST_REQUIRE_EQUAL(cache.get_usage_bytes(), 1024);
    BOOST_REQUIRE_EQUAL(cache.get_usage_objects(), 1);
}
