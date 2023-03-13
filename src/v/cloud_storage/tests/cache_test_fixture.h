/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "bytes/iobuf.h"
#include "cloud_storage/cache_service.h"
#include "config/property.h"
#include "seastarx.h"
#include "test_utils/tmp_dir.h"
#include "units.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <boost/filesystem/operations.hpp>

#include <chrono>
#include <filesystem>

using namespace std::chrono_literals;

static inline std::filesystem::path get_cache_dir(std::filesystem::path p) {
    return p / "test_cache_dir";
}

// In cloud_storage namespace so we can befriend this fixture from
// the class under test.
namespace cloud_storage {

class cache_test_fixture {
public:
    const std::filesystem::path KEY{"abc001/test_topic/test_cache_file.txt"};
    const std::filesystem::path KEY2{"abc002/test_topic2/test_cache_file2.txt"};
    const std::filesystem::path TEMP_KEY{
      "abc002/test_topic2/test_cache_file2.txt_0_0.part"};
    const std::filesystem::path WRONG_KEY{"abc001/test_topic/wrong_key.txt"};

    temporary_dir test_dir;
    const std::filesystem::path CACHE_DIR;
    ss::sharded<cloud_storage::cache> sharded_cache;

    cache_test_fixture()
      : test_dir("test_cache_dir")
      , CACHE_DIR(get_cache_dir(test_dir.get_path())) {
        cache::initialize(CACHE_DIR).get();
        sharded_cache
          .start(CACHE_DIR, config::mock_binding<uint64_t>(1_MiB + 500_KiB))
          .get();
        sharded_cache
          .invoke_on_all([](cloud_storage::cache& c) { return c.start(); })
          .get();
    }

    ~cache_test_fixture() {
        sharded_cache.stop().get();
        test_dir.remove().get();
    }

    ss::sstring create_data_string(char symbol_to_fill, uint64_t size) {
        ss::sstring data_string;
        data_string.resize(size, symbol_to_fill);

        return data_string;
    }

    void put_into_cache(auto data_string, auto key) {
        iobuf buf;
        buf.append(data_string.data(), data_string.length());

        auto input = make_iobuf_input_stream(std::move(buf));
        sharded_cache.local().put(key, input).get();
    }

    ss::future<> clean_up_at_start() {
        return sharded_cache.local().clean_up_at_start();
    }

    void trim_cache() {
        sharded_cache
          .invoke_on(
            ss::shard_id{0}, [](cloud_storage::cache& c) { return c.trim(); })
          .get();
    }
};

} // namespace cloud_storage
