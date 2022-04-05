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
#include "seastarx.h"
#include "units.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include <boost/filesystem/operations.hpp>

#include <chrono>
#include <filesystem>

using namespace cloud_storage;
using namespace std::chrono_literals;

class cache_test_fixture {
public:
    const std::filesystem::path KEY{"abc001/test_topic/test_cache_file.txt"};
    const std::filesystem::path KEY2{"abc002/test_topic2/test_cache_file2.txt"};
    const std::filesystem::path TEMP_KEY{
      "abc002/test_topic2/test_cache_file2.txt_0_0.part"};
    const std::filesystem::path WRONG_KEY{"abc001/test_topic/wrong_key.txt"};
    const std::filesystem::path CACHE_DIR{"test_cache_dir"};

    cloud_storage::cache cache_service;

    cache_test_fixture()
      : cache_service(
        CACHE_DIR, 1_MiB + 500_KiB, ss::lowres_clock::duration(1s)) {
        cache_service.start().get();
    }

    ~cache_test_fixture() {
        cache_service.stop().get();
        boost::filesystem::remove_all(CACHE_DIR.native());
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
        cache_service.put(key, input).get();
    }
};
