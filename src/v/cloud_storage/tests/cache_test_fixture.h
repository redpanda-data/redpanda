/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "cloud_storage/cache_service.h"
#include "seastarx.h"

#include <seastar/core/seastar.hh>

#include <boost/filesystem/operations.hpp>

#include <filesystem>

using namespace cloud_storage;

class cache_test_fixture {
public:
    const std::filesystem::path KEY{"abc001/test_topic/test_cache_file.txt"};
    const std::filesystem::path WRONG_KEY{"abc001/test_topic/wrong_key.txt"};

    const std::filesystem::path CACHE_DIR{"test_cache_dir"};

    const ss::sstring SAMPLE_STRING1 = ss::sstring("Test data");
    const ss::sstring SAMPLE_STRING2 = ss::sstring("New test data!");

    cloud_storage::cache cache_service;

    cache_test_fixture()
      : cache_service(CACHE_DIR) {
        cache_service.start().get();
    }

    ~cache_test_fixture() {
        boost::filesystem::remove_all(CACHE_DIR.native());
        cache_service.stop().get();
    }
};
