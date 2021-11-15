/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/cache_service.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/tmp_file.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;
using namespace cloud_storage;

struct cloud_storage_fixture : s3_imposter_fixture {
    cloud_storage_fixture() {
        tmp_directory.create().get();
        constexpr size_t cache_size = 1024 * 1024 * 1024;
        const ss::lowres_clock::duration cache_duration = 1000s;
        cache = ss::make_shared<cloud_storage::cache>(
          tmp_directory.get_path(), cache_size, cache_duration);
        cache->start().get();
    }

    ~cloud_storage_fixture() {
        cache->stop().get();
        tmp_directory.remove().get();
    }

    cloud_storage_fixture(const cloud_storage_fixture&) = delete;
    cloud_storage_fixture operator=(const cloud_storage_fixture&) = delete;
    cloud_storage_fixture(cloud_storage_fixture&&) = delete;
    cloud_storage_fixture operator=(cloud_storage_fixture&&) = delete;

    ss::tmp_dir tmp_directory;
    ss::shared_ptr<cloud_storage::cache> cache;
};
