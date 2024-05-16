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

#include "base/vlog.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/tmp_file.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;
using namespace cloud_storage;

static constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

struct cloud_storage_fixture : s3_imposter_fixture {
    static const cloud_storage_clients::bucket_name bucket;
    cloud_storage_fixture();

    ~cloud_storage_fixture();

    cloud_storage_fixture(const cloud_storage_fixture&) = delete;
    cloud_storage_fixture operator=(const cloud_storage_fixture&) = delete;
    cloud_storage_fixture(cloud_storage_fixture&&) = delete;
    cloud_storage_fixture operator=(cloud_storage_fixture&&) = delete;

    ss::tmp_dir tmp_directory;
    ss::sharded<cloud_storage::cache> cache;
    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<remote> api;
};
