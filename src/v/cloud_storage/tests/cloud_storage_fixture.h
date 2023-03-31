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

#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "test_utils/fixture.h"
#include "vlog.h"

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
    cloud_storage_fixture() {
        tmp_directory.create().get();
        cache
          .start(
            tmp_directory.get_path(),
            config::mock_binding<uint64_t>(1024 * 1024 * 1024))
          .get();

        cache.invoke_on_all([](cloud_storage::cache& c) { return c.start(); })
          .get();

        auto conf = get_configuration();
        pool
          .start(
            10, ss::sharded_parameter([this] { return get_configuration(); }))
          .get();
        api
          .start(
            std::ref(pool),
            ss::sharded_parameter([this] { return get_configuration(); }),
            ss::sharded_parameter([] { return config_file; }))
          .get();
        api
          .invoke_on_all([](cloud_storage::remote& api) { return api.start(); })
          .get();
    }

    ~cloud_storage_fixture() {
        if (!pool.local().shutdown_initiated()) {
            pool.local().shutdown_connections();
        }
        api.stop().get();
        pool.stop().get();
        cache.stop().get();
        tmp_directory.remove().get();
    }

    cloud_storage_fixture(const cloud_storage_fixture&) = delete;
    cloud_storage_fixture operator=(const cloud_storage_fixture&) = delete;
    cloud_storage_fixture(cloud_storage_fixture&&) = delete;
    cloud_storage_fixture operator=(cloud_storage_fixture&&) = delete;

    ss::tmp_dir tmp_directory;
    ss::sharded<cloud_storage::cache> cache;
    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<remote> api;
};
