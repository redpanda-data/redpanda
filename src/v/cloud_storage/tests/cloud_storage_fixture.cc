/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_fixture.h"

#include "cloud_storage/tests/s3_imposter.h"

cloud_storage_fixture::cloud_storage_fixture() {
    config::shard_local_cfg()
      .cloud_storage_max_segment_readers_per_shard.reset();
    config::shard_local_cfg()
      .cloud_storage_max_partition_readers_per_shard.reset();
    config::shard_local_cfg()
      .cloud_storage_max_materialized_segments_per_shard.reset();

    tmp_directory.create().get();
    cache
      .start(
        tmp_directory.get_path(),
        30_GiB, // disk size
        config::mock_binding<double>(0.0),
        config::mock_binding<uint64_t>(1024 * 1024 * 1024),
        config::mock_binding<std::optional<double>>(std::nullopt),
        config::mock_binding<uint32_t>(100000))
      .get();

    cache.invoke_on_all([](cloud_storage::cache& c) { return c.start(); })
      .get();

    // Supply some phony disk stats so that the cache doesn't panic and
    // think it has zero bytes of space
    cache
      .invoke_on(
        ss::shard_id{0},
        [](cloud_storage::cache& c) {
            c.notify_disk_status(
              100ULL * 1024 * 1024 * 1024,
              50ULL * 1024 * 1024 * 1024,
              storage::disk_space_alert::ok);
        })
      .get();

    pool
      .start(10, ss::sharded_parameter([this] { return get_configuration(); }))
      .get();
    api
      .start(
        std::ref(pool),
        ss::sharded_parameter([this] { return get_configuration(); }),
        ss::sharded_parameter([] { return config_file; }))
      .get();
    api.invoke_on_all([](cloud_storage::remote& api) { return api.start(); })
      .get();
}

cloud_storage_fixture::~cloud_storage_fixture() {
    if (!pool.local().shutdown_initiated()) {
        pool.local().shutdown_connections();
    }
    api.stop().get();
    pool.stop().get();
    cache.stop().get();
    tmp_directory.remove().get();
}
