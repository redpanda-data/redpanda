/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once
#include "cloud_io/remote.h"

namespace iceberg {
class catalog;
class filesystem_catalog;
class rest_catalog;
} // namespace iceberg

namespace datalake::coordinator {

class catalog_factory {
public:
    virtual ~catalog_factory() = default;
    virtual ss::future<std::unique_ptr<iceberg::catalog>> create_catalog() = 0;
};
/**
 * Rest catalog factory, the catalog properties are set based on the
 * configuration provided.
 */
class rest_catalog_factory : public catalog_factory {
public:
    explicit rest_catalog_factory(config::configuration& config);

    ss::future<std::unique_ptr<iceberg::catalog>> create_catalog() final;

private:
    config::configuration* config_;
};
/**
 * Filesystem catalog factory, the will use provided cloud_io::remote and bucket
 */

class filesystem_catalog_factory : public catalog_factory {
public:
    filesystem_catalog_factory(
      config::configuration& config,
      cloud_io::remote& remote,
      const cloud_storage_clients::bucket_name& bucket);

    ss::future<std::unique_ptr<iceberg::catalog>> create_catalog() final;

private:
    config::configuration* config_;
    cloud_io::remote* remote_;
    cloud_storage_clients::bucket_name bucket_;
};

/**
 * Returns a catalog factory based on the configuration provided.
 */
std::unique_ptr<catalog_factory> get_catalog_factory(
  config::configuration& config,
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket);

} // namespace datalake::coordinator
