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
#include "config/configuration.h"

namespace iceberg {
class catalog;
class filesystem_catalog;
class rest_catalog;
} // namespace iceberg

namespace datalake::coordinator {
/**
 * This function creates an Iceberg catalog for the datalake_coordinator manager
 * to use. The catalog type is decided base on the configuration provided.
 * The method always accept the cloud storage primitives to be able to create a
 * filesystem catalog if required.
 */
ss::future<std::unique_ptr<iceberg::catalog>> create_catalog(
  cloud_io::remote& remote,
  const cloud_storage_clients::bucket_name& bucket_name,
  config::configuration& cluster_configuration);

class catalog_factory {
public:
    catalog_factory(
      cloud_io::remote& io,
      cloud_storage_clients::bucket_name bucket_name,
      config::configuration& cluster_configuration)
      : _io(io)
      , _bucket(std::move(bucket_name))
      , _cfg(cluster_configuration) {}

    ss::future<std::unique_ptr<iceberg::catalog>>
      make_catalog(ss::sstring) const;

private:
    cloud_io::remote& _io;
    const cloud_storage_clients::bucket_name _bucket;
    config::configuration& _cfg;
};

} // namespace datalake::coordinator
