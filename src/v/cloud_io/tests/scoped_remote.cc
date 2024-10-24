// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_io/tests/scoped_remote.h"

#include "cloud_io/remote.h"
#include "cloud_storage_clients/client_pool.h"

namespace cloud_io {

namespace {
constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};
} // namespace

std::unique_ptr<scoped_remote> scoped_remote::create(
  size_t pool_size, cloud_storage_clients::s3_configuration config) {
    auto ret = std::unique_ptr<scoped_remote>(new scoped_remote);
    auto sharded_config = ss::sharded_parameter([&config] { return config; });
    ret->pool.start(pool_size, config).get();
    ret->remote
      .start(std::ref(ret->pool), sharded_config, ss::sharded_parameter([] {
                 return config_file;
             }))
      .get();
    ret->remote
      .invoke_on_all(
        [](cloud_io::remote& cloud_io) { return cloud_io.start(); })
      .get();
    return ret;
}

void scoped_remote::request_stop() {
    if (pool.local_is_initialized()) {
        pool
          .invoke_on_all(
            &cloud_storage_clients::client_pool::shutdown_connections)
          .get();
    }
    if (remote.local_is_initialized()) {
        remote.invoke_on_all(&cloud_io::remote::request_stop).get();
    }
}

scoped_remote::~scoped_remote() {
    request_stop();
    remote.stop().get();
    pool.stop().get();
}

} // namespace cloud_io
