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
#include "cloud_storage_clients/configuration.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

#include <cstddef>
#include <functional>
#include <memory>

namespace cloud_io {

std::unique_ptr<scoped_remote> scoped_remote::create(
  size_t pool_size, cloud_storage_clients::s3_configuration config) {
    auto ret = std::unique_ptr<scoped_remote>(new scoped_remote);
    ret->upstreams.start(config).get();
    ret->pool
      .start(
        ss::sharded_parameter([&] { return std::ref(ret->upstreams.local()); }),
        pool_size,
        config)
      .get();
    ret->pool
      .invoke_on_all(&cloud_storage_clients::client_pool::start, std::nullopt)
      .get();
    ret->remote
      .start(
        std::ref(ret->pool),
        config,
        model::cloud_credentials_source::config_file,
        ss::sharded_parameter([] { return ss::default_scheduling_group(); }))
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
    upstreams.stop().get();
}

} // namespace cloud_io
