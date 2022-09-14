/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/broker_authn_endpoint.h"
#include "config/rest_authn_endpoint.h"
#include "pandaproxy/types.h"

#include <seastar/core/sharded.hh>

#include <vector>

namespace pandaproxy {

class kafka_client_cache;

class sharded_client_cache {
public:
    ss::future<> start(
      ss::smp_service_group sg,
      YAML::Node const& cfg,
      size_t size,
      std::vector<config::broker_authn_endpoint> kafka_api,
      model::timestamp::type keep_alive = 30000);

    ss::future<> stop();

    ss::future<client_ptr>
    fetch_client(credential_t user, config::rest_authn_type authn_type);

private:
    ss::smp_submit_to_options _smp_opts;
    ss::sharded<kafka_client_cache> _cache;
};
} // namespace pandaproxy
