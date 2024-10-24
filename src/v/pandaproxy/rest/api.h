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

#include "base/seastarx.h"
#include "kafka/client/fwd.h"
#include "pandaproxy/fwd.h"
#include "pandaproxy/rest/fwd.h"

#include <seastar/core/sharded.hh>

#include <any>

namespace cluster {
class controller;
}

namespace pandaproxy::rest {

class api {
public:
    api(
      ss::smp_service_group sg,
      size_t max_memory,
      kafka::client::configuration& client_cfg,
      configuration& cfg,
      cluster::controller*) noexcept;
    ~api() noexcept;

    ss::future<> start();
    ss::future<> stop();
    ss::future<> restart();

    ss::future<> set_config(ss::sstring name, std::any val);
    ss::future<> set_client_config(ss::sstring name, std::any val);

private:
    ss::smp_service_group _sg;
    size_t _max_memory;
    kafka::client::configuration& _client_cfg;
    configuration& _cfg;
    cluster::controller* _controller;

    ss::sharded<kafka::client::client> _client;
    ss::sharded<kafka_client_cache> _client_cache;
    ss::sharded<pandaproxy::rest::proxy> _proxy;
};

} // namespace pandaproxy::rest
