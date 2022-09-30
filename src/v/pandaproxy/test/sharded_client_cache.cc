// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/sharded_client_cache.h"

#include "config/node_config.h"
#include "config/rest_authn_endpoint.h"
#include "kafka/client/configuration.h"
#include "pandaproxy/kafka_client_cache.h"
#include "pandaproxy/types.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace std::chrono_literals;

static constexpr auto clean_timer_period = 10s;

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(test_keep_alive) {
    pp::credential_t user{"red", "panda"};
    size_t cache_max_size = 5;
    std::chrono::milliseconds keep_alive{50ms};

    pp::sharded_client_cache client_cache;
    client_cache
      .start(
        ss::default_smp_service_group(),
        to_yaml(kafka::client::configuration{}, config::redact_secrets::no),
        cache_max_size,
        keep_alive)
      .get();
    auto stop_client_cache = ss::defer(
      [&client_cache]() { client_cache.stop().get(); });

    auto add_user_handle = [user](pp::kafka_client_cache& cache) {
        cache.fetch_or_insert(user, config::rest_authn_method::none);
    };

    auto get_size_handle = [](pp::kafka_client_cache& cache) {
        return cache.size();
    };

    client_cache.invoke_on_cache(user, add_user_handle).get();
    size_t s = client_cache.invoke_on_cache(user, get_size_handle).get();
    BOOST_CHECK(s == 1);
    ss::sleep(ss::lowres_clock::duration(clean_timer_period + keep_alive))
      .get();
    s = client_cache.invoke_on_cache(user, get_size_handle).get();
    BOOST_CHECK(s == 0);
}
