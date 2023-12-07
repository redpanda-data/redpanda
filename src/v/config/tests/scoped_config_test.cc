// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "test_utils/scoped_config.h"

#include <seastar/testing/thread_test_case.hh>

// Test that when multiple test cases update a config using the
// scoped_config, it is reset in between each one.

SEASTAR_THREAD_TEST_CASE(test_scoped_config_a) {
    BOOST_REQUIRE_MESSAGE(
      !config::shard_local_cfg().cluster_id().has_value(),
      config::shard_local_cfg().cluster_id()->c_str());
    scoped_config cfg;
    cfg.get("cluster_id").set_value(std::make_optional<ss::sstring>("a"));
}

SEASTAR_THREAD_TEST_CASE(test_scoped_config_b) {
    BOOST_REQUIRE_MESSAGE(
      !config::shard_local_cfg().cluster_id().has_value(),
      config::shard_local_cfg().cluster_id()->c_str());
    scoped_config cfg;
    cfg.get("cluster_id").set_value(std::make_optional<ss::sstring>("b"));
}

SEASTAR_THREAD_TEST_CASE(test_scoped_config_c) {
    BOOST_REQUIRE_MESSAGE(
      !config::shard_local_cfg().cluster_id().has_value(),
      config::shard_local_cfg().cluster_id()->c_str());
    scoped_config cfg;
    cfg.get("cluster_id").set_value(std::make_optional<ss::sstring>("c"));
}
