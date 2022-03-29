// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/offset_commit.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

FIXTURE_TEST(
  offset_commit_static_membership_not_supported, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::offset_commit_request req;
    req.data.group_instance_id = kafka::group_instance_id("g");
    req.data.topics = {{
      .name = model::topic("t"),
      .partitions = {{}},
    }};

    auto resp = client.dispatch(req, kafka::api_version(7)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(
      resp.data.topics[0].partitions[0].error_code
      == kafka::error_code::unsupported_version);
}
