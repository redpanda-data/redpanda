// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/offset_fetch.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

FIXTURE_TEST(offset_fetch, redpanda_thread_fixture) {
    auto client = make_kafka_client().get();
    client.connect().get();

    kafka::offset_fetch_request req;
    req.data.group_id = kafka::group_id("g");

    auto resp = client.dispatch(req, kafka::api_version(2)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.data.error_code == kafka::error_code::not_coordinator);
}
