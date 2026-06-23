/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/describe_redpanda_roles.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/types.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/boost_fixture.h"

// Proves a reserved-range (key 15000) request dispatches end-to-end: it
// survives the pre-dispatch throttle check, routes through the dispatch LUT to
// the DescribeRedpandaRoles stub handler, and returns a well-formed response.
FIXTURE_TEST(describe_redpanda_roles_dispatches, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::describe_redpanda_roles_request req;
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();

    BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);
}
