/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/data_policy_frontend.h"
#include "cluster/errc.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "test_utils/fixture.h"
#include "v8_engine/data_policy.h"

#include <seastar/core/lowres_clock.hh>

#include <system_error>

FIXTURE_TEST(test_change_data_policy, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    wait_for_controller_leadership(model::node_id{0}).get();
    wait_for_all_members(3s).get();

    model::topic_namespace topic1(test_ns, model::topic("0"));
    model::topic_namespace topic2(test_ns, model::topic("1"));
    model::topic_namespace topic3(test_ns, model::topic("2"));

    v8_engine::data_policy dp1("1", "2");
    auto res = n1->controller->get_data_policy_frontend()
                 .local()
                 .create_data_policy(
                   topic1, dp1, 3s + model::timeout_clock::now())
                 .get();
    BOOST_REQUIRE_EQUAL(res, cluster::errc::success);

    res = n1->controller->get_data_policy_frontend()
            .local()
            .create_data_policy(topic2, dp1, 3s + model::timeout_clock::now())
            .get();
    BOOST_REQUIRE_EQUAL(res, cluster::errc::success);

    v8_engine::data_policy dp2("2", "2");
    res = n1->controller->get_data_policy_frontend()
            .local()
            .create_data_policy(topic1, dp2, 3s + model::timeout_clock::now())
            .get();
    BOOST_REQUIRE_EQUAL(res, cluster::errc::data_policy_already_exists);

    res = n1->controller->get_data_policy_frontend()
            .local()
            .clear_data_policy(topic2, 3s + model::timeout_clock::now())
            .get();
    BOOST_REQUIRE_EQUAL(res, cluster::errc::success);

    res = n1->controller->get_data_policy_frontend()
            .local()
            .clear_data_policy(topic3, 3s + model::timeout_clock::now())
            .get();
    BOOST_REQUIRE_EQUAL(res, cluster::errc::data_policy_not_exists);

    auto dp_table = n1->data_policies.local();

    BOOST_REQUIRE_EQUAL(dp_table.size(), 1);
    auto dp = dp_table.get_data_policy(topic1);
    BOOST_ASSERT(dp.has_value());
    BOOST_REQUIRE_EQUAL(dp.value().function_name(), dp1.function_name());
    BOOST_REQUIRE_EQUAL(dp.value().script_name(), dp1.script_name());
}
