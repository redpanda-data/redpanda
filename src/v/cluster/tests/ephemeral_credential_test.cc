// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/feature_manager.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/scram_authenticator.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <iterator>
#include <system_error>
#include <vector>

using namespace std::chrono_literals;
using namespace cluster;

FIXTURE_TEST(test_ephemeral_credential_frontend, cluster_test_fixture) {
    std::vector<application*> apps{
      create_node_application(model::node_id{0}),
      create_node_application(model::node_id{1}),
      create_node_application(model::node_id{2})};

    wait_for_all_members(3s).get();

    tests::cooperative_spin_wait_with_timeout(10s, [&apps] {
        return apps.front()->controller->get_feature_table().local().is_active(
          features::feature::ephemeral_secrets);
    }).get();

    auto& fe_0
      = apps[0]->controller->get_ephemeral_credential_frontend().local();
    auto& fe_1
      = apps[1]->controller->get_ephemeral_credential_frontend().local();
    auto& cs_1 = apps[1]->controller->get_credential_store().local();

    security::acl_principal principal{
      security::principal_type::ephemeral_user, "principal"};

    // Get credentials on broker 0
    auto cred_0 = fe_0.get(principal).get();
    BOOST_REQUIRE_EQUAL(cred_0.err, cluster::errc::success);
    BOOST_REQUIRE_EQUAL(cred_0.credential.principal(), principal);
    auto user_0 = cred_0.credential.user();

    // Get credentials again on broker 0 should return same
    {
        auto get_res_0 = fe_0.get(principal).get();
        BOOST_REQUIRE_EQUAL(errc::success, get_res_0.err);
        BOOST_REQUIRE_EQUAL(cred_0.credential, get_res_0.credential);
    }

    // Get credentials from broker 1, expect different to broker 0
    {
        auto get_res_1 = fe_1.get(principal).get();
        BOOST_REQUIRE_EQUAL(errc::success, get_res_1.err);
        BOOST_REQUIRE_NE(cred_0.credential, get_res_1.credential);
    }

    // Check inform works
    {
        auto res = fe_0.inform(apps[1]->controller->self(), principal).get();
        BOOST_REQUIRE_EQUAL(res, errc::success);
    }

    // Check fe_1 has the credential fe_0 sent it
    {
        auto cred_1 = cs_1.get<security::scram_credential>(user_0);
        BOOST_REQUIRE(cred_1.has_value());
        BOOST_REQUIRE_EQUAL(cred_1->principal(), principal);

        bool check_password = security::scram_sha512::validate_password(
          cred_0.credential.password(),
          cred_1->stored_key(),
          cred_1->salt(),
          cred_1->iterations());
        BOOST_REQUIRE(check_password);
    }
}
