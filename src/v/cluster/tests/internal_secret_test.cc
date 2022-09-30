// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/feature_manager.h"
#include "cluster/internal_secret.h"
#include "cluster/internal_secret_frontend.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "model/metadata.h"
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

namespace {

// Generate successive size_t starting from init
constexpr auto iota = [](size_t init = 0) {
    return [i = init]() mutable { return i++; };
};

constexpr auto fetch_secret_from =
  [](application* app, internal_secret::key_t const& key) {
      return app->controller->get_internal_secret_frontend()
        .local()
        .fetch(key, 5s)
        .get();
  };

}; // namespace

FIXTURE_TEST(test_internal_secret_fetch, cluster_test_fixture) {
    std::vector<application*> apps{
      create_node_application(model::node_id{0}),
      create_node_application(model::node_id{1}),
      create_node_application(model::node_id{2})};

    wait_for_all_members(3s).get();

    tests::cooperative_spin_wait_with_timeout(10s, [&apps] {
        return apps.front()->controller->get_feature_table().local().is_active(
          features::feature::internal_secrets);
    }).get();

    // Create a key via each application
    std::vector<internal_secret::key_t> keys;
    keys.reserve(apps.size());
    std::generate_n(
      std::back_inserter(keys), apps.size(), [index{iota()}]() mutable {
          const auto i = index();
          return internal_secret::key_t{fmt::format("n{}", i).c_str()};
      });

    std::vector<fetch_internal_secret_reply> replies;
    replies.reserve(apps.size());

    // Fetch (create) a secret from each application
    std::generate_n(
      std::back_inserter(replies), apps.size(), [&, index{iota()}]() mutable {
          const auto i = index();
          return fetch_secret_from(apps[i], keys[i]);
      });

    // validate the secrets were created successfuly and that the keys match
    for (size_t i{0}; i < apps.size(); ++i) {
        BOOST_REQUIRE_EQUAL(replies[i].ec, errc::success);
        BOOST_REQUIRE_EQUAL(replies[i].secret.key()(), keys[i]);
        BOOST_REQUIRE_EQUAL(replies[i].secret.value()().length(), 128);
    }

    // Check secrets are all different
    {
        std::vector<internal_secret> validate_secrets;
        std::sort(validate_secrets.begin(), validate_secrets.end());
        auto it = std::unique(validate_secrets.begin(), validate_secrets.end());
        BOOST_REQUIRE(it == validate_secrets.end());
    }

    // Check all secrets retrieved from all brokers match
    for (auto const& key : keys) {
        std::optional<internal_secret::value_t> val;
        for (auto app : apps) {
            auto reply = fetch_secret_from(app, key);
            if (!val.has_value()) {
                val = reply.secret.value();
            }
            BOOST_REQUIRE_EQUAL(reply.ec, cluster::errc::success);
            BOOST_REQUIRE_EQUAL(*val, reply.secret.value());
        }
        val.reset();
    }
}

FIXTURE_TEST(test_internal_secret_disabled, cluster_test_fixture) {
    std::vector<application*> apps{
      create_node_application(model::node_id{0}),
      create_node_application(model::node_id{1}),
      create_node_application(model::node_id{2})};

    wait_for_all_members(3s).get();

    // Disable Internal Secret
    {
        auto disable_internal_secret = cluster::feature_update_action{
          .feature_name{"internal_secrets"},
          .action = cluster::feature_update_action::action_t::deactivate};
        auto res = apps.front()
                     ->controller->get_feature_manager()
                     .local()
                     .write_action(disable_internal_secret)
                     .get();
        BOOST_REQUIRE_EQUAL(res, cluster::errc::success);

        tests::cooperative_spin_wait_with_timeout(10s, [&apps] {
            return !apps.front()
                      ->controller->get_feature_table()
                      .local()
                      .is_active(features::feature::internal_secrets);
        }).get();
    }

    // Fetching a secret should fail with invalid_node_operation
    auto res = fetch_secret_from(
      apps.front(), internal_secret::key_t{"expect failure"});
    BOOST_REQUIRE_EQUAL(res.ec, errc::invalid_node_operation);
}
