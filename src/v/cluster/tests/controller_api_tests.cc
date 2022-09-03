// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "test_utils/async.h"

#include <seastar/core/loop.hh>

using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(test_querying_ntp_status, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    auto n2 = create_node_application(model::node_id{1});
    model::ntp test_ntp(test_ns, model::topic("tp"), model::partition_id(0));
    wait_for_all_members(3s).get();

    auto state = n1->controller->get_api()
                   .local()
                   .get_reconciliation_state(test_ntp)
                   .get();
    /**
     * No topic yet, error expected
     */
    BOOST_REQUIRE_EQUAL(
      state.error(), make_error_code(cluster::errc::partition_not_exists));

    std::optional<model::node_id> leader_id;

    while (!leader_id) {
        leader_id = n1->metadata_cache.local().get_controller_leader_id();
    }
    auto leader = get_node_application(*leader_id);

    // create topic
    std::vector<cluster::topic_configuration> topics;
    topics.emplace_back(test_ntp.ns, test_ntp.tp.topic, 3, 1);

    leader->controller->get_topics_frontend()
      .local()
      .create_topics(
        cluster::without_custom_assignments(topics),
        1s + model::timeout_clock::now())
      .get();

    // wait for reconciliation to be finished
    tests::cooperative_spin_wait_with_timeout(2s, [&n2, &test_ntp] {
        return n2->controller->get_api()
          .local()
          .get_reconciliation_state(test_ntp)
          .then([](result<cluster::ntp_reconciliation_state> r) {
              if (!r) {
                  return false;
              }
              return r.value().status() == cluster::reconciliation_status::done;
          });
    }).get();
}
