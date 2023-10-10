/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/offsets_lookup_batcher.h"
#include "cluster/controller.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timed_out_error.hh>

using cluster::cloud_metadata::offsets_lookup_batcher;

namespace {
constexpr size_t batch_size = 4;
const model::topic topic{"redbean"};
ss::abort_source never_abort;

model::ntp get_ntp(int pid) {
    return model::ntp{model::kafka_namespace, topic, model::partition_id{pid}};
}

absl::btree_set<model::ntp> get_ntps(size_t num) {
    absl::btree_set<model::ntp> ret;
    for (int i = 0; i < num; i++) {
        ret.emplace(get_ntp(i));
    }
    return ret;
}
} // anonymous namespace

class offsets_lookup_batcher_fixture : public cluster_test_fixture {
public:
    offsets_lookup_batcher_fixture()
      : cluster_test_fixture() {
        model::node_id id{0};
        app = create_node_application(id);
        rp = instance(id);
        wait_for_controller_leadership(id).get();
        batcher = std::make_unique<offsets_lookup_batcher>(
          id,
          app->offsets_lookup.local(),
          app->controller->get_partition_leaders().local(),
          app->_connection_cache.local(),
          batch_size);
    }

    // Validates fetching offsets in steps by gradually increasing the number
    // of NTPs to call run_lookups() with at once.
    void check_batcher(int num_partitions, int steps) {
        batcher->clear();
        auto step_size = num_partitions / steps;
        // NOTE: knowingly doesn't always lookup up to num_partitions, if
        // step_size isn't a factor.
        for (int i = step_size; i < num_partitions; i += step_size) {
            retry_chain_node retry_node(never_abort, 30s, 1s);
            batcher->run_lookups(get_ntps(i), retry_node).get();
            BOOST_REQUIRE_EQUAL(i, batcher->offsets_by_ntp().size());
        }
    }

    application* app;
    redpanda_thread_fixture* rp;
    std::unique_ptr<offsets_lookup_batcher> batcher;
};

// Test that the lookup works when all partition leaders are local (in this
// case, single node).
FIXTURE_TEST(test_batch_list_offsets_local, offsets_lookup_batcher_fixture) {
    int num_partitions = batch_size * 5;
    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    rp->add_topic({model::kafka_namespace, topic}, num_partitions, props).get();

    for (int num_steps = 1; num_steps < 10; num_steps++) {
        check_batcher(num_partitions, num_steps);
    }
}

// Test that the lookup works when some partition leaders are remote.
FIXTURE_TEST(test_batch_list_offsets_remote, offsets_lookup_batcher_fixture) {
    auto app2 = create_node_application(model::node_id(1));
    wait_for_all_members(10s).get();

    int num_partitions = batch_size * 10;
    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    // Don't wait for topics: this app may not get all the partitions.
    rp->add_topic(
        {model::kafka_namespace, topic}, num_partitions, props, 1, false)
      .get();
    RPTEST_REQUIRE_EVENTUALLY(10s, [app2] {
        return app2->partition_manager.local().partitions().size() > 0;
    });

    for (int num_steps = 1; num_steps < 20; num_steps++) {
        check_batcher(num_partitions, num_steps);
    }
}

// Test that running lookups for missing NTPs will timeout.
FIXTURE_TEST(
  test_batch_list_offsets_missing_ntp, offsets_lookup_batcher_fixture) {
    int num_partitions = batch_size * 10;
    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    rp->add_topic({model::kafka_namespace, topic}, num_partitions, props).get();

    retry_chain_node retry_node(never_abort, 3s, 1s);
    BOOST_REQUIRE_THROW(
      batcher->run_lookups(get_ntps(num_partitions * 2), retry_node).get(),
      ss::timed_out_error);
}
