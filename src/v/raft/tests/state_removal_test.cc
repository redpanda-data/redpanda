// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "utils/directory_walker.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

bytes kvstore_key(raft::metadata_key key, raft::group_id group) {
    iobuf buf;
    reflection::serialize(buf, key, group);
    return iobuf_to_bytes(buf);
}

bool is_group_state_cleared(raft_node& n) {
    // check all metadata in kvstore
    for (int8_t i = 0; i < static_cast<int8_t>(raft::metadata_key::last); ++i) {
        auto key = static_cast<raft::metadata_key>(i);
        auto buf = n.storage.local().kvs().get(
          storage::kvstore::key_space::consensus,
          kvstore_key(key, n.consensus->group()));

        if (buf.has_value()) {
            return false;
        }
    }

    return true;
}

bool snapshot_exists(raft_node& n) {
    bool snapshot_exists = false;
    directory_walker::walk(
      n.log->config().work_directory(),
      [&snapshot_exists](ss::directory_entry ent) {
          if (!ent.type || *ent.type != ss::directory_entry_type::regular) {
              return ss::now();
          }

          if (ent.name.find("snapshot") != ss::sstring::npos) {
              snapshot_exists = true;
          }
          return ss::now();
      })
      .get0();

    return snapshot_exists;
}

/**
 * Specialized vs. raft_node::stop_node because in the below
 * tests we explicitly stop consensus early, so need to avoid
 * double-stopping it during stop_node.
 */
void stop_node(raft_node& node) {
    node.recovery_throttle.stop().get();
    node.server.stop().get0();
    node._as.request_abort();
    if (node._nop_stm != nullptr) {
        node._nop_stm->stop().get0();
    }
    node.raft_manager.stop().get0();
    node.consensus = nullptr;
    node.hbeats->stop().get0();
    node.hbeats.reset();
    node.cache.stop().get0();
    node.log.reset();
    node.storage.stop().get0();
    node.feature_table.stop().get0();

    node.started = false;
}

FIXTURE_TEST(remove_persistent_state_test_no_snapshot, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();

    bool success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);
    auto& node = gr.get_member(model::node_id(0));
    node.consensus->stop().get0();
    auto defered = ss::defer([&node] { stop_node(node); });
    BOOST_REQUIRE_EQUAL(is_group_state_cleared(node), false);
    BOOST_REQUIRE_EQUAL(snapshot_exists(node), false);
    BOOST_REQUIRE_EQUAL(node.consensus->get_snapshot_size(), 0);

    // remove state
    node.consensus->remove_persistent_state().get0();
    BOOST_REQUIRE_EQUAL(is_group_state_cleared(node), true);
};

FIXTURE_TEST(remove_persistent_state_test_with_snapshot, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();

    bool success = replicate_random_batches(gr, 10).get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);
    auto& node = gr.get_member(model::node_id(0));
    // write snapshot
    node.consensus
      ->write_snapshot(
        raft::write_snapshot_cfg(node.consensus->last_visible_index(), iobuf()))
      .get0();
    node.consensus->stop().get0();

    auto defered = ss::defer([&node] { stop_node(node); });
    BOOST_REQUIRE_EQUAL(is_group_state_cleared(node), false);
    BOOST_REQUIRE_EQUAL(snapshot_exists(node), true);
    BOOST_REQUIRE_EQUAL(
      node.consensus->get_snapshot_size(), get_snapshot_size_from_disk(node));

    // remove state
    node.consensus->remove_persistent_state().get0();
    BOOST_REQUIRE_EQUAL(is_group_state_cleared(node), true);
    BOOST_REQUIRE_EQUAL(snapshot_exists(node), false);
    BOOST_REQUIRE_EQUAL(node.consensus->get_snapshot_size(), 0);
};
