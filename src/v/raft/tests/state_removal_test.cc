// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/types.h"
#include "utils/directory_walker.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>
using namespace raft;
struct state_removal_test : public raft::raft_fixture {
    bytes kvstore_key(raft::metadata_key key, raft::group_id group) {
        iobuf buf;
        reflection::serialize(buf, key, group);
        return iobuf_to_bytes(buf);
    }

    bool is_group_state_cleared(raft::raft_node_instance& n) {
        // check all metadata in kvstore
        for (int8_t i = 0; i < static_cast<int8_t>(raft::metadata_key::last);
             ++i) {
            auto key = static_cast<raft::metadata_key>(i);
            auto buf = n.get_kvstore().get(
              storage::kvstore::key_space::consensus,
              kvstore_key(key, n.raft()->group()));

            if (buf.has_value()) {
                return false;
            }
        }

        return true;
    }

    bool snapshot_exists(raft::raft_node_instance& n) {
        bool snapshot_exists = false;
        directory_walker::walk(
          n.raft()->log()->config().work_directory(),
          [&snapshot_exists](ss::directory_entry ent) {
              if (!ent.type || *ent.type != ss::directory_entry_type::regular) {
                  return ss::now();
              }

              if (ent.name.find("snapshot") != ss::sstring::npos) {
                  snapshot_exists = true;
              }
              return ss::now();
          })
          .get();

        return snapshot_exists;
    }
    ss::future<result<replicate_result>> replicate_random_batches() {
        return retry_with_leader(
          default_timeout(), 1s, [this](raft_node_instance& leader) {
              auto batches = make_batches(10, 5, 128);
              return leader.raft()->replicate(
                std::move(batches),
                raft::replicate_options(raft::consistency_level::quorum_ack));
          });
    }
};

TEST_F(state_removal_test, remove_persistent_state_test_no_snapshot) {
    create_simple_group(1).get();

    auto res = replicate_random_batches().get();
    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(assert_logs_equal().get());
    auto& node = nodes().begin()->second;

    ASSERT_FALSE(is_group_state_cleared(*node));
    ASSERT_FALSE(snapshot_exists(*node));
    ASSERT_EQ(node->raft()->get_snapshot_size(), 0);

    // remove state
    node->raft()->remove_persistent_state().get();
    ASSERT_TRUE(is_group_state_cleared(*node));
};

TEST_F(state_removal_test, remove_persistent_state_test_with_snapshot) {
    create_simple_group(1).get();

    auto res = replicate_random_batches().get();
    ASSERT_TRUE(res.has_value());

    ASSERT_TRUE(assert_logs_equal().get());
    auto& node = nodes().begin()->second;
    node->raft()
      ->write_snapshot(
        raft::write_snapshot_cfg(node->raft()->last_visible_index(), iobuf()))
      .get();

    ASSERT_FALSE(is_group_state_cleared(*node));
    ASSERT_TRUE(snapshot_exists(*node));
    ASSERT_GT(node->raft()->get_snapshot_size(), 0);

    // remove state
    node->raft()->remove_persistent_state().get();
    ASSERT_TRUE(is_group_state_cleared(*node));
    ASSERT_FALSE(snapshot_exists(*node));
    ASSERT_EQ(node->raft()->get_snapshot_size(), 0);
};
