// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_overlay.h"
#include "cloud_topics/dl_stm/dl_stm.h"
#include "cloud_topics/dl_stm/dl_stm_api.h"
#include "cloud_topics/dl_stm/dl_stm_factory.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/test.h"

namespace ct = experimental::cloud_topics;

class dl_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;

            experimental::cloud_topics::dl_stm_factory stm_factory;
            stm_factory.create(builder, &*node->raft());

            vlog(ct::cd_log.info, "Starting node {}", id);

            co_await node->start(std::move(builder));

            stm_by_vnode[node->get_vnode()]
              = node->raft()->stm_manager()->get<ct::dl_stm>();

            api_by_vnode.emplace(
              node->get_vnode(),
              ss::make_shared<ct::dl_stm_api>(
                ct::cd_log, node->raft()->stm_manager()->get<ct::dl_stm>()));
        }
    }

    ct::dl_stm_api& api(raft::raft_node_instance& node) {
        return *api_by_vnode[node.get_vnode()];
    }

    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::dl_stm>> stm_by_vnode;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::dl_stm_api>>
      api_by_vnode;
};

TEST_F_CORO(dl_stm_fixture, test_basic) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    ASSERT_FALSE_CORO(
      api(node(*get_leader())).lower_bound(kafka::offset(0)).has_value());

    auto res = co_await retry_with_leader(
      raft::default_timeout(), [&](raft::raft_node_instance& node) {
          return api(node).push_overlay(ct::dl_overlay(
            kafka::offset(0),
            kafka::offset(1),
            model::timestamp::now(),
            model::timestamp::now(),
            {},
            ct::dl_overlay_object(
              ct::object_id(uuid_t::create()),
              ct::first_byte_offset_t(0),
              ct::byte_range_size_t(10),
              ct::dl_stm_object_ownership::exclusive)));
      });

    ASSERT_FALSE_CORO(res.has_error());

    ASSERT_TRUE_CORO(
      api(node(*get_leader())).lower_bound(kafka::offset(0)).has_value());
}
